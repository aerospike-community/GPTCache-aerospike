from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import uuid

import numpy as np
import pytest


@pytest.fixture
def sc_storage(request):
    path = os.environ.get("GPTCACHE_AEROSPIKE_SC_TEST_CONFIG")
    if not path:
        if request.config.getoption("--aerospike-mandatory-live"):
            pytest.fail("Supply GPTCACHE_AEROSPIKE_SC_TEST_CONFIG for an owned SC test server")
        pytest.skip("No explicitly configured SC test server")
    if importlib.util.find_spec("aerospike_sdk") is None:
        if request.config.getoption("--aerospike-mandatory-live"):
            pytest.fail("Mandatory SC tests require the optional aerospike-sdk")
        pytest.skip("Optional aerospike-sdk is not installed")
    from aerospike_sdk.sync import ClusterDefinition
    from gptcache.manager import CacheBase

    config = json.loads(Path(path).read_text())
    storage = CacheBase(
        "aerospike", cluster_definition=ClusterDefinition(config["host"], config["port"]),
        namespace=config["namespace"], cache_scope="sc_test_" + uuid.uuid4().hex,
    )
    try:
        storage.create()
        assert storage._sc, "This suite requires an SC namespace with MRT"
        yield storage, config, storage._caller
    finally:
        storage.close()


def test_sc_self_initialization_reopen_and_atomic_initial_sessions(sc_storage):
    from aerospike_sdk.sync import ClusterDefinition
    from gptcache.manager import CacheBase
    from gptcache.manager.scalar_data.base import CacheData

    storage, config, behavior = sc_storage
    ids = storage.batch_insert([
        CacheData("first", "a", session_id="chat"),
        CacheData("second", "b", session_id="chat"),
    ])
    assert ids == [1, 2]
    other = CacheBase(
        "aerospike", cluster_definition=ClusterDefinition(config["host"], config["port"]),
        namespace=config["namespace"], cache_scope=storage.cache_scope, behavior=behavior)
    try:
        assert other.count() == 2
        assert other._boot == storage._boot
        assert sorted(row.question_id for row in other.list_sessions("chat")) == ids
        assert other.batch_insert([CacheData("third", "c")]) == [3]
    finally:
        other.close()


@pytest.mark.parametrize("failure", ["payload", "initial_session"])
def test_sc_batch_failure_rolls_back_questions_sessions_and_allocators(sc_storage, failure):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    oversized = "x" * (9 * 1024 * 1024)
    data = (CacheData(oversized, "a") if failure == "payload" else
            CacheData("second", "a", session_id=oversized))
    with pytest.raises(AerospikeStorageError):
        storage.batch_insert([CacheData("first", "a", session_id="chat"), data])
    assert storage.count(is_all=True) == 0
    assert storage.list_sessions() == []
    assert storage._read("qid")[1]["hi"] == 0
    assert storage._read("sid")[1]["hi"] == 0
    assert storage.batch_insert([CacheData("after abort", "a", session_id="chat")]) == [1]


def test_sc_touch_rolls_back_if_session_enumeration_fails(sc_storage, monkeypatch):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    qid = storage.batch_insert([CacheData("q", "a")])[0]
    accessed = storage._read("q", qid)[1]["last_access"]
    original = storage._scan

    def fail(kind, *predicates):
        if kind == "s":
            raise AerospikeStorageError("injected session query failure")
        return original(kind, *predicates)

    monkeypatch.setattr(storage, "_scan", fail)
    with pytest.raises(AerospikeStorageError):
        storage.get_data_by_id(qid)
    assert storage._read("q", qid)[1]["last_access"] == accessed


def test_sc_report_failure_rolls_back_report_allocator(sc_storage):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError

    storage, _, _ = sc_storage
    with pytest.raises(AerospikeStorageError):
        storage.report_cache("q", "cached", 1, "x" * (9 * 1024 * 1024), 0.9, 0.1)
    assert storage._read("rid")[1]["hi"] == 0
    assert storage._scan("r") == []
    storage.report_cache("q", "cached", 1, "answer", 0.9, 0.1)
    assert storage._read("rid")[1]["hi"] == 1
    assert len(storage._scan("r")) == 1


def test_sc_association_delete_failure_rolls_back_earlier_deletion(sc_storage, monkeypatch):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    storage.batch_insert([CacheData("first", "a", session_id="chat"),
                          CacheData("second", "b", session_id="chat")])
    ids = sorted(row.id for row in storage.list_sessions("chat"))
    original = storage._delete_child

    def fail(sid):
        if sid == ids[1]:
            raise AerospikeStorageError("injected second association deletion failure")
        return original(sid)

    monkeypatch.setattr(storage, "_delete_child", fail)
    with pytest.raises(AerospikeStorageError):
        storage.delete_session(ids)
    assert sorted(row.id for row in storage.list_sessions("chat")) == ids


def test_sc_purge_rolls_back_the_whole_call(sc_storage, monkeypatch):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    ids = storage.batch_insert([
        CacheData("first", "a", session_id="chat"),
        CacheData("second", "b", session_id="chat")])
    storage.mark_deleted(ids)
    original = storage._purge_parent
    calls = []

    def fail_second(qid):
        calls.append(qid)
        if len(calls) == 2:
            raise AerospikeStorageError("injected second-parent failure")
        original(qid)

    monkeypatch.setattr(storage, "_purge_parent", fail_second)
    with pytest.raises(AerospikeStorageError):
        storage.clear_deleted_data()
    assert len(calls) == 2
    assert set(storage.get_ids(deleted=True)) == set(ids)
    assert len(storage.list_sessions("chat")) == 2
    monkeypatch.setattr(storage, "_purge_parent", original)
    storage.clear_deleted_data()
    assert storage.count(is_all=True) == 0
    assert storage.list_sessions() == []


def test_sc_concurrent_append_and_purge_never_leave_orphaned_sessions(sc_storage):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    qid = storage.batch_insert([CacheData("q", "a")])[0]

    def append():
        try:
            storage.add_session(qid, "chat", "q")
        except AerospikeStorageError:
            pass

    def purge():
        try:
            storage.mark_deleted([qid])
            storage.clear_deleted_data()
        except AerospikeStorageError:
            pass

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda fn: fn(), [append, append, purge, append]))
    storage.mark_deleted([qid])
    storage.clear_deleted_data()
    assert storage._read("q", qid) is None
    assert storage.list_sessions(key=qid) == []
    assert storage._transaction_session.get() is None


def test_sc_multiple_associations_use_transactional_batch_reads_and_purge(sc_storage):
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    qid = storage.batch_insert([CacheData("q", "a", session_id="initial")])[0]
    storage.add_session(qid, "second", "q")
    storage.add_session(qid, "third", "q")
    assert {row.session_id for row in storage.get_data_by_id(qid).session_id} == {
        "initial", "second", "third"}
    storage.mark_deleted([qid])
    storage.clear_deleted_data()
    assert storage.list_sessions(key=qid) == []
    assert storage.get_data_by_id(qid) is None


@pytest.mark.parametrize("metric", ["L2", "cosine", "inner_product"])
def test_native_vector_top_k_matches_numpy_and_supports_lifecycle(sc_storage, metric):
    from gptcache.manager import VectorBase
    from gptcache.manager.vector_data.base import VectorData

    storage, _, _ = sc_storage
    vector = VectorBase("aerospike", scalar_storage=storage, dimension=3, top_k=3, metric=metric)
    embeddings = np.array([[1, 2, 3], [2, 1, 0], [-1, 1, 2], [4, 2, 1]], dtype=np.float32)
    query = np.array([1, 2, 3], dtype=np.float32)
    try:
        vector.mul_add([VectorData(i + 1, row) for i, row in enumerate(embeddings)])
        if metric == "L2":
            expected = ((embeddings - query) ** 2).sum(axis=1)
        elif metric == "cosine":
            expected = 1 - embeddings @ query / (np.linalg.norm(embeddings, axis=1) * np.linalg.norm(query))
        else:
            expected = -(embeddings @ query)
        actual = vector.search(query)
        ordered = sorted((float(score), i + 1) for i, score in enumerate(expected))[:3]
        assert [identity for _, identity in actual] == [identity for _, identity in ordered]
        np.testing.assert_allclose([score for score, _ in actual], [score for score, _ in ordered], atol=1e-6)
        np.testing.assert_array_equal(vector.get_embeddings(2), embeddings[1])
        assert vector.get_embeddings(999) is None
        assert vector.delete([2, 999]) is True
        assert vector.delete([2]) is True
        assert vector.get_embeddings(2) is None
        vector.update_embeddings(1, np.array([3, 2, 1], dtype=np.float32))
        np.testing.assert_array_equal(vector.get_embeddings(1), [3, 2, 1])
        assert vector.rebuild() is True
        vector.flush()
    finally:
        vector.close()
    assert not storage._closed


def test_vector_scope_isolation_dimension_validation_and_standalone_reopen(sc_storage):
    from aerospike_sdk.sync import ClusterDefinition
    from gptcache.manager import VectorBase
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.vector_data.base import VectorData

    storage, config, behavior = sc_storage
    params = dict(cluster_definition=ClusterDefinition(config["host"], config["port"]),
                  namespace=config["namespace"], behavior=behavior, dimension=3)
    vector = VectorBase("aerospike", cache_scope=storage.cache_scope, **params)
    vector.mul_add([VectorData(1, np.array([1, 2, 3]))])
    vector.close()
    with pytest.raises(RuntimeError):
        vector.search(np.ones(3))
    reopened = VectorBase("aerospike", scalar_storage=storage, dimension=3)
    isolated = VectorBase("aerospike", cache_scope=storage.cache_scope + "_other", **params)
    mismatch = VectorBase("aerospike", scalar_storage=storage, dimension=2)
    try:
        assert reopened.search(np.array([1, 2, 3])) == [(0.0, 1)]
        assert isolated.search(np.array([1, 2, 3])) == []
        with pytest.raises(AerospikeStorageError):
            mismatch.mul_add([VectorData(1, np.ones(2))])
        np.testing.assert_array_equal(reopened.get_embeddings(1), [1, 2, 3])
    finally:
        reopened.close()
        isolated.close()
        mismatch.close()


def test_sc_vector_batch_aborts_even_after_write_acknowledgments(sc_storage, monkeypatch):
    from gptcache.manager import VectorBase
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.vector_data.base import VectorData

    storage, _, _ = sc_storage
    vector = VectorBase("aerospike", scalar_storage=storage, dimension=3)
    original = storage._batch

    def fail_after_ack(builder, keys):
        result = original(builder, keys)
        if keys[0].set_name == "gptc_v":
            raise AerospikeStorageError("injected failure after vector write acknowledgment")
        return result

    try:
        monkeypatch.setattr(storage, "_batch", fail_after_ack)
        with pytest.raises(AerospikeStorageError) as error:
            vector.mul_add([VectorData(1, np.ones(3)), VectorData(2, np.zeros(3))])
        assert error.value.phase == "ABORTED_TRANSACTION"
        monkeypatch.setattr(storage, "_batch", original)
        assert vector.search(np.ones(3)) == []
        assert vector.get_embeddings(1) is None
        assert vector.get_embeddings(2) is None
    finally:
        vector.close()


def test_data_manager_roundtrip_uses_both_aerospike_backends(sc_storage):
    from gptcache.manager import VectorBase, get_data_manager

    storage, _, _ = sc_storage
    vector = VectorBase("aerospike", scalar_storage=storage, dimension=3)
    try:
        manager = get_data_manager(storage, vector)
        embedding = np.array([1, 2, 3], dtype=np.float32)
        manager.save("question", "answer", embedding)
        result = manager.search(embedding)[0]
        assert result[0] == pytest.approx(0, abs=1e-6)
        assert manager.get_scalar_data(result).answers[0].answer == "answer"
        manager.flush()
    finally:
        vector.close()


def test_nonpositive_neighbors_are_scalar_misses_through_the_manager(sc_storage):
    from gptcache.manager import VectorBase, get_data_manager
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    vector = VectorBase("aerospike", scalar_storage=storage, dimension=3)
    try:
        manager = get_data_manager(storage, vector)
        qid = storage.batch_insert([CacheData("q", "a")])[0]
        assert manager.get_scalar_data((0.0, qid)).answers[0].answer == "a"
        for identity in (-1, 0, np.int64(-1)):
            assert manager.get_scalar_data((float("inf"), identity)) is None
        assert storage.count() == 1
    finally:
        vector.close()


@pytest.mark.parametrize("operation", ["insert", "session", "report"])
def test_sc_concurrent_writes_retry_confirmed_conflicts(sc_storage, operation):
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    qid = storage.batch_insert([CacheData("parent", "a")])[0]

    def write(index):
        if operation == "insert":
            return storage.batch_insert([
                CacheData("q" + str(index), "a", session_id="chat")])[0]
        if operation == "session":
            return storage.add_session(qid, "chat", "q" + str(index))
        return storage.report_cache("q" + str(index), "parent", qid, "a", 0.9, 0.1)

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(write, range(64)))
    if operation == "insert":
        assert len(set(results)) == 64
        assert storage.count() == 65
        assert len(storage.list_sessions("chat")) == 64
    elif operation == "session":
        assert len(storage.list_sessions(key=qid)) == 64
        assert storage._read("sid")[1]["hi"] == 64
    else:
        assert len(storage._scan("r")) == 64
        assert storage._read("rid")[1]["hi"] == 64
    assert storage._transaction_session.get() is None


@pytest.mark.parametrize("metric", ["L2", "cosine", "inner_product"])
def test_large_top_k_fallback_matches_numpy_without_truncation(sc_storage, metric):
    from gptcache.manager import VectorBase
    from gptcache.manager.vector_data.base import VectorData

    storage, _, _ = sc_storage
    vector = VectorBase(
        "aerospike", scalar_storage=storage, dimension=3, top_k=1001, metric=metric)
    embeddings = np.random.default_rng(42).normal(size=(1007, 3)).astype(np.float32)
    query = np.array([1, 2, 3], dtype=np.float32)
    try:
        vector.mul_add([VectorData(i + 1, row) for i, row in enumerate(embeddings)])
        if metric == "L2":
            scores = ((embeddings - query) ** 2).sum(axis=1)
        elif metric == "cosine":
            scores = 1 - embeddings @ query / (
                np.linalg.norm(embeddings, axis=1) * np.linalg.norm(query))
        else:
            scores = -(embeddings @ query)
        expected = sorted((float(score), i + 1) for i, score in enumerate(scores))
        for limit in (-1, 2000):
            actual = vector.search(query, top_k=limit)
            chosen = expected[:1001] if limit == -1 else expected
            assert [identity for _, identity in actual] == [identity for _, identity in chosen]
            np.testing.assert_allclose([score for score, _ in actual],
                                       [score for score, _ in chosen], atol=1e-5)
    finally:
        vector.close()


def test_sc_capacity_rejection_aborts_without_partial_publication(sc_storage):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    storage, _, _ = sc_storage
    with pytest.raises(AerospikeStorageError) as error:
        storage.batch_insert([CacheData("q", "a") for _ in range(4096)])
    assert error.value.phase == "ABORTED_TRANSACTION"
    assert not error.value.retryable
    assert storage.count(is_all=True) == 0
    assert storage._read("qid")[1]["hi"] == 0
