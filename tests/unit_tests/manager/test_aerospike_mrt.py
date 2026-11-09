from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from test_aerospike_storage import (
    Row, model, no_network_or_installs, queue, record_row, sdk, slots_row,
)


@pytest.fixture
def transactional(model):
    storage, session, module = model
    storage._sc = True
    events = []
    session.abort = Mock(return_value=storage._transport.AbortStatus.OK)
    session.commit = Mock(return_value=storage._transport.CommitStatus.OK)

    @contextmanager
    def transaction():
        events.append("begin")
        try:
            yield session
        except BaseException:
            events.append("abort")
            raise
        else:
            events.append("commit")

    session.transaction = Mock(side_effect=transaction)
    return storage, session, module, events


def test_sc_batch_is_one_transaction_including_initial_session(transactional):
    storage, session, module, events = transactional
    queue(session, slots_row(storage, "qid", None, None, 1),
          Row(storage._key("q", 1)),
          slots_row(storage, "sid", None, None, 1),
          Row(storage._key("q", 1)), Row(storage._key("s", 1)))
    assert storage.batch_insert([module.CacheData("q", "a", session_id="chat")]) == [1]
    assert events == ["begin", "commit"]
    assert all(name != "with_txn" for b in session.executed for name, _ in b.calls)
    assert not any(name == "add" and args[0] == "adm"
                   for b in session.executed for name, args in b.calls)


def test_sc_batch_aborts_on_later_record_failure(transactional):
    storage, session, module, events = transactional
    queue(session, slots_row(storage, "qid", None, None, 2),
          [Row(storage._key("q", 1), index=0),
           Row(storage._key("q", 2), index=1, result_code=module_code("RECORD_TOO_BIG"))])
    with pytest.raises(module.AerospikeStorageError):
        storage.batch_insert([module.CacheData("q", "a"), module.CacheData("q2", "a2")])
    assert events == ["begin", "abort"]


@pytest.mark.parametrize("code", ["MRT_BLOCKED", "MRT_VERSION_MISMATCH"])
def test_sc_allocator_conflict_retries_a_fresh_transaction(transactional, monkeypatch, code):
    storage, session, module, events = transactional
    monkeypatch.setattr(module.time, "sleep", Mock())
    queue(session, Row(storage._key("qid"), result_code=module_code(code)),
          slots_row(storage, "qid", None, None, 1), Row(storage._key("q", 1)))
    assert storage.batch_insert(iter([module.CacheData("q", "a")])) == [1]
    assert events == ["begin", "abort", "begin", "commit"]
    assert storage._transaction_session.get() is None


def test_sc_direct_sdk_conflict_is_retryable_only_after_confirmed_abort(transactional, monkeypatch):
    from test_aerospike_storage import SDKError

    storage, session, module, events = transactional
    monkeypatch.setattr(module.time, "sleep", Mock())
    session.results.append(SDKError(module_code("MRT_BLOCKED")))
    queue(session, slots_row(storage, "qid", None, None, 1), Row(storage._key("q", 1)))
    assert storage.batch_insert([module.CacheData("q", "a")]) == [1]
    assert events == ["begin", "abort", "begin", "commit"]


def test_sc_batch_conflict_after_acknowledgment_retries_all_inputs(transactional, monkeypatch):
    storage, session, module, events = transactional
    monkeypatch.setattr(module.time, "sleep", Mock())
    queue(session, slots_row(storage, "qid", None, None, 2),
          [Row(storage._key("q", 1), index=0),
           Row(storage._key("q", 2), index=1, result_code=module_code("MRT_BLOCKED"))],
          slots_row(storage, "qid", None, None, 2),
          [Row(storage._key("q", 1), index=0), Row(storage._key("q", 2), index=1)])
    assert storage.batch_insert(all_data=iter([
        module.CacheData("q", "a"), module.CacheData("q2", "a2")])) == [1, 2]
    assert events == ["begin", "abort", "begin", "commit"]


def test_sc_conflict_retry_is_bounded(transactional, monkeypatch):
    storage, session, module, events = transactional
    monkeypatch.setattr(module.time, "sleep", Mock())
    for _ in range(32):
        queue(session, Row(storage._key("qid"), result_code=module_code("MRT_BLOCKED")))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.batch_insert([module.CacheData("q", "a")])
    assert error.value.phase == "ABORTED_TRANSACTION"
    assert error.value.retryable
    assert events == ["begin", "abort"] * 32


def test_sc_conflict_retry_respects_time_budget(transactional, monkeypatch):
    storage, session, module, events = transactional
    monkeypatch.setattr(module.time, "monotonic", Mock(side_effect=[0, 100]))
    monkeypatch.setattr(module.time, "sleep", Mock())
    queue(session, Row(storage._key("qid"), result_code=module_code("MRT_BLOCKED")))
    with pytest.raises(module.AerospikeStorageError):
        storage.batch_insert([module.CacheData("q", "a")])
    assert events == ["begin", "abort"]
    module.time.sleep.assert_not_called()


def test_sc_unconfirmed_abort_is_never_retried(transactional):
    storage, session, module, events = transactional
    session.abort.return_value = storage._transport.AbortStatus.ROLL_BACK_ABANDONED
    queue(session, Row(storage._key("qid"), result_code=module_code("MRT_BLOCKED")))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.batch_insert([module.CacheData("q", "a")])
    assert error.value.phase == "UNKNOWN_TRANSACTION"
    assert session.transaction.call_count == 1
    assert storage._transaction_session.get() is None


def test_sc_unconfirmed_commit_is_never_retried(transactional):
    storage, session, module, events = transactional
    session.commit.return_value = storage._transport.CommitStatus.ROLL_FORWARD_ABANDONED
    queue(session, slots_row(storage, "qid", None, None, 1), Row(storage._key("q", 1)))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.batch_insert([module.CacheData("q", "a")])
    assert error.value.phase == "UNKNOWN_TRANSACTION"
    assert session.transaction.call_count == 1
    assert storage._transaction_session.get() is None


@pytest.mark.parametrize("failure", ["uncertain", "mismatched", "terminal"])
def test_sc_conflict_does_not_hide_other_batch_failures(transactional, failure):
    from test_aerospike_storage import SDKError

    storage, session, module, events = transactional
    code = module_code("MRT_BLOCKED")
    other = (Row(storage._key("q", 2), index=1, result_code=code, in_doubt=True)
             if failure == "uncertain" else
             Row(storage._key("q", 2), index=1, result_code=code,
                 exception=SDKError(module_code("PARAMETER_ERROR")))
             if failure == "mismatched" else
             Row(storage._key("q", 2), index=1, result_code=module_code("RECORD_TOO_BIG")))
    queue(session, slots_row(storage, "qid", None, None, 2),
          [Row(storage._key("q", 1), index=0, result_code=code), other])
    with pytest.raises(module.AerospikeStorageError):
        storage.batch_insert([module.CacheData("q", "a"), module.CacheData("q2", "a2")])
    assert events == ["begin", "abort"]


def module_code(name):
    from test_aerospike_storage import Code
    return getattr(Code, name)


def test_sc_session_failure_aborts_without_leaking_admission(transactional):
    storage, session, module, events = transactional
    queue(session, slots_row(storage, "sid", None, None, 1),
          Row(storage._key("q", 1)), RuntimeError("child failed"))
    with pytest.raises(module.AerospikeStorageError):
        storage.add_session(1, "chat", "q")
    assert events == ["begin", "abort"]
    assert len(session.executed) == 3


def test_sc_query_in_transaction_rereads_enumerated_keys(transactional):
    storage, session, module, events = transactional
    queue(session, [record_row(storage, "s", 1)],
          Row(storage._key("s", 1), result_code=module_code("KEY_NOT_FOUND_ERROR")))
    with storage._transaction():
        assert storage.list_sessions(key=1) == []
    assert events == ["begin", "commit"]
    assert len(session.executed) == 2


def test_sc_commit_failure_is_not_retried_or_reported_as_success(transactional):
    storage, session, module, events = transactional

    @contextmanager
    def failed_commit():
        yield session
        raise RuntimeError("commit outcome unknown")

    session.transaction.side_effect = failed_commit
    queue(session, slots_row(storage, "qid", None, None, 1),
          Row(storage._key("q", 1)))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.batch_insert([module.CacheData("q", "a")])
    assert error.value.phase == "UNKNOWN_TRANSACTION"
    assert session.transaction.call_count == 1
    assert storage._transaction_session.get() is None


def test_sc_validation_errors_remain_value_errors_and_do_not_write(transactional):
    storage, session, module, events = transactional
    with pytest.raises(ValueError):
        storage.batch_insert([module.CacheData("q", module.Answer(object()))])
    assert not session.executed
    assert events == ["begin", "abort"]


def test_sc_successful_abort_is_distinguished_from_uncertain_abort(transactional):
    storage, session, module, events = transactional
    queue(session, slots_row(storage, "sid", None, None, 1),
          Row(storage._key("q", 1)), RuntimeError("child failed"))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.add_session(1, "chat", "q")
    assert error.value.phase == "ABORTED_TRANSACTION"
    assert error.value.identity == (1, 1)
    assert events == ["begin", "abort"]


def test_sc_abort_failure_is_not_claimed_to_be_rolled_back(transactional):
    storage, session, module, events = transactional

    @contextmanager
    def failed_abort():
        try:
            yield session
        except BaseException:
            raise RuntimeError("abort outcome unknown")

    session.transaction.side_effect = failed_abort
    queue(session, slots_row(storage, "sid", None, None, 1),
          Row(storage._key("q", 1)), RuntimeError("child failed"))
    with pytest.raises(module.AerospikeStorageError) as error:
        storage.add_session(1, "chat", "q")
    assert error.value.phase == "UNKNOWN_TRANSACTION"
    assert session.transaction.call_count == 1
    assert storage._transaction_session.get() is None


@pytest.mark.parametrize("mode", [None, "unknown"])
def test_unverified_consistency_mode_never_silently_disables_mrt(model, mode):
    storage, session, module = model
    command = "get-config:context=namespace;id=explicit-ns"
    body = "default-read-touch-ttl-pct=0"
    if mode is not None:
        body += ";strong-consistency=" + mode
    session.configuration.values[command]["n"][command] = body
    with pytest.raises(module.AerospikeStorageError, match="consistency mode"):
        storage._configuration()
    assert not session.executed


def test_sc_lightweight_enumeration_projects_no_payload_and_closes_stream(transactional):
    from test_aerospike_storage import Record

    storage, session, module, events = transactional
    queue(session, Row(storage._key("q", 1), Record(
        dict(fmt=1, scope=storage.cache_scope, qid=1, state=0))))
    assert storage.get_ids(deleted=False) == [1]
    assert ("bins", (["fmt", "scope", "qid", "state"],)) in session.executed[0].calls
    assert not events


@pytest.mark.parametrize("missing", ["root", "qid", "sid", "rid"])
def test_sc_existing_history_is_never_backfilled(transactional, missing):
    storage, session, module, events = transactional
    command = "get-config:context=namespace;id=explicit-ns"
    session.configuration.values[command]["n"][command] = (
        "default-read-touch-ttl-pct=0;strong-consistency=true")
    if missing == "root":
        queue(session, Row(storage._key("root"), result_code=module_code("KEY_NOT_FOUND_ERROR")))
    else:
        queue(session, record_row(storage, "root"))
        for kind in ("qid", "sid", "rid"):
            if kind == missing:
                queue(session, Row(storage._key(kind), result_code=module_code("KEY_NOT_FOUND_ERROR")))
                break
            queue(session, record_row(storage, kind))
    with pytest.raises(module.AerospikeStorageError):
        storage.create()
    assert events == ["begin", "abort"]
    assert not storage._active
    assert all(name not in ("put", "add", "delete_record")
               for builder in session.executed for name, _ in builder.calls)


def test_sc_absent_root_with_orphan_allocator_refuses_initialization(transactional):
    storage, session, module, events = transactional
    storage._boot = None
    command = "get-config:context=namespace;id=explicit-ns"
    session.configuration.values[command]["n"][command] = (
        "default-read-touch-ttl-pct=0;strong-consistency=true")
    queue(session, Row(storage._key("root"), result_code=module_code("KEY_NOT_FOUND_ERROR")),
          [], [], [], [], Row(storage._key("qid")))
    with pytest.raises(module.AerospikeStorageError):
        storage.create()
    assert storage._boot is None
    assert events == ["begin", "abort"]
    assert all(name not in ("put", "add", "delete_record")
               for builder in session.executed for name, _ in builder.calls)
