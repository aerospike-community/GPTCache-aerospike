"""Spawn-only, registered application writer for the real Aerospike tests.

No Docker provisioning, localhost adoption or simulated SDK responses live here.
The parent must register this exact argv before sending the request on stdin.
"""

import fcntl
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
from unittest.mock import patch
import uuid

import numpy as np


def inspect_point(provider, kind, identity=None, controls=False):
    """Ordinary public SDK read, not adapter validation or computed void-time."""
    from aerospike_sdk import ErrorStrategy, RecordNotFoundError, ResultCode

    rows = list(provider._session.query(provider._key(kind, identity)).with_txn(None).include_missing_keys()
                .execute(on_error=ErrorStrategy.IN_STREAM))
    assert len(rows) == 1
    row = rows[0]
    if row.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
        assert not row.in_doubt and row.record is None
        assert row.exception is None or (
            isinstance(row.exception, RecordNotFoundError) and
            row.exception.result_code == ResultCode.KEY_NOT_FOUND_ERROR and
            not row.exception.in_doubt)
        return None
    assert row.is_ok and row.exception is None and not row.in_doubt
    assert row.record is not None and row.record.ttl is None
    if controls:
        return dict(generation=row.record.generation, **{
            name: row.record.bins[name] for name in ("qid", "sid", "rid", "state", "adm", "hi", "phase")
            if name in row.record.bins})
    return dict(row.record.bins)


def configured_node(manifest):
    config = manifest["configs"]["public"]
    raw = Path(config["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == config["sha256"]
    nodes = [line.strip().split()[1:] for line in raw.decode("ascii").splitlines()
             if line.strip().startswith("node-id ")]
    assert len(nodes) == 1 and len(nodes[0]) == 1
    node = nodes[0][0].upper()
    assert node and all(char in "0123456789ABCDEF" for char in node)
    return node


def safe_failure(exc, depth=0):
    """Control-only diagnostics: never serialize exception messages or payloads."""
    if depth > 4:
        return {"truncated": True}
    if not isinstance(exc, BaseException):
        return exc if exc in ("ACKNOWLEDGED", "PURGED_OR_ABSENT", "ACKNOWLEDGED_OR_ABSENT") else "outcome"
    result = {"type": type(exc).__name__, "phase": getattr(exc, "phase", None)}
    code = getattr(exc, "result_code", getattr(exc, "_observed_code", None))
    if isinstance(code, int):
        result["code"] = int(code)
    native_type = getattr(exc, "_observed_exception", None)
    if native_type is not None:
        result["native_type"] = native_type
    outcomes = getattr(exc, "outcomes", None)
    if isinstance(outcomes, dict):
        result["outcomes"] = {str(key): safe_failure(value, depth + 1)
                              for key, value in outcomes.items() if type(key) is int}
    elif isinstance(outcomes, list):
        result["outcomes"] = [safe_failure(value, depth + 1) for value in outcomes[:32]]
        result["outcome_count"] = len(outcomes)
    if exc.__cause__ is not None:
        result["cause"] = safe_failure(exc.__cause__, depth + 1)
    return result


def independent_controls(provider, definition):
    """Separate ordinary SDK client, read-only complete streams, no backend fallback."""
    from aerospike_sdk import DataSet, ErrorStrategy

    cluster = definition.connect()
    try:
        session = cluster.create_session(provider._behavior)
        result = {}
        for kind, set_name in (("q", "gptc_q"), ("s", "gptc_s"), ("r", "gptc_r"),
                               ("meta", "gptc_meta")):
            controls = []
            try:
                stream = session.query(DataSet.of(provider.namespace, set_name)).with_txn(None).where(
                    provider._eq("scope", provider.cache_scope)).execute(on_error=ErrorStrategy.IN_STREAM)
                for row in stream:
                    assert row.is_ok and row.exception is None and not row.in_doubt
                    bins = row.record.bins
                    control = dict(generation=row.record.generation, **{
                        key: bins[key] for key in ("qid", "sid", "rid", "state", "adm", "hi", "phase", "boot")
                        if key in bins})
                    if kind == "meta":
                        matches = [name for name in ("root", "qid", "sid", "rid")
                                   if row.key.digest == provider._key(name).digest]
                        assert len(matches) == 1
                        control["kind"] = matches[0]
                    controls.append(control)
                result[kind] = {"terminal": "EOF", "rows": controls}
            except Exception as exc:
                result[kind] = {"terminal": safe_failure(exc), "rows": controls}
        return result
    finally:
        cluster.close()


def observe(label, value):
    print("AEROSPIKE_LIVE_OBSERVATION " + json.dumps({label: value}), flush=True)


def wait_schedule(directory, name, timeout=25):
    deadline = time.monotonic() + timeout
    while not (directory / name).exists():
        assert time.monotonic() < deadline, "Bounded schedule handshake expired"
        time.sleep(.02)


def concurrent_mutation(provider, request, definition):
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    scenario = request["scenario"]
    directory = Path(request["schedule"])
    if scenario == "mutation_seed":
        ids = provider.batch_insert([CacheData("concurrent-" + uuid.uuid4().hex, ["owned"])])
        return {"ids": ids, "sid_before": inspect_point(provider, "sid")["hi"]}
    qid = request["qid"]
    if scenario == "mutation_observe":
        children = provider.list_sessions("parallel-active", None)
        return {"qid": qid, "session_ids": [row.id for row in children],
                "controls": independent_controls(provider, definition)}
    if scenario == "mutation_mark_for_recovery":
        parent = inspect_point(provider, "q", qid)
        assert parent["state"] == 0 and parent["adm"] == 1
        provider.mark_deleted([qid])
        parent = inspect_point(provider, "q", qid)
        assert parent["state"] == -1 and parent["adm"] == 1
        return {"qid": qid, "adm": 1, "state": -1}
    if scenario == "mutation_recovered_inspection":
        assert inspect_point(provider, "q", qid) is None
        assert provider.list_sessions(None, qid) == []
        assert all(inspect_point(provider, "s", sid) is None for sid in request["sids"])
        controls = independent_controls(provider, definition)
        assert all(value["terminal"] == "EOF" for value in controls.values())
        assert all(row.get("qid") != qid for kind in ("q", "s") for row in controls[kind]["rows"])
        return {"qid": qid, "controls": controls}
    if scenario in ("mutation_append", "mutation_append_for_recovery"):
        original = provider._insert

        def paused_insert(kind, identity, bins):
            if kind == "s":
                parent = inspect_point(provider, "q", qid)
                assert parent["state"] == 0 and parent["adm"] == 1
                (directory / "admitted").write_text(json.dumps({"sid": identity, "qid": qid}))
                wait_schedule(directory, "release",
                              timeout=120 if scenario == "mutation_append_for_recovery" else 25)
            return original(kind, identity, bins)

        provider._insert = paused_insert
        started = time.monotonic()
        provider.add_session(qid, "paused-append", "real-acknowledged-admission")
        return {"started": started, "ended": time.monotonic(), "qid": qid}
    if scenario in ("mutation_close", "mutation_close_for_recovery"):
        wait_schedule(directory, "admitted")
        started = time.monotonic()
        assert provider.get_data_by_id(qid) is not None
        provider.add_session(qid, "parallel-active", "acknowledged")
        active = provider.list_sessions("parallel-active", None)
        if len(active) != 1:
            observe("concurrent_session_mismatch", dict(
                qid=qid, actual_count=len(active), actual_ids=[row.id for row in active],
                independent=independent_controls(provider, definition)))
        assert len(active) == 1
        admitted = json.loads((directory / "admitted").read_text())
        assert active[0].id != admitted["sid"]
        provider.mark_deleted([qid])
        assert provider.get_data_by_id(qid) is None
        assert inspect_point(provider, "q", qid)["adm"] == 1
        try:
            provider.add_session(qid, "closed-reject", "must-not-persist")
        except AerospikeStorageError as exc:
            assert exc.phase == "ENTRY_REJECTED"
        else:
            raise AssertionError("Closed parent admitted a child")
        try:
            provider.clear_deleted_data()
        except AerospikeStorageError as exc:
            assert isinstance(exc.outcomes[qid], AerospikeStorageError)
            assert str(exc.outcomes[qid]) == "Pending admissions; operator recovery required"
            assert exc.outcomes[qid].identity == qid
            observe("expected_pending_purge", safe_failure(exc))
        else:
            raise AssertionError("Pending purge reported success")
        parent = inspect_point(provider, "q", qid)
        assert parent["state"] == -1 and parent["adm"] == 1
        assert provider.list_sessions("closed-reject", None) == []
        ended = time.monotonic()
        if scenario == "mutation_close":
            (directory / "release").write_text("release after actual pending refusal")
        return {"started": started, "ended": ended, "sids": [active[0].id, admitted["sid"]]}
    assert scenario == "mutation_cleanup"
    parent = inspect_point(provider, "q", qid)
    assert parent["state"] == -1 and parent["adm"] == 0
    children = provider.list_sessions(None, qid)
    assert len(children) == 2 and len({child.id for child in children}) == 2
    assert {child.id for child in children} == set(request["sids"])
    assert inspect_point(provider, "sid")["hi"] == request["sid_before"] + 3
    provider.clear_deleted_data()
    provider.clear_deleted_data()
    assert inspect_point(provider, "q", qid) is None
    assert provider.get_data_by_id(qid) is None
    assert provider.list_sessions(None, qid) == []
    assert all(inspect_point(provider, "s", sid) is None for sid in request["sids"])
    return {"qid": qid, "sids": request["sids"], "cases": ["concurrent_mutation"]}


def snapshot(provider, definition=None):
    physical = {}
    for kind in ("q", "s", "r"):
        physical[kind] = sorted(
            (bins[kind + "id"], hashlib.sha256(json.dumps(
                bins, sort_keys=True, default=lambda value: value.hex()).encode()).hexdigest())
            for _, bins in provider._scan(kind))
    result = {
        "active": sorted(provider.get_ids(deleted=False)),
        "deleted": sorted(provider.get_ids(deleted=True)),
        "count": provider.count(is_all=True),
        "sessions": sorted([list(row) for row in provider.list_sessions(None, None)]),
        "reports": sorted([bins["rid"] for _, bins in provider._scan("r")]),
        "physical": physical,
        "metadata": {kind: inspect_point(provider, kind) for kind in ("root", "qid", "sid", "rid")},
    }
    if definition is not None:
        result["controls"] = independent_controls(provider, definition)
    if definition is not None and result["count"] != len(result["active"]) + len(result["deleted"]):
        observe("snapshot_mismatch", dict(
            active=result["active"], deleted=result["deleted"], count=result["count"],
            independent=independent_controls(provider, definition)))
    return result


def contract(provider, definition):
    from gptcache.manager.scalar_data.base import (
        Answer, CacheData, DataType, Question, QuestionDep,
    )
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError

    baseline = snapshot(provider, definition)
    baseline_controls = independent_controls(provider, definition)
    token = uuid.uuid4().hex
    question = Question("unicode-\u96ea-" + token, [
        QuestionDep("dep-\u03bb", "value-\u2603", DataType.IMAGE_URL),
        QuestionDep("second", "ordered", DataType.STR)])
    image = {"image": b"\x00\xffPNG", "url": "https://invalid.test/\u96ea",
             "nested": [None, True, -7, 1.25]}
    embedding = np.array([.25, -1.5, 2.125], dtype=np.float32)
    before = datetime.now()
    ids = provider.batch_insert([
        CacheData(question, [Answer(image, DataType.IMAGE_BASE64), b"\x00\xff", "\u00e9"],
                  embedding, session_id="initial-" + token),
        CacheData("second-" + token, ["two"]),
    ])
    provider._test_known_qids = ids
    after = datetime.now()
    assert len(ids) == 2 and ids[1] == ids[0] + 1
    assert provider.batch_insert([]) == []
    first = inspect_point(provider, "q", ids[0])
    old_access = datetime.fromisoformat(first["last_access"])
    old_create = datetime.fromisoformat(first["create_on"])
    assert before <= old_access <= after and before <= old_create <= after
    time.sleep(.02)
    data = provider.get_data_by_id(ids[0])
    assert data.question == question
    assert data.answers == [Answer(image, DataType.IMAGE_BASE64), Answer(b"\x00\xff"), Answer("\u00e9")]
    assert data.embedding_data.dtype == np.dtype("float32")
    np.testing.assert_array_equal(data.embedding_data, embedding)
    assert data.last_access == old_access and data.create_on == old_create
    touched = inspect_point(provider, "q", ids[0])
    assert datetime.fromisoformat(touched["last_access"]) > old_access
    assert touched["payload"] == first["payload"] and touched["create_on"] == first["create_on"]
    count = provider.count()
    if count != len(baseline["active"]) + 2:
        observe("count_mismatch", dict(
            baseline_active=baseline["active"], baseline_count=baseline["count"],
            acknowledged_ids=ids, actual_count=count, actual_ids=provider.get_ids(False),
            before=baseline_controls,
            independent=independent_controls(provider, definition)))
    assert count == len(baseline["active"]) + 2
    assert provider.count(is_all=True) == baseline["count"] + 2
    assert set(provider.get_ids(False)) == set(baseline["active"]) | set(ids)
    assert provider.get_data_by_id((1 << 63) - 1) is None

    name = "duplicate-" + token
    for _ in range(2):
        provider.add_session(ids[0], name, "same-question")
    sessions = provider.list_sessions(name, ids[1])
    assert len(sessions) == 2 and len({row.id for row in sessions}) == 2
    assert all(isinstance(row, tuple) and row.question_id == ids[0] and
               row.session_id == name and row.session_question == "same-question"
               for row in sessions)
    assert len(provider.list_sessions("", ids[0])) == 3
    assert provider.list_sessions("", ids[1]) == []
    assert len(provider.list_sessions(None, 0)) == len(baseline["sessions"]) + 3
    provider.delete_session([sessions[0].id, sessions[0].id, (1 << 63) - 1])
    assert [row.id for row in provider.list_sessions(name, None)] == [sessions[1].id]

    prior_reports = {bins["rid"]: dict(bins) for _, bins in provider._scan("r")}
    for _ in range(2):
        provider.report_cache("user-\u96ea", question.content, ids[0], image, .75, .125)
    reports = {bins["rid"]: dict(bins) for _, bins in provider._scan("r")}
    new_reports = {rid: bins for rid, bins in reports.items() if rid not in prior_reports}
    assert len(new_reports) == 2
    for rid, bins in new_reports.items():
        assert inspect_point(provider, "r", rid) == bins
        payload = provider._decode_report(bins["payload"])
        assert payload["cache_question_id"] == ids[0]
        assert payload["user_question"] == "user-\u96ea"
    assert all(reports[rid] == bins for rid, bins in prior_reports.items())
    provider.flush()
    provider.mark_deleted([ids[0], ids[0], (1 << 63) - 1])
    provider.mark_deleted([ids[0]])
    assert provider.get_data_by_id(ids[0]) is None
    assert ids[0] in provider.get_ids(True) and ids[0] not in provider.get_ids(False)
    assert provider.count(-1) == len(baseline["deleted"]) + 1
    try:
        provider.add_session(ids[0], "must-not-resurrect", "rejected")
    except AerospikeStorageError as exc:
        assert exc.phase == "ENTRY_REJECTED"
    else:
        raise AssertionError("Deleted parent admitted an association")
    provider.clear_deleted_data()
    provider.clear_deleted_data()
    assert inspect_point(provider, "q", ids[0]) is None
    assert provider.list_sessions(None, ids[0]) == []
    provider.mark_deleted([ids[1]])
    provider.clear_deleted_data()
    assert inspect_point(provider, "q", ids[1]) is None
    assert {bins["rid"]: bins for _, bins in provider._scan("r")} == reports
    return {"ids": ids, "cases": [
        "method:" + name for name in (
            "create", "batch_insert", "get_data_by_id", "mark_deleted",
            "clear_deleted_data", "get_ids", "count", "flush", "add_session",
            "list_sessions", "delete_session", "report_cache", "close")
    ] + ["last_access", "session_inspection", "report_inspection"]}


def partial_failure(provider):
    from aerospike_sdk import ResultCode
    from gptcache.manager.scalar_data.base import CacheData
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError

    before = inspect_point(provider, "qid")["hi"]
    try:
        provider.batch_insert([CacheData("small", ["persisted-prefix"]),
                               CacheData("oversize", [b"x" * (9 * 1024 * 1024)])])
    except AerospikeStorageError as exc:
        outcomes = exc.outcomes
        assert len(outcomes) == 2
        assert outcomes[0]["payload"] == "ACKNOWLEDGED"
        assert isinstance(outcomes[1]["payload"], Exception)
        rejection = outcomes[1]["payload"]
        assert rejection.phase == "TERMINAL_REJECTION"
        assert str(rejection) == "Command failed (result code {})".format(ResultCode.RECORD_TOO_BIG)
    else:
        raise AssertionError("Oversize batch silently succeeded or truncated")
    ids = [before + 1, before + 2]
    assert [row["qid"] for row in outcomes] == ids
    assert inspect_point(provider, "q", ids[0]) is not None
    assert inspect_point(provider, "q", ids[1]) is None
    assert inspect_point(provider, "qid")["hi"] == ids[1]
    assert provider.get_data_by_id(ids[0]).answers[0].answer == "persisted-prefix"
    next_id = provider.batch_insert([CacheData("after-failure", ["not-reused"])])[0]
    assert next_id > ids[-1]
    provider.mark_deleted([ids[0], next_id])
    provider.clear_deleted_data()
    return {"ids": ids, "cases": ["per_key_partial_failure", "oversize_no_truncation"]}


def shared(provider, request):
    from gptcache.manager.scalar_data.base import CacheData

    scenario = request["scenario"]
    if scenario == "shared_write":
        token = uuid.uuid4().hex
        time.sleep(max(0, request["start_at"] - time.monotonic()))
        started = time.monotonic()
        ids = provider.batch_insert([
            CacheData("spawn-" + token, [token]) for _ in range(8)])
        for qid in ids:
            provider.add_session(qid, token, "spawn-session")
            time.sleep(.02)
        return {"ids": ids, "token": token, "started": started, "ended": time.monotonic()}
    ids, token = request["ids"], request["token"]
    if scenario == "shared_cleanup":
        provider.mark_deleted(ids)
        provider.clear_deleted_data()
        assert all(inspect_point(provider, "q", qid) is None for qid in ids)
    else:
        for qid in ids:
            data = provider.get_data_by_id(qid)
            if scenario == "shared_isolation":
                assert data is None or data.answers[0].answer != token
                assert provider.list_sessions(token, qid) == []
            else:
                assert data is not None and data.answers[0].answer == token
                sessions = provider.list_sessions(token, qid)
                assert sum(row.question_id == qid for row in sessions) == 1
    return {"ids": ids}


def retention(provider, expected_node):
    from aerospike_sdk import DataSet, ErrorStrategy, RecordNotFoundError, ResultCode
    from aerospike_sdk.policy import Settings
    from gptcache.manager.scalar_data.base import CacheData

    replies = provider._session.info().info_on_all_nodes(
        "get-config:context=namespace;id=" + provider.namespace)
    assert set(replies) == {expected_node}
    for envelope in replies.values():
        body = envelope["get-config:context=namespace;id=" + provider.namespace]
        values = dict(part.split("=", 1) for part in body.split(";") if "=" in part)
        assert values["default-ttl"] == "12"
        assert values["default-read-touch-ttl-pct"] == "0"
    token = uuid.uuid4().hex
    ids = provider.batch_insert([CacheData("retention-" + token, ["active"]),
                                 CacheData("retention-deleted-" + token, ["deleted"])])
    provider.add_session(ids[0], token, "retained-child")
    sid = provider.list_sessions(token, None)[0].id
    provider.report_cache("retained-user", "retained-cache", ids[0], b"\x00\xff", .5, .25)
    rid = inspect_point(provider, "rid")["hi"]
    provider.mark_deleted([ids[1]])
    provider.get_data_by_id(ids[0])
    categories = [(kind, None) for kind in ("root", "qid", "sid", "rid")]
    categories += [("q", qid) for qid in ids] + [("s", sid), ("r", rid)]
    initial = {(kind, identity): inspect_point(provider, kind, identity)
               for kind, identity in categories}
    assert all(row is not None for row in initial.values())
    controls = DataSet.of(provider.namespace, "gptc_verify")
    keys = {name: controls.id(json.dumps([provider.cache_scope, token, name]))
            for name in ("unread", "no_touch", "positive")}
    positive = provider._cluster.create_session(provider._behavior.derive_with_changes(
        "positive_" + token, reads=Settings(read_touch_ttl_percent=50)))

    def read(session, key):
        rows = list(session.query(key).include_missing_keys().with_txn(None).execute(
            on_error=ErrorStrategy.IN_STREAM))
        assert len(rows) == 1 and not rows[0].in_doubt
        row = rows[0]
        if row.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
            assert row.record is None
            assert row.exception is None or (
                isinstance(row.exception, RecordNotFoundError) and
                row.exception.result_code == ResultCode.KEY_NOT_FOUND_ERROR and
                not row.exception.in_doubt)
            return None
        assert row.is_ok and row.exception is None
        return row.record

    for key in keys.values():
        rows = list(provider._session.insert(key).expire_record_after_seconds(12)
                    .with_txn(None).put({"owner": token}).execute())
        assert len(rows) == 1 and rows[0].is_ok
        record = read(provider._session, key)
        assert record is not None and record.ttl is not None
    started = time.monotonic()
    for target in (7, 11, 15):
        time.sleep(max(0, started + target - time.monotonic()))
        if target < 15:
            assert read(provider._session, keys["no_touch"]) is not None
        assert read(positive, keys["positive"]) is not None
        for kind, identity in categories:
            assert inspect_point(provider, kind, identity) == initial[kind, identity]
    assert time.monotonic() - started > 12
    assert read(provider._session, keys["unread"]) is None
    assert read(provider._session, keys["no_touch"]) is None
    assert read(provider._session, keys["positive"]) is not None
    for key in keys.values():
        rows = list(provider._session.update(key).never_expire().with_txn(None).include_missing_keys()
                    .with_durable_delete().delete_record().execute(on_error=ErrorStrategy.IN_STREAM))
        assert len(rows) == 1 and not rows[0].in_doubt
        assert rows[0].is_ok or (
            rows[0].result_code == ResultCode.KEY_NOT_FOUND_ERROR and
            (rows[0].exception is None or isinstance(rows[0].exception, RecordNotFoundError)))
        assert read(provider._session, key) is None
    provider.mark_deleted([ids[0]])
    provider.clear_deleted_data()
    return {"ids": ids, "elapsed": time.monotonic() - started,
            "categories": len(categories), "cases": ["finite_default_ttl_all_categories"]}


class VectorInterfaceDouble:
    """Only the unrelated vector boundary is doubled; this is NOT real FAISS."""

    def __init__(self):
        self.rows = {}
        self.deleted = []
        self.closed = False
        self.flushed = False

    def mul_add(self, rows):
        self.rows.update({row.id: row.data for row in rows})

    def search(self, data, top_k=-1):
        return [(float(np.dot(data, value)), qid) for qid, value in self.rows.items()]

    def delete(self, ids):
        self.deleted.extend(ids)
        for qid in ids:
            self.rows.pop(qid, None)
        return True

    def rebuild(self, ids=None):
        return True

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def manager_composition(provider, definition):
    from gptcache.manager.data_manager import SSDataManager
    from gptcache.manager.factory import manager_factory
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorage

    initial_count = provider.count()
    params = dict(cluster_definition=definition, namespace=provider.namespace,
                  cache_scope=provider.cache_scope, behavior=provider._behavior)
    provider.close()
    vector = VectorInterfaceDouble()
    with TemporaryDirectory(prefix="gptcache-owned-aero-") as directory:
        with patch("gptcache.manager.factory.VectorBase", return_value=vector):
            manager = manager_factory(
                "aerospike,test-vector-interface-double", data_dir=directory,
                scalar_params=params,
                eviction_params={"max_size": initial_count + 1, "clean_size": 1})
        assert isinstance(manager, SSDataManager) and isinstance(manager.s, AerospikeStorage)
        try:
            token = uuid.uuid4().hex
            manager.save("manager-" + token, "manager-answer", np.array([1., 0.]))
            first = max(vector.rows)
            data = manager.get_scalar_data((1., first))
            assert data.answers[0].answer == "manager-answer"
            manager.add_session((1., first), token, "manager-session")
            assert len(manager.list_sessions(key=first)) == 1
            assert manager.list_sessions(session_id=token)
            manager.delete_session(token)
            assert manager.list_sessions(key=first) == []
            manager.report_cache("manager-user", "manager-cache", first, "answer", .5, .1)
            assert manager.search(np.array([1., 0.]))
            manager.hit_cache_callback((1., first))
            for _ in range(initial_count + 2):
                manager.save("evict-" + token, "eviction-answer", np.array([0., 1.]))
            assert manager.eviction_manager.delete_count > 0
            assert vector.deleted and first in vector.deleted
            assert manager.s.get_data_by_id(first) is None
            manager.flush()
            assert vector.flushed
            remaining = list(vector.rows)
            manager.s.mark_deleted(remaining)
            manager.s.clear_deleted_data()
        finally:
            manager.close()
    assert vector.closed
    return {"ids": [first], "cases": ["manager_composition"], "vector": "interface-double-not-FAISS"}


def stream_failure(provider, port, mode="deadline"):
    from aerospike_sdk.sync import ClusterDefinition
    from aerospike_sdk.policy import SystemSettings, TransactionSettings
    from aerospike_stream_fault import StreamFaultProxy
    from gptcache.manager.scalar_data.base import CacheData
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError

    token = "fault-prefix-" + uuid.uuid4().hex
    ids = provider.batch_insert([CacheData(token, ["x" * 32768]) for _ in range(256)])
    selected = set(ids)

    def truth():
        return {bins["qid"]: dict(
            generation=row.record.generation,
            hash=hashlib.sha256(json.dumps(
                bins, sort_keys=True, default=lambda value: value.hex()).encode()).hexdigest())
                for row, bins in provider._scan("q") if bins["qid"] in selected}

    before = truth()
    assert len(before) == len(ids)
    expected_ids = {bins["qid"] for _, bins in provider._scan("q")}
    definition = ClusterDefinition("127.0.0.2", port).with_ip_map(
        {"127.0.0.1": "127.0.0.2"}).using_services_alternate(False).with_strict_config()
    definition.with_system_settings(SystemSettings(transactions=TransactionSettings(
        implicit_batch_write_transactions=False)))
    with StreamFaultProxy(port, mode) as wire:
        doomed = type(provider)(
            cluster_definition=definition, namespace=provider.namespace,
            cache_scope=provider.cache_scope, behavior=provider._behavior)
        try:
            doomed.create()
            doomed.throttle = True
            wire.arm()
            started = time.monotonic()
            errors = []
            returned = None
            try:
                returned = doomed.count(is_all=True)
            except AerospikeStorageError as exc:
                assert exc.outcomes
                errors = [type(error).__name__ for error in exc.outcomes]
            observed = doomed.observed_rows
            elapsed = time.monotonic() - started
            evidence = wire.snapshot()
            retry_cuts = sum(event.get("injected") == "eof_retry" for event in evidence["events"])
            print("AEROSPIKE_LIVE_OBSERVATION " + json.dumps(dict(
                mode=mode, delivered_rows=observed, expected_count=len(expected_ids),
                returned_count=returned, seeded_ids=ids, errors=errors,
                query_seconds=elapsed, wire_prefix=wire.prefix, retry_cuts=retry_cuts,
                decoder_errors=wire.errors)), flush=True)
            assert wire.prefix and not wire.errors
            if mode == "eof":
                # The reviewed contract permits bounded partition resumption.
                assert not errors and returned == len(expected_ids)
                assert doomed.observed_ids == expected_ids
            else:
                assert returned is None and errors
                assert 0 < observed < len(expected_ids)
                assert any("Timeout" in name for name in errors)
                if mode == "eof_persistent":
                    assert retry_cuts > 0
        finally:
            doomed.close()
    assert truth() == before, "Read-only fault changed persisted records"
    # Cleanup only after the strict-prefix and unchanged-data assertions pass.
    provider.mark_deleted(ids)
    provider.clear_deleted_data()
    return {"ids": [], "observed_rows": observed, "elapsed": elapsed,
            "errors": errors, "returned_count": returned, "expected_count": len(expected_ids),
            "fault_mode": mode, "cases": ["stream_failure"]}


def scope_method(provider, method):
    from gptcache.manager.scalar_data.base import CacheData

    baseline = snapshot(provider)
    token = uuid.uuid4().hex
    ids = provider.batch_insert([CacheData("scope-" + token, [token])])
    provider._test_known_qids = ids
    qid = ids[0]
    provider.add_session(qid, token, "scope-session")
    sid = provider.list_sessions(token, None)[0].id
    if method == "create":
        provider.create()
    elif method == "batch_insert":
        ids.extend(provider.batch_insert([CacheData("scope-second", [token])]))
    elif method == "get_data_by_id":
        assert provider.get_data_by_id(qid).answers[0].answer == token
    elif method == "mark_deleted":
        provider.mark_deleted([qid])
    elif method == "clear_deleted_data":
        provider.mark_deleted([qid])
        provider.clear_deleted_data()
        assert inspect_point(provider, "q", qid) is None
    elif method == "get_ids":
        assert set(provider.get_ids(False)) == set(baseline["active"]) | set(ids)
        assert set(provider.get_ids(True)) == set(baseline["deleted"])
    elif method == "count":
        assert provider.count(is_all=True) == baseline["count"] + 1
    elif method == "flush":
        provider.flush()
    elif method == "add_session":
        provider.add_session(qid, token, "second-association")
        assert len(provider.list_sessions(token, None)) == 2
    elif method == "list_sessions":
        rows = provider.list_sessions(token, None)
        assert len(rows) == 1 and rows[0].question_id == qid
    elif method == "delete_session":
        provider.delete_session([sid])
        assert provider.list_sessions(token, None) == []
        assert provider.get_data_by_id(qid) is not None
    elif method == "report_cache":
        provider.report_cache(token, "scope-cache", qid, token, .5, .1)
    else:
        raise AssertionError("Unknown scoped method")
    provider.mark_deleted(ids)
    provider.clear_deleted_data()
    return {"ids": ids, "cases": ["scope:" + method]}


def main():
    request = json.loads(sys.stdin.readline())
    assert request["scenario"] in (
        "smoke", "contract", "partial_failure", "snapshot", "shared_write",
        "shared_read", "shared_cleanup", "shared_isolation", "retention", "manager",
        "stream_failure", "scope_method", "mutation_seed", "mutation_append",
        "mutation_close", "mutation_cleanup", "mutation_observe",
        "mutation_mark_for_recovery", "mutation_recovered_inspection", "enum_diagnostic", "enum_minimal",
        "enum_wire", "enum_wire_seed", "enum_churn", "eof_diagnostic", "retained_eof_points",
        "ack_read_failure", "mutation_append_for_recovery", "mutation_close_for_recovery")
    from gptcache.manager.scalar_data.aerospike_operator import (
        DockerMaintenanceOperator, _Journal, _host_boot, _process,
    )

    operator = DockerMaintenanceOperator.from_manifest(
        request["manifest"], journal_dir=request["journal"])
    # Admission/identity check only: a paused DB writer must not block the
    # controller's exclusive settlement lock, which terminates registered PIDs.
    with open(Path(request["journal"]) / "lock", "rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        status = _Journal.status(request["journal"])
        assert status["state"] == "PUBLIC" and not status["launch_blocked"]
        receipt = json.loads((Path(request["journal"]) / "receipt.json").read_text())
        info = os.fstat(lock.fileno())
        assert [info.st_dev, info.st_ino] == receipt["lock"], "Shared lock inode changed"
        process = _process(os.getpid())
        identity = {key: process[key] for key in ("pid", "ticks", "argv_sha256")}
        identity.update(boot=_host_boot(), uid=os.getuid())
        assert identity in receipt["controller"]["writers"], "Child was not registered"
        assert receipt["manifest"] == operator.digest
    print("AEROSPIKE_LIVE_ADMITTED", flush=True)
    from aerospike_sdk.sync import ClusterDefinition
    from aerospike_sdk.policy import Behavior, Settings, SystemSettings, TransactionSettings
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorage
    from gptcache.manager.scalar_data.base import CacheData

    storage_type = AerospikeStorage
    if request["scenario"] in ("stream_failure", "eof_diagnostic"):
        from aerospike_sdk import DataSet, QueryDuration, QueryHint

        class ObservedStorage(AerospikeStorage):
            """Count real validated delivery; never replace a native outcome."""

            throttle = False
            observed_rows = 0
            observed_ids = None

            def _query(self, target):
                builder = super()._query(target)
                if self.throttle and isinstance(target, DataSet):
                    return builder.with_hint(QueryHint(query_duration=QueryDuration.LONG))
                return builder

            def _validate(self, kind, row, identity=None):
                bins = super()._validate(kind, row, identity)
                if self.throttle and kind == "q":
                    self.observed_rows += 1
                    if self.observed_ids is None:
                        self.observed_ids = set()
                    self.observed_ids.add(bins["qid"])
                return bins

        storage_type = ObservedStorage

    definition = ClusterDefinition("127.0.0.1", operator.manifest["public_port"])
    definition.using_services_alternate(False).with_strict_config()
    definition.with_system_settings(SystemSettings(transactions=TransactionSettings(
        implicit_batch_write_transactions=False)))
    if request["scenario"] in ("enum_minimal", "enum_wire"):
        from aerospike_enum_minimal import run
        native = Path(sys.modules["aerospike_async._aerospike_async_native"].__file__).resolve()
        native_sha256 = hashlib.sha256(native.read_bytes()).hexdigest()
        observe("native_provenance", {"path": str(native), "sha256": native_sha256,
                                      "scratch_experiment": "expected_native_sha256" in request})
        if "expected_native_sha256" in request:
            assert native_sha256 == request["expected_native_sha256"]
            assert str(native) == request["expected_native_path"]
        if request["scenario"] == "enum_wire":
            from aerospike_wire_observer import WireObserver

            port = operator.manifest["public_port"]
            query_definition = ClusterDefinition("127.0.0.2", port).with_ip_map(
                {"127.0.0.1": "127.0.0.2"}).using_services_alternate(False).with_strict_config()
            query_definition.with_system_settings(SystemSettings(transactions=TransactionSettings(
                implicit_batch_write_transactions=False)))
            with WireObserver(port) as wire:
                try:
                    result = run(definition, request, observe, query_definition=query_definition, wire=wire)
                finally:
                    observe("wire_complete", wire.snapshot())
        else:
            result = run(definition, request, observe)
        result.update(scenario=request["scenario"], pid=os.getpid())
        print("AEROSPIKE_LIVE_RESULT " + json.dumps(result), flush=True)
        return
    provider = storage_type(
        cluster_definition=definition, namespace=operator.manifest["namespace"],
        cache_scope=request["scope"],
        behavior=Behavior.DEFAULT.derive_with_changes(
            "gptcache_live_worker_" + str(os.getpid()),
            reads_query=Settings(total_timeout=timedelta(seconds=3),
                                 record_queue_size=1 if request["scenario"] in (
                                     "stream_failure", "eof_diagnostic") else None)))
    try:
        provider.create()
        original_status = provider._status

        def observed_status(row):
            try:
                return original_status(row)
            except Exception as exc:
                if isinstance(row.result_code, int):
                    exc._observed_code = int(row.result_code)
                exc._observed_exception = type(row.exception).__name__
                raise

        provider._status = observed_status
        original_purge = provider.clear_deleted_data
        original_scan = provider._scan
        purge_scans = None

        def observed_scan(kind, *predicates):
            try:
                rows = original_scan(kind, *predicates)
            except Exception as exc:
                if purge_scans is not None:
                    purge_scans.append({"kind": kind, "terminal": safe_failure(exc)})
                raise
            if purge_scans is not None:
                purge_scans.append(dict(kind=kind, terminal="EOF", rows=[
                    dict(generation=row.record.generation, **{
                        name: bins[name] for name in ("qid", "sid", "rid", "state", "adm")
                        if name in bins}) for row, bins in rows]))
            return rows

        provider._scan = observed_scan

        def observed_purge():
            nonlocal purge_scans
            before = independent_controls(provider, definition)
            known = getattr(provider, "_test_known_qids", [])
            points_before = {qid: inspect_point(provider, "q", qid, controls=True) for qid in known}
            purge_scans = []
            try:
                result = original_purge()
            except Exception as exc:
                observe("purge_failure", dict(
                    error=safe_failure(exc), before=before, points_before=points_before,
                    actual_scans=purge_scans,
                    after=independent_controls(provider, definition)))
                raise
            else:
                points_after = {qid: inspect_point(provider, "q", qid, controls=True) for qid in known}
                if any(row is not None and row["state"] == -1 for row in points_after.values()):
                    observe("purge_returned_with_tombstones", dict(
                        before=before, points_before=points_before, actual_scans=purge_scans,
                        points_after=points_after, after=independent_controls(provider, definition)))
                return result
            finally:
                purge_scans = None

        if request["scenario"] in (
                "contract", "scope_method", "mutation_close", "mutation_close_for_recovery",
                "mutation_cleanup"):
            provider.clear_deleted_data = observed_purge
        if request["scenario"] == "smoke":
            ids = provider.batch_insert([CacheData("real-smoke", ["real-answer"])])
            assert len(ids) == 1
            assert provider.get_data_by_id(ids[0]).answers[0].answer == "real-answer"
            result = {"ids": ids}
        elif request["scenario"] == "enum_wire_seed":
            assert type(request["deleted"]) is bool
            assert all(inspect_point(provider, kind)["hi"] == 0 for kind in ("qid", "sid", "rid"))
            token = uuid.uuid4().hex
            ids = provider.batch_insert([CacheData("wire-fixture-" + token + str(i), ["synthetic"])
                                         for i in range(2)])
            assert len(ids) == len(set(ids)) == 2
            if request["deleted"]:
                provider.mark_deleted(ids)
            else:
                for qid in ids:
                    assert provider.get_data_by_id(qid).answers[0].answer == "synthetic"
            rows = {qid: inspect_point(provider, "q", qid, controls=True) for qid in ids}
            assert all(row["state"] == (-1 if request["deleted"] else 0) and
                       row["adm"] == 0 and row["generation"] == 2 for row in rows.values())
            result = {"ids": ids, "controls": rows, "synthetic_fixture_only": True}
        elif request["scenario"] == "enum_churn":
            from aerospike_enum_churn import run
            result = run(provider, definition, observe, inspect_point, safe_failure)
        elif request["scenario"] == "contract":
            result = contract(provider, definition)
        elif request["scenario"] == "partial_failure":
            result = partial_failure(provider)
        elif request["scenario"].startswith("shared_"):
            result = shared(provider, request)
        elif request["scenario"].startswith("mutation_"):
            result = concurrent_mutation(provider, request, definition)
        elif request["scenario"] == "retention":
            result = retention(provider, configured_node(operator.manifest))
        elif request["scenario"] == "manager":
            result = manager_composition(provider, definition)
        elif request["scenario"] == "stream_failure":
            result = stream_failure(provider, operator.manifest["public_port"],
                                    request.get("fault_mode", "deadline"))
        elif request["scenario"] == "eof_diagnostic":
            from aerospike_eof_diagnostic import run
            assert request["public_port"] == operator.manifest["public_port"]
            result = run(provider, definition, request, observe, inspect_point)
        elif request["scenario"] == "retained_eof_points":
            from aerospike_eof_diagnostic import verify_retained
            result = verify_retained(provider, request, observe)
        elif request["scenario"] == "ack_read_failure":
            from aerospike_ack_read_fault import run
            result = run(provider, operator.manifest["public_port"], observe, inspect_point)
        elif request["scenario"] == "scope_method":
            result = scope_method(provider, request["method"])
        elif request["scenario"] == "enum_diagnostic":
            from aerospike_enum_diagnostic import run
            result = run(provider, definition, request, observe, safe_failure)
        else:
            result = snapshot(provider, definition)
    finally:
        provider.close()
    provider.close()
    try:
        provider.flush()
    except RuntimeError:
        pass
    else:
        raise AssertionError("Closed provider accepted flush")
    result.update(scenario=request["scenario"], pid=os.getpid())
    print("AEROSPIKE_LIVE_RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
