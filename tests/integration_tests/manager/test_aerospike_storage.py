"""Real-provider first-live gate; NOT the full mandatory 01-03 case inventory."""

from concurrent.futures import ThreadPoolExecutor
import json
import time

import pytest


def test_operator_initializes_two_scopes_on_one_deployment(aerospike_initialized):
    config = aerospike_initialized
    status = config["operator"].status()
    for intent in config["scopes"]:
        assert status["intents"][intent["intent_id"]]["status"] == "ACTIVE"
    assert status["state"] == "PUBLIC"
    assert status["launch_blocked"] is False


def test_registered_child_provider_smoke(aerospike_initialized, aerospike_worker):
    for intent in aerospike_initialized["scopes"]:
        result = aerospike_worker(intent["cache_scope"])
        assert len(result["ids"]) == 1 and result["ids"][0] > 0


def test_all_methods_codec_sessions_reports(aerospike_initialized, aerospike_worker):
    scopes = aerospike_initialized["scopes"]
    for index, intent in enumerate(scopes):
        other = scopes[1 - index]["cache_scope"]
        before = aerospike_worker(other, "snapshot")
        result = aerospike_worker(intent["cache_scope"], "contract")
        after = aerospike_worker(other, "snapshot")
        for name in ("active", "deleted", "count", "sessions", "reports", "physical", "metadata"):
            assert before[name] == after[name], (
                "Unequal scope changed: " + name + " before=" + str(before[name]) +
                " after=" + str(after[name]))
        for case in result["cases"]:
            print("OBS " + case + " real-provider acknowledged and inspected")


def test_real_batch_partial_failure_no_truncation(aerospike_initialized, aerospike_worker):
    for intent in aerospike_initialized["scopes"]:
        result = aerospike_worker(intent["cache_scope"], "partial_failure")
        for case in result["cases"]:
            print("OBS " + case + " actual oversize rejected, persisted prefix inspected")


def test_spawn_process_sharing_and_unique_ids(aerospike_initialized, aerospike_worker):
    scope, other = [row["cache_scope"] for row in aerospike_initialized["scopes"]]
    start_at = time.monotonic() + 15
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(aerospike_worker, scope, "shared_write", start_at=start_at)
                   for _ in range(2)]
        results = [future.result() for future in futures]
    assert results[0]["pid"] != results[1]["pid"]
    ids = [qid for row in results for qid in row["ids"]]
    assert len(ids) == len(set(ids)) == 16
    assert max(row["started"] for row in results) < min(row["ended"] for row in results)
    for row in results:
        parameters = {"ids": row["ids"], "token": row["token"]}
        read = aerospike_worker(scope, "shared_read", **parameters)
        assert read["pid"] not in [r["pid"] for r in results]
        aerospike_worker(other, "shared_isolation", **parameters)
        aerospike_worker(scope, "shared_cleanup", **parameters)
    print("OBS cross_process_sharing independent spawned clients, unique IDs and unequal-scope isolation")


def test_concurrent_mutation_admitted_append_mark_get_purge(
        aerospike_initialized, aerospike_worker, tmp_path):
    for index, intent in enumerate(aerospike_initialized["scopes"]):
        directory = tmp_path / str(index)
        directory.mkdir()
        scope = intent["cache_scope"]
        seed = aerospike_worker(scope, "mutation_seed", schedule=str(directory))
        qid = seed["ids"][0]
        with ThreadPoolExecutor(max_workers=2) as pool:
            append = pool.submit(aerospike_worker, scope, "mutation_append",
                                 schedule=str(directory), qid=qid)
            close = pool.submit(aerospike_worker, scope, "mutation_close",
                                schedule=str(directory), qid=qid)
            try:
                closer = close.result()
            finally:
                # Release a TEST pause on assertion failure too; never strand
                # a real admission merely because the observer failed.
                (directory / "release").write_text("parent schedule settlement")
                appender = append.result()
        assert closer["pid"] != appender["pid"]
        assert appender["started"] < closer["started"] < closer["ended"] < appender["ended"]
        result = aerospike_worker(
            scope, "mutation_cleanup", schedule=str(directory), qid=qid,
            sids=closer["sids"], sid_before=seed["sid_before"])
        assert result["cases"] == ["concurrent_mutation"]
    print("OBS concurrent_mutation real overlapping registered spawned clients; acknowledged admission "
          "paused, active append unique SID, mark/get/closed rejection/pending purge error; "
          "quiescent release adm0 and complete q/s cleanup, no resurrection")


def test_finite_default_retention_all_categories(aerospike_initialized, aerospike_worker):
    result = aerospike_worker(aerospike_initialized["scopes"][0]["cache_scope"], "retention")
    assert result["categories"] == 8 and result["elapsed"] > 12
    print("OBS finite_default_ttl_all_categories root, three allocators, active/deleted q, s, r "
          "TTLNone beyond12s; finite unread/no-touch expired and +50 survived")


def test_manager_composition(aerospike_initialized, aerospike_worker):
    preserved, scope = [row["cache_scope"] for row in aerospike_initialized["scopes"]]
    before = aerospike_worker(preserved, "snapshot")
    # Eviction is intentionally destructive: keep it away from retained diagnostics.
    result = aerospike_worker(scope, "manager")
    after = aerospike_worker(preserved, "snapshot")
    for name in ("active", "deleted", "count", "sessions", "reports", "physical", "metadata"):
        assert before[name] == after[name], "Manager eviction changed foreign scope: " + name
    assert result["vector"] == "interface-double-not-FAISS"
    print("OBS manager_composition actual scalar registry/factory/SSDataManager/memory eviction; "
          "vector interface DOUBLE, not real FAISS")


@pytest.mark.aerospike_diagnostic
def test_quiescent_enum_point_truth_diagnostic(aerospike_initialized, aerospike_worker):
    """One registered writer, bounded fixed rounds; stop at first disagreement."""
    scopes = [intent["cache_scope"] for intent in aerospike_initialized["scopes"]]
    result = aerospike_worker(scopes[0], "enum_diagnostic", owned_scopes=scopes)
    assert result["rounds"] == 9 and result["checkpoints"] == 28


@pytest.mark.aerospike_diagnostic
def test_quiescent_enum_read_only_diagnostic(aerospike_initialized, aerospike_worker):
    """Retain failed-round rows; discriminate query omission without any writes."""
    scopes = [intent["cache_scope"] for intent in aerospike_initialized["scopes"]]
    result = aerospike_worker(scopes[0], "enum_diagnostic", owned_scopes=scopes, read_only=True)
    assert result["checkpoints"] == 30


@pytest.mark.aerospike_diagnostic
def test_sdk_only_enum_minimal(aerospike_initialized, aerospike_worker):
    scopes = [intent["cache_scope"] for intent in aerospike_initialized["scopes"]]
    result = aerospike_worker(scopes[0], "enum_minimal", owned_scopes=scopes, namespace="test")
    assert result["repetitions"] == 30


@pytest.mark.aerospike_diagnostic
def test_sdk_only_enum_wire_attribution(aerospike_initialized, aerospike_worker):
    scopes = [intent["cache_scope"] for intent in aerospike_initialized["scopes"]]
    result = aerospike_worker(scopes[0], "enum_wire", owned_scopes=scopes, namespace="test")
    assert result["repetitions"] == 30


@pytest.mark.parametrize("fault_mode", ["deadline", "eof_persistent"])
def test_real_query_deadline_after_prefix(aerospike_initialized, aerospike_worker, fault_mode):
    result = aerospike_worker(aerospike_initialized["scopes"][0]["cache_scope"],
                              "stream_failure", fault_mode=fault_mode)
    assert result["observed_rows"] > 0
    assert result["fault_mode"] == fault_mode
    print("OBS stream_failure actual " + fault_mode + " after forwarded real-server frame; "
          "adapter count raised after strict prefix=" + str(result["observed_rows"]))


def test_transient_eof_resumes_complete_query(aerospike_initialized, aerospike_worker):
    result = aerospike_worker(aerospike_initialized["scopes"][0]["cache_scope"],
                              "stream_failure", fault_mode="eof")
    assert result["errors"] == []
    assert result["returned_count"] == result["expected_count"]
    print("OBS transient_eof partition resumption completes all independently inventoried identities")


def test_acknowledged_touch_then_session_transport_failure(aerospike_initialized, aerospike_worker):
    result = aerospike_worker(aerospike_initialized["scopes"][0]["cache_scope"], "ack_read_failure")
    assert result["cases"] == ["ack_then_read_failure"]
    assert result["touch_requests"] == result["actual_acknowledgments"] == 1
    assert not result["touch_replayed"]
    print("OBS real touch ACK then actual session transport timeout; error surfaced, no touch replay")


def test_operator_recovers_crashed_admitted_writer(aerospike_initialized, aerospike_worker, tmp_path):
    config = aerospike_initialized
    scope, other = [row["cache_scope"] for row in config["scopes"]]
    foreign = aerospike_worker(other, "snapshot")
    seed = aerospike_worker(scope, "mutation_seed", schedule=str(tmp_path))
    qid = seed["ids"][0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        stranded = pool.submit(
            aerospike_worker, scope, "mutation_append_for_recovery",
            expected_termination=-9, schedule=str(tmp_path), qid=qid)
        close = aerospike_worker(scope, "mutation_close_for_recovery",
                                 schedule=str(tmp_path), qid=qid)
        assert not stranded.done(), "Admitted writer already ended before real controller exclusion"
        # The actual operator must terminate this registered writer and settle ASD
        # before resetting the stranded admission and deleting the owned records.
        recovery = config["operator"].recover_session_purge(
            namespace=config["scopes"][0]["namespace"], cache_scope=scope, qids=[qid])
        assert recovery["status"] == "OK" and recovery["results"] == [{"qid": qid, "status": "PURGED"}]
        crashed = stranded.result()
        assert crashed["terminated"] and crashed["returncode"] == -9
    finished = config["operator"].finish_maintenance()
    assert finished["status"] == "PUBLIC"
    result = aerospike_worker(
        scope, "mutation_recovered_inspection", schedule=str(tmp_path), qid=qid, sids=close["sids"])
    assert result["qid"] == qid
    after = aerospike_worker(other, "snapshot")
    for name in ("active", "deleted", "count", "sessions", "reports", "physical", "metadata"):
        assert foreign[name] == after[name], "Recovery changed foreign scope: " + name
    print("OBS operator_crash_recovery actual SIGKILL of admitted registered writer, clean settlement, "
          "tombstone adm1 recovery/PURGED, exact q/s absence, foreign scope unchanged")


@pytest.mark.parametrize("method", [
    "create", "batch_insert", "get_data_by_id", "mark_deleted", "clear_deleted_data",
    "get_ids", "count", "flush", "add_session", "list_sessions", "delete_session", "report_cache",
])
def test_each_method_preserves_unequal_scope(aerospike_initialized, aerospike_worker, method):
    scope, other = [row["cache_scope"] for row in aerospike_initialized["scopes"]]
    before = aerospike_worker(other, "snapshot")
    result = aerospike_worker(scope, "scope_method", method=method)
    after = aerospike_worker(other, "snapshot")
    for name in ("active", "deleted", "count", "sessions", "reports", "physical", "metadata"):
        if before[name] != after[name]:
            print("AEROSPIKE_LIVE_OBSERVATION " + json.dumps({
                "foreign_snapshot_mismatch": dict(
                    method=method, field=name, before=before[name], after=after[name],
                    controls_before=before["controls"], controls_after=after["controls"])}))
        assert before[name] == after[name], "Foreign scope changed during " + method + ": " + name
    assert result["cases"] == ["scope:" + method]
    print("OBS scope:" + method + " unequal scope physical hashes, metadata, sessions and reports unchanged")
