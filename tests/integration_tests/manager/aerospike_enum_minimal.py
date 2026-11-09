"""Read-only public SDK reproducer; no AerospikeStorage instance or calls.

Run only through the already allowlisted, registered live worker. Both scopes
come from the explicit OWN environment, never discovered/adopted from localhost.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time


def run(definition, request, observe, *, query_definition=None, wire=None):
    from aerospike_sdk import Behavior, DataSet, ErrorStrategy, ResultCode
    from aerospike_sdk.policy import Settings
    from gptcache.manager.scalar_data.aerospike_operator import _Journal, _process

    namespace, scopes = request["namespace"], request["owned_scopes"]
    assert namespace == "test" and len(scopes) == 2 and len(set(scopes)) == 2
    receipt = json.loads((Path(request["journal"]) / "receipt.json").read_text())
    live = []
    for writer in receipt["controller"]["writers"]:
        current = _process(writer["pid"])
        if current and current["ticks"] == writer["ticks"] and current["state"] not in ("Z", "X"):
            live.append(current["pid"])
    assert live == [os.getpid()]
    status = _Journal.status(request["journal"])
    assert status["state"] == "PUBLIC" and not status["launch_blocked"]
    assert set(scopes) == {row["cache_scope"] for row in receipt["controller"]["intents"].values()}
    behavior = Behavior.DEFAULT.derive_with_changes(
        "enum_minimal_" + str(os.getpid()), reads_query=Settings(total_timeout=timedelta(seconds=3)))
    dataset = DataSet.of(namespace, "gptc_q")

    def key(scope, kind, identity=None):
        parts = ["gptc", 1, scope, kind]
        if identity is not None:
            parts.append(identity)
        return DataSet.of(namespace, "gptc_q" if kind == "q" else "gptc_meta").id(
            json.dumps(parts, ensure_ascii=True, allow_nan=False, separators=(",", ":")))

    def copy_row(row):
        result = {"digest": row.key.digest, "code": str(row.result_code),
                  "in_doubt": row.in_doubt, "exception": type(row.exception).__name__ if row.exception else None}
        if row.record is not None:
            bins = row.record.bins
            result.update(generation=row.record.generation, ttl=row.record.ttl,
                          hash=hashlib.sha256(json.dumps(
                              bins, sort_keys=True, default=lambda item: item.hex()).encode()).hexdigest(),
                          controls={name: bins[name] for name in
                                    ("fmt", "scope", "boot", "phase", "hi", "qid", "state", "adm")
                                    if name in bins})
        return result

    def point_batch(session, keys):
        rows = list(session.query(*keys).with_txn(None).include_missing_keys().execute(
            on_error=ErrorStrategy.IN_STREAM))
        assert len(rows) == len(keys) and {r.key.digest for r in rows} == {k.digest for k in keys}
        for row in rows:
            assert not row.in_doubt
            if row.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
                assert row.record is None
                assert row.exception is None or (
                    row.exception.result_code == ResultCode.KEY_NOT_FOUND_ERROR and not row.exception.in_doubt)
            else:
                assert row.is_ok and row.result_code == ResultCode.OK
                assert row.exception is None and row.record is not None
        return [copy_row(row) for row in rows]

    def info(session):
        api = session.info()
        result = {"sets": api.info_on_all_nodes("sets"), "stats": {}}
        for command in ("statistics", "namespace/" + namespace):
            values = api.info_on_all_nodes(command)
            result["stats"][command] = {}
            for node, response in values.items():
                fields = dict(item.split("=", 1) for item in response[command].split(";") if "=" in item)
                result["stats"][command][node] = {k: v for k, v in fields.items()
                    if k.startswith(("query_", "scan_")) or k in
                    ("objects", "tombstones", "client_write_success", "client_delete_success", "client_udf_complete")}
        return result

    if query_definition is None:
        query_definition = definition
    with query_definition.connect() as query_cluster, definition.connect() as point_cluster:
        query_session = query_cluster.create_session(behavior)
        point_session = point_cluster.create_session(behavior)
        meta = point_batch(point_session, [key(scope, kind) for scope in scopes for kind in ("root", "qid")])
        high_by_scope = {row["controls"]["scope"]: row["controls"]["hi"]
                         for row in meta if "hi" in row.get("controls", {})}
        assert set(high_by_scope) == set(scopes)
        assert all(type(high) is int and 0 <= high <= 2048 for high in high_by_scope.values())
        keys = [key(scope, "q", qid) for scope in scopes for qid in range(1, high_by_scope[scope] + 1)]
        initial = []
        for low in range(0, len(keys), 64):
            initial.extend(point_batch(point_session, keys[low:low + 64]))
        allocated_present = [row for row in initial if row["code"] == str(ResultCode.OK)]
        known_keys = [physical for physical in keys
                      if physical.digest in {row["digest"] for row in allocated_present}]
        expected_rows = request.get("expected_rows", 4)
        assert type(expected_rows) is int and 0 < expected_rows <= 1024
        assert len(known_keys) == expected_rows, "Independently present rows differ from the supplied fixture"
        observe("minimal_allocated_truth", {"meta": meta, "rows": initial, "live_writers": live})
        for repetition in range(30):
            evidence = {"repetition": repetition, "utc": datetime.now(timezone.utc).isoformat(),
                        "start_ns": time.monotonic_ns(), "before": point_batch(point_session, known_keys),
                        "server_before": info(point_session), "query_rows": []}
            wire_start = wire.mark() if wire is not None else None
            evidence["query_start_ns"] = time.monotonic_ns()
            stream = query_session.query(dataset).with_txn(None).execute(on_error=ErrorStrategy.IN_STREAM)
            native = stream._closeable  # Observe only; do not replace/iterate/cancel the native producer.
            evidence["native_type"] = type(native).__module__ + "." + type(native).__name__
            try:
                for row in stream:
                    evidence["query_rows"].append(copy_row(row))
                evidence["terminal"] = "NORMAL_EOF"
            except Exception as exc:
                evidence["terminal"] = {"type": type(exc).__name__,
                                        "code": str(getattr(exc, "result_code", None)),
                                        "in_doubt": getattr(exc, "in_doubt", None)}
            finally:
                # The SDK's generator finally already closes native on EOS.
                evidence["native_active_after_iteration"] = native.active
                cursor = native.partition_filter_sync()
                if cursor is not None:
                    partitions = cursor.partitions
                    evidence["native_cursor"] = {
                        "begin": cursor.begin, "count": cursor.count, "done": cursor.done(),
                        "partitions": len(partitions) if partitions is not None else None,
                        "retry_count": sum(partition.retry for partition in partitions) if partitions else 0}
                stream.close()
            evidence["query_end_ns"] = time.monotonic_ns()
            if wire is not None:
                # Observe late bytes too; never replay/retry or change the query.
                time.sleep(.2)
                evidence["wire"] = wire.snapshot(wire_start)
                assert not evidence["wire"]["decoder_errors"], "Wire observer decoder failed"
            evidence.update(after=point_batch(point_session, known_keys),
                            server_after=info(point_session), end_ns=time.monotonic_ns())
            canonical = lambda rows: {row["digest"]: (row.get("hash"), row.get("generation")) for row in rows}
            evidence["point_truth_stable"] = canonical(evidence["before"]) == canonical(evidence["after"]) == canonical(allocated_present)
            evidence["complete"] = canonical(evidence["query_rows"]) == canonical(allocated_present)
            final_status = _Journal.status(request["journal"])
            evidence["gate_sequence"] = final_status["sequence"]
            assert final_status == status
            observe("minimal_sdk_query", evidence)
            assert evidence["point_truth_stable"] and evidence["complete"] and evidence["terminal"] == "NORMAL_EOF", (
                "Public SDK-only query omitted independently known-present rows; stop and preserve evidence")
    return {"cases": ["enum_minimal"], "repetitions": 30}
