"""Bounded quiescent diagnostic, never a provider fallback or SDK replacement."""

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time


def fingerprint(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, default=lambda item: item.hex()).encode()).hexdigest()


def run(provider, definition, request, observe, safe_failure):
    from aerospike_sdk import ErrorStrategy, ResultCode
    from gptcache.manager.scalar_data.aerospike_operator import _Journal, _process
    from gptcache.manager.scalar_data.base import CacheData

    def gate():
        status = _Journal.status(request["journal"])
        assert status["state"] == "PUBLIC" and not status["launch_blocked"]
        receipt = json.loads((Path(request["journal"]) / "receipt.json").read_text())
        live = []
        for writer in receipt["controller"]["writers"]:
            process = _process(writer["pid"])
            if (process and process["ticks"] == writer["ticks"] and
                    process["state"] not in ("Z", "X")):
                live.append(process["pid"])
        assert live == [os.getpid()], ("Other registered writers", live)
        return {"sequence": status["sequence"], "saved": len(receipt["controller"]["writers"]),
                "live": live}

    def controls(row, bins_object=None):
        record = row.record
        if record is not None and bins_object is None:
            bins_object = record.bins
        result = {"code": str(row.result_code), "in_doubt": row.in_doubt,
                  "exception": type(row.exception).__name__ if row.exception else None,
                  "digest": row.key.digest, "row_identity": id(row),
                  "record_identity": id(record), "bins_identity": id(bins_object) if record else None}
        if record is not None:
            bins = deepcopy(bins_object)
            result.update(generation=record.generation, ttl=record.ttl, hash=fingerprint(bins),
                          bins={name: bins[name] for name in
                                ("fmt", "scope", "boot", "qid", "state", "adm", "hi", "phase")
                                if name in bins})
        return result

    def drain(builder, missing=False):
        """Retain originals AND copies to distinguish aliasing from raw omission."""
        originals, copied = [], []
        result = {"start_ns": time.monotonic_ns()}
        try:
            with builder.with_txn(None).execute(on_error=ErrorStrategy.IN_STREAM) as stream:
                for row in stream:
                    bins_object = row.record.bins if row.record is not None else None
                    originals.append((row, bins_object))
                    copied.append(controls(row, bins_object))
                    assert row.exception is None or (
                        missing and row.result_code == ResultCode.KEY_NOT_FOUND_ERROR
                        and not row.in_doubt)
                    assert not row.in_doubt
                    assert row.is_ok or (missing and row.result_code == ResultCode.KEY_NOT_FOUND_ERROR)
                    if row.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
                        assert row.record is None
                    else:
                        assert row.record is not None
            result["terminal"] = "EOF"
        except Exception as exc:
            result["terminal"] = safe_failure(exc)
        result.update(end_ns=time.monotonic_ns(), rows=copied,
                      alias_changed=[i for i, (row, bins) in enumerate(originals)
                                     if controls(row, bins) != copied[i]])
        if result["terminal"] != "EOF" or result["alias_changed"]:
            observe("enum_drain_failure", result)
        return result

    scopes = request["owned_scopes"]
    assert len(scopes) == 2 and provider.cache_scope in scopes
    # No extra provider (which would run validation or alter usage); only canonical
    # key construction using the actual production method and explicit scope.
    def key_for(scope, kind, identity=None):
        from aerospike_sdk import DataSet
        parts = ["gptc", 1, scope, kind]
        if identity is not None:
            parts.append(identity)
        return DataSet.of(provider.namespace, "gptc_q" if kind == "q" else "gptc_meta").id(
            json.dumps(parts, ensure_ascii=True, allow_nan=False, separators=(",", ":")))

    def point_truth(session):
        result, highs = {}, {}
        for scope in scopes:
            meta_keys = [key_for(scope, name) for name in ("root", "qid")]
            meta = drain(session.query(*meta_keys).include_missing_keys(), missing=True)
            assert meta["terminal"] == "EOF" and len(meta["rows"]) == 2
            by_digest = {row["digest"]: row for row in meta["rows"]}
            assert len(by_digest) == 2
            assert set(by_digest) == {key.digest for key in meta_keys}
            high = by_digest[meta_keys[1].digest]["bins"]["hi"]
            assert type(high) is int and 0 <= high <= 2048, "Diagnostic allocation bound exceeded"
            highs[scope] = {"hi": high, "meta": meta}
            rows = []
            for low in range(1, high + 1, 64):
                keys = [key_for(scope, "q", qid) for qid in range(low, min(high + 1, low + 64))]
                batch = drain(session.query(*keys).include_missing_keys(), missing=True)
                assert batch["terminal"] == "EOF" and not batch["alias_changed"]
                assert len(batch["rows"]) == len(keys)
                assert {row["digest"] for row in batch["rows"]} == {key.digest for key in keys}
                for row in batch["rows"]:
                    if row["code"] == str(ResultCode.OK):
                        bins = row["bins"]
                        assert bins["scope"] == scope
                        assert key_for(scope, "q", bins["qid"]).digest == row["digest"]
                rows.extend(batch["rows"])
            result[scope] = rows
        return {"highs": highs, "rows": result}

    def present(truth):
        return {row["digest"]: (row["hash"], row["generation"])
                for rows in truth["rows"].values() for row in rows
                if row["code"] == str(ResultCode.OK)}

    def server(session):
        info = session.info()
        raw = info.info_on_all_nodes("namespace/" + provider.namespace)
        counters = {}
        for node, values in raw.items():
            fields = dict(item.split("=", 1) for item in
                          values["namespace/" + provider.namespace].split(";") if "=" in item)
            counters[node] = {name: value for name, value in fields.items()
                              if name in ("objects", "tombstones", "client_write_success",
                                          "client_delete_success", "client_udf_complete")
                              or name.startswith("query_")}
        return {"namespace": counters, "sets": info.info_on_all_nodes("sets")}

    # Observe production aggregation boundaries without changing target, policy,
    # iterator, errors or candidate handling.
    original_status, original_scan = provider._status, provider._scan
    scan_events, raw_scan_rows = [], None

    def observed_status(row):
        if raw_scan_rows is not None:
            raw_scan_rows.append(controls(row))
        return original_status(row)

    def observed_scan(kind, *predicates):
        nonlocal raw_scan_rows
        event = {"kind": kind, "start_ns": time.monotonic_ns(), "raw_rows": []}
        raw_scan_rows = event["raw_rows"]
        try:
            rows = original_scan(kind, *predicates)
            event.update(terminal="EOF", returned=[controls(row) for row, _ in rows])
            return rows
        except Exception as exc:
            event["terminal"] = safe_failure(exc)
            raise
        finally:
            raw_scan_rows = None
            event["end_ns"] = time.monotonic_ns()
            scan_events.append(event)

    provider._status, provider._scan = observed_status, observed_scan

    def checkpoint(label):
        from gptcache.manager.scalar_data.aerospike_storage import _BINS
        evidence = {"label": label, "utc": datetime.now(timezone.utc).isoformat(),
                    "start_ns": time.monotonic_ns(), "gate_before": gate()}
        scan_start = len(scan_events)
        problems = []
        # Fresh independent SDK client for each checkpoint; always closed before
        # returning. Same explicit Behavior, no new policies or query hints.
        with definition.connect() as cluster:
            session = cluster.create_session(provider._behavior)
            evidence["server_before"] = server(session)
            evidence["points_before"] = point_truth(session)
            expected = present(evidence["points_before"])
            own = [row for row in evidence["points_before"]["rows"][provider.cache_scope]
                   if row["code"] == str(ResultCode.OK)]
            evidence["queries"] = {}
            for name, client_session, predicate in (
                    ("fresh_unfiltered", session, None),
                    ("provider_unfiltered", provider._session, None),
                    ("fresh_scope", session, provider._eq("scope", provider.cache_scope)),
                    ("provider_scope", provider._session, provider._eq("scope", provider.cache_scope)),
                    ("fresh_projected_scope", session, provider._eq("scope", provider.cache_scope)),
                    ("provider_projected_scope", provider._session, provider._eq("scope", provider.cache_scope)),
                    ("fresh_active", session, provider._and(
                        provider._eq("scope", provider.cache_scope), provider._eq("state", 0))),
                    ("fresh_deleted", session, provider._and(
                        provider._eq("scope", provider.cache_scope), provider._eq("state", -1)))):
                builder = client_session.query(provider._dataset("q"))
                if predicate is not None:
                    builder = builder.where(predicate)
                if "projected" in name:
                    builder = builder.bins(list(_BINS["q"]))
                query = drain(builder)
                evidence["queries"][name] = query
                selected = own if "scope" in name else None
                if "active" in name:
                    selected = [row for row in own if row["bins"]["state"] == 0]
                elif "deleted" in name:
                    selected = [row for row in own if row["bins"]["state"] == -1]
                wanted = expected if selected is None else {
                    row["digest"]: (row["hash"], row["generation"]) for row in selected}
                actual = {row["digest"]: (row.get("hash"), row.get("generation")) for row in query["rows"]}
                if query["terminal"] != "EOF" or query["alias_changed"] or actual != wanted:
                    problems.append({"path": name, "missing": sorted(set(wanted) - set(actual)),
                                     "extra": sorted(set(actual) - set(wanted)),
                                     "changed": sorted(key for key in set(actual) & set(wanted)
                                                       if actual[key] != wanted[key])})
                    # Do not perform more query variants after first mismatch.
                    break
            if not problems:
                scan = provider._scan("q")
                evidence["adapter"] = {
                    "scan": [controls(row) for row, _ in scan],
                    "active": provider.get_ids(False), "deleted": provider.get_ids(True),
                    "count": provider.count(), "all_count": provider.count(is_all=True)}
                adapter = evidence["adapter"]
                active = sorted(row["bins"]["qid"] for row in own if row["bins"]["state"] == 0)
                deleted = sorted(row["bins"]["qid"] for row in own if row["bins"]["state"] == -1)
                if (sorted(adapter["active"]) != active or sorted(adapter["deleted"]) != deleted
                        or adapter["count"] != len(active) or adapter["all_count"] != len(own)
                        or {row["digest"] for row in adapter["scan"]} != {row["digest"] for row in own}):
                    problems.append({"path": "adapter"})
            evidence["points_after"] = point_truth(session)
            evidence["server_after"] = server(session)
            evidence["sdk_metrics_enabled"] = cluster.metrics_enabled()
            evidence["sdk_metrics"] = cluster.metrics().to_dict()
            if present(evidence["points_after"]) != expected:
                problems.append({"path": "point_truth_changed"})
            # Server set object count is an independent physical-total corroborator.
            for node, values in evidence["server_after"]["sets"].items():
                for entry in values["sets"].split(";"):
                    fields = dict(item.split("=", 1) for item in entry.split(":") if "=" in item)
                    if fields.get("ns") == provider.namespace and fields.get("set") == "gptc_q":
                        if int(fields["objects"]) != len(expected):
                            problems.append({"path": "server_set_total", "node": node})
        evidence.update(end_ns=time.monotonic_ns(), gate_after=gate(), problems=problems,
                        readonly_client_closed=True, adapter_scan_events=scan_events[scan_start:])
        observe("enum_checkpoint", evidence)
        assert not problems, "Quiescent enumeration diagnostic mismatch; preserve first evidence"
        return evidence

    if request.get("read_only"):
        for repetition in range(30):
            checkpoint("readonly/" + str(repetition))
        return {"cases": ["enum_readonly_diagnostic"], "checkpoints": 30}
    checkpoint("baseline")
    # Prespecified nine rounds, no retry or cleanup on failure (retain evidence).
    for size in (2, 16, 64):
        for repetition in range(3):
            label = str(size) + "/" + str(repetition)
            ids = provider.batch_insert([CacheData("enum-diagnostic-" + label, ["owned"])
                                         for _ in range(size)])
            observe("enum_insert_ack", {"ids": ids, "monotonic_ns": time.monotonic_ns()})
            assert len(ids) == size and len(set(ids)) == size
            checkpoint(label + "/insert")
            provider.mark_deleted(ids)
            observe("enum_mark_ack", {"ids": ids, "monotonic_ns": time.monotonic_ns()})
            checkpoint(label + "/mark")
            provider.clear_deleted_data()
            observe("enum_purge_return", {"ids": ids, "monotonic_ns": time.monotonic_ns()})
            after = checkpoint(label + "/purge")
            allocated = {row["digest"]: row for row in after["points_after"]["rows"][provider.cache_scope]}
            assert all(allocated[provider._key("q", qid).digest]["code"] ==
                       str(ResultCode.KEY_NOT_FOUND_ERROR) for qid in ids), "Purge left known parent"
    return {"cases": ["enum_diagnostic"], "rounds": 9, "checkpoints": 28}
