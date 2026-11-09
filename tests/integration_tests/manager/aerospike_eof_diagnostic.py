"""One read-only EOF attribution on retained records; no seed/cleanup/recount."""

import hashlib
from importlib import metadata
import json
from pathlib import Path
import sys
import time

from aerospike_stream_fault import StreamFaultProxy


def verify_retained(provider, request, observe):
    from aerospike_sdk import ErrorStrategy

    expected = request["expected_records"]
    ids = sorted(int(key) for key in expected)
    assert ids == list(range(542, 798)) and len(expected) == 256
    actual = {}
    for low in range(0, len(ids), 64):
        batch = ids[low:low + 64]
        keys = [provider._key("q", qid) for qid in batch]
        rows = list(provider._session.query(*keys).with_txn(None).include_missing_keys().execute(
            on_error=ErrorStrategy.IN_STREAM))
        assert len(rows) == len(keys)
        assert {row.key.digest for row in rows} == {key.digest for key in keys}
        for row in rows:
            assert row.is_ok and row.exception is None and not row.in_doubt
            assert row.record is not None and row.record.ttl is None
            bins = row.record.bins
            assert bins["scope"] == provider.cache_scope
            qid = str(bins["qid"])
            actual[qid] = dict(
                digest=row.key.digest, generation=row.record.generation,
                state=bins["state"], adm=bins["adm"],
                hash=hashlib.sha256(json.dumps(
                    bins, sort_keys=True, default=lambda value: value.hex()).encode()).hexdigest())
    observe("retained_eof_points", dict(records=actual, unchanged=actual == expected))
    assert actual == expected
    return dict(readonly=True, retained_count=256, original_hashes_generations_unchanged=True)


def run(provider, definition, request, observe, inspect_point):
    from aerospike_sdk import ErrorStrategy, ResultCode
    from aerospike_sdk.policy import SystemSettings, TransactionSettings
    from aerospike_sdk.sync import ClusterDefinition
    from gptcache.manager.scalar_data.aerospike_storage import _decode_data

    native = Path(sys.modules["aerospike_async._aerospike_async_native"].__file__).resolve()
    provenance = dict(
        path=str(native), sha256=hashlib.sha256(native.read_bytes()).hexdigest(),
        sdk_version=metadata.version("aerospike-sdk"),
        native_version=metadata.version("aerospike-async"))
    assert provenance["sha256"] == (
        "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d")
    assert "/enumfix-venv/" in str(native)
    observe("eof_native_provenance", provenance)
    before_meta = {kind: inspect_point(provider, kind) for kind in ("root", "qid", "sid", "rid")}
    high = before_meta["qid"]["hi"]
    assert type(high) is int and 0 < high <= 1024
    keys = [provider._key("q", qid) for qid in range(1, high + 1)]
    # A separate direct SDK connection supplies primary-key truth, not scans.
    with definition.connect() as cluster:
        point = cluster.create_session(provider._behavior)

        def inventory():
            result, retained = {}, {}
            for low in range(0, len(keys), 64):
                batch = keys[low:low + 64]
                rows = list(point.query(*batch).with_txn(None).include_missing_keys().execute(
                    on_error=ErrorStrategy.IN_STREAM))
                assert len(rows) == len(batch)
                assert {row.key.digest for row in rows} == {key.digest for key in batch}
                for row in rows:
                    assert not row.in_doubt
                    if row.result_code == ResultCode.KEY_NOT_FOUND_ERROR:
                        assert row.record is None
                        assert row.exception is None or (
                            row.exception.result_code == ResultCode.KEY_NOT_FOUND_ERROR)
                        continue
                    assert row.is_ok and row.exception is None and row.record is not None
                    assert row.record.ttl is None
                    bins = row.record.bins
                    assert bins["scope"] == provider.cache_scope and bins["fmt"] == 1
                    qid = bins["qid"]
                    assert row.key.digest == provider._key("q", qid).digest
                    result[qid] = dict(
                        digest=row.key.digest, generation=row.record.generation,
                        state=bins["state"], adm=bins["adm"],
                        hash=hashlib.sha256(json.dumps(
                            bins, sort_keys=True, default=lambda value: value.hex()).encode()).hexdigest())
                    question = _decode_data(bins["payload"])[0]
                    if isinstance(question, str) and question.startswith("fault-prefix-"):
                        retained[qid] = hashlib.sha256(question.encode()).hexdigest()
            return result, retained

        before, retained = inventory()
        assert len(retained) == 256 and len(set(retained.values())) == 1
        assert all(before[qid]["state"] == before[qid]["adm"] == 0 for qid in retained)
        observe("eof_point_truth_before", dict(
            high=high, metadata=before_meta, records=before, retained_ids=sorted(retained),
            retained_token_sha256=next(iter(retained.values()))))
        port = request["public_port"]
        proxied = ClusterDefinition("127.0.0.2", port).with_ip_map(
            {"127.0.0.1": "127.0.0.2"}).using_services_alternate(False).with_strict_config()
        proxied.with_system_settings(SystemSettings(transactions=TransactionSettings(
            implicit_batch_write_transactions=False)))
        outcome = dict(expected_physical_count=len(before), caller_query_max_retries=0)
        with StreamFaultProxy(port, "eof") as wire:
            observed = type(provider)(
                cluster_definition=proxied, namespace=provider.namespace,
                cache_scope=provider.cache_scope, behavior=provider._behavior)
            try:
                observed.create()
                observed.throttle = True
                wire.arm()
                start = time.monotonic()
                try:
                    outcome.update(returned_count=observed.count(is_all=True), terminal="NORMAL_EOF")
                except Exception as exc:
                    outcome.update(terminal="EXCEPTION", type=type(exc).__name__,
                                   causes=[type(error).__name__
                                           for error in getattr(exc, "outcomes", ())])
                outcome.update(seconds=time.monotonic() - start,
                               delivered_rows=observed.observed_rows, prefix=wire.prefix)
                observe("eof_actual_outcome", outcome)
                observe("eof_wire", wire.snapshot())
                assert wire.prefix is not None and not wire.errors
            finally:
                observed.close()
        after, retained_after = inventory()
        after_meta = {kind: inspect_point(provider, kind) for kind in ("root", "qid", "sid", "rid")}
        observe("eof_point_truth_after", dict(records=after, metadata=after_meta))
        assert before == after and retained == retained_after and before_meta == after_meta
        observe("eof_readonly_proof", dict(
            records_unchanged=True, metadata_unchanged=True, seeded=False, cleanup=False,
            explicit_application_queries=1))
        return dict(outcome=outcome, retained_ids=sorted(retained), readonly=True,
                    contract_qualified=False, cases=["eof_diagnostic_only"])
