"""Explicit offline CLI: genuine installed SDK factories, never execute/connect.

The provider factory/policy methods are compiled verbatim from its AST so this
signature-only probe also runs in an SDK-only release environment without core
numpy dependencies. It is not a server or full provider-contract test.
"""

import ast
from dataclasses import fields
from datetime import timedelta
from importlib import import_module, metadata
import json
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlparse
import uuid

from aerospike_sdk import DataSet
from aerospike_sdk.policy import Behavior, Settings
from aerospike_sdk.sync.client import SyncClient


class NoTransport:
    def __getattr__(self, name):
        raise AssertionError("Unexpected transport access: " + name)


def main():
    attempts = []

    def audit(event, args):
        if event in ("socket.connect", "socket.sendto", "subprocess.Popen"):
            attempts.append(event)
            raise AssertionError("Offline builder probe attempted IO")

    sys.addaudithook(audit)
    source = Path(__file__).resolve().parents[3] / (
        "gptcache/manager/scalar_data/aerospike_storage.py")
    tree = ast.parse(source.read_text())
    names = {"_module_owned", "_json", "AerospikeStorageError"}
    nodes = [n for n in tree.body if getattr(n, "name", None) in names]
    nodes.extend(n for n in tree.body if isinstance(n, ast.Assign) and
                 any(isinstance(t, ast.Name) and t.id == "MAX_ID" for t in n.targets))
    provider = next(n for n in tree.body if getattr(n, "name", None) == "AerospikeStorage")
    methods = {"_open", "_configure", "_check_behavior", "_create_session",
               "_query", "_write", "_and", "_eq", "_key", "_dataset", "_guard", "_range"}
    provider.body = [n for n in provider.body if getattr(n, "name", None) in methods]
    provider.bases = []
    nodes.append(provider)
    scope = dict(globals())
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), scope)
    storage = scope["AerospikeStorage"]()
    storage._closed, storage._behavior = False, None
    storage.namespace, storage.cache_scope = "test", "offline-builders"
    caller = Behavior.DEFAULT.derive_with_changes(
        "gptcache_offline_caller_" + uuid.uuid4().hex,
        all=Settings(total_timeout=timedelta(seconds=3)))
    storage._caller = caller
    storage._boot = str(uuid.uuid4())
    storage._configure()
    client = SyncClient("127.0.0.1:1")
    # This is the sole transport boundary double; SDK Session/builders are real.
    client._client, client._connected = NoTransport(), True
    storage._cluster = client
    session = storage._create_session()
    storage._session = session
    assert session.behavior is storage._behavior
    before = storage._behavior
    storage._configure()
    assert storage._behavior is before and storage._session is session
    dataset = DataSet.of("test", "gptc_q")
    key, second = dataset.id("first"), dataset.id("second")
    predicate = storage._eq("scope", storage.cache_scope)
    assert storage._and(predicate) is predicate
    for target in (key, dataset):
        assert storage._query(target).where(predicate) is not None
    for verb in ("insert", "update"):
        first = storage._write(verb, key, predicate).put({"state": 0})
        assert first is not None
        chained = getattr(first, verb)(second).never_expire().with_txn(None).put({"state": -1})
        assert chained is not None
    root = storage._write("insert", storage._key("root")).put(
        dict(fmt=1, scope=storage.cache_scope, boot=storage._boot, bind="a" * 64, phase=0))
    reservation = storage._write("update", storage._key("qid"), storage._and(
        storage._guard("qid"), storage._range("hi", 0, (1 << 63) - 2))).add("hi", 1).get("hi")
    touch = storage._write("update", storage._key("q", 1), storage._and(
        storage._guard("q", 1), storage._eq("state", 0)))
    for name in ("fmt", "scope", "qid", "state", "payload", "create_on", "last_access"):
        touch = touch.get(name)
    touch = touch.put({"last_access": "2026-11-06T01:02:03.000004"})
    assert all(builder is not None for builder in (root, reservation, touch))
    try:
        session.query(None)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("SDK accepted old keyless seed")
    assert not attempts
    print(json.dumps({"sdk": metadata.version("aerospike-sdk"),
                      "point_and_set_queries": 2, "single_write_factories": 5,
                      "two_segment_write_chains": 2, "sessions": 1,
                      "root_reservation_touch_builders": 3,
                      "connects": 0, "executes": 0, "io_attempts": attempts}))


if __name__ == "__main__":
    main()
