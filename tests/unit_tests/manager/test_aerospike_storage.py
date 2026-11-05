"""Offline plumbing tests: source-shaped SDK doubles are not live evidence."""

import ast
from collections import deque
from dataclasses import dataclass, fields, replace
from datetime import timedelta
from enum import Enum
import hashlib
import importlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import types
import textwrap
from unittest.mock import Mock

import numpy as np
import pytest


class TestLiveLauncherStructure:
    @pytest.mark.parametrize("node", ["BB9000000000009", "BB900000000000A"])
    def test_retention_expected_node_is_bound_to_pinned_deployment(self, tmp_path, node):
        root = Path(__file__).resolve().parents[3]
        spec = importlib.util.spec_from_file_location(
            "live_worker_node_probe", root / "tests/integration_tests/manager/aerospike_live_worker.py")
        worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(worker)
        config = tmp_path / "public.conf"
        raw = ("service {\n    node-id " + node + "\n}\n").encode("ascii")
        config.write_bytes(raw)
        manifest = {"configs": {"public": {
            "path": str(config), "sha256": hashlib.sha256(raw).hexdigest()}}}
        assert worker.configured_node(manifest) == node
        config.write_bytes(raw + b"\n")
        with pytest.raises(AssertionError):
            worker.configured_node(manifest)

    def test_schedule_releases_acknowledged_admission_even_when_observer_fails(self):
        root = Path(__file__).resolve().parents[3]
        tree = ast.parse((root / "tests/integration_tests/manager/test_aerospike_storage.py").read_text())
        scenario = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "test_concurrent_mutation_admitted_append_mark_get_purge")
        settlement = next(node for node in ast.walk(scenario)
                          if isinstance(node, ast.Try) and node.finalbody)
        release, joined = settlement.finalbody
        assert isinstance(release, ast.Expr) and release.value.func.attr == "write_text"
        assert isinstance(joined, ast.Assign) and joined.value.func.attr == "result"
        assert isinstance(joined.value.func.value, ast.Name) and joined.value.func.value.id == "append"
        assert release.lineno < joined.lineno

    def test_worker_releases_shared_lock_before_native_client_construction(self):
        root = Path(__file__).resolve().parents[3]
        tree = ast.parse((root / "tests/integration_tests/manager/aerospike_live_worker.py").read_text())
        main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main")
        locks = [node for node in main.body if isinstance(node, ast.With)]
        assert len(locks) == 1
        lock = locks[0]
        calls = [node.func for node in ast.walk(lock) if isinstance(node, ast.Call)]
        assert not any(isinstance(node, ast.Name) and node.id in (
            "ClusterDefinition", "AerospikeStorage", "storage_type") for node in calls)
        admitted = next(node for node in main.body if isinstance(node, ast.Expr)
                        and isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Name) and node.value.func.id == "print")
        definition = next(node for node in main.body if isinstance(node, ast.Assign)
                          and any(isinstance(name, ast.Name) and name.id == "definition"
                                  for name in node.targets))
        assert lock.end_lineno < admitted.lineno < definition.lineno

    def test_parent_serializes_only_admission_not_database_work(self):
        root = Path(__file__).resolve().parents[3]
        tree = ast.parse((root / "tests/integration_tests/manager/conftest.py").read_text())
        fixture = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                       and node.name == "aerospike_worker")
        run = next(node for node in fixture.body if isinstance(node, ast.FunctionDef) and node.name == "run")
        admission = next(node for node in ast.walk(run) if isinstance(node, ast.With)
                         and isinstance(node.items[0].context_expr, ast.Name)
                         and node.items[0].context_expr.id == "admission_lock")
        communicates = [node for node in ast.walk(run) if isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute) and node.func.attr == "communicate"]
        assert communicates and all(node.lineno > admission.end_lineno for node in communicates)


@pytest.fixture(autouse=True)
def no_network_or_installs(monkeypatch):
    # Host imports have auto-install paths; fail before any subprocess can run.
    monkeypatch.setattr(subprocess, "check_call", Mock(side_effect=AssertionError("installer")))
    monkeypatch.setattr(socket.socket, "connect", Mock(side_effect=AssertionError("network")))


@pytest.fixture
def sdk(monkeypatch):
    if sys.version_info < (3, 11):
        pytest.skip("Optional Aerospike runtime tests require Python >=3.11")

    class ClusterDefinition:
        def __init__(self):
            self.cluster = types.SimpleNamespace(
                create_session=Mock(return_value=object()), close=Mock()
            )
            self.connect = Mock(return_value=self.cluster)

    package = types.ModuleType("aerospike_sdk")
    package.__path__ = []
    sync = types.ModuleType("aerospike_sdk.sync")
    sync.ClusterDefinition = ClusterDefinition
    monkeypatch.setitem(sys.modules, "aerospike_sdk", package)
    monkeypatch.setitem(sys.modules, "aerospike_sdk.sync", sync)
    _install_sdk_policy(monkeypatch)
    definition = ClusterDefinition()
    session = Session(Behavior.DEFAULT)
    definition.cluster.create_session.return_value = session

    def create_session(behavior=None):
        session.behavior = behavior if behavior is not None else Behavior.DEFAULT
        return session

    definition.cluster.create_session.side_effect = create_session
    return definition


def select(builder, **kwargs):
    from gptcache.manager import CacheBase

    return CacheBase(
        "aerospike", cluster_definition=builder,
        namespace=kwargs.pop("namespace", "explicit-ns"),
        cache_scope=kwargs.pop("cache_scope", "explicit-scope"), **kwargs
    )


OPERATIONS = [
    ("create", ()), ("batch_insert", ([],)), ("get_data_by_id", (1,)),
    ("mark_deleted", ([1],)), ("clear_deleted_data", ()), ("get_ids", ()),
    ("count", ()), ("flush", ()), ("add_session", (1, "s", "q")),
    ("list_sessions", ("s", 1)), ("delete_session", ([1],)),
    ("report_cache", ("q", "cq", 1, "a", 1.0, 0.1)),
]


SUBPROCESS_GUARD = """
import importlib.abc
import socket
import subprocess
import sys
import types
import platform
platform.uname()
def forbidden(*args, **kwargs):
    raise AssertionError("network or runtime installation attempted")
subprocess.check_call = forbidden
subprocess.Popen = forbidden
socket.socket.connect = forbidden
attempts = []
class BlockSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "aerospike_sdk" or fullname.startswith("aerospike_sdk."):
            attempts.append(fullname)
            raise ModuleNotFoundError("blocked SDK", name="aerospike_sdk")
sys.meta_path.insert(0, BlockSDK())
"""


def fresh_process(code):
    result = subprocess.run(
        [sys.executable, "-c", SUBPROCESS_GUARD + textwrap.dedent(code)],
        cwd=str(Path(__file__).resolve().parents[3]),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


class TestOptionalSelection:
    def test_fresh_core_and_other_selection_do_not_load_sdk(self):
        fresh_process("""
            import gptcache
            from gptcache.manager import CacheBase
            from gptcache.manager.vector_data.aerospike import AerospikeVectorStore
            storage = CacheBase("sqlite", sql_url="sqlite:///:memory:")
            storage.create()
            assert storage.count() == 0
            storage.close()
            assert not attempts
            assert not any(n.startswith("aerospike_sdk") for n in sys.modules)
        """)

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="Optional SDK requires Python >=3.11")
    def test_fresh_missing_sdk_is_actionable_without_installer(self):
        fresh_process("""
            from gptcache.manager import CacheBase
            try:
                CacheBase("aerospike", cluster_definition=object(),
                          namespace="n", cache_scope="s")
            except ModuleNotFoundError as exc:
                assert exc.name == "aerospike_sdk"
                assert "Install it separately" in str(exc)
                assert isinstance(exc.__cause__, ModuleNotFoundError)
            else:
                raise AssertionError("missing SDK accepted")
            assert attempts == ["aerospike_sdk"]
        """)

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="Optional SDK requires Python >=3.11")
    def test_fresh_transitive_import_failure_is_truthful(self):
        fresh_process("""
            sys.meta_path.pop(0)
            error = ModuleNotFoundError("missing transport", name="aerospike_async")
            class MissingTransport(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "aerospike_sdk":
                        raise error
            sys.meta_path.insert(0, MissingTransport())
            from gptcache.manager import CacheBase
            try:
                CacheBase("aerospike", cluster_definition=object(),
                          namespace="n", cache_scope="s")
            except ModuleNotFoundError as exc:
                assert exc is error
                assert exc.name == "aerospike_async"
                assert "Install it separately" not in str(exc)
            else:
                raise AssertionError("transitive failure accepted")
        """)

    @pytest.mark.parametrize("minor", [8, 9, 10])
    def test_simulated_floor_rejects_before_sdk_import(self, minor):
        fresh_process("""
            from gptcache.manager import CacheBase
            sys.version_info = (3, MINOR, 0)
            try:
                CacheBase("aerospike", cluster_definition=object(),
                          namespace="n", cache_scope="s")
            except RuntimeError as exc:
                assert "Python >=3.11" in str(exc)
            else:
                raise AssertionError("unsupported runtime accepted")
            assert not attempts
        """.replace("MINOR", str(minor)))

    @pytest.mark.parametrize("field", ["namespace", "cache_scope"])
    @pytest.mark.parametrize("value", [None, "", " \t", 1, b"scope"])
    def test_scope_validation_before_connect(self, sdk, field, value):
        with pytest.raises(ValueError, match=field) as caught:
            select(sdk, **{field: value})
        assert repr(value) not in str(caught.value)
        sdk.connect.assert_not_called()

    def test_required_kwargs_and_sync_builder(self, sdk):
        from gptcache.manager import CacheBase

        for field in ("cluster_definition", "namespace", "cache_scope"):
            params = dict(cluster_definition=sdk, namespace="n", cache_scope="s")
            del params[field]
            with pytest.raises(TypeError):
                CacheBase("aerospike", **params)
        with pytest.raises(TypeError, match="sync SDK ClusterDefinition"):
            select(object())
        sdk.connect.assert_not_called()

    def test_exact_scope_and_builder_ambient_configuration_delegated(self, sdk, monkeypatch):
        # SDK source: cluster_shared.py reads alternate services at construction;
        # sync/cluster_definition.py loads CONFIG_URL at connect. Do not suppress
        # or claim to test the real SDK's configuration loader with this double.
        monkeypatch.setenv("AEROSPIKE_SDK_CONFIG_URL", "/caller/config.yaml")
        monkeypatch.setenv("AEROSPIKE_USE_SERVICES_ALTERNATE", "true")
        original_connect = sdk.connect.side_effect

        def connect():
            assert os.environ["AEROSPIKE_SDK_CONFIG_URL"] == "/caller/config.yaml"
            assert os.environ["AEROSPIKE_USE_SERVICES_ALTERNATE"] == "true"
            return sdk.cluster

        sdk.connect.side_effect = connect
        storage = select(sdk, namespace=" caller-ns ", cache_scope=" caller-scope ")
        assert storage.namespace == " caller-ns "
        assert storage.cache_scope == " caller-scope "
        sdk.connect.assert_called_once_with()
        storage.close()
        sdk.connect.side_effect = original_connect

    def test_python38_grammar_only_and_unchanged_packaging_floor(self):
        root = Path(__file__).resolve().parents[3]
        for relative in (
            "gptcache/manager/scalar_data/manager.py",
            "gptcache/manager/scalar_data/aerospike_storage.py",
            "gptcache/manager/vector_data/manager.py",
            "gptcache/manager/vector_data/aerospike.py",
        ):
            ast.parse((root / relative).read_text(), feature_version=(3, 8))
        tree = ast.parse((root / "setup.py").read_text())
        floors = [
            keyword.value.value for node in ast.walk(tree) if isinstance(node, ast.Call)
            for keyword in node.keywords if keyword.arg == "python_requires"
        ]
        assert floors == [">=3.8.1"]


class TestLifecycleTracer:
    def test_explicit_caller_behavior_is_bound_once_without_default_mutation(self, sdk):
        caller = Behavior()
        default = Behavior.DEFAULT
        storage = select(sdk, behavior=caller)
        try:
            derived = storage._behavior
            assert storage._caller is caller and derived.parent is caller
            assert storage._session.behavior is derived
            storage._configure()
            storage._configure()
            assert storage._behavior is derived
            assert len(caller.calls) == 1 and default.calls == []
            sdk.connect.assert_called_once_with()
            sdk.cluster.create_session.assert_called_once_with(derived)
        finally:
            storage.close()

    def test_invalid_behavior_refuses_before_session_and_closes_owned_cluster(self, sdk):
        with pytest.raises(TypeError, match="public SDK Behavior"):
            select(sdk, behavior=object())
        sdk.cluster.create_session.assert_not_called()
        sdk.cluster.close.assert_called_once_with()

    @pytest.mark.parametrize("deadline", [None, timedelta(0)])
    def test_omitted_behavior_supplies_a_bounded_query_default(self, sdk, monkeypatch, deadline):
        original = Behavior.DEFAULT.get_settings
        monkeypatch.setattr(Behavior.DEFAULT, "get_settings",
                            lambda *args: replace(original(*args), total_timeout=deadline))
        storage = select(sdk)
        try:
            for mode in Mode:
                settings = storage._behavior.get_settings(Kind.READ, Shape.QUERY, mode)
                assert settings.total_timeout == timedelta(seconds=15)
            assert Behavior.DEFAULT.get_settings(Kind.READ, Shape.QUERY, Mode.SC).total_timeout == deadline
        finally:
            storage.close()

    def test_explicit_unbounded_behavior_is_not_silently_replaced(self, sdk, monkeypatch):
        caller = Behavior()
        original = caller.get_settings
        monkeypatch.setattr(caller, "get_settings",
                            lambda *args: replace(original(*args), total_timeout=None))
        with pytest.raises(RuntimeError, match="finite positive"):
            select(sdk, behavior=caller)
        sdk.cluster.create_session.assert_not_called()
        sdk.cluster.close.assert_called_once_with()

    def test_connect_session_and_owned_close(self, sdk):
        storage = select(sdk)
        assert storage.namespace == "explicit-ns"
        assert storage.cache_scope == "explicit-scope"
        assert storage._cluster is sdk.cluster
        assert storage._session is sdk.cluster.create_session.return_value
        sdk.connect.assert_called_once_with()
        sdk.cluster.create_session.assert_called_once_with(storage._behavior)
        assert storage._session.behavior is storage._behavior
        assert storage._caller.parent is Behavior.DEFAULT
        sdk.cluster.close.assert_not_called()
        storage.close()
        storage.close()
        sdk.cluster.close.assert_called_once_with()

    def test_exact_constructor_forwarding(self, monkeypatch):
        module = importlib.import_module("gptcache.manager.scalar_data.aerospike_storage")
        constructor = Mock()
        monkeypatch.setattr(module, "AerospikeStorage", constructor)
        params = dict(cluster_definition=object(), namespace=object(), cache_scope=object())
        from gptcache.manager import CacheBase

        assert CacheBase("aerospike", **params) is constructor.return_value
        constructor.assert_called_once_with(**params)

    def test_factory_boundary_forwarding(self, monkeypatch, tmp_path):
        factory = importlib.import_module("gptcache.manager.factory")
        provider = importlib.import_module("gptcache.manager.scalar_data.aerospike_storage")
        constructor = Mock()
        monkeypatch.setattr(provider, "AerospikeStorage", constructor)
        monkeypatch.setattr(factory, "VectorBase", Mock())
        manager = Mock()
        monkeypatch.setattr(factory, "get_data_manager", manager)
        params = dict(cluster_definition=object(), namespace=object(), cache_scope=object())
        original = params.copy()
        result = factory.manager_factory(
            "aerospike,boundary", data_dir=str(tmp_path), scalar_params=params
        )
        assert result is manager.return_value
        assert params == original
        constructor.assert_called_once_with(**params)
        assert manager.call_args.args[0] is constructor.return_value

    def test_session_failure_cleans_up(self, sdk):
        error = RuntimeError("session failed")
        sdk.cluster.create_session.side_effect = error
        with pytest.raises(RuntimeError) as caught:
            select(sdk)
        assert caught.value is error
        sdk.connect.assert_called_once_with()
        sdk.cluster.close.assert_called_once_with()

    def test_connect_failure_propagates(self, sdk):
        error = ConnectionError("connect failed")
        sdk.connect.side_effect = error
        with pytest.raises(ConnectionError) as caught:
            select(sdk)
        assert caught.value is error
        sdk.cluster.close.assert_not_called()

    @pytest.mark.parametrize("method,args", OPERATIONS)
    def test_closed_methods_remain_an_explicit_boundary(self, sdk, method, args):
        storage = select(sdk)
        storage.close()
        with pytest.raises(RuntimeError, match="closed"):
            getattr(storage, method)(*args)
        sdk.connect.assert_called_once_with()
        sdk.cluster.close.assert_called_once_with()


class Kind(Enum):
    READ = "read"
    WRITE_RETRYABLE = "write_retryable"
    WRITE_NON_RETRYABLE = "write_non_retryable"


class Shape(Enum):
    POINT = "point"
    BATCH = "batch"
    QUERY = "query"


class Mode(Enum):
    AP = "ap"
    SC = "sc"


@dataclass(frozen=True)
class Settings:
    total_timeout: object = None
    socket_timeout: object = None
    max_retries: object = None
    retry_delay: object = None
    read_touch_ttl_percent: object = None
    allow_scans_with_where: object = None
    timeout_delay: object = None
    replica: object = None
    durable_delete: object = None


class Behavior:
    def __init__(self, parent=None, patches=None):
        self.parent = parent
        self.patches = patches or {}
        self.calls = []

    def derive_with_changes(self, name, **patches):
        self.calls.append((name, patches))
        return Behavior(self, patches)

    def get_settings(self, kind, shape, mode):
        if self.parent is None:
            return Settings(timedelta(milliseconds=2300), timedelta(milliseconds=900), 2,
                            timedelta(milliseconds=11), 50, False,
                            timedelta(milliseconds=37), "caller-consistency", False)
        base = self.parent.get_settings(kind, shape, mode)
        scopes = ["reads"] if kind == Kind.READ else ["writes"]
        if kind == Kind.READ and shape == Shape.QUERY:
            scopes.append("reads_query")
        for scope in scopes:
            patch = self.patches.get(scope, Settings())
            base = replace(base, **{f.name: getattr(patch, f.name) for f in fields(Settings)
                                    if getattr(patch, f.name) is not None})
        return base


Behavior.DEFAULT = Behavior()


@dataclass(frozen=True)
class Key:
    namespace: str
    set_name: str
    value: object

    @property
    def digest(self):
        return hashlib.sha256((self.namespace + self.set_name + str(self.value)).encode()).digest()


class DataSet:
    def __init__(self, namespace, set_name):
        self.namespace, self.set_name = namespace, set_name

    @classmethod
    def of(cls, namespace, set_name):
        return cls(namespace, set_name)

    def id(self, value):
        return Key(self.namespace, self.set_name, value)


class Exp:
    @staticmethod
    def int_bin(name):
        return ("int_bin", name)

    @staticmethod
    def string_bin(name):
        return ("string_bin", name)

    @staticmethod
    def int_val(value):
        assert type(value) is int
        return ("int_val", value)

    @staticmethod
    def string_val(value):
        assert type(value) is str
        return ("string_val", value)

    @staticmethod
    def eq(left, right):
        return ("eq", left, right)

    @staticmethod
    def ge(left, right):
        return ("ge", left, right)

    @staticmethod
    def le(left, right):
        return ("le", left, right)

    @staticmethod
    def and_(terms):
        assert type(terms) is list
        assert len(terms) >= 2, "Server AND requires at least two operands"
        return ("and", terms)

    @staticmethod
    def or_(terms):
        assert type(terms) is list
        return ("or", terms)


class Code(Enum):
    OK = 0
    KEY_NOT_FOUND_ERROR = 2
    GENERATION_ERROR = 3
    PARAMETER_ERROR = 4
    KEY_EXISTS_ERROR = 5
    SERVER_MEM_ERROR = 8
    TIMEOUT = 9
    RECORD_TOO_BIG = 13
    KEY_BUSY = 14
    FAIL_FORBIDDEN = 22
    FILTERED_OUT = 27
    MRT_BLOCKED = 120
    MRT_VERSION_MISMATCH = 121


class SDKError(Exception):
    def __init__(self, result_code, in_doubt=False):
        self.result_code, self.in_doubt = Code(result_code), in_doubt


class NotFound(SDKError):
    pass


class Filtered(SDKError):
    pass


class Exists(SDKError):
    pass


@dataclass
class Record:
    bins: dict
    ttl: object = None
    generation: int = 7
    slots: tuple = ()


@dataclass
class Row:
    key: Key
    record: object = None
    result_code: object = Code.OK
    in_doubt: bool = False
    index: int = -1
    exception: object = None

    def __post_init__(self):
        self.result_code = Code(self.result_code)

    @property
    def is_ok(self):
        return self.result_code == Code.OK and self.exception is None

    def operation_result(self, index):
        if self.record is None or index >= len(self.record.slots):
            return None
        return self.record.slots[index]


class Fluent:
    """Only inspected public builder methods, with exact list/flag signatures."""

    def __init__(self, session, target, behavior):
        self.session = session
        self.target = target
        self.behavior = behavior
        self.calls = []

    def _call(self, name, *args):
        self.calls.append((name, args))
        return self

    def with_txn(self, txn):
        assert txn is None
        return self._call("with_txn", txn)

    def insert(self, key):
        return self._call("insert", key)

    def update(self, key):
        return self._call("update", key)

    def where(self, expression):
        assert type(expression) is tuple
        return self._call("where", expression)

    def bins(self, names):
        assert type(names) is list
        return self._call("bins", names)

    def get(self, name):
        return self._call("get", name)

    def put(self, values):
        assert type(values) is dict
        return self._call("put", values.copy())

    def add(self, name, amount):
        return self._call("add", name, amount)

    def never_expire(self):
        return self._call("never_expire")

    def include_missing_keys(self):
        return self._call("include_missing_keys")

    def fail_on_filtered_out(self):
        return self._call("fail_on_filtered_out")

    def with_durable_delete(self):
        return self._call("with_durable_delete")

    def delete_record(self):
        return self._call("delete_record")

    def ensure_generation_is(self, value):
        return self._call("ensure_generation_is", value)

    def execute(self, *, on_error):
        assert on_error == "IN_STREAM"
        self.session.executed.append(self)
        if not self.session.results:
            raise AssertionError("Unexpected SDK command: " + str([name for name, _ in self.calls]))
        result = self.session.results.popleft()
        if isinstance(result, Exception):
            raise result
        return iter(result)


class Session:
    """Common public factory signatures, deliberately no keyless/keyword query."""

    def __init__(self, behavior):
        self.behavior = behavior
        self.results, self.executed = deque(), []
        self.configuration = Info()

    def query(self, target, /, *keys):
        assert isinstance(target, (Key, DataSet))
        assert not keys
        return Fluent(self, target, self.behavior)

    def insert(self, key, /):
        assert isinstance(key, Key)
        return Fluent(self, None, self.behavior).insert(key)

    def update(self, key, /):
        assert isinstance(key, Key)
        return Fluent(self, None, self.behavior).update(key)

    def info(self):
        return self.configuration


class Info:
    def __init__(self):
        self.values = {
            "node": {"n": {"node": "NODE1"}},
            "statistics": {"n": {"statistics": "cluster_size=1;objects=0"}},
            "get-config:context=namespace;id=explicit-ns": {
                "n": {"get-config:context=namespace;id=explicit-ns":
                      "default-read-touch-ttl-pct=0;strong-consistency=false"}},
            "sets/explicit-ns": {"n": {"sets/explicit-ns": ""}},
        }
        self.calls = []

    def info_on_all_nodes(self, command):
        self.calls.append(command)
        return self.values[command]


def _install_sdk_policy(monkeypatch):
    monkeypatch.setattr(Behavior, "DEFAULT", Behavior())
    module = importlib.import_module("gptcache.manager.scalar_data.aerospike_storage")
    package = sys.modules["aerospike_sdk"]
    package.__file__ = __file__
    package.DataSet, package.Exp = DataSet, Exp
    package.ErrorStrategy = types.SimpleNamespace(IN_STREAM="IN_STREAM")
    package.ResultCode = Code
    package.RecordNotFoundError, package.FilteredOutError, package.RecordExistsError = NotFound, Filtered, Exists
    package.AerospikeError = SDKError
    transport = types.ModuleType("aerospike_async")
    transport.__file__ = __file__
    transport.Key, transport.FilterExpression = Key, Exp
    transport.AbortStatus = types.SimpleNamespace(
        OK="aborted", ALREADY_ABORTED="already_aborted", ROLL_BACK_ABANDONED="rollback_abandoned")
    transport.CommitStatus = types.SimpleNamespace(
        OK="committed", ALREADY_COMMITTED="already_committed", ROLL_FORWARD_ABANDONED="rollforward_abandoned")
    monkeypatch.setitem(sys.modules, "aerospike_async", transport)
    policy = types.ModuleType("aerospike_sdk.policy")
    policy.Settings, policy.Mode, policy.OpKind, policy.OpShape = Settings, Mode, Kind, Shape
    policy.Behavior = Behavior
    monkeypatch.setitem(sys.modules, policy.__name__, policy)
    monkeypatch.setattr(module.metadata, "distribution", lambda name: types.SimpleNamespace(
        requires=["aerospike-async @ git+https://approved"],
        version="0.9.0a5" if name == "aerospike-sdk" else "0.6.0a6+gptcache.enumfix1",
        files=[__file__], locate_file=lambda path: path, read_text=lambda name: None))
    monkeypatch.setattr(module.metadata, "version", lambda _: "0.6.0a6+gptcache.enumfix1")


@pytest.fixture
def model(monkeypatch, sdk):
    module = importlib.import_module("gptcache.manager.scalar_data.aerospike_storage")
    session = sdk.cluster.create_session.return_value
    storage = select(sdk, behavior=Behavior.DEFAULT)
    storage._configure()
    storage._boot = "00000000-0000-4000-8000-000000000001"
    storage._active = True
    yield storage, session, module
    storage.close()
    assert not session.results, "Unused queued SDK outcomes"


def bins_for(storage, kind, identity=1, **changes):
    bins = dict(fmt=1, scope=storage.cache_scope)
    if kind == "q":
        bins.update(qid=identity, state=0, afmt=1, adm=0, create_on="2026-11-06T01:02:03.000004",
                    last_access="2026-11-06T02:03:04.000005",
                    payload=b'{"v":1,"question":"q","answers":[[0,["s","a"]]],"deps":[],"embedding":null}')
    elif kind == "s":
        bins.update(sid=identity, qid=1, sname="chat", squestion="original")
    elif kind == "root":
        bins.update(boot=storage._boot, bind="a" * 64, phase=1)
    else:
        bins.update(boot=storage._boot, hi=0)
    bins.update(changes)
    return bins


def record_row(storage, kind, identity=None, **changes):
    return Row(storage._key(kind, identity), Record(bins_for(storage, kind, identity, **changes)))


def slots_row(storage, kind, identity, *slots):
    return Row(storage._key(kind, identity), Record({}, slots=slots))


def queue(session, *rows):
    session.results.extend([row] if isinstance(row, Row) else row for row in rows)


def activation(storage, session, **root_changes):
    queue(session, record_row(storage, "root", **root_changes),
          *(record_row(storage, kind) for kind in ("qid", "sid", "rid")))


class TestLazyActivation:
    def test_selected_constructor_stays_inactive_for_sealed_bootstrap(self, sdk):
        storage = select(sdk)
        try:
            assert storage._boot is None and not storage._active
            assert not storage._session.executed
            sdk.connect.assert_called_once_with()
            sdk.cluster.create_session.assert_called_once_with(storage._behavior)
        finally:
            storage.close()

    @pytest.mark.parametrize("method", ["get_ids", "manager"])
    def test_fresh_selection_validates_existing_history_before_first_scan(
            self, model, sdk, monkeypatch, tmp_path, method):
        storage, session, _ = model
        activation(storage, session)
        storage._boot = None
        storage._active = False
        owned_session, owned_behavior = storage._session, storage._behavior
        queue(session, [])
        if method == "get_ids":
            assert storage.get_ids() == []
        else:
            from gptcache.manager import factory
            from gptcache.manager.data_manager import SSDataManager

            monkeypatch.setattr(factory, "VectorBase", lambda **kwargs: Mock())
            storage.close()
            manager = factory.manager_factory(
                "aerospike,interface-double", data_dir=str(tmp_path),
                scalar_params=dict(cluster_definition=sdk, namespace=storage.namespace,
                                   cache_scope=storage.cache_scope))
            assert isinstance(manager, SSDataManager)
            storage = manager.s
        assert storage._active and storage._boot is not None
        assert storage._session is owned_session
        if method == "get_ids":
            assert storage._behavior is owned_behavior
            sdk.connect.assert_called_once_with()
            sdk.cluster.create_session.assert_called_once_with(owned_behavior)
        else:
            storage.close()
        assert len(session.executed) == 5
        assert all(not any(name in ("insert", "update", "put", "add", "delete_record")
                           for name, _ in builder.calls) for builder in session.executed)

    @pytest.mark.parametrize("kind", ["root", "qid", "sid", "rid"])
    @pytest.mark.parametrize("fault", ["missing", "corrupt"])
    def test_fresh_selection_refuses_incomplete_history_without_writes(self, model, kind, fault):
        storage, session, module = model
        for prior in ["root", "qid", "sid", "rid"][:["root", "qid", "sid", "rid"].index(kind)]:
            queue(session, record_row(storage, prior))
        if fault == "missing":
            queue(session, Row(storage._key(kind), result_code=2, exception=NotFound(2)))
        else:
            queue(session, record_row(storage, kind, fmt=2))
        storage._boot = None
        storage._active = False
        with pytest.raises((ValueError, module.AerospikeStorageError)):
            storage.get_ids()
        assert not storage._active
        assert all(not any(name in ("insert", "update", "put", "add", "delete_record")
                           for name, _ in builder.calls) for builder in session.executed)

    def test_lazy_initializing_refusal_is_read_only(self, model):
        storage, session, module = model
        queue(session, record_row(storage, "root", phase=0))
        storage._boot = None
        storage._active = False
        with pytest.raises(module.AerospikeStorageError, match="INITIALIZING"):
            storage.get_ids()
        assert len(session.executed) == 1 and not storage._active

    @pytest.mark.parametrize("kind", ["root", "qid", "sid", "rid"])
    def test_observed_history_loss_cannot_lazily_reattach_or_repair(self, model, kind):
        storage, session, module = model
        boot = storage._boot
        queue(session, Row(storage._key(kind), result_code=2, exception=NotFound(2)))
        with pytest.raises(module.AerospikeStorageError, match="history"):
            storage._read(kind)
        for method in (storage.get_ids, storage.count):
            with pytest.raises(module.AerospikeStorageError, match="ACTIVE"):
                method()
        assert storage._boot == boot and not storage._active
        assert len(session.executed) == 1


class TestModelContract:
    def test_single_predicate_remains_a_valid_typed_expression(self, model):
        storage, _, _ = model
        predicate = storage._eq("scope", storage.cache_scope)
        assert storage._and(predicate) is predicate

    def test_multiple_predicates_use_typed_and(self, model):
        storage, _, _ = model
        first, second = storage._eq("fmt", 1), storage._eq("scope", storage.cache_scope)
        assert storage._and(first, second) == ("and", [first, second])

    def test_empty_predicate_combination_refuses(self, model):
        storage, _, _ = model
        with pytest.raises(ValueError, match="predicate"):
            storage._and()

    def test_create_validates_all_four_records_without_writes(self, model):
        storage, session, _ = model
        activation(storage, session)
        storage.create()
        assert storage._active
        assert len(session.executed) == 4
        assert all(not any(n in ("insert", "update", "put", "add") for n, _ in b.calls)
                   for b in session.executed)
        assert session.configuration.calls == [
            "node", "statistics", "get-config:context=namespace;id=explicit-ns", "sets/explicit-ns", "node"]

    @pytest.mark.parametrize("kind", ["root", "qid", "sid", "rid"])
    def test_missing_history_never_zero_fills(self, model, kind):
        storage, session, module = model
        kinds = ["root", "qid", "sid", "rid"]
        for prior in kinds[:kinds.index(kind)]:
            queue(session, record_row(storage, prior))
        queue(session, Row(storage._key(kind), result_code=2, exception=NotFound(2)))
        with pytest.raises(module.AerospikeStorageError, match="history"):
            storage.create()
        assert not storage._active
        with pytest.raises(module.AerospikeStorageError, match="ACTIVE"):
            storage.get_ids()

    def test_initializing_root_is_operator_only(self, model):
        storage, session, module = model
        queue(session, record_row(storage, "root", phase=0))
        with pytest.raises(module.AerospikeStorageError, match="INITIALIZING"):
            storage.create()
        assert not storage._active

    @pytest.mark.parametrize("command,body", [
        ("node", ""), ("statistics", "cluster_size=2"),
        ("get-config:context=namespace;id=explicit-ns", "default-read-touch-ttl-pct=50"),
        ("get-config:context=namespace;id=explicit-ns", "default-read-touch-ttl-pct=0;default-read-touch-ttl-pct=0"),
        ("get-config:context=namespace;id=explicit-ns", "ERROR:permission"),
        ("statistics", "ERROR:80:permission denied"),
        ("statistics", "FAIL:permission denied"),
        ("statistics", "ERROR"),
        ("statistics", "failed_best_practices=true"),
        ("sets/explicit-ns", "ERROR:80:permission denied"),
        ("sets/explicit-ns", "FAIL:unknown namespace"),
        ("sets/explicit-ns", "ns=explicit-ns:set=gptc_q:default-read-touch-ttl-pct=50;"),
        ("sets/explicit-ns", "ns=explicit-ns:set=gptc_meta;"),
        ("sets/explicit-ns", "ns=wrong:set=gptc_q:default-read-touch-ttl-pct=0;"),
    ])
    def test_configuration_refusal_before_any_record_command(self, model, command, body):
        storage, session, module = model
        session.configuration.values[command]["n"][command] = body
        with pytest.raises(module.AerospikeStorageError):
            storage.create()
        assert not session.executed
        assert not storage._active

    def test_node_coverage_missing_refuses(self, model):
        storage, session, module = model
        session.configuration.values["sets/explicit-ns"] = {"other": {"sets/explicit-ns": ""}}
        with pytest.raises(module.AerospikeStorageError, match="coverage"):
            storage.create()
        assert not session.executed

    def test_statistics_failed_best_practices_is_not_error_envelope(self, model):
        storage, session, _ = model
        session.configuration.values["statistics"]["n"]["statistics"] = (
            "failed_best_practices=true;cluster_size=1;objects=0")
        storage._configuration()
        assert not session.executed

    @pytest.mark.parametrize("override", ["0", "-1", "4294967295"])
    def test_safe_set_override(self, model, override):
        storage, session, _ = model
        command = "sets/explicit-ns"
        session.configuration.values[command]["n"][command] = (
            "ns=explicit-ns:set=gptc_q:default-read-touch-ttl-pct=" + override + ";")
        activation(storage, session)
        storage.create()

    @pytest.mark.parametrize("override", ["-1", "4294967295"])
    def test_disabled_owned_sets_do_not_require_disabled_shared_namespace(self, model, override):
        storage, session, _ = model
        command = "get-config:context=namespace;id=explicit-ns"
        session.configuration.values[command]["n"][command] = (
            "default-read-touch-ttl-pct=50;strong-consistency=true")
        command = "sets/explicit-ns"
        session.configuration.values[command]["n"][command] = ";".join(
            "ns=explicit-ns:set=" + name + ":default-read-touch-ttl-pct=" + override
            for name in ("gptc_q", "gptc_s", "gptc_r", "gptc_meta", "gptc_v"))
        storage._configuration()
        assert storage._sc
        assert not session.executed

    @pytest.mark.parametrize("override", [None, "0", "50"])
    def test_shared_namespace_requires_each_owned_set_to_disable_touching(self, model, override):
        storage, session, module = model
        command = "get-config:context=namespace;id=explicit-ns"
        session.configuration.values[command]["n"][command] = (
            "default-read-touch-ttl-pct=50;strong-consistency=true")
        command = "sets/explicit-ns"
        session.configuration.values[command]["n"][command] = ";".join(
            "ns=explicit-ns:set=" + name + ":default-read-touch-ttl-pct=" + value
            for name, value in (("gptc_q", "-1"), ("gptc_s", "-1"), ("gptc_r", "-1"),
                                ("gptc_meta", "-1"), ("gptc_v", override))
            if value is not None)
        with pytest.raises(module.AerospikeStorageError, match="read touching"):
            storage._configuration()
        assert not session.executed

    def test_behavior_preserves_all_unrelated_caller_cells(self, model):
        storage, _, _ = model
        storage._check_behavior()
        for mode in Mode:
            for kind in Kind:
                for shape in Shape:
                    actual = storage._behavior.get_settings(kind, shape, mode)
                    assert actual.replica == "caller-consistency"
                    assert actual.timeout_delay == timedelta(milliseconds=37)
                    assert actual.retry_delay == timedelta(milliseconds=11)
                    assert actual.total_timeout == timedelta(milliseconds=2300)
                    if kind != Kind.READ:
                        assert actual.max_retries == 0
                    else:
                        assert actual.read_touch_ttl_percent == -1

    @pytest.mark.parametrize("deadline", [None, timedelta(0), timedelta(microseconds=900),
                                         timedelta(milliseconds=2**32), timedelta(microseconds=1100)])
    def test_unsafe_effective_deadline_refuses(self, model, deadline):
        storage, _, module = model
        original = storage._caller.get_settings
        storage._caller.get_settings = lambda *args: replace(original(*args), total_timeout=deadline)
        with pytest.raises(module.AerospikeStorageError, match="deadline|Deadline"):
            storage._check_behavior()

    def test_batch_order_reservation_and_full_input_prevalidation(self, model):
        storage, session, module = model
        data = module.CacheData("q", "a", create_on=object(), last_access=object())
        queue(session, slots_row(storage, "qid", None, None, 8),
              [Row(storage._key("q", 7), index=0), Row(storage._key("q", 8), index=1)])
        assert storage.batch_insert([data, data]) == [7, 8]
        allocator = session.executed[0]
        assert ("add", ("hi", 2)) in allocator.calls
        assert ("get", ("hi",)) in allocator.calls
        for builder in session.executed:
            names = [name for name, _ in builder.calls]
            assert "never_expire" in names and "with_txn" in names
            assert "include_missing_keys" in names and "fail_on_filtered_out" in names
        payload = dict(session.executed[1].calls)["put"][0]
        assert payload["afmt"] == 1 and payload["adm"] == 0
        assert len(payload["create_on"]) == 26
        before = len(session.executed)
        with pytest.raises(ValueError):
            storage.batch_insert([data, module.CacheData("bad", module.Answer(object()))])
        assert len(session.executed) == before
        assert storage.batch_insert([]) == []

    @pytest.mark.parametrize("high", [True, None, 0, -1, 2**63, "1"])
    def test_allocator_raw_slot_not_typed_coercion(self, model, high):
        storage, session, module = model
        queue(session, slots_row(storage, "qid", None, high, high))
        with pytest.raises(ValueError):
            storage.batch_insert([module.CacheData("q", "a")])
        assert len(session.executed) == 1

    def test_allocator_exhaustion_is_guarded_and_diagnosed(self, model):
        storage, session, module = model
        queue(session, Row(storage._key("qid"), result_code=27, exception=Filtered(27)),
              record_row(storage, "qid", hi=module.MAX_ID))
        with pytest.raises(module.AerospikeStorageError, match="exhausted"):
            storage.batch_insert([module.CacheData("q", "a")])
        assert ("where", (storage._and(storage._guard("qid"), storage._range("hi", 0, module.MAX_ID - 1)),)) in session.executed[0].calls

    def test_read_returns_original_slot_six_and_sql_name_rows(self, model):
        storage, session, _ = model
        bins = bins_for(storage, "q")
        old = bins["last_access"]
        queue(session, slots_row(storage, "q", 1, *(bins[n] for n in (
            "fmt", "scope", "qid", "state", "payload", "create_on", "last_access")), None),
              [record_row(storage, "s", 11), record_row(storage, "s", 12)])
        result = storage.get_data_by_id(np.int64(1))
        assert result.last_access.isoformat(timespec="microseconds") == old
        assert result.session_id == [("chat",), ("chat",)]
        assert result.session_id[0].session_id == "chat"
        calls = session.executed[0].calls
        operations = [(name, args) for name, args in calls if name in ("get", "put")]
        assert [args[0] for name, args in operations[:7]] == [
            "fmt", "scope", "qid", "state", "payload", "create_on", "last_access"]
        assert operations[7][0] == "put"

    @pytest.mark.parametrize("deleted", [True, False])
    def test_filtered_get_diagnostic_not_automatic_miss(self, model, deleted):
        storage, session, module = model
        queue(session, Row(storage._key("q", 1), result_code=27, exception=Filtered(27)),
              record_row(storage, "q", 1, state=-1 if deleted else 0))
        if deleted:
            assert storage.get_data_by_id(1) is None
        else:
            with pytest.raises(module.AerospikeStorageError, match="not replayed"):
                storage.get_data_by_id(1)
        assert len(session.executed) == 2

    @pytest.mark.parametrize("value", [True, 2**63, -(2**63) - 1, 1.0, "1"])
    def test_public_id_bounds(self, model, value):
        storage, session, _ = model
        with pytest.raises(ValueError):
            storage.get_data_by_id(value)
        assert not session.executed

    @pytest.mark.parametrize("value", [-1, 0, np.int64(-1), -(2**63)])
    def test_nonpositive_neighbor_ids_are_misses_but_not_valid_mutations(self, model, value):
        storage, session, _ = model
        assert storage.get_data_by_id(value) is None
        with pytest.raises(ValueError):
            storage.mark_deleted([value])
        assert not session.executed

    def test_absent_get(self, model):
        storage, session, _ = model
        queue(session, Row(storage._key("q", 1), result_code=2, exception=NotFound(2)))
        assert storage.get_data_by_id(1) is None

    def test_question_ids_and_counts_first_observed_state(self, model):
        storage, session, _ = model
        rows = [record_row(storage, "q", 2, state=-1), record_row(storage, "q", 1),
                record_row(storage, "q", 2, state=0)]
        queue(session, rows, rows, rows, rows)
        assert storage.get_ids() == [2]
        assert storage.get_ids(False) == [1]
        assert storage.count() == 1
        assert storage.count(is_all=True) == 2
        assert all(("bins", (list(("fmt", "scope", "qid", "state", "payload", "create_on", "last_access", "afmt", "adm")),))
                   in b.calls for b in session.executed)

    def test_mark_is_existing_only_deduplicated_and_no_resurrection(self, model):
        storage, session, _ = model
        queue(session, [Row(storage._key("q", 1), index=0),
                        Row(storage._key("q", 2), result_code=2, index=1)])
        storage.mark_deleted([np.int64(1), 1, 2])
        assert len(session.executed) == 1
        for builder in session.executed:
            assert ("put", ({"state": -1},)) in builder.calls
            assert any(name == "update" for name, _ in builder.calls)

    def test_flush_noop_and_close_owned_idempotently(self, model):
        storage, session, _ = model
        assert storage.flush() is None
        assert not session.executed
        storage.close()
        storage.close()
        with pytest.raises(RuntimeError, match="closed"):
            storage.flush()

    def test_add_session_independent_ids_duplicates_and_single_release(self, model):
        storage, session, _ = model
        for sid in (12, 13):
            queue(session, slots_row(storage, "sid", None, None, sid),
                  slots_row(storage, "q", 1, None, 1), Row(storage._key("s", sid)),
                  slots_row(storage, "q", 1, None, 0))
            assert storage.add_session(np.int64(1), "", "original") is None
        assert len(session.executed) == 8
        for offset in (0, 4):
            calls = session.executed[offset + 1].calls
            assert ("add", ("adm", 1)) in calls
            assert ("get", ("adm",)) in calls
            assert ("add", ("adm", -1)) in session.executed[offset + 3].calls
            bins = dict(session.executed[offset + 2].calls)["put"][0]
            assert bins["qid"] == 1 and bins["sname"] == ""

    @pytest.mark.parametrize("boundary", ["entry", "child", "release"])
    def test_session_unknown_never_replays_or_compensates(self, model, boundary):
        storage, session, module = model
        queue(session, slots_row(storage, "sid", None, None, 5))
        if boundary != "entry":
            queue(session, slots_row(storage, "q", 1, None, 1))
        if boundary == "release":
            queue(session, Row(storage._key("s", 5)))
        session.results.append(TimeoutError("lost reply"))
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.add_session(1, "chat", "q")
        assert caught.value.phase == "UNKNOWN_" + boundary.upper()
        assert caught.value.identity == (1, 5)
        assert len(session.executed) == {"entry": 2, "child": 3, "release": 4}[boundary]

    def test_known_child_rejection_releases_once_but_fails_append(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "sid", None, None, 5),
              slots_row(storage, "q", 1, None, 1),
              Row(storage._key("s", 5), result_code=5, exception=Exists(5)),
              slots_row(storage, "q", 1, None, 0))
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.add_session(1, "chat", "q")
        assert caught.value.phase == "CHILD_REJECTED"
        assert len(session.executed) == 4

    @pytest.mark.parametrize("deleted", [True, False])
    def test_session_missing_or_deleted_parent_never_creates_child(self, model, deleted):
        storage, session, module = model
        queue(session, slots_row(storage, "sid", None, None, 4))
        if deleted:
            queue(session, Row(storage._key("q", 1), result_code=27),
                  record_row(storage, "q", 1, state=-1))
        else:
            queue(session, Row(storage._key("q", 1), result_code=2),
                  Row(storage._key("q", 1), result_code=2))
        with pytest.raises(module.AerospikeStorageError):
            storage.add_session(1, "chat", "original")
        assert not any(name == "insert" for b in session.executed for name, _ in b.calls)

    @pytest.mark.parametrize("name,key,filtered", [
        ("chat", object(), "sname"), ("", np.int64(3), "qid"), (None, 0, None), ("", None, None),
    ])
    def test_sql_name_filter_precedence_and_tuple_dtos(self, model, name, key, filtered):
        storage, session, _ = model
        queue(session, [record_row(storage, "s", 8, qid=3), record_row(storage, "s", 9, qid=3)])
        result = storage.list_sessions(name, key)
        assert result == [(8, 3, "chat", "original"), (9, 3, "chat", "original")]
        assert result[0].question_id == 3
        expression = dict(session.executed[0].calls)["where"][0]
        terms = expression[1] if expression[0] == "and" else [expression]
        assert terms[0] == storage._eq("scope", storage.cache_scope)
        selected = terms[1:]
        assert len(selected) == (0 if filtered is None else 1)
        if filtered:
            assert selected[0][1][1] == filtered

    def test_delete_session_uses_association_id_generation_full_guard_durable(self, model):
        storage, session, _ = model
        queue(session, record_row(storage, "s", 9), Row(storage._key("s", 9)))
        storage.delete_session([np.int64(9), 9])
        assert len(session.executed) == 2
        calls = session.executed[1].calls
        assert ("update", (storage._key("s", 9),)) in calls
        assert ("ensure_generation_is", (7,)) in calls
        assert ("with_durable_delete", ()) in calls
        assert ("delete_record", ()) in calls
        assert len(dict(calls)["where"][0][1]) == 6

    def test_session_generation_failure_is_visible(self, model):
        storage, session, module = model
        queue(session, record_row(storage, "s", 9), Row(storage._key("s", 9), result_code=3))
        with pytest.raises(module.AerospikeStorageError):
            storage.delete_session([9])

    def test_purge_closed_zero_before_full_cleanup_and_guarded_final_delete(self, model):
        storage, session, _ = model
        queue(session, [record_row(storage, "q", 1, state=-1)],
              record_row(storage, "q", 1, state=-1),
              [record_row(storage, "s", 8)], record_row(storage, "s", 8),
              Row(storage._key("s", 8)), [], Row(storage._key("q", 1)))
        assert storage.clear_deleted_data() is None
        final = session.executed[-1]
        assert ("with_durable_delete", ()) in final.calls
        assert ("delete_record", ()) in final.calls
        assert storage._eq("adm", 0) in dict(final.calls)["where"][0][1]

    def test_pending_admission_purge_fails_closed_without_child_query(self, model):
        storage, session, module = model
        queue(session, [record_row(storage, "q", 1, state=-1)],
              record_row(storage, "q", 1, state=-1, adm=1))
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.clear_deleted_data()
        assert "recovery required" in str(caught.value.outcomes[1])
        assert len(session.executed) == 2

    def test_purge_remaining_child_no_final_delete(self, model):
        storage, session, module = model
        queue(session, [record_row(storage, "q", 1, state=-1)],
              record_row(storage, "q", 1, state=-1), [], [record_row(storage, "s", 10)])
        with pytest.raises(module.AerospikeStorageError):
            storage.clear_deleted_data()
        assert not any(name == "delete_record" for b in session.executed for name, _ in b.calls)

    def test_report_preserves_all_six_typed_fields_no_parent_dependency(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "rid", None, None, 12), Row(storage._key("r", 12)))
        assert storage.report_cache("u\ud800", "cached", np.int64(7), {"answer": [b"\0", None]},
                                    -0.0, -3) is None
        bins = dict(session.executed[-1].calls)["put"][0]
        obj = module.AerospikeStorage._decode_report(bins["payload"])
        assert obj["cache_question_id"] == 7
        assert module._unnode(obj["similarity"]).hex() == (-0.0).hex()
        assert module._unnode(obj["cache_delta_time"]) == -3
        assert module._unnode(obj["cache_answer"]) == {"answer": [b"\0", None]}
        assert all(b.target is None for b in session.executed)
        assert bins["rid"] == 12

    @pytest.mark.parametrize("answer,metric", [(object(), 1.0), (None, True), (None, np.float32(1)),
                                               (None, float("nan")), (None, 2**63)])
    def test_bad_report_fails_before_counter(self, model, answer, metric):
        storage, session, _ = model
        with pytest.raises(ValueError):
            storage.report_cache("u", "q", 1, answer, metric, 0.1)
        assert not session.executed

    @pytest.mark.parametrize("problem", ["embedded", "doubt", "correlation", "index", "empty", "duplicate"])
    def test_point_error_rows_never_become_cache_miss(self, model, problem):
        storage, session, module = model
        row = Row(storage._key("q", 1), result_code=2)
        if problem == "embedded":
            row.result_code, row.exception = 0, TimeoutError("transport")
        elif problem == "doubt":
            row.in_doubt = True
        elif problem == "correlation":
            row.key = storage._key("q", 2)
        elif problem == "index":
            row.index = 5
        queue(session, [] if problem == "empty" else [row, row] if problem == "duplicate" else row)
        with pytest.raises(module.AerospikeStorageError):
            storage.get_data_by_id(1)

    @pytest.mark.parametrize("problem", ["scope", "namespace", "set", "digest", "control", "version", "ttl"])
    def test_query_validation_before_dedup(self, model, problem):
        storage, session, module = model
        valid = record_row(storage, "q", 1)
        bad = record_row(storage, "q", 1)
        if problem == "scope":
            bad.record.bins["scope"] = "other"
        elif problem in ("namespace", "set", "digest"):
            bad.key = (Key("other", "gptc_q", bad.key.value) if problem == "namespace" else
                       Key(storage.namespace, "other", bad.key.value) if problem == "set" else
                       types.SimpleNamespace(namespace=storage.namespace, set_name="gptc_q", digest=None))
        elif problem == "control":
            bad.record.bins["adm"] = True
        elif problem == "version":
            bad.record.bins["fmt"] = 2
        else:
            bad.record.ttl = 12
        queue(session, [valid, bad])
        with pytest.raises(module.AerospikeStorageError):
            storage.count(is_all=True)

    def test_stream_fully_drained_after_error_and_terminal_error_exposed(self, model):
        storage, session, module = model
        visited = []
        def delivery():
            yield Row(storage._key("q", 1), exception=TimeoutError("transport"))
            visited.append("drained")
            yield record_row(storage, "q", 2)
            raise TimeoutError("terminal")
        session.results.append(delivery())
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.get_ids()
        assert visited == ["drained"]
        assert len(caught.value.outcomes) == 2

    def test_batch_child_failures_drain_and_preserve_each_input(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "qid", None, None, 2),
              [Row(storage._key("q", 1), index=0),
               Row(storage._key("q", 2), index=1, result_code=5, exception=Exists(5))])
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.batch_insert([module.CacheData("one", "a"), module.CacheData("two", "b")])
        assert caught.value.outcomes[0]["payload"] == "ACKNOWLEDGED"
        assert caught.value.outcomes[1]["qid"] == 2
        assert len(session.executed) == 2
        batch = session.executed[-1]
        assert len([1 for name, _ in batch.calls if name == "never_expire"]) == 2
        puts = [args[0] for name, args in batch.calls if name == "put"]
        assert [json.loads(bins["payload"])["question"] for bins in puts] == ["one", "two"]

    def test_batch_terminal_stream_preserves_known_prefix(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "qid", None, None, 2))
        def delivery():
            yield Row(storage._key("q", 1), index=0)
            raise TimeoutError("terminal")
        session.results.append(delivery())
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.batch_insert([module.CacheData("one", "a"), module.CacheData("two", "b")])
        assert caught.value.outcomes[0]["payload"] == "ACKNOWLEDGED"
        assert caught.value.outcomes[1]["payload"] == "UNKNOWN"

    def test_initial_truthy_association_only_after_payload_ack(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "qid", None, None, 3), Row(storage._key("q", 3)),
              slots_row(storage, "sid", None, None, 7), slots_row(storage, "q", 3, None, 1),
              Row(storage._key("s", 7)), slots_row(storage, "q", 3, None, 0))
        assert storage.batch_insert([module.CacheData(module.Question("original"), "a", session_id="chat")]) == [3]
        bins = [args[0] for name, args in session.executed[4].calls if name == "put"][0]
        assert bins["squestion"] == "original"
        assert bins["qid"] == 3

    def test_no_operator_import_during_ordinary_crud(self, model, monkeypatch):
        storage, session, module = model
        original = module.import_module
        def ordinary_import(name):
            assert name not in (
                "gptcache.manager.scalar_data._aerospike_operator_runtime",
                "gptcache.manager.scalar_data.aerospike_operator")
            return original(name)
        monkeypatch.setattr(module, "import_module", ordinary_import)
        queue(session, [])
        assert storage.count() == 0

    @pytest.mark.parametrize("native_version,accepted", [
        ("0.6.0a6+gptcache.enumfix1", True),
        ("0.6.0a6", False),
        ("0.6.0a6+unapproved", False),
    ])
    def test_private_development_transport_pair_is_explicit(
            self, model, monkeypatch, native_version, accepted):
        storage, session, module = model
        original = module.metadata.distribution
        def distribution(name):
            dist = original(name)
            if name == "aerospike-async":
                dist.version = native_version
            return dist
        monkeypatch.setattr(module.metadata, "distribution", distribution)
        if accepted:
            storage._configure()
        else:
            with pytest.raises(module.AerospikeStorageError, match="Unsupported SDK"):
                storage._configure()

    @pytest.mark.parametrize("problem", ["missing_dependency", "ambiguous_dependency", "version",
                                         "expression_type", "foreign_module"])
    def test_selected_sdk_provenance_refuses_no_fallback(self, model, monkeypatch, problem):
        storage, session, module = model
        original = module.metadata.distribution
        if problem in ("missing_dependency", "ambiguous_dependency", "version"):
            def distribution(name):
                dist = original(name)
                if name == "aerospike-sdk":
                    if problem == "missing_dependency":
                        dist.requires = []
                    elif problem == "ambiguous_dependency":
                        dist.requires.append("aerospike-native>=1.0.0")
                    else:
                        dist.version = "unverified"
                return dist
            monkeypatch.setattr(module.metadata, "distribution", distribution)
        elif problem == "expression_type":
            monkeypatch.setattr(sys.modules["aerospike_async"], "FilterExpression", object)
        else:
            monkeypatch.setattr(sys.modules["aerospike_async"], "__file__", "/foreign/transport.py")
        with pytest.raises(module.AerospikeStorageError):
            storage._configure()
        assert not session.executed


class TestCodec:
    def test_executable_formats_rejected_and_code_looking_data_inert(self, monkeypatch):
        import pickle
        import marshal
        from gptcache.manager.scalar_data import aerospike_storage as module

        def forbidden(*args, **kwargs):
            raise AssertionError("executable deserialization")

        monkeypatch.setattr(pickle, "loads", forbidden)
        monkeypatch.setattr(marshal, "loads", forbidden)
        monkeypatch.setattr(np, "load", forbidden)
        # Protocol-0 GLOBAL/REDUCE is deliberately NOT sent to pickle.loads.
        attack = b"cos\nsystem\n(S'echo unsafe'\ntR."
        with pytest.raises(ValueError):
            module._decode_data(attack)
        text = "__import__('os').system('echo unsafe')"
        payload, _ = module._encode_data(module.CacheData(text, [attack, {"code": text}]))
        question, answers, embedding = module._decode_data(payload)
        assert question == text
        assert [answer.answer for answer in answers] == [attack, {"code": text}]
        assert embedding is None

    def test_empty_collections_and_image_keys_are_not_fetched(self):
        from gptcache.manager.scalar_data import aerospike_storage as module
        # The fresh subprocess/suite transport guards also prohibit fetching.
        for question in ("", module.Question("", [])):
            payload, _ = module._encode_data(module.CacheData(question, []))
            assert module._decode_data(payload) == ("", [], None)
        answers = [module.Answer("https://not-contacted.invalid/image", 2),
                   module.Answer("s3://bucket/\u00e9", 1), module.Answer(None)]
        payload, _ = module._encode_data(module.CacheData("e\u0301", answers))
        question, actual, _ = module._decode_data(payload)
        assert question == "e\u0301" and question != "\u00e9"
        assert actual == answers

    def test_safe_nodes_roundtrip_unicode_bytes_negative_zero_and_order(self):
        from gptcache.manager.scalar_data import aerospike_storage as module
        value = {"unicode": "\ud800\u00e9", "values": [None, True, -(2**63), 2**63 - 1, -0.0, b"\0\xff", [], {}]}
        decoded = module._unnode(json.loads(module._json(module._node(value))))
        assert decoded == value
        assert decoded["values"][4].hex() == (-0.0).hex()
        assert list(decoded) == ["unicode", "values"]

    @pytest.mark.parametrize("value", [(), set(), object(), np.int64(1), np.float32(1),
                                       2**63, -(2**63) - 1, float("inf"), float("nan"), {1: "bad"}])
    def test_unsupported_native_types_never_coerce(self, value):
        from gptcache.manager.scalar_data import aerospike_storage as module
        with pytest.raises(ValueError):
            module._node(value)

    def test_cycle_rejected(self):
        from gptcache.manager.scalar_data import aerospike_storage as module
        value = []
        value.append(value)
        with pytest.raises(ValueError, match="Cyclic"):
            module._node(value)

    @pytest.mark.parametrize("node", [
        [], ["n", None], ["b", 1], ["i", "01"], ["i", "-0"], ["i", str(2**63)],
        ["f", "nan"], ["f", "0x1p+0"], ["y", "a==="], ["y", "YR=="],
        ["unknown", 1], ["d", [["a", ["n"]], ["a", ["n"]]]], ["l", None],
    ])
    def test_malformed_nodes_fail_explicitly(self, node):
        from gptcache.manager.scalar_data import aerospike_storage as module
        with pytest.raises(ValueError):
            module._unnode(node)

    @pytest.mark.parametrize("embedding", [None, np.array([], dtype=np.float64),
                                           np.array([1.25, -2, float("nan")])[::-1]])
    def test_ordered_dtos_and_float32_none_vs_empty(self, embedding):
        from gptcache.manager.scalar_data import aerospike_storage as module
        data = module.CacheData(module.Question("q", [module.QuestionDep("second", "d\u00e9", -1),
                                                    module.QuestionDep("first", "", 2)]),
                                [module.Answer(["one", None], 2), module.Answer(b"", 1)], embedding)
        payload, _ = module._encode_data(data)
        question, answers, actual = module._decode_data(payload)
        assert question == data.question
        assert answers == data.answers
        if embedding is None:
            assert actual is None
        else:
            assert actual.dtype == np.dtype("<f4")
            np.testing.assert_array_equal(actual, embedding.astype("<f4"))

    @pytest.mark.parametrize("payload", [
        b'{"v":1,"v":1,"question":"q","answers":[],"deps":[],"embedding":null}',
        b'{"v":true,"question":"q","answers":[],"deps":[],"embedding":null}',
        b'{"v":1,"question":"q","answers":[],"deps":[],"embedding":{"dtype":"<f4","data":"AA=="}}',
        b'{"v":1,"question":"q","answers":[],"deps":[],"embedding":null,"unknown":0}',
        b'{"v":1,"question":"q","answers":[[true,["n"]]],"deps":[],"embedding":null}',
    ])
    def test_corrupt_payload_never_miss_or_fallback(self, payload):
        from gptcache.manager.scalar_data import aerospike_storage as module
        with pytest.raises(ValueError):
            module._decode_data(payload)

    @pytest.mark.parametrize("value", ["2026-11-06T01:02:03Z", "2026-11-06T01:02:03.000000+00:00",
                                       "2026-02-30T01:02:03.000000", "2026-11-06 01:02:03.000000"])
    def test_timestamp_grammar(self, value):
        from gptcache.manager.scalar_data import aerospike_storage as module
        with pytest.raises(ValueError):
            module._time(value)

    def test_nonvector_embedding_rejected_before_allocation(self, model):
        storage, session, module = model
        with pytest.raises(ValueError, match="one-dimensional"):
            storage.batch_insert([module.CacheData("q", "a", np.zeros((2, 2)))])
        assert not session.executed


class TestScalarContract:
    """Additional public contract gaps; reuse model, not inherited test methods."""

    def test_two_instances_share_allocator_controls_not_local_counters(self, model, sdk):
        storage, session, module = model
        other = select(sdk)
        high = 0
        persisted = {}

        def reserve():
            nonlocal high
            builder = session.executed[-1]
            assert ("update", (storage._key("qid"),)) in builder.calls
            assert ("where", (storage._and(storage._guard("qid"),
                                           storage._range("hi", 0, module.MAX_ID - 1)),)) in builder.calls
            high += 1
            yield slots_row(storage, "qid", None, None, high)

        def append():
            builder = session.executed[-1]
            bins = dict(builder.calls)["put"][0]
            qid = bins["qid"]
            assert qid not in persisted
            persisted[qid] = bins.copy()
            yield Row(storage._key("q", qid))

        try:
            activation(storage, session)
            other.create()  # same root and immutable boot on all three allocators
            for writer in (storage, other):
                session.results.extend((reserve(), append()))
                assert writer.batch_insert([module.CacheData("same", "payload")]) == [high]
            assert high == 2 and list(persisted) == [1, 2]
            assert other._boot == storage._boot
        finally:
            other.close()

    def test_batch_reordered_acknowledgements_return_input_order_and_naive_now(self, model):
        from datetime import datetime
        storage, session, module = model
        before = datetime.now()
        queue(session, slots_row(storage, "qid", None, None, 42),
              [Row(storage._key("q", 42), index=1), Row(storage._key("q", 41), index=0)])
        assert storage.batch_insert([
            module.CacheData("first", [], create_on=object(), last_access=object()),
            module.CacheData("second", []),
        ]) == [41, 42]
        after = datetime.now()
        puts = [args[0] for name, args in session.executed[-1].calls if name == "put"]
        assert [json.loads(b["payload"])["question"] for b in puts] == ["first", "second"]
        for bins in puts:
            for name in ("create_on", "last_access"):
                value = module._time(bins[name])
                assert value.tzinfo is None and before <= value <= after

    def test_unknown_state_counts_are_fixed_first_observation_not_snapshot(self, model):
        storage, session, _ = model
        first = record_row(storage, "q", 1)
        later = record_row(storage, "q", 1, state=-1)
        queue(session, [first, later, record_row(storage, "q", 2, state=-1)],
              [first, later], [later, first])
        assert storage.count(np.int64(77)) == 0
        assert storage.count(is_all=True, state=object()) == 1
        assert storage.get_ids(deleted=True) == [1]

    def test_scope_recipes_cover_every_record_category_without_normalization(self, model):
        storage, _, _ = model
        original = storage.cache_scope
        for kind, identity in (("q", 1), ("s", 1), ("r", 1), ("root", None),
                               ("qid", None), ("sid", None), ("rid", None)):
            one = storage._key(kind, identity)
            storage.cache_scope = original + " "
            other = storage._key(kind, identity)
            storage.cache_scope = original
            assert one.namespace == other.namespace == storage.namespace
            assert one.set_name == other.set_name
            assert one.digest != other.digest
            assert json.loads(one.value)[2] == original
            assert json.loads(other.value)[2] == original + " "

    def test_reports_append_distinct_immutable_rows_with_retention_commands(self, model):
        storage, session, module = model
        for rid in (9, 10):
            queue(session, slots_row(storage, "rid", None, None, rid),
                  Row(storage._key("r", rid)))
            storage.report_cache("u", "q", np.int64(999), b"\xff", 1.0, 0.1)
        appends = session.executed[1::2]
        assert [dict(b.calls)["insert"][0] for b in appends] == [
            storage._key("r", 9), storage._key("r", 10)]
        for builder in appends:
            names = [name for name, _ in builder.calls]
            assert "never_expire" in names and "update" not in names
            bins = dict(builder.calls)["put"][0]
            obj = storage._decode_report(bins["payload"])
            assert obj["cache_question_id"] == 999
            assert module._time(obj["cache_time"]).tzinfo is None
        # This is command intent, not a claim of live server TTL metadata.

    def test_all_input_ids_prevalidated_before_mark_or_association_delete(self, model):
        storage, session, _ = model
        for method in (storage.mark_deleted, storage.delete_session):
            with pytest.raises(ValueError):
                method([np.int64(1), True])
            assert not session.executed
        for args in ((True, "name", "question"), (1, object(), "question"),
                     (1, "name", object())):
            with pytest.raises(ValueError):
                storage.add_session(*args)
            assert not session.executed

    def test_caller_mode_specific_policy_cells_survive_provider_derivation(self, model):
        storage, session, _ = model
        base = storage._caller.get_settings

        def caller(kind, shape, mode):
            return replace(base(kind, shape, mode), replica=mode.value,
                           socket_timeout=timedelta(milliseconds=600 if mode == Mode.AP else 700))

        storage._caller.get_settings = caller
        storage._check_behavior()
        for mode in Mode:
            for shape in Shape:
                effective = storage._behavior.get_settings(Kind.READ, shape, mode)
                assert effective.replica == mode.value
                assert effective.socket_timeout == caller(Kind.READ, shape, mode).socket_timeout
                assert effective.total_timeout == timedelta(milliseconds=2300)
                assert effective.read_touch_ttl_percent == -1
                assert effective.max_retries == (0 if shape == Shape.QUERY else 2)
        # Query wire uses server read-touch=0, established by namespace checks;
        # this is NOT an assertion about transport partition-retry counts.
        assert not session.executed


class TestFailureBoundaries:
    @pytest.mark.parametrize("fault", ["bool_fmt", "wrong_qid", "bad_payload",
                                      "bad_created", "bad_previous", "finite_ttl"])
    def test_acknowledged_touch_corruption_never_returns_dto_or_replays(self, model, fault):
        storage, session, module = model
        bins = bins_for(storage, "q", 1)
        slots = [bins[name] for name in module._BINS["q"][:7]]
        if fault == "bool_fmt":
            slots[0] = True
        elif fault == "wrong_qid":
            slots[2] = 2
        elif fault == "bad_payload":
            slots[4] = b"not-json"
        elif fault == "bad_created":
            slots[5] = "2026-01-01"
        elif fault == "bad_previous":
            slots[6] = None
        row = slots_row(storage, "q", 1, *slots, "must-not-use-write-slot")
        if fault == "finite_ttl":
            row.record.ttl = 12
        queue(session, row)
        with pytest.raises((ValueError, module.AerospikeStorageError)):
            storage.get_data_by_id(1)
        assert len(session.executed) == 1  # no session fetch, diagnostic or retry

    def test_initial_association_failure_preserves_all_payload_and_sid_correlations(self, model):
        storage, session, module = model
        queue(session, slots_row(storage, "qid", None, None, 2),
              [Row(storage._key("q", 2), index=1), Row(storage._key("q", 1), index=0)],
              slots_row(storage, "sid", None, None, 8),
              slots_row(storage, "q", 1, None, 1), Row(storage._key("s", 8)),
              slots_row(storage, "q", 1, None, 0),
              slots_row(storage, "sid", None, None, 9),
              slots_row(storage, "q", 2, None, 1))
        session.results.append(TimeoutError("uncertain second child"))
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.batch_insert([module.CacheData("one", "a", session_id="chat"),
                                  module.CacheData("two", "b", session_id="chat")])
        first, second = caught.value.outcomes
        assert first == dict(position=0, qid=1, payload="ACKNOWLEDGED",
                             association="ACKNOWLEDGED")
        assert second == dict(position=1, qid=2, payload="ACKNOWLEDGED",
                              association="UNKNOWN_CHILD", sid=9)
        assert caught.value.identity == 2
        assert len(session.executed) == 9  # no uncertain child replay/release

    @pytest.mark.parametrize("fault", ["doubt", "exception_ok", "filtered", "collision"])
    def test_report_failure_never_replays_allocation_or_append(self, model, fault):
        storage, session, module = model
        row = Row(storage._key("r", 9))
        if fault == "doubt":
            row.in_doubt = True
        elif fault == "exception_ok":
            row.exception = TimeoutError("client failure with OK code")
        else:
            row.result_code = Code.FILTERED_OUT if fault == "filtered" else Code.KEY_EXISTS_ERROR
            row.exception = Filtered(27) if fault == "filtered" else Exists(5)
        queue(session, slots_row(storage, "rid", None, None, 9), row)
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.report_cache("u", "q", 1, "a", 0.5, 0.1)
        assert caught.value.identity == 9
        assert len(session.executed) == 2
        assert [n for b in session.executed for n, _ in b.calls].count("insert") == 1

    @pytest.mark.parametrize("fault", ["wrong_key", "duplicate_index", "missing_row"])
    def test_batch_correlation_failure_keeps_prefix_and_sends_no_associations(self, model, fault):
        storage, session, module = model
        rows = [Row(storage._key("q", 1), index=0)]
        if fault == "wrong_key":
            rows.append(Row(storage._key("q", 1), index=1))
        elif fault == "duplicate_index":
            rows.append(Row(storage._key("q", 1), index=0))
        queue(session, slots_row(storage, "qid", None, None, 2), rows)
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.batch_insert([module.CacheData("one", "a", session_id="chat"),
                                  module.CacheData("two", "b", session_id="chat")])
        outcomes = caught.value.outcomes
        assert [(o["position"], o["qid"]) for o in outcomes] == [(0, 1), (1, 2)]
        assert outcomes[0]["payload"] == "ACKNOWLEDGED"
        assert outcomes[1]["payload"] != "ACKNOWLEDGED"
        assert all(o["association"] == "NOT_SENT" for o in outcomes)
        assert len(session.executed) == 2

    def test_persisted_subset_is_observed_independently_after_stream_failure(self, model):
        storage, session, module = model
        persisted = {}
        queue(session, slots_row(storage, "qid", None, None, 2))

        def delivery():
            # Explicit simulated storage state, independent from error.outcomes.
            builder = session.executed[-1]
            bins = [args[0] for name, args in builder.calls if name == "put"]
            persisted[1] = bins[0].copy()
            yield Row(storage._key("q", 1), index=0)
            raise TimeoutError("prefix then terminal transport error")

        session.results.append(delivery())
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.batch_insert([module.CacheData("one", "a"), module.CacheData("two", "b")])
        assert list(persisted) == [1]
        assert caught.value.outcomes[0]["qid"] == 1
        assert caught.value.outcomes[0]["payload"] == "ACKNOWLEDGED"
        assert caught.value.outcomes[1]["payload"] == "UNKNOWN"
        queue(session, [Row(storage._key("q", qid), Record(bins))
                        for qid, bins in persisted.items()])
        assert storage.get_ids(deleted=False) == [1]
        assert len(session.executed) == 3  # no retry, rollback or second allocator

    @pytest.mark.parametrize("kind", ["s", "r"])
    def test_immutable_duplicate_with_different_content_fails_after_drain(self, model, kind):
        storage, session, module = model
        if kind == "s":
            first = record_row(storage, "s", 1)
            changed = record_row(storage, "s", 1, squestion="different")
        else:
            payload = module._json(dict(
                v=1, user_question="u", cache_question="q", cache_question_id=1,
                cache_answer=["n"], similarity=["i", "1"], cache_delta_time=["i", "0"],
                cache_time="2026-11-06T01:02:03.000004")).encode()
            first = record_row(storage, "r", 1, payload=payload)
            changed = record_row(storage, "r", 1, payload=payload.replace(b'"u"', b'"v"'))
        visited = []

        def delivery():
            yield first
            yield changed
            visited.append("fully drained")

        session.results.append(delivery())
        with pytest.raises(module.AerospikeStorageError, match="incomplete"):
            storage.list_sessions() if kind == "s" else storage._scan("r")
        assert visited == ["fully drained"]

    @pytest.mark.parametrize("kind,changes", [
        ("root", {"unknown": 1}), ("root", {"boot": "not-a-uuid"}),
        ("root", {"bind": "A" * 64}),
        ("qid", {"boot": "00000000-0000-4000-8000-000000000002"}),
        ("sid", {"hi": True}), ("rid", {"hi": -1}),
    ])
    def test_corrupt_initialization_history_fails_closed_without_writes(self, model, kind, changes):
        storage, session, module = model
        for prior in ("root", "qid", "sid", "rid"):
            queue(session, record_row(storage, prior, **(changes if prior == kind else {})))
            if prior == kind:
                break
        with pytest.raises((ValueError, module.AerospikeStorageError)):
            storage.create()
        with pytest.raises(module.AerospikeStorageError, match="ACTIVE"):
            storage.batch_insert([module.CacheData("q", "a")])
        assert not any(name in ("insert", "update", "put", "add")
                       for builder in session.executed for name, _ in builder.calls)

    def test_partial_purge_reports_each_parent_and_never_deletes_failed_parent(self, model):
        storage, session, module = model
        queue(session, [record_row(storage, "q", 1, state=-1),
                        record_row(storage, "q", 2, state=-1)],
              record_row(storage, "q", 1, state=-1), [], [],
              Row(storage._key("q", 1)),
              record_row(storage, "q", 2, state=-1, adm=1))
        with pytest.raises(module.AerospikeStorageError) as caught:
            storage.clear_deleted_data()
        assert caught.value.outcomes[1] == "PURGED_OR_ABSENT"
        assert "recovery required" in str(caught.value.outcomes[2])
        deletes = [dict(b.calls)["update"][0] for b in session.executed
                   if any(n == "delete_record" for n, _ in b.calls)]
        assert deletes == [storage._key("q", 1)]


class TestOperatorPrimitiveBoundary:
    @staticmethod
    def validator(monkeypatch, callback):
        runtime = types.ModuleType("gptcache.manager.scalar_data._aerospike_operator_runtime")
        runtime._validate_provider_context = callback
        monkeypatch.setitem(sys.modules, runtime.__name__, runtime)

    def test_context_refusal_precedes_all_record_io(self, model, monkeypatch):
        storage, session, module = model
        def refuse(context, **binding):
            assert binding["provider"] is storage
            assert binding["qids"] == (1,)
            raise module.AerospikeStorageError("Sealed context required")
        self.validator(monkeypatch, refuse)
        with pytest.raises(module.AerospikeStorageError, match="Sealed"):
            storage.recover_session_purge([1], recovery_context=True)
        assert not session.executed

    def test_recovery_exact_reset_slot_one_then_empty_then_final_delete(self, model, monkeypatch):
        storage, session, _ = model
        actions = []
        self.validator(monkeypatch, lambda context, **binding: actions.append(binding["action"]))
        activation(storage, session)
        queue(session, record_row(storage, "q", 1, state=-1, adm=3), [], [],
              slots_row(storage, "q", 1, None, 0), [], Row(storage._key("q", 1)))
        assert storage.recover_session_purge([1], recovery_context=object()) == {1: "PURGED"}
        assert actions == ["recover", "recover", "recover", "reset", "purge", "complete"]
        reset = session.executed[7]
        assert ("put", ({"adm": 0},)) in reset.calls
        assert ("get", ("adm",)) in reset.calls

    def test_initialization_resumes_only_same_root_then_activates(self, model, monkeypatch):
        storage, session, _ = model
        actions = []
        self.validator(monkeypatch, lambda context, **binding: actions.append(binding["action"]))
        queue(session, record_row(storage, "root", phase=0), [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, record_row(storage, kind))
        queue(session, [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, record_row(storage, kind))
        queue(session, Row(storage._key("root")))
        activation(storage, session)
        storage._initialize_scope(initialization_context=object(), boot=storage._boot,
                                  binding="a" * 64, root_attempted=True)
        assert storage._active
        assert actions[-1] == "activate"
        assert not any(name == "insert" for b in session.executed for name, _ in b.calls)

    def test_absent_root_after_attempt_refuses_not_recreates(self, model, monkeypatch):
        storage, session, module = model
        self.validator(monkeypatch, lambda context, **binding: None)
        queue(session, Row(storage._key("root"), result_code=2))
        with pytest.raises(module.AerospikeStorageError, match="prior attempt"):
            storage._initialize_scope(initialization_context=object(), boot=storage._boot,
                                      binding="a" * 64, root_attempted=True)
        assert not any(name == "insert" for b in session.executed for name, _ in b.calls)

    def test_fresh_initialization_creates_root_then_all_three_then_active(self, model, monkeypatch):
        storage, session, _ = model
        actions = []
        self.validator(monkeypatch, lambda context, **binding: actions.append(binding["action"]))
        queue(session, Row(storage._key("root"), result_code=2), [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, Row(storage._key(kind), result_code=2))
        queue(session, Row(storage._key("root")), record_row(storage, "root", phase=0), [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, Row(storage._key(kind), result_code=2), Row(storage._key(kind)),
                  record_row(storage, kind))
        queue(session, [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, record_row(storage, kind))
        queue(session, Row(storage._key("root")))
        activation(storage, session)
        storage._initialize_scope(initialization_context=object(), boot=storage._boot,
                                  binding="a" * 64, root_attempted=False)
        assert storage._active
        inserted = [args[0] for builder in session.executed for name, args in builder.calls if name == "insert"]
        assert inserted == [storage._key(kind) for kind in ("root", "qid", "sid", "rid")]
        assert actions == ["initialize", "create_root", "initialize", "initialize", "initialize", "activate"]
