"""Real seal, journal, flock and pipe tests with explicit OS/SDK boundaries.

No Docker or network is used. Container mount/process/network observations are
boundary fixtures, not evidence that Docker propagated EOF or isolated a server.
"""

import ast
import base64
from copy import deepcopy
import csv
import hashlib
from importlib import metadata
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid

import pytest

from gptcache.manager.scalar_data import aerospike_operator as op
from gptcache.manager.scalar_data import _aerospike_operator_runtime as rt
from test_aerospike_storage import (
    model, sdk, activation, queue, record_row, slots_row, Row,
)


def test_runtime_diagnostic_never_serializes_exception_values(capsys):
    secret = "credential-password-payload-TOPSECRET"
    try:
        raise ValueError(secret)
    except ValueError as exc:
        rt._diagnostic(exc)
    captured = capsys.readouterr()
    assert secret not in captured.err
    assert json.loads(captured.err) == {
        "diagnostic": "RUNTIME_REFUSED", "type": "ValueError", "frames": []}


@pytest.mark.parametrize("version,digest,count,accepted", [
    ("0.6.0a6", "44f770ffd3fb78c6a36d10b13195f984eeba0c003b73743e0b0254e26a12bd91", 1, False),
    ("0.6.0a6+gptcache.enumfix1",
     "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d", 1, True),
    ("0.6.0a6", "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d", 1, False),
    ("0.6.0a6+gptcache.enumfix1",
     "44f770ffd3fb78c6a36d10b13195f984eeba0c003b73743e0b0254e26a12bd91", 1, False),
    ("0.6.0a6+unapproved", "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d", 1, False),
    ("0.6.0a6+gptcache.enumfix1",
     "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d", 0, False),
    ("0.6.0a6+gptcache.enumfix1",
     "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d", 2, False),
])
def test_development_native_version_and_artifact_are_bound(
        monkeypatch, tmp_path, version, digest, count, accepted):
    native = SimpleNamespace(
        version=version,
        files=["aerospike_async/_aerospike_async_native%d.so" % n for n in range(count)],
        locate_file=lambda name: tmp_path / name)
    monkeypatch.setattr(rt, "_sha", lambda path: digest)
    if accepted:
        rt._verify_development_native(native)
    else:
        with pytest.raises(op.OperatorRefusal,
                           match="^DEVELOPMENT_NATIVE_REQUIRES_APPROVED_CP314_ARTIFACT$"):
            rt._verify_development_native(native)


BOOT = str(uuid.UUID(int=1))
MANIFEST = "a" * 64
PUBLIC, MAINTENANCE = "b" * 64, "c" * 64
START = "2026-01-01T00:00:03.000000000Z"
INTENT = {"namespace": "test", "cache_scope": "owned", "intent_id": "new",
          "vector_binding": "new-vector", "boot": BOOT, "root_attempted": True,
          "status": "INITIALIZING"}
INTENT["binding"] = hashlib.sha256(op._json({
    k: INTENT[k] for k in ("namespace", "cache_scope", "intent_id", "vector_binding")
})).hexdigest()


@pytest.fixture
def boundary(tmp_path, monkeypatch):
    journal = tmp_path / "journal"
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    op._Journal.enroll(str(journal), MANIFEST)
    root, data, proc, net = [tmp_path / name for name in ("runtime", "data", "proc", "net")]
    for p in (root, data, proc, net):
        p.mkdir(mode=0o755)
        p.chmod(0o755)
    (root / "source.py").write_text("fixed framework fixture\n")
    (root / "source.py").chmod(0o444)
    (data / "operator.dat").write_bytes(b"persistent-file")
    (data / "operator.dat").chmod(0o644)
    (proc / "1").mkdir()
    (net / "lo").mkdir()
    config = tmp_path / "aerospike.conf"
    config.write_bytes(b"fixed reviewed config\n")
    config.chmod(0o444)
    for name, value in (("_ROOT", root), ("_JOURNAL", journal), ("_DATA", data),
                        ("_CONFIG", config), ("_PROC", proc), ("_NET", net)):
        monkeypatch.setattr(rt, name, str(value))
    payload = op._tree_digest(str(root))
    deployment = {
        "namespace": "test", "runtime_sha256": payload,
        "containers": {"public": PUBLIC, "maintenance": MAINTENANCE},
        "data": {"device": data.stat().st_dev, "inode": data.stat().st_ino},
        "configs": {mode: {"device": config.stat().st_dev, "inode": config.stat().st_ino,
                           "sha256": rt._sha(config)} for mode in ("public", "maintenance")}}
    server = {"pid": 4321, "ticks": 12345, "state": "S", "argv_sha256": "d" * 64}
    incarnation = {"mode": "maintenance", "container": MAINTENANCE, "started": START,
                   "pid": 4321, "identity": "e" * 64, "processes": [server]}
    monkeypatch.setattr(op, "_process", lambda pid: deepcopy(server) if pid == 1 else None)
    monkeypatch.setattr(rt.socket, "gethostname", lambda: MAINTENANCE[:12])
    mounts = {str(p): {"ro"} for p in (root, journal, config)}
    mounts[str(data)] = {"rw"}
    monkeypatch.setattr(rt, "_mounts", lambda: deepcopy(mounts))
    stopped = {"started": "2026-01-01T00:00:01.000000000Z",
               "finished": "2026-01-01T00:00:02.000000000Z",
               "identity": "f" * 64, "log_sha256": "0" * 64,
               "clean_times_ns": [op._timestamp_ns("2026-01-01T00:00:02.000000000Z")]}
    holder = op._Journal(str(journal), MANIFEST)
    state = holder.state
    state["stop_proofs"] = {PUBLIC: stopped}
    holder.store(state)
    controller = {"pid": os.getpid(), "ticks": 999, "state": "S", "argv_sha256": "1" * 64}
    request = {"version": 1, "operation": "initialize", "attempt": "2" * 32,
               "nonce": "3" * 32, "namespace": "test", "intent": deepcopy(INTENT),
               "qids": [], "historical_root_attempted": False, "proof": {}}
    control = {"nonce": request["nonce"], "incarnation": incarnation,
               "data_file": op._identity((data / "operator.dat").stat()) +
               [(data / "operator.dat").stat().st_size], "deployment": deployment,
               "intents": {"new": deepcopy(INTENT)},
               "pending": {"attempt": request["attempt"], "operation": "initialize",
                           "intent_id": "new", "qids": [], "outcome": "UNKNOWN",
                           "historical_root_attempted": False,
                           "controller_process": controller, "host_boot": op._host_boot()}}
    holder._control(control)
    state = holder.state
    request["proof"] = {k: state[k] for k in (
        "manifest", "sequence", "directory", "lock", "stop_proofs")}
    request["proof"].update(controller_pid=os.getpid(), host_boot=op._host_boot(),
                            controller_process=controller, deployment=deployment,
                            incarnation=incarnation, data_file=control["data_file"],
                            payload=payload)
    read, write = os.pipe()
    result = SimpleNamespace(request=request, read=read, write=write, holder=holder,
                             control=control, root=root, data=data, config=config,
                             proc=proc, net=net, mounts=mounts, journal=journal)
    yield result
    rt._CONTEXTS.clear()
    monkeypatch.setattr(rt, "_OWNED_PROVIDER", None)
    holder.close()
    os.close(read)
    if result.write is not None:
        os.close(result.write)


def provider():
    return SimpleNamespace(namespace="test", cache_scope="owned", _closed=False,
                           _session=object(), _cluster=object(), _boot=None)


def mint(boundary, monkeypatch):
    lease = rt._Lease(boundary.request, boundary.read)
    p = provider()
    monkeypatch.setattr(rt, "_OWNED_PROVIDER", p)
    return lease, p, rt._mint(p, lease)


def validate(context, p, action="initialize", **kwargs):
    values = dict(provider=p, action=action, namespace="test", cache_scope="owned",
                  qids=(), boot=BOOT, binding=INTENT["binding"])
    values.update(kwargs)
    rt._validate_provider_context(context, **values)


def test_real_context_and_current_one_shot_attempt(boundary, monkeypatch):
    lease, p, context = mint(boundary, monkeypatch)
    try:
        assert validate(context, p) is None
        assert validate(context, p, "create_root") is None
        validate(context, p, "activate")
        with pytest.raises(op.OperatorRefusal, match="ROOT_ATTEMPT"):
            validate(context, p, "create_root")
    finally:
        lease.close()
    with pytest.raises(op.OperatorRefusal, match="LEASE_EXPIRED"):
        validate(context, p)


@pytest.mark.parametrize("forged", [True, False, None, {}, {"verified": True}, object()])
def test_no_deserialized_context_authority(boundary, forged):
    with pytest.raises(op.OperatorRefusal, match="INVALID_PROVIDER_CONTEXT"):
        validate(forged, provider())
    with pytest.raises(TypeError):
        rt.DockerMaintenanceContext()


@pytest.mark.parametrize("field,value", [
    ("namespace", "foreign"), ("cache_scope", "foreign"), ("qids", (1,)),
    ("qids", []), ("boot", str(uuid.UUID(int=2))), ("binding", "f" * 64),
    ("action", "reset"), ("action", "unknown")])
def test_exact_context_action_scope_and_targets(boundary, monkeypatch, field, value):
    lease, p, context = mint(boundary, monkeypatch)
    try:
        with pytest.raises(op.OperatorRefusal):
            validate(context, p, **{field: value})
    finally:
        lease.close()


@pytest.mark.parametrize("fault", ["provider", "session", "cluster", "closed", "pid",
                                 "expired", "eof", "extra-input", "lock", "journal",
                                 "nonce", "pending", "history", "intent", "deployment"])
def test_seal_lease_and_durable_boundary_refusals(boundary, monkeypatch, fault):
    lease, p, context = mint(boundary, monkeypatch)
    try:
        if fault == "provider":
            p = provider()
        elif fault in ("session", "cluster"):
            setattr(p, "_" + fault, object())
        elif fault == "closed":
            p._closed = True
        elif fault == "pid":
            lease.pid += 1
        elif fault == "expired":
            lease.deadline = 0
        elif fault == "eof":
            os.close(boundary.write)
            boundary.write = None
        elif fault == "extra-input":
            os.write(boundary.write, b"second request\n")
        elif fault == "lock":
            boundary.holder.close()
        elif fault == "journal":
            (boundary.journal / "receipt.json").chmod(0o644)
        else:
            control = deepcopy(boundary.control)
            if fault == "nonce":
                control["nonce"] = "0" * 32
            elif fault == "pending":
                control["pending"]["attempt"] = "0" * 32
            elif fault == "history":
                control["pending"]["historical_root_attempted"] = True
            elif fault == "intent":
                control["intents"]["new"]["boot"] = str(uuid.UUID(int=2))
            else:
                control["deployment"]["namespace"] = "foreign"
            boundary.holder._control(control)
        with pytest.raises((op.OperatorRefusal, OSError)):
            validate(context, p)
    finally:
        lease.close()


@pytest.mark.parametrize("fault", [
    "uid", "gid", "hostname", "boot", "network", "mount", "process", "data",
    "config", "source", "symlink", "hardlink", "hook", "permissions", "mode",
    "settlement", "container", "namespace"])
def test_actual_os_guard_with_boundary_observations(boundary, monkeypatch, fault):
    # Do not replace _os_boundary, _Lease.check, _mint or the context validator.
    lease, p, context = mint(boundary, monkeypatch)
    try:
        if fault == "uid":
            monkeypatch.setattr(rt.os, "getresuid", lambda: (0, 0, 0))
        elif fault == "gid":
            monkeypatch.setattr(rt.os, "getresgid", lambda: (0, 0, 0))
        elif fault == "hostname":
            monkeypatch.setattr(rt.socket, "gethostname", lambda: "foreign")
        elif fault == "boot":
            monkeypatch.setattr(op, "_host_boot", lambda: str(uuid.UUID(int=3)))
        elif fault == "network":
            (boundary.net / "eth0").mkdir()
        elif fault == "mount":
            boundary.mounts[str(boundary.root)] = {"rw"}
        elif fault == "process":
            (boundary.proc / "999").mkdir()
            monkeypatch.setattr(op, "_process", lambda pid: {
                "pid": pid, "ticks": 1, "state": "S", "argv_sha256": "0" * 64})
        elif fault == "data":
            (boundary.data / "operator.dat").write_bytes(b"different size")
        elif fault == "config":
            boundary.config.chmod(0o644)
            boundary.config.write_bytes(b"changed")
        elif fault in ("source", "permissions"):
            path = boundary.root / "source.py"
            path.chmod(0o666 if fault == "permissions" else 0o644)
            if fault == "source":
                path.write_text("changed")
        elif fault == "symlink":
            (boundary.root / "link").symlink_to(boundary.root / "source.py")
        elif fault == "hardlink":
            os.link(boundary.root / "source.py", boundary.root / "link")
        elif fault == "hook":
            (boundary.root / "evil.pth").write_text("import malicious")
        elif fault in ("mode", "container"):
            # Even replacing only a transported proof cannot re-authorize it.
            lease.request["proof"]["incarnation"][
                "mode" if fault == "mode" else "container"] = "foreign"
        elif fault == "settlement":
            lease.request["proof"]["stop_proofs"] = {}
        else:
            lease.request["namespace"] = "foreign"
        with pytest.raises((op.OperatorRefusal, OSError)):
            validate(context, p)
        assert lease.closed
    finally:
        lease.close()


def persist(boundary):
    request = boundary.request
    control = deepcopy(boundary.control)
    control["deployment"] = request["proof"]["deployment"]
    control["incarnation"] = request["proof"]["incarnation"]
    control["intents"] = {request["intent"]["intent_id"]: deepcopy(request["intent"])}
    control["pending"].update(operation=request["operation"], qids=request["qids"],
                              historical_root_attempted=request["historical_root_attempted"])
    boundary.holder._control(control)
    boundary.control = control
    request["proof"]["sequence"] = boundary.holder.state["sequence"]


def test_recovery_boot_revalidation_full_targets_and_per_qid_reset(boundary, monkeypatch):
    boundary.request.update(operation="recover", qids=[7, 8])
    boundary.request["intent"]["status"] = "ACTIVE"
    persist(boundary)
    lease, p, context = mint(boundary, monkeypatch)
    try:
        validate(context, p, "recover", qids=(7, 8), boot=None, binding=None)
        p._boot = BOOT
        for action in ("recover", "reset", "purge", "recover", "reset", "purge", "complete"):
            validate(context, p, action, qids=(7, 8), binding=None)
        with pytest.raises(op.OperatorRefusal):
            validate(context, p, "reset", qids=(7,), binding=None)
    finally:
        lease.close()


def test_recovery_cannot_reuse_initial_unknown_boot(boundary, monkeypatch):
    boundary.request.update(operation="recover", qids=[7])
    boundary.request["intent"]["status"] = "ACTIVE"
    persist(boundary)
    lease, p, context = mint(boundary, monkeypatch)
    try:
        validate(context, p, "recover", qids=(7,), boot=None, binding=None)
        with pytest.raises(op.OperatorRefusal, match="BOOT_NOT_REVALIDATED"):
            validate(context, p, "recover", qids=(7,), boot=None, binding=None)
    finally:
        lease.close()


def test_context_target_true_is_not_integer_one(boundary, monkeypatch):
    boundary.request.update(operation="recover", qids=[1])
    boundary.request["intent"]["status"] = "ACTIVE"
    persist(boundary)
    lease, p, context = mint(boundary, monkeypatch)
    try:
        p._boot = BOOT
        with pytest.raises(op.OperatorRefusal, match="SCOPE_CHANGED"):
            validate(context, p, "reset", qids=(True,), binding=None)
    finally:
        lease.close()


def test_historical_attempt_cannot_authorize_new_root(boundary, monkeypatch):
    boundary.request["historical_root_attempted"] = True
    persist(boundary)
    lease, p, context = mint(boundary, monkeypatch)
    try:
        validate(context, p)
        with pytest.raises(op.OperatorRefusal, match="ROOT_ATTEMPT"):
            validate(context, p, "create_root")
    finally:
        lease.close()


@pytest.mark.parametrize("field,value", [
    ("version", True), ("operation", "custom"), ("attempt", "not-a-uuid"),
    ("nonce", None), ("namespace", "x;set-config"), ("qids", [True]),
    ("qids", [1, 1]), ("qids", [-1]), ("qids", [2**63]),
    ("historical_root_attempted", "false"), ("unexpected", "payload")])
def test_request_allowlist(boundary, field, value):
    request = deepcopy(boundary.request)
    request[field] = value
    with pytest.raises((op.OperatorRefusal, ValueError, TypeError)):
        rt._request(request)


def test_single_line_reader_does_not_wait_for_eof(boundary):
    raw = op._json(boundary.request)
    os.write(boundary.write, raw + b"\n")
    assert rt._read_request(boundary.read) == boundary.request
    lease = rt._Lease(boundary.request, boundary.read)
    lease.check()
    os.close(boundary.write)
    boundary.write = None
    with pytest.raises(op.OperatorRefusal, match="TRANSPORT_LOST"):
        lease.check()
    lease.close()


@pytest.mark.parametrize("data", [b"", b"{}", b'{"v":1,"v":2}\n'])
def test_invalid_or_eof_input_never_dispatches(data):
    read, write = os.pipe()
    os.write(write, data)
    os.close(write)
    try:
        with pytest.raises(op.OperatorRefusal):
            rt._read_request(read)
    finally:
        os.close(read)


class Distribution:
    def __init__(self, root, fault=None):
        self.root = root
        self.version = "1.0"
        self.metadata = {"Name": "owned"}
        self.fault = fault

    def read_text(self, name):
        if name != "RECORD":
            return None
        data = b"safe = 1\n"
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        if self.fault == "hash":
            digest = "0" * 43
        name = "evil.pth" if self.fault == "hook" else "owned.py"
        return "{},sha256={},{}\n".format(name, digest, len(data))

    def locate_file(self, name):
        return self.root / name


@pytest.mark.parametrize("fault", [None, "hash", "hook"])
def test_distribution_record_provenance_and_link_free_copy(tmp_path, fault):
    source, target = tmp_path / "installed", tmp_path / "frozen"
    source.mkdir()
    name = "evil.pth" if fault == "hook" else "owned.py"
    (source / name).write_bytes(b"safe = 1\n")
    dist = Distribution(source, fault)
    if fault:
        with pytest.raises(op.OperatorRefusal):
            rt._distribution(dist, target)
    else:
        result = rt._distribution(dist, target)
        assert result["members"]["owned.py"] == rt._sha(source / "owned.py")
        assert (target / "owned.py").read_bytes() == (source / "owned.py").read_bytes()
        assert (target / "owned.py").stat().st_nlink == 1
        assert (target / "owned.py").stat().st_mode & 0o222 == 0


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Optional SDK runtime requires Python >=3.11")
def test_missing_sdk_is_actionable_before_artifact_or_journal_changes(tmp_path, monkeypatch):
    def absent(name):
        raise metadata.PackageNotFoundError(name)
    monkeypatch.setattr(rt.metadata, "distribution", absent)
    with pytest.raises(op.OperatorRefusal, match="BUNDLE_DEPENDENCY_MISSING_INSTALL_TRUSTED"):
        rt._prepare_runtime_bundle(str(tmp_path / "does-not-exist"))
    assert not list(tmp_path.iterdir())


def test_runtime_source_parses_for_core_python38_and_has_early_floor(monkeypatch):
    ast.parse(Path(rt.__file__).read_text(), feature_version=(3, 8))
    monkeypatch.setattr(rt.sys, "version_info", (3, 8))
    with pytest.raises(op.OperatorRefusal, match="REQUIRES_PYTHON_311"):
        rt._dispatch({}, 0)


def align_model(boundary, storage):
    request = boundary.request
    request["namespace"] = storage.namespace
    request["intent"].update(namespace=storage.namespace, cache_scope=storage.cache_scope,
                             boot=storage._boot)
    request["intent"]["binding"] = hashlib.sha256(op._json({
        k: request["intent"][k] for k in (
            "namespace", "cache_scope", "intent_id", "vector_binding")})).hexdigest()
    request["proof"]["deployment"]["namespace"] = storage.namespace
    persist(boundary)


@pytest.mark.parametrize("historical,phase", [(False, None), (True, 0), (True, 1)])
def test_real_core_initialization_with_actual_seal(boundary, model, monkeypatch,
                                                 historical, phase):
    storage, session, _ = model
    align_model(boundary, storage)
    boundary.request["historical_root_attempted"] = historical
    persist(boundary)
    binding = boundary.request["intent"]["binding"]
    if phase is None:
        queue(session, Row(storage._key("root"), result_code=2), [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, Row(storage._key(kind), result_code=2))
        queue(session, Row(storage._key("root")),
              record_row(storage, "root", phase=0, bind=binding), [], [], [])
    else:
        queue(session, record_row(storage, "root", phase=phase, bind=binding))
        if phase == 0:
            queue(session, [], [], [])
    if phase != 1:
        for kind in ("qid", "sid", "rid"):
            if phase is None:
                queue(session, Row(storage._key(kind), result_code=2),
                      Row(storage._key(kind)), record_row(storage, kind))
            else:
                queue(session, record_row(storage, kind))
        queue(session, [], [], [])
        for kind in ("qid", "sid", "rid"):
            queue(session, record_row(storage, kind))
        queue(session, Row(storage._key("root")))
    activation(storage, session, bind=binding)
    monkeypatch.setattr(rt, "_provider", lambda lease: storage)
    response = rt._dispatch(boundary.request, boundary.read)
    assert response["status"] == "OK"
    assert storage._closed
    assert rt._CONTEXTS == {}
    if phase == 1:
        assert not any(name in ("insert", "put", "add")
                       for builder in session.executed for name, _ in builder.calls)


def test_real_core_prior_attempt_absent_root_refuses_without_writes(boundary, model, monkeypatch):
    storage, session, _ = model
    align_model(boundary, storage)
    boundary.request["historical_root_attempted"] = True
    persist(boundary)
    queue(session, Row(storage._key("root"), result_code=2))
    monkeypatch.setattr(rt, "_provider", lambda lease: storage)
    result = rt._dispatch(boundary.request, boundary.read)
    assert result["status"] == "REFUSED"
    assert len(session.executed) == 1
    assert not any(name in ("insert", "put", "add")
                   for builder in session.executed for name, _ in builder.calls)


@pytest.mark.parametrize("fault", [None, "active", "eof-before-reset"])
def test_real_core_recovery_with_real_context_and_correlated_results(boundary, model,
                                                                   monkeypatch, fault):
    storage, session, _ = model
    align_model(boundary, storage)
    boundary.request.update(operation="recover", qids=[1, 2])
    boundary.request["intent"]["status"] = "ACTIVE"
    persist(boundary)
    activation(storage, session, bind=boundary.request["intent"]["binding"])
    queue(session, record_row(storage, "q", 1, state=0 if fault == "active" else -1, adm=3))
    if fault != "active":
        queue(session, [], [])
        if fault != "eof-before-reset":
            queue(session, slots_row(storage, "q", 1, None, 0), [],
                  Row(storage._key("q", 1)), Row(storage._key("q", 2), result_code=2), [])
        else:
            original = storage._children_empty
            calls = []
            def children(qid, delete=False):
                result = original(qid, delete=delete)
                calls.append(delete)
                if len(calls) == 2:
                    os.close(boundary.write)
                    boundary.write = None
                return result
            monkeypatch.setattr(storage, "_children_empty", children)
    monkeypatch.setattr(rt, "_provider", lambda lease: storage)
    result = rt._dispatch(boundary.request, boundary.read)
    assert result["attempt"] == boundary.request["attempt"]
    assert result["nonce"] == boundary.request["nonce"]
    if fault is None:
        assert result["status"] == "OK"
        assert result["results"] == [{"qid": 1, "status": "PURGED"},
                                     {"qid": 2, "status": "ABSENT"}]
    else:
        assert result["status"] in ("PARTIAL", "REFUSED")
        assert not any(name == "put" and args == ({"adm": 0},)
                       for builder in session.executed for name, args in builder.calls)
    assert rt._CONTEXTS == {}


@pytest.mark.parametrize("activation_check", ["eof", "active", "wrong-binding"])
def test_fixed_factory_one_owned_cluster_and_eof_before_execute(boundary, model,
                                                              sdk, monkeypatch,
                                                              activation_check):
    storage, session, _ = model
    align_model(boundary, storage)
    definitions = []
    definition_type = type(sdk)
    def definition(self, host, port):
        definitions.append((host, port))
        self.connect = sdk.connect
    monkeypatch.setattr(definition_type, "__init__", definition)
    monkeypatch.setattr(definition_type, "using_services_alternate",
                        lambda self, flag: self if flag is False else None, raising=False)
    monkeypatch.setattr(definition_type, "with_system_settings", lambda self, value: self,
                        raising=False)
    policy = sys.modules["aerospike_sdk.policy"]
    monkeypatch.setattr(policy, "SystemSettings", lambda **kw: kw, raising=False)
    monkeypatch.setattr(policy, "TransactionSettings", lambda **kw: kw, raising=False)
    monkeypatch.setattr(rt, "_runtime_behavior", lambda: session.behavior)
    lease = rt._Lease(boundary.request, boundary.read)
    owned = rt._provider(lease)
    try:
        assert definitions == [("127.0.0.1", 3000)]
        assert sdk.connect.call_count == 2  # model fixture's connection, then owned runtime
        assert owned._session is session
        owned._configure()
        if activation_check != "eof":
            binding = ("f" * 64 if activation_check == "wrong-binding" else
                       boundary.request["intent"]["binding"])
            activation(storage, session, bind=binding)
            queue(session, record_row(storage, "root", bind=binding))
            if activation_check == "wrong-binding":
                with pytest.raises(op.OperatorRefusal, match="ROOT_INTENT_MISMATCH"):
                    owned.create()
            else:
                owned.create()
                assert owned._active
            assert not any(name in ("put", "insert", "delete_record")
                           for b in session.executed for name, _ in b.calls)
        builder = owned._write("update", owned._key("q", 1)).put({"adm": 0})
        before = len(session.executed)
        os.close(boundary.write)
        boundary.write = None
        with pytest.raises(op.OperatorRefusal, match="TRANSPORT_LOST"):
            owned._point(builder, owned._key("q", 1))
        assert len(session.executed) == before
    finally:
        owned.close()
        owned.close()
        lease.close()
    assert sdk.cluster.close.call_count == 1


@pytest.mark.parametrize("operation,wrong_binding", [
    ("ready", False), ("validate", False), ("validate", True)])
def test_readonly_configuration_and_active_metadata_validation(boundary, model,
                                                             monkeypatch, operation,
                                                             wrong_binding):
    storage, session, _ = model
    align_model(boundary, storage)
    boundary.request["operation"] = operation
    if operation == "validate":
        boundary.request["intent"]["status"] = "ACTIVE"
        binding = boundary.request["intent"]["binding"]
        activation(storage, session, bind=binding)
        queue(session, record_row(storage, "root", bind="0" * 64 if wrong_binding else binding))
        persist(boundary)
    else:
        boundary.request["intent"] = None
        control = deepcopy(boundary.control)
        control["pending"].update(operation="ready", intent_id=None)
        boundary.holder._control(control)
        boundary.request["proof"]["sequence"] = boundary.holder.state["sequence"]
    monkeypatch.setattr(rt, "_provider", lambda lease: storage)
    result = rt._dispatch(boundary.request, boundary.read)
    assert result["status"] == ("REFUSED" if wrong_binding else "OK")
    assert result["results"] == []
    assert session.configuration.calls
    assert not any(name in ("insert", "put", "add", "delete_record")
                   for builder in session.executed for name, _ in builder.calls)


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Optional SDK runtime requires Python >=3.11")
def test_runtime_redacts_all_exception_payloads_and_closes_owned_session(boundary, monkeypatch):
    p = provider()
    closed = []
    p.close = lambda: closed.append(True)
    p._initialize_scope = lambda **kw: (_ for _ in ()).throw(
        RuntimeError("password://credential-private-payload"))
    monkeypatch.setattr(rt, "_provider", lambda lease: p)
    result = rt._dispatch(boundary.request, boundary.read)
    assert result["status"] == "REFUSED"
    assert b"credential" not in op._json(result)
    assert closed == [True]
    assert rt._CONTEXTS == {}


def test_forged_exact_type_and_foreign_provider_cannot_mint(boundary):
    forged = object.__new__(rt.DockerMaintenanceContext)
    with pytest.raises(op.OperatorRefusal, match="INVALID_PROVIDER_CONTEXT"):
        validate(forged, provider())
    lease = rt._Lease(boundary.request, boundary.read)
    try:
        with pytest.raises(op.OperatorRefusal, match="FOREIGN_RUNTIME_PROVIDER"):
            rt._mint(provider(), lease)
    finally:
        lease.close()


def test_nonpipe_cannot_be_a_controller_lease(boundary):
    with boundary.config.open("rb") as stream:
        with pytest.raises(op.OperatorRefusal, match="RETAINED_STDIN_PIPE"):
            rt._Lease(boundary.request, stream.fileno())


def test_large_target_results_refuse_before_controller_changes(boundary, monkeypatch):
    qids = list(range(1, 30001))
    assert len(op._json(qids)) < 256 * 1024
    instance = op.DockerMaintenanceOperator()
    instance.manifest = {"namespace": "test"}
    with pytest.raises(op.OperatorRefusal, match="RESULTS_TOO_LARGE"):
        instance.recover_session_purge(namespace="test", cache_scope="owned", qids=qids)
    request = deepcopy(boundary.request)
    request.update(operation="recover", qids=qids)
    with pytest.raises(op.OperatorRefusal, match="RESULTS_TOO_LARGE"):
        rt._request(request)


@pytest.fixture
def frozen(tmp_path, monkeypatch):
    current = tmp_path / "installed/gptcache"
    entry = current / "manager/scalar_data/_aerospike_operator_runtime.py"
    entry.parent.mkdir(parents=True)
    entry.write_text("fixed current source")
    entry.chmod(0o444)
    monkeypatch.setattr(rt, "__file__", str(entry))
    root = tmp_path / "bundle"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    rt._copy_source(current, root / "source/gptcache")
    (root / "lib").mkdir()
    (root / "lib/owned.so").write_bytes(b"ELF closure fixture, not executable")
    (root / "lib/owned.so").chmod(0o444)
    site = root / "python/lib/python3.14/site-packages"
    site.mkdir(parents=True)
    for name in ("aerospike_native/__init__.py", "numpy/__init__.py"):
        path = site / name
        path.parent.mkdir()
        path.write_text("nonexecuted package fixture")
        path.chmod(0o444)
    sdk_source = root / "source/aerospike_sdk/__init__.py"
    sdk_source.parent.mkdir()
    sdk_source.write_text("nonexecuted sdk fixture")
    sdk_source.chmod(0o444)
    description = {"version": 1, "platform": "linux-x86_64", "python": {"version": "fixture"},
                   "packages": [{"name": "aerospike-sdk", "version": "1.0.0",
                                 "members": {"frozen_sdk_source": {"__init__.py": rt._sha(sdk_source)}}},
                                {"name": "aerospike-native", "version": "1.0.0",
                                 "members": {"aerospike_native/__init__.py":
                                             rt._sha(site / "aerospike_native/__init__.py")}},
                                {"name": "numpy", "version": "fixture",
                                 "members": {"numpy/__init__.py": rt._sha(site / "numpy/__init__.py")}}],
                   "libraries": {"owned.so": {"source": "/trusted/owned.so",
                                             "sha256": rt._sha(root / "lib/owned.so")}},
                   "files": rt._inventory(root)}
    (root / "runtime.json").write_bytes(op._json(description))
    (root / "runtime.json").chmod(0o444)
    for path in root.rglob("*"):
        if path.is_dir():
            path.chmod(0o555)
    return root, current, description


def test_bundle_provenance_exact_member_and_framework_positive(frozen):
    root, _, description = frozen
    assert rt._verify_bundle(root) == description


@pytest.mark.parametrize("fault", [
    "member", "source", "extra-source", "missing-source", "version", "profile",
    "native-version", "link", "hardlink", "pth", "hook", "writable", "library"])
def test_bundle_provenance_refuses_foreign_or_mutated_artifact(frozen, fault):
    root, current, description = frozen
    if fault in ("member", "library"):
        path = root / "lib/owned.so"
        path.chmod(0o644)
        path.write_bytes(b"changed")
        if fault == "library":
            description["files"] = rt._inventory(root)
    elif fault == "source":
        path = current / "manager/scalar_data/_aerospike_operator_runtime.py"
        path.chmod(0o644)
        path.write_text("changed current source")
    elif fault == "extra-source":
        (current / "extra.py").write_text("not in the bundle")
    elif fault == "missing-source":
        (current / "manager/scalar_data/_aerospike_operator_runtime.py").unlink()
    elif fault == "version":
        description["packages"][0]["version"] = "unknown"
    elif fault == "profile":
        description["platform"] = "foreign"
    elif fault == "native-version":
        description["packages"][1]["version"] = "unknown"
    elif fault == "link":
        (root / "link").symlink_to(root / "lib/owned.so")
    elif fault == "hardlink":
        os.link(root / "lib/owned.so", root / "link")
    elif fault in ("pth", "hook"):
        (root / ("evil.pth" if fault == "pth" else "sitecustomize.cpython-314.so")).write_text("hook")
    else:
        (root / "lib/owned.so").chmod(0o666)
    if fault in ("version", "profile", "native-version", "library"):
        (root / "runtime.json").chmod(0o644)
        (root / "runtime.json").write_bytes(op._json(description))
    with pytest.raises((op.OperatorRefusal, OSError)):
        rt._verify_bundle(root)


@pytest.mark.parametrize("operation", ["validate", "initialize", "recover"])
def test_public_incarnation_readonly_only(boundary, monkeypatch, operation):
    request = boundary.request
    request["operation"] = operation
    request["intent"]["status"] = "ACTIVE"
    incarnation = request["proof"]["incarnation"]
    incarnation.update(mode="public", container=PUBLIC)
    monkeypatch.setattr(rt.socket, "gethostname", lambda: PUBLIC[:12])
    state = boundary.holder.state
    state["stop_proofs"][MAINTENANCE] = deepcopy(state["stop_proofs"][PUBLIC])
    boundary.holder.store(state)
    request["proof"]["stop_proofs"] = boundary.holder.state["stop_proofs"]
    persist(boundary)
    if operation == "validate":
        lease = rt._Lease(request, boundary.read)
        lease.check()
        lease.close()
    else:
        with pytest.raises(op.OperatorRefusal, match="MODE_NOT_AUTHORIZED"):
            rt._Lease(request, boundary.read)


def test_bundle_can_precede_enrollment_but_not_a_substitute_for_it(frozen, tmp_path):
    root, _, _ = frozen
    journal = tmp_path / "new-journal"
    journal.mkdir(mode=0o700)
    journal.chmod(0o700)
    root.rename(journal / "runtime")
    op._Journal.enroll(str(journal), MANIFEST)
    assert op._Journal.status(str(journal))["state"] == "MAINTENANCE"
    with pytest.raises(op.OperatorRefusal, match="NOT_EMPTY"):
        op._Journal.enroll(str(journal), MANIFEST)


@pytest.mark.parametrize("table,reason", [
    (" (NEEDED) Shared library: [/host/libbad.so]\n", "ABSOLUTE_ELF_DEPENDENCY"),
    (" (RPATH) Library rpath: [/host/lib]\n", "NONRELOCATABLE_ELF_SEARCH"),
    (" (RUNPATH) Library runpath: [$ORIGIN/../../../outside]\n", "ELF_SEARCH_ESCAPES_ROOT"),
])
def test_elf_closure_refuses_nonrelocatable_dependencies(tmp_path, monkeypatch, table, reason):
    elf = tmp_path / "python/bin/python3"
    elf.parent.mkdir(parents=True)
    elf.write_bytes(b"\x7fELFfixture")
    def readelf(argv, **kwargs):
        assert argv[0] == "/usr/bin/readelf"
        return SimpleNamespace(returncode=0, stdout=table.encode(), stderr=b"")
    monkeypatch.setattr(rt.subprocess, "run", readelf)
    with pytest.raises(op.OperatorRefusal, match=reason):
        rt._libraries(tmp_path)
