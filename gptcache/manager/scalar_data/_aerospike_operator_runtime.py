"""Fixed, private maintenance payload. Serialized proofs are never contexts.

The host preparation helper is explicit and offline. Importing this module does
not import the SDK, contact Docker, create directories, or connect to a server.
"""

import base64
from copy import deepcopy
import csv
from datetime import timedelta
import fcntl
import hashlib
from importlib import metadata
import os
from pathlib import Path
import re
import select
import shutil
import socket
import stat
import subprocess
import sys
import sysconfig
import tempfile
import time
import uuid
from urllib.parse import unquote, urlparse


_CONTEXTS = {}
_OP = None
_ROOT = "/op"
_JOURNAL = "/op/journal"
_DATA = "/opt/aerospike/data"
_CONFIG = "/etc/aerospike/aerospike.conf"
_PROC = "/proc"
_NET = "/sys/class/net"
_MODULE = "gptcache.manager.scalar_data._aerospike_operator_runtime"
_OWNED_PROVIDER = None
# The unpatched development native can lose buffered records at normal EOF.
_BUNDLE_SDK_PAIRS = (
    ("0.9.0a5", "aerospike-async", "0.6.0a6+gptcache.enumfix1"),
    ("1.0.0", "aerospike-native", "1.0.0"),
)
_DEVELOPMENT_NATIVE_ARTIFACTS = {
    "0.6.0a6+gptcache.enumfix1":
        "0a5e039b06bd9961778df88c3528d625a562cd9220875adc2123745c9ca9550d",
}


def _operator():
    global _OP
    if _OP is None:
        from gptcache.manager.scalar_data import aerospike_operator
        _OP = aerospike_operator
    return _OP


def _require(condition, code):
    _operator()._require(condition, code)


def _request(value):
    op = _operator()
    op._keys(value, "version operation attempt nonce namespace intent qids "
             "historical_root_attempted proof")
    _require(type(value["version"]) is int and value["version"] == 1 and
             value["operation"] in ("ready", "initialize", "recover", "validate"),
             "INVALID_RUNTIME_OPERATION")
    for name in ("attempt", "nonce"):
        _require(type(value[name]) is str and
                 re.fullmatch("[0-9a-f]{32}", value[name]), "INVALID_RUNTIME_IDENTITY")
    _require(type(value["namespace"]) is str and
             re.fullmatch("[A-Za-z0-9_-]{1,31}", value["namespace"]),
             "UNSAFE_NAMESPACE")
    qids = value["qids"]
    _require(type(qids) is list and all(type(q) is int and 1 <= q < 2 ** 63
                                      for q in qids) and
             len(set(qids)) == len(qids) and
             len(op._json(qids)) <= 256 * 1024, "INVALID_RUNTIME_TARGETS")
    _require(len(op._json([{"qid": q, "status": "NOT_ATTEMPTED"} for q in qids]))
             <= 1024 * 1024 - 1024, "RUNTIME_TARGET_RESULTS_TOO_LARGE")
    _require(type(value["historical_root_attempted"]) is bool,
             "INVALID_ROOT_ATTEMPT_HISTORY")
    intent = value["intent"]
    if value["operation"] == "ready":
        _require(intent is None and not qids and
                 not value["historical_root_attempted"], "INVALID_READY_REQUEST")
    else:
        op._keys(intent, "namespace cache_scope intent_id vector_binding boot binding "
                 "root_attempted status")
        _require(intent["namespace"] == value["namespace"] and
                 all(type(intent[k]) is str and intent[k].strip()
                     for k in ("cache_scope", "intent_id", "vector_binding")) and
                 re.fullmatch("[A-Za-z0-9][A-Za-z0-9_.-]{0,255}",
                              intent["vector_binding"]) and
                 type(intent["root_attempted"]) is bool and
                 intent["root_attempted"] is True and
                 intent["status"] in ("INITIALIZING", "ACTIVE"),
                 "INVALID_RUNTIME_INTENT")
        _require(type(intent["boot"]) is str and
                 str(uuid.UUID(intent["boot"])) == intent["boot"],
                 "INVALID_RUNTIME_BOOT")
        binding = {k: intent[k] for k in (
            "namespace", "cache_scope", "intent_id", "vector_binding")}
        _require(hashlib.sha256(op._json(binding)).hexdigest() == intent["binding"],
                 "INVALID_RUNTIME_BINDING")
        _require(value["operation"] == "recover" or not qids,
                 "UNEXPECTED_RUNTIME_TARGETS")
        _require(value["operation"] == "initialize" or
                 intent["status"] == "ACTIVE", "INACTIVE_RUNTIME_INTENT")
    proof = value["proof"]
    op._keys(proof, "manifest sequence directory lock controller_pid host_boot "
             "controller_process deployment incarnation stop_proofs data_file payload")
    _require(type(proof["sequence"]) is int and proof["sequence"] > 0 and
             type(proof["controller_pid"]) is int and proof["controller_pid"] > 1,
             "INVALID_RUNTIME_PROOF")
    op._keys(proof["controller_process"], "pid ticks state argv_sha256")
    _require(proof["controller_process"]["pid"] == proof["controller_pid"] and
             proof["controller_process"]["state"] not in ("Z", "X"),
             "INVALID_CONTROLLER_IDENTITY")
    return deepcopy(value)


def _mounts():
    result = {}
    with open("/proc/self/mountinfo", encoding="ascii") as stream:
        for line in stream:
            fields = line.split()
            target = fields[4]
            if target in (_ROOT, _JOURNAL, _DATA, _CONFIG):
                _require(target not in result, "AMBIGUOUS_RUNTIME_MOUNT")
                result[target] = set(fields[5].split(","))
    return result


def _os_boundary(request):
    """Recheck what is observable without granting the payload a Docker socket.

    Host PID/daemon/clean-stop evidence is pinned in the locked receipt. Container
    PID namespace, process start ticks, mounts, data, config and network are
    independently checked here. Stdin is the retained host-controller lease.
    """
    op = _operator()
    proof = request["proof"]
    deployment, incarnation = proof["deployment"], proof["incarnation"]
    mode = incarnation["mode"]
    _require(mode in ("maintenance", "public") and
             (mode == "maintenance" or request["operation"] == "validate"),
             "RUNTIME_MODE_NOT_AUTHORIZED")
    _require(incarnation["container"] == deployment["containers"][mode] and
             socket.gethostname() == incarnation["container"][:12] and
             deployment["namespace"] == request["namespace"] and
             proof["payload"] == deployment["runtime_sha256"],
             "RUNTIME_CONTAINER_CHANGED")
    _require(os.getresuid() == (1000, 1000, 1000) and
             os.getresgid() == (1000, 1000, 1000), "RUNTIME_REQUIRES_UID_1000")
    _require(op._host_boot() == proof["host_boot"], "RUNTIME_HOST_BOOT_CHANGED")
    other = "maintenance" if mode == "public" else "public"
    settled = proof["stop_proofs"].get(deployment["containers"][other])
    _require(type(settled) is dict and bool(settled.get("clean_times_ns")),
             "RUNTIME_PRIOR_OWNER_NOT_SETTLED")
    lower = op._timestamp_ns(settled["started"])
    upper = op._timestamp_ns(settled["finished"])
    _require(lower < upper < op._timestamp_ns(incarnation["started"]) and
             all(type(t) is int and lower <= t <= upper
                 for t in settled["clean_times_ns"]), "RUNTIME_STALE_SETTLEMENT")
    mounts = _mounts()
    _require(all("ro" in mounts.get(p, ()) for p in (
        _ROOT, _JOURNAL, _CONFIG)) and
        "rw" in mounts.get(_DATA, ()), "RUNTIME_MOUNT_MODE_CHANGED")
    directory = op._path_fd(_DATA, directory=True)
    data_file = None
    try:
        data_file = op._path_fd(_DATA + "/operator.dat", metadata=True)
        info = os.fstat(data_file)
        _require(op._identity(os.fstat(directory)) ==
                 [deployment["data"]["device"], deployment["data"]["inode"]] and
                 op._identity(info) + [info.st_size] == proof["data_file"],
                 "RUNTIME_DATA_CHANGED")
    finally:
        if data_file is not None:
            os.close(data_file)
        os.close(directory)
    config = deployment["configs"][mode]
    _require(op._identity(os.stat(_CONFIG)) ==
             [config["device"], config["inode"]] and
             hashlib.sha256(op._bytes(_CONFIG)).hexdigest()
             == config["sha256"], "RUNTIME_CONFIG_CHANGED")
    if mode == "maintenance":
        _require(sorted(os.listdir(_NET)) == ["lo"],
                 "RUNTIME_NETWORK_EXPOSED")
    expected = sorted((p["ticks"], p["argv_sha256"]) for p in incarnation["processes"])
    actual = []
    for name in os.listdir(_PROC):
        if name.isdigit() and int(name) != os.getpid():
            row = op._process(int(name))
            if row and row["state"] not in ("Z", "X"):
                _require(row["state"] not in ("T", "t"), "RUNTIME_SERVER_STOPPED")
                actual.append((row["ticks"], row["argv_sha256"]))
    _require(sorted(actual) == expected and op._process(1) is not None,
             "RUNTIME_PROCESS_INVENTORY_CHANGED")
    _require(op._tree_digest(_ROOT, _journal_overlay=True) == proof["payload"],
             "RUNTIME_PAYLOAD_CHANGED")


class _Lease:
    def __init__(self, request, transport):
        self.request = _request(request)
        self.transport = transport
        self.pid = os.getpid()
        self.deadline = time.monotonic() + 70
        self.closed = False
        self.directory = None
        self.lock = None
        try:
            op = _operator()
            self.directory = op._directory(_JOURNAL)
            self.lock = os.open("lock", os.O_RDONLY | os.O_NOFOLLOW,
                                dir_fd=self.directory)
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self):
        try:
            self._check()
        except BaseException:
            self.closed = True
            raise

    def _check(self):
        op = _operator()
        _require(not self.closed and self.pid == os.getpid() and
                 time.monotonic() < self.deadline, "RUNTIME_LEASE_EXPIRED")
        _require(stat.S_ISFIFO(os.fstat(self.transport).st_mode),
                 "RUNTIME_REQUIRES_RETAINED_STDIN_PIPE")
        poll = select.poll()
        poll.register(self.transport, select.POLLIN | select.POLLHUP | select.POLLERR)
        _require(not poll.poll(0), "RUNTIME_CONTROLLER_TRANSPORT_LOST")
        current = op._directory(_JOURNAL)
        try:
            proof = self.request["proof"]
            _require(op._identity(os.fstat(current)) ==
                     op._identity(os.fstat(self.directory)) == proof["directory"],
                     "RUNTIME_JOURNAL_CHANGED")
            info = os.stat("lock", dir_fd=current, follow_symlinks=False)
            op._private(info)
            _require(op._identity(info) == op._identity(os.fstat(self.lock)) ==
                     proof["lock"], "RUNTIME_LOCK_CHANGED")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                fcntl.flock(self.lock, fcntl.LOCK_UN)
                _require(False, "RUNTIME_CONTROLLER_LOCK_LOST")
            state = op._read_at(current, "receipt.json")
        finally:
            os.close(current)
        control = state.get("controller", {})
        request = self.request
        _require(type(state.get("version")) is int and state["version"] == 1 and
                 state["state"] == "MAINTENANCE" and
                 all(state.get(k) == proof[k] for k in (
                     "manifest", "sequence", "directory", "lock", "stop_proofs")) and
                 control.get("nonce") == request["nonce"] and
                 control.get("incarnation") == proof["incarnation"] and
                 control.get("data_file") == proof["data_file"] and
                 control.get("deployment") == proof["deployment"],
                 "RUNTIME_DURABLE_BOUNDARY_CHANGED")
        expected = {"attempt": request["attempt"], "operation": request["operation"],
                    "intent_id": request["intent"]["intent_id"] if request["intent"] else None,
                    "qids": request["qids"], "outcome": "UNKNOWN",
                    "historical_root_attempted": request["historical_root_attempted"],
                    "controller_process": proof["controller_process"],
                    "host_boot": proof["host_boot"]}
        _require(control.get("pending") == expected and
                 (request["intent"] is None or
                  control.get("intents", {}).get(request["intent"]["intent_id"]) ==
                  request["intent"]), "RUNTIME_REQUEST_NOT_DURABLE")
        _os_boundary(request)

    def close(self):
        self.closed = True
        for name in ("lock", "directory"):
            fd = getattr(self, name, None)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)


class DockerMaintenanceContext:
    """Identity-only capability; no serialized fields or public constructor."""

    __slots__ = ()

    def __new__(cls):
        raise TypeError("Contexts are minted only by the fixed runtime")


def _mint(provider, lease):
    _require(type(lease) is _Lease and provider is _OWNED_PROVIDER,
             "FOREIGN_RUNTIME_PROVIDER")
    lease.check()
    context = object.__new__(DockerMaintenanceContext)
    _CONTEXTS[id(context)] = (context, provider, provider._session,
                              provider._cluster, lease, {"create_root": False})
    return context


def _validate_provider_context(context, *, provider, action, namespace,
                               cache_scope, qids, boot, binding):
    entry = _CONTEXTS.get(id(context))
    _require(type(context) is DockerMaintenanceContext and entry is not None and
             entry[0] is context and entry[1] is provider and
             entry[2] is provider._session and entry[3] is provider._cluster and
             not provider._closed, "INVALID_PROVIDER_CONTEXT")
    lease, used = entry[4:]
    try:
        _validate_bound(lease, used, provider, action, namespace, cache_scope,
                        qids, boot, binding)
    except BaseException:
        lease.closed = True
        raise


def _validate_bound(lease, used, provider, action, namespace, cache_scope,
                    qids, boot, binding):
    lease.check()
    request = lease.request
    intent = request["intent"]
    _require(type(action) is str and type(namespace) is str and type(cache_scope) is str and
             (boot is None or type(boot) is str) and
             (binding is None or type(binding) is str) and
             intent is not None and namespace == intent["namespace"] ==
             provider.namespace and cache_scope == intent["cache_scope"] ==
             provider.cache_scope and type(qids) is tuple and
             all(type(qid) is int for qid in qids) and
             qids == tuple(request["qids"]), "PROVIDER_CONTEXT_SCOPE_CHANGED")
    if request["operation"] == "initialize":
        _require(action in ("initialize", "create_root", "activate") and
                 boot == intent["boot"] and binding == intent["binding"],
                 "PROVIDER_CONTEXT_ACTION_CHANGED")
        if action == "create_root":
            _require(not used["create_root"] and
                     not request["historical_root_attempted"] and
                     intent["root_attempted"] is True, "ROOT_ATTEMPT_NOT_AUTHORIZED")
            used["create_root"] = True
    else:
        _require(request["operation"] == "recover" and action in (
            "recover", "reset", "purge", "complete") and binding is None and
            (boot == intent["boot"] or
             action == "recover" and boot is None and provider._boot is None) and
            (provider._boot is None or provider._boot == intent["boot"]),
            "PROVIDER_CONTEXT_ACTION_CHANGED")
        if action == "recover":
            _require(boot is not None or not used.get("recover_entered"),
                     "RECOVERY_BOOT_NOT_REVALIDATED")
            used["recover_entered"] = True


def _runtime_behavior():
    from aerospike_sdk.policy import Behavior, Settings
    return Behavior.DEFAULT.derive_with_changes(
        "gptcache_fixed_runtime_" + uuid.uuid4().hex,
        all=Settings(max_retries=0, total_timeout=timedelta(seconds=3),
                     socket_timeout=timedelta(seconds=1), send_key=True,
                     read_touch_ttl_percent=-1),
        reads_query=Settings(max_retries=0, total_timeout=timedelta(seconds=3),
                             allow_scans_with_where=True),
        writes=Settings(max_retries=0, total_timeout=timedelta(seconds=3),
                        durable_delete=True),
    )


def _provider(lease):
    # Only this fixed constructor owns a session; no caller definition is used.
    from aerospike_sdk.sync import ClusterDefinition
    from aerospike_sdk.policy import SystemSettings, TransactionSettings
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorage

    class _RuntimeProvider(AerospikeStorage):
        def _create_session(self):
            lease.check()
            return super()._create_session()

        def create(self):
            lease.check()
            super().create()
            _, root = self._read("root")
            intent = lease.request["intent"]
            _require(intent is not None and root["boot"] == intent["boot"] and
                     root["bind"] == intent["binding"], "RUNTIME_ROOT_INTENT_MISMATCH")
            lease.check()

        def _point(self, builder, key):
            lease.check()
            result = super()._point(builder, key)
            lease.check()
            return result

        def _scan(self, kind, *predicates):
            lease.check()
            result = super()._scan(kind, *predicates)
            lease.check()
            return result

    request = lease.request
    lease.check()
    return _RuntimeProvider(
        cluster_definition=ClusterDefinition("127.0.0.1", 3000).using_services_alternate(
            False).with_system_settings(SystemSettings(transactions=TransactionSettings(
                implicit_batch_write_transactions=False))),
        namespace=request["namespace"],
        cache_scope=request["intent"]["cache_scope"] if request["intent"] else "__runtime_ready__",
        behavior=_runtime_behavior(),
    )


def _dispatch(request, transport):
    global _OWNED_PROVIDER
    _require(sys.version_info >= (3, 11), "SDK_RUNTIME_REQUIRES_PYTHON_311")
    request = _request(request)
    response = {k: request[k] for k in ("version", "attempt", "nonce", "operation")}
    response.update(uid=os.getuid(), pid=os.getpid(), status="REFUSED", results=[])
    lease, provider, context = None, None, None
    try:
        _require(_OWNED_PROVIDER is None, "RUNTIME_ALREADY_OWNED")
        lease = _Lease(request, transport)
        provider = _provider(lease)
        _OWNED_PROVIDER = provider
        operation = request["operation"]
        intent = request["intent"]
        if operation == "ready":
            provider._configure()
            lease.check()
            provider._configuration()
        elif operation == "validate":
            provider.create()
            _, root = provider._read("root")
            _require(root["boot"] == intent["boot"] and root["bind"] == intent["binding"],
                     "VALIDATION_INTENT_MISMATCH")
        else:
            context = _mint(provider, lease)
            if operation == "initialize":
                provider._initialize_scope(
                    initialization_context=context, boot=intent["boot"],
                    binding=intent["binding"],
                    root_attempted=request["historical_root_attempted"])
            else:
                outcomes = provider.recover_session_purge(
                    request["qids"], recovery_context=context)
                _require(type(outcomes) is dict and
                         list(outcomes) == request["qids"], "INCOMPLETE_RECOVERY_RESULT")
                for qid in request["qids"]:
                    value = outcomes[qid]
                    if isinstance(value, Exception):
                        status = ("FAILED" if getattr(value, "phase", None) ==
                                  "TERMINAL_REJECTION" else "UNKNOWN")
                    else:
                        _require(type(value) is str and value in (
                            "PURGED", "ABSENT", "NOT_ATTEMPTED"),
                            "INVALID_RECOVERY_RESULT")
                        status = value
                    response["results"].append({"qid": qid, "status": status})
        lease.check()
        response["status"] = ("OK" if all(
            r["status"] in ("PURGED", "ABSENT") for r in response["results"]) else "PARTIAL")
    except Exception as exc:
        # In particular do not stringify SDK/Docker/JSON exceptions.
        _diagnostic(exc)
        response["status"] = "REFUSED"
        if request["operation"] == "recover":
            response["results"] = [{"qid": qid, "status": "UNKNOWN"} for qid in request["qids"]]
    finally:
        if context is not None:
            _CONTEXTS.pop(id(context), None)
        if provider is not None:
            try:
                provider.close()
            except Exception:
                response["status"] = "REFUSED"
        _OWNED_PROVIDER = None
        if lease is not None:
            lease.close()
    return response


def _diagnostic(exc):
    """Bounded source coordinates only; exception values are never serialized."""
    allowed = {"aerospike_storage.py", "aerospike_operator.py",
               "_aerospike_operator_runtime.py"}
    frames = []
    tb = exc.__traceback__
    while tb is not None:
        code = tb.tb_frame.f_code
        filename = os.path.basename(code.co_filename)
        if filename in allowed:
            frames.append({"file": filename, "line": tb.tb_lineno})
        tb = tb.tb_next
    kind = type(exc).__name__
    if kind not in {"AerospikeStorageError", "OperatorRefusal", "TypeError",
                    "ValueError", "RuntimeError", "KeyError", "OSError"}:
        kind = "Exception"
    sys.stderr.write(_operator()._json(
        {"diagnostic": "RUNTIME_REFUSED", "type": kind, "frames": frames[-8:]}
    ).decode("ascii") + "\n")


def _read_request(fd):
    raw = bytearray()
    while len(raw) <= 512 * 1024:
        part = os.read(fd, 1)
        _require(part, "RUNTIME_REQUEST_EOF")
        if part == b"\n":
            return _request(_operator()._load(bytes(raw)))
        raw.extend(part)
    _require(False, "RUNTIME_REQUEST_TOO_LARGE")


def _main():
    if sys.version_info < (3, 11):
        return 2
    # -I ignores PYTHONPATH. The mounted fixed source is the only added path.
    if os.path.abspath(__file__) != "/op/source/gptcache/manager/scalar_data/_aerospike_operator_runtime.py":
        return 2
    sys.path.insert(0, "/op/source")
    sys.modules[_MODULE] = sys.modules[__name__]
    os.environ.clear()
    os.environ.update(PATH="/usr/bin:/bin", LANG="C", PYTHONDONTWRITEBYTECODE="1")
    try:
        response = _dispatch(_read_request(0), 0)
        raw = _operator()._json(response)
        _require(len(raw) <= 1024 * 1024, "RUNTIME_RESPONSE_TOO_LARGE")
        os.write(1, raw + b"\n")
        return 0
    except Exception:
        return 2


def _sha(path):
    with open(path, "rb") as stream:
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _copy(source, target):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _require(_sha(source) == _sha(target), "BUNDLE_MEMBER_COLLISION")
        return
    shutil.copyfile(source, target)
    target.chmod(0o555 if os.access(source, os.X_OK) else 0o444)


def _copy_source(source, target):
    source = Path(source)
    for parent, directories, files in os.walk(source, followlinks=False):
        directories[:] = sorted(d for d in directories if d != "__pycache__")
        for name in directories + files:
            path = Path(parent) / name
            _require(not path.is_symlink(), "SOURCE_LINK_NOT_SUPPORTED")
        for name in sorted(files):
            if name.endswith((".pyc", ".pyo")):
                continue
            _require(not name.endswith(".pth") and name not in (
                "sitecustomize.py", "usercustomize.py"), "SOURCE_IMPORT_HOOK")
            path = Path(parent) / name
            _copy(path, Path(target) / path.relative_to(source))


def _verify_development_native(native):
    expected = _DEVELOPMENT_NATIVE_ARTIFACTS.get(native.version)
    extensions = [native.locate_file(p) for p in native.files or ()
                  if str(p).endswith(".so") and "_aerospike_async_native" in str(p)]
    _require(expected is not None and len(extensions) == 1 and
             _sha(extensions[0]) == expected,
             "DEVELOPMENT_NATIVE_REQUIRES_APPROVED_CP314_ARTIFACT")


def _dependencies():
    """Resolve installed metadata only. No index, installer or import fallback."""
    from packaging.requirements import Requirement

    found, todo = {}, ["aerospike-sdk", "numpy", "cachetools", "requests"]
    while todo:
        name = todo.pop()
        key = re.sub("[-_.]+", "-", name).lower()
        if key in found:
            continue
        try:
            dist = metadata.distribution(name)
        except metadata.PackageNotFoundError:
            raise _operator().OperatorRefusal(
                "BUNDLE_DEPENDENCY_MISSING_INSTALL_TRUSTED_" + key.upper()) from None
        found[key] = dist
        for literal in dist.requires or ():
            req = Requirement(literal)
            if req.marker is not None and not req.marker.evaluate({"extra": ""}):
                continue
            try:
                selected = metadata.distribution(req.name)
            except metadata.PackageNotFoundError:
                raise _operator().OperatorRefusal(
                    "BUNDLE_DEPENDENCY_MISSING_INSTALL_TRUSTED_" +
                    req.name.upper().replace("-", "_")) from None
            _require(not req.specifier or selected.version in req.specifier,
                     "BUNDLE_DEPENDENCY_VERSION_MISMATCH")
            todo.append(req.name)
    sdk = found["aerospike-sdk"]
    transport = [k for k in found if k in ("aerospike-async", "aerospike-native")]
    _require(len(transport) == 1 and (
        sdk.version, transport[0], found[transport[0]].version) in _BUNDLE_SDK_PAIRS,
             "UNSUPPORTED_BUNDLE_SDK_PAIR")
    if sdk.version == "0.9.0a5":
        _verify_development_native(found["aerospike-async"])
    return found


def _distribution(dist, target, editable_sdk=False):
    op = _operator()
    rows = list(csv.reader((dist.read_text("RECORD") or "").splitlines()))
    _require(bool(rows), "DEPENDENCY_RECORD_REQUIRED")
    provenance, editable_origin = {}, None
    for name, encoded, size in rows:
        path = Path(name)
        # Entrypoints outside site-packages are not imported by the payload.
        if path.is_absolute() or ".." in path.parts:
            continue
        if name.endswith((".pyc", ".pyo")) or "__pycache__" in path.parts:
            continue
        if editable_sdk and (name.endswith(".pth") or path.name.startswith("__editable__")):
            continue
        _require(not name.endswith(".pth") and path.name not in (
            "sitecustomize.py", "usercustomize.py"), "DEPENDENCY_IMPORT_HOOK")
        source = Path(dist.locate_file(name))
        raw = source.read_bytes()
        if encoded:
            algorithm, digest = encoded.split("=", 1)
            _require(algorithm == "sha256" and
                     base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b"=").decode()
                     == digest and len(raw) == int(size), "DEPENDENCY_RECORD_MISMATCH")
        else:
            _require(path.name in ("RECORD", "direct_url.json", "INSTALLER", "REQUESTED"),
                     "DEPENDENCY_MEMBER_UNHASHED")
        _copy(source, Path(target) / path)
        provenance[name] = hashlib.sha256(raw).hexdigest()
    if editable_sdk:
        origin = op._load(dist.read_text("direct_url.json") or "{}")
        url = urlparse(origin.get("url", ""))
        _require(url.scheme == "file" and
                 origin.get("dir_info", {}).get("editable") is True,
                 "SDK_EDITABLE_ORIGIN_REQUIRED")
        source = Path(unquote(url.path)) / "aerospike_sdk"
        _require(source.is_dir(), "SDK_SOURCE_MISSING")
        head = subprocess.run(
            ["/usr/bin/git", "-C", str(source.parent), "rev-parse", "HEAD"],
            capture_output=True, timeout=10, env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        status = subprocess.run(
            ["/usr/bin/git", "-C", str(source.parent), "status", "--porcelain",
             "--untracked-files=no"], capture_output=True, timeout=10,
            env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        _require(dist.version == "0.9.0a5" and head.returncode == status.returncode == 0 and
                 head.stdout.strip() ==
                 b"2a7f45248d66d8904b4daf232207baa08345a0cd" and not status.stdout,
                 "EDITABLE_SDK_REQUIRES_CLEAN_APPROVED_DEVELOPMENT_REVISION")
        tracked = subprocess.run(
            ["/usr/bin/git", "-C", str(source.parent), "ls-files", "-z", "--",
             "aerospike_sdk"], capture_output=True, timeout=10,
            env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        actual = {p.relative_to(source.parent).as_posix() for p in source.rglob("*")
                  if p.is_file() and "__pycache__" not in p.parts and
                  p.suffix not in (".pyc", ".pyo")}
        _require(tracked.returncode == 0 and actual ==
                 set(tracked.stdout.decode("utf-8").rstrip("\0").split("\0")),
                 "EDITABLE_SDK_UNTRACKED_OR_MISSING_PACKAGE_MEMBER")
        editable_origin = {"source": str(source), "commit": head.stdout.strip().decode("ascii")}
        _copy_source(source, Path(target) / "aerospike_sdk")
        # Replace editable ownership with a complete installed RECORD, not a .pth.
        info = next(p for p in Path(target).glob("aerospike_sdk-*.dist-info"))
        record = info / "RECORD"
        record.chmod(0o600)
        with record.open("w", newline="") as stream:
            writer = csv.writer(stream)
            for path in sorted(list((Path(target) / "aerospike_sdk").rglob("*")) +
                               list(info.rglob("*"))):
                if path.is_file() and path != record:
                    raw = path.read_bytes()
                    writer.writerow([path.relative_to(target).as_posix(),
                                     "sha256=" + base64.urlsafe_b64encode(
                                         hashlib.sha256(raw).digest()).rstrip(b"=").decode(),
                                     len(raw)])
            writer.writerow([record.relative_to(target).as_posix(), "", ""])
        record.chmod(0o444)
        provenance["frozen_sdk_source"] = {
            p.relative_to(source).as_posix(): _sha(p)
            for p in sorted(source.rglob("*")) if p.is_file() and
            "__pycache__" not in p.parts and p.suffix not in (".pyc", ".pyo")}
    return {"name": dist.metadata["Name"], "version": dist.version,
            "members": provenance, "editable_origin": editable_origin}


def _libraries(root):
    """Host-built ELF closure including the GLIBC loader, never image Python."""
    pending = []
    for path in Path(root).rglob("*"):
        if path.is_file():
            with path.open("rb") as stream:
                if stream.read(4) == b"\x7fELF":
                    pending.append(path)
    pending.append(Path("/lib64/ld-linux-x86-64.so.2").resolve())
    visited, origins = set(), {}
    while pending:
        path = pending.pop().resolve()
        if path in visited:
            continue
        visited.add(path)
        dynamic = subprocess.run(
            ["/usr/bin/readelf", "-d", str(path)], capture_output=True, timeout=15,
            env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        table = dynamic.stdout.decode("ascii", "strict")
        needed = re.findall(r"\(NEEDED\).*?\[([^]]+)\]", table)
        _require(dynamic.returncode == 0 and all(
            "/" not in name or name.startswith(("$ORIGIN", "${ORIGIN}"))
            for name in needed), "BUNDLE_ABSOLUTE_ELF_DEPENDENCY")
        location = path if Path(root) in path.parents else Path(root) / "lib" / path.name
        searches = re.findall(r"\((?:RPATH|RUNPATH)\).*?\[([^]]+)\]", table)
        searches += [name for name in needed if "/" in name]
        for search in searches:
            for item in search.split(":"):
                _require(item.startswith(("$ORIGIN", "${ORIGIN}")),
                         "BUNDLE_NONRELOCATABLE_ELF_SEARCH")
                expanded = item.replace("${ORIGIN}", str(location.parent)).replace(
                    "$ORIGIN", str(location.parent))
                resolved = Path(expanded).resolve()
                _require(resolved == Path(root) or Path(root) in resolved.parents,
                         "BUNDLE_ELF_SEARCH_ESCAPES_ROOT")
        result = subprocess.run(
            ["/usr/bin/ldd", str(path)], capture_output=True, timeout=15,
            env={"PATH": "/usr/bin:/bin", "LANG": "C",
                 "LD_LIBRARY_PATH": str(Path(root) / "lib")})
        output = result.stdout.decode("ascii", "strict")
        _require(result.returncode == 0 and "not found" not in output,
                 "BUNDLE_ELF_DEPENDENCY_UNRESOLVED")
        dependencies = re.findall(r"(?:=>\s+|^\s*)(/[^\s()]+)", output, re.M)
        for literal in dependencies:
            source = Path(literal).resolve()
            name = Path(literal).name
            digest = _sha(source)
            _require(name not in origins or origins[name]["sha256"] == digest,
                     "BUNDLE_ELF_SONAME_COLLISION")
            origin = ("bundle:" + source.relative_to(root).as_posix()
                      if Path(root) in source.parents else str(source))
            origins[name] = {"source": origin, "sha256": digest}
            _copy(source, Path(root) / "lib" / name)
            pending.append(source)
    _require("ld-linux-x86-64.so.2" in origins and "libc.so.6" in origins,
             "BUNDLE_GLIBC_CLOSURE_MISSING")
    return origins


def _inventory(root):
    return {p.relative_to(root).as_posix(): _sha(p)
            for p in sorted(Path(root).rglob("*")) if p.is_file() and
            p.relative_to(root).as_posix() != "runtime.json"}


def _verify_members(root, packages):
    op = _operator()
    sites = list(root.glob("python/lib/python*/site-packages"))
    _require(len(sites) == 1, "BUNDLE_SITE_PACKAGES_LAYOUT")
    site, expected, sdk_members = sites[0], set(), {}
    for package in packages:
        members = package["members"]
        _require(type(members) is dict and bool(members), "BUNDLE_PACKAGE_MEMBERS_MISSING")
        sdk = package["name"].lower().replace("_", "-") == "aerospike-sdk"
        for name, digest in members.items():
            if name == "frozen_sdk_source":
                _require(sdk and type(digest) is dict, "BUNDLE_SDK_SOURCE_PROVENANCE")
                sdk_members.update(digest)
                continue
            _require(not Path(name).is_absolute() and ".." not in Path(name).parts,
                     "BUNDLE_MEMBER_ESCAPES_ROOT")
            if sdk and name.startswith("aerospike_sdk/"):
                sdk_members[name[len("aerospike_sdk/"):]] = digest
                continue
            expected.add(name)
            # SDK RECORD is deliberately rewritten to relocate editable/wheel
            # ownership. The original is retained as provenance, not claimed as
            # the hash of that rewritten file. All executable members still bind.
            if not (sdk and name.endswith(".dist-info/RECORD")):
                _require(_sha(site / name) == digest, "BUNDLE_PACKAGE_MEMBER_MISMATCH")
    actual = {p.relative_to(site).as_posix() for p in site.rglob("*") if p.is_file()}
    _require(expected == actual, "BUNDLE_UNACCOUNTED_SITE_PACKAGE")
    sdk_source = root / "source/aerospike_sdk"
    actual_sdk = {p.relative_to(sdk_source).as_posix(): _sha(p)
                  for p in sdk_source.rglob("*") if p.is_file()}
    _require(bool(sdk_members) and sdk_members == actual_sdk,
             "BUNDLE_SDK_SOURCE_MEMBER_MISMATCH")


def _verify_bundle(root):
    """Verify concrete member provenance, not merely the presence of ELF magic."""
    op = _operator()
    root = Path(root)
    op._tree_digest(str(root))
    description = op._load(op._bytes(str(root / "runtime.json"), limit=8 * 1024 * 1024))
    op._keys(description, "version platform python packages libraries files")
    _require(description["version"] == 1 and description["platform"] ==
             "linux-x86_64" and description["files"] == _inventory(root),
             "BUNDLE_PROVENANCE_CHANGED")
    packages = {p["name"].lower().replace("_", "-"): p for p in description["packages"]}
    _require("aerospike-sdk" in packages and "numpy" in packages and
             ("aerospike-async" in packages) != ("aerospike-native" in packages),
             "BUNDLE_SDK_PROVENANCE_MISSING")
    transport = "aerospike-async" if "aerospike-async" in packages else "aerospike-native"
    _require((packages["aerospike-sdk"]["version"], transport,
              packages[transport]["version"]) in _BUNDLE_SDK_PAIRS,
             "UNSUPPORTED_BUNDLE_SDK_PAIR")
    _verify_members(root, description["packages"])
    source = Path(__file__).parents[2]
    current = {p.relative_to(source).as_posix(): _sha(p)
               for p in sorted(source.rglob("*")) if p.is_file() and
               "__pycache__" not in p.parts and p.suffix not in (".pyc", ".pyo")}
    frozen = {p.relative_to(root / "source/gptcache").as_posix(): _sha(p)
              for p in sorted((root / "source/gptcache").rglob("*")) if p.is_file()}
    _require(current == frozen, "BUNDLE_FRAMEWORK_SOURCE_MISMATCH")
    for name, origin in description["libraries"].items():
        _require(_sha(root / "lib" / name) == origin["sha256"],
                 "BUNDLE_ELF_PROVENANCE_CHANGED")
    return description


def _prepare_runtime_bundle(journal_dir):
    """Explicit artifact preparation from this installed trusted environment.

    Call before writing/enrolling the deployment manifest or creating Docker
    resources. Returns the exact manifest fragment and full local provenance.
    No custom source, program, SDK, installer or executable option is accepted.
    The destination must not exist; incomplete staging never becomes runtime/.
    """
    op = _operator()
    _require(sys.version_info >= (3, 11) and sys.platform == "linux" and
             os.uname().machine == "x86_64" and os.getuid() == 1000,
             "BUNDLE_REQUIRES_LINUX_X86_64_UID1000_PYTHON311")
    dependencies = _dependencies()
    # Resolve every dependency before touching even the artifact destination.
    directory = op._directory(os.fspath(journal_dir))
    os.close(directory)
    target = Path(journal_dir) / "runtime"
    _require(not target.exists() and not target.is_symlink(), "BUNDLE_ALREADY_EXISTS")
    with tempfile.TemporaryDirectory(prefix=".runtime-build-", dir=journal_dir) as temporary:
        root = Path(temporary)
        (root / "journal").mkdir()
        base = Path(sys.base_prefix)
        _copy(Path(sys.executable).resolve(), root / "python/bin/python3")
        # This is a headless SDK payload, not a GUI Python installation. Tk's
        # build-machine RPATH and Tcl data are deliberately not part of it.
        # Flatten trusted symlinks into independent files.
        stdlib = Path(sysconfig.get_path("stdlib"))
        for parent, directories, files in os.walk(stdlib, followlinks=False):
            directories[:] = sorted(d for d in directories if d not in (
                "site-packages", "__pycache__", "tkinter"))
            for name in sorted(files):
                if name.endswith((".pyc", ".pyo")) or name.startswith("_tkinter."):
                    continue
                path = Path(parent) / name
                _copy(path.resolve(), root / "python/lib" / stdlib.name /
                      path.relative_to(stdlib))
        for path in (base / "lib").glob("libpython*.so*"):
            _copy(path.resolve(), root / "lib" / path.name)
        site = root / "python/lib" / stdlib.name / "site-packages"
        site.mkdir(parents=True)
        packages = []
        for name, dist in sorted(dependencies.items()):
            origin = dist.read_text("direct_url.json")
            editable = bool(origin and op._load(origin).get("dir_info", {}).get("editable"))
            _require(not editable or name == "aerospike-sdk",
                     "ONLY_SDK_EDITABLE_SOURCE_SUPPORTED")
            packages.append(_distribution(dist, site, editable_sdk=editable))
        sdk_source = site / "aerospike_sdk"
        _require(sdk_source.is_dir(), "BUNDLE_SDK_PACKAGE_MISSING")
        # One SDK location, also discoverable by installed metadata ownership.
        shutil.move(str(sdk_source), str(root / "source-sdk"))
        (root / "source").mkdir()
        shutil.move(str(root / "source-sdk"), str(root / "source/aerospike_sdk"))
        sdk_record = next(site.glob("aerospike_sdk-*.dist-info")) / "RECORD"
        sdk_record.chmod(0o600)
        rows = list(csv.reader(sdk_record.read_text().splitlines()))
        with sdk_record.open("w", newline="") as stream:
            writer = csv.writer(stream)
            for name, digest, size in rows:
                if name.startswith("aerospike_sdk/"):
                    name = os.path.relpath(root / "source" / name, site)
                writer.writerow([name, digest, size])
        sdk_record.chmod(0o444)
        _copy_source(Path(__file__).parents[2], root / "source/gptcache")
        libraries = _libraries(root)
        description = {
            "version": 1, "platform": "linux-x86_64",
            "python": {"version": sys.version, "base": str(base),
                       "executable_sha256": _sha(Path(sys.executable).resolve()),
                       "excluded_optional_stdlib": ["tkinter", "_tkinter"]},
            "packages": packages, "libraries": libraries, "files": _inventory(root)}
        (root / "runtime.json").write_bytes(op._json(description))
        (root / "runtime.json").chmod(0o444)
        for parent, directories, files in os.walk(root):
            for name in directories:
                (Path(parent) / name).chmod(0o555)
        _verify_bundle(root)
        # Relocation/import self-test in a fresh isolated process, no SDK connect.
        program = (
            "import sys,importlib;sys.path.insert(0,sys.argv[1]);"
            "import ssl,sqlite3,numpy,aerospike_sdk;"
            "from gptcache.manager.scalar_data.aerospike_storage import _module_owned;"
            "from importlib.metadata import distribution;"
            "from gptcache.manager.scalar_data._aerospike_operator_runtime import _runtime_behavior;"
            "from aerospike_sdk.policy import Mode,OpKind,OpShape,SystemSettings,TransactionSettings;"
            "b=_runtime_behavior();"
            "assert all(b.get_settings(OpKind.READ,OpShape.QUERY,m).total_timeout.total_seconds()==3 "
            "for m in Mode);"
            "assert all(b.get_settings(k,s,m).max_retries==0 "
            "for k in OpKind for s in OpShape for m in Mode);"
            "_module_owned(distribution('aerospike-sdk'),aerospike_sdk);"
            "_module_owned(distribution(sys.argv[3]),"
            "importlib.import_module(sys.argv[3].replace('-','_')));"
            "assert SystemSettings(transactions=TransactionSettings("
            "implicit_batch_write_transactions=False)).transactions."
            "implicit_batch_write_transactions is False;"
            "assert distribution('aerospike-sdk').version in ('0.9.0a5','1.0.0');"
            "assert sys.prefix==sys.argv[2]"
        )
        result = subprocess.run(
            [str(root / "lib/ld-linux-x86-64.so.2"), "--library-path", str(root / "lib"),
             str(root / "python/bin/python3"), "-I", "-B", "-c", program,
             str(root / "source"), str(root / "python"),
             "aerospike-async" if "aerospike-async" in dependencies else "aerospike-native"],
            capture_output=True, timeout=30, env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        _require(result.returncode == 0, "BUNDLE_RELOCATION_IMPORT_SELFTEST_FAILED")
        root.chmod(0o555)
        os.rename(root, target)
    digest = op._tree_digest(str(target))
    return {"manifest": {"runtime_sha256": digest}, "provenance": description}


if __name__ == "__main__":
    sys.exit(_main())
