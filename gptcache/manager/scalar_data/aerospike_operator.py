"""Local, pre-enrolled Docker maintenance controller (optional, SDK lazy).

The private SDK runtime is a separate fixed payload, never a user program hook.
Missing runtime/bundle is a refusal before any Docker or journal mutation.
"""

import argparse
import calendar
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import fcntl
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid


class OperatorRefusal(RuntimeError):
    """A redacted, fixed reason code; never includes process or server output."""


def _require(condition, code):
    if not condition:
        raise OperatorRefusal(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":"), allow_nan=False).encode("ascii")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "DUPLICATE_JSON_FIELD")
        result[key] = value
    return result


def _load(raw):
    try:
        return json.loads(raw, object_pairs_hook=_pairs,
                          parse_constant=lambda value: _require(False, "INVALID_JSON"))
    except (ValueError, TypeError, UnicodeError):
        raise OperatorRefusal("INVALID_JSON") from None


def _directory(path):
    path = os.fspath(path)
    _require(type(path) is str and path.startswith("/") and
             path == os.path.normpath(path), "NONCANONICAL_DIRECTORY")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.split("/")[1:]:
            if part:
                new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=fd)
                os.close(fd)
                fd = new
        info = os.fstat(fd)
        _require(info.st_uid == os.getuid() and
                 stat.S_IMODE(info.st_mode) == 0o700, "JOURNAL_DIRECTORY_PERMISSIONS")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _private(info):
    _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and
             stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1,
             "JOURNAL_FILE_PERMISSIONS")


def _identity(info):
    return [info.st_dev, info.st_ino]


def _read_at(fd, name):
    file_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    try:
        _private(os.fstat(file_fd))
        with os.fdopen(file_fd, "rb", closefd=False) as stream:
            raw = stream.read(1024 * 1024 + 1)
        _require(len(raw) <= 1024 * 1024, "JOURNAL_TOO_LARGE")
        return _load(raw)
    finally:
        os.close(file_fd)


def _gate_valid(value):
    if value.get("state") == "PUBLIC":
        control = value.get("controller", {})
        _require(control.get("stage") == "finished" and
                 control.get("public_verified") is True and
                 control.get("pending") is None, "INVALID_JOURNAL")


class _Journal:
    """One pre-enrolled stable lock inode and a durable, fail-closed launch gate.

    Enrollment is deliberately separate from opening an existing deployment.
    Missing files are never silently recreated by an operator successor.
    """

    @staticmethod
    def enroll(path, manifest_sha256):
        _require(type(manifest_sha256) is str and
                 re.fullmatch("[0-9a-f]{64}", manifest_sha256), "INVALID_MANIFEST_HASH")
        directory = _directory(path)
        lock = None
        try:
            members = os.listdir(directory)
            _require(not members or members == ["runtime"], "JOURNAL_NOT_EMPTY")
            if members:
                from gptcache.manager.scalar_data._aerospike_operator_runtime import _verify_bundle
                _tree_digest(os.path.join(path, "runtime"))
                _verify_bundle(os.path.join(path, "runtime"))
            lock = os.open("lock", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                           0o600, dir_fd=directory)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.fsync(lock)
            state = {"version": 1, "manifest": manifest_sha256, "sequence": 0,
                     "directory": _identity(os.fstat(directory)),
                     "lock": _identity(os.fstat(lock)), "state": "MAINTENANCE",
                     "stop_intents": {}, "stop_proofs": {}}
            receipt = os.open("receipt.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                              os.O_NOFOLLOW, 0o600, dir_fd=directory)
            with os.fdopen(receipt, "wb") as stream:
                stream.write(_json(state))
                stream.flush()
                os.fsync(stream.fileno())
            os.fsync(directory)
        finally:
            if lock is not None:
                os.close(lock)
            os.close(directory)

    @staticmethod
    def status(path):
        """Read only; absent/corrupt/unregistered state never permits launches."""
        directory = _directory(path)
        try:
            value = _read_at(directory, "receipt.json")
            _require(type(value) is dict and value.get("version") == 1 and
                     type(value.get("version")) is int and
                     value.get("state") in ("MAINTENANCE", "PUBLIC") and
                     type(value.get("sequence")) is int and value["sequence"] >= 0 and
                     value.get("directory") == _identity(os.fstat(directory)),
                     "INVALID_JOURNAL")
            lock = os.stat("lock", dir_fd=directory, follow_symlinks=False)
            _private(lock)
            _require(value.get("lock") == _identity(lock), "LOCK_REPLACED")
            _gate_valid(value)
            return {"state": value["state"], "launch_blocked": value["state"] != "PUBLIC",
                    "sequence": value["sequence"]}
        finally:
            os.close(directory)

    def __init__(self, path, manifest_sha256):
        _require(type(manifest_sha256) is str and
                 re.fullmatch("[0-9a-f]{64}", manifest_sha256), "INVALID_MANIFEST_HASH")
        self.path = os.fspath(path)
        self.directory = _directory(self.path)
        self.lock = None
        self.failed = False
        self.pid = os.getpid()
        try:
            self.lock = os.open("lock", os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK,
                                dir_fd=self.directory)
            _private(os.fstat(self.lock))
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._state = _read_at(self.directory, "receipt.json")
            _require(type(self._state) is dict and set(self._state) in ({
                "version", "manifest", "sequence", "directory", "lock", "state",
                "stop_intents", "stop_proofs"}, {
                "version", "manifest", "sequence", "directory", "lock", "state",
                "stop_intents", "stop_proofs", "controller"}), "INVALID_JOURNAL")
            _require(type(self._state["version"]) is int and self._state["version"] == 1
                     and self._state["manifest"] == manifest_sha256 and
                     self._state["state"] in ("MAINTENANCE", "PUBLIC") and
                     type(self._state["sequence"]) is int and self._state["sequence"] >= 0
                     and type(self._state["stop_intents"]) is dict and
                     type(self._state["stop_proofs"]) is dict, "INVALID_JOURNAL")
            _gate_valid(self._state)
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self):
        _require(not self.failed and self.lock is not None and self.pid == os.getpid(),
                 "JOURNAL_NOT_HELD")
        info = os.stat("lock", dir_fd=self.directory, follow_symlinks=False)
        _private(info)
        _require(_identity(info) == _identity(os.fstat(self.lock)) ==
                 self._state["lock"], "LOCK_REPLACED")
        current = _directory(self.path)
        try:
            _require(_identity(os.fstat(current)) == _identity(os.fstat(self.directory)) ==
                     self._state["directory"], "JOURNAL_DIRECTORY_REPLACED")
        finally:
            os.close(current)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _require(_read_at(self.directory, "receipt.json") == self._state,
                 "JOURNAL_CHANGED")

    @property
    def state(self):
        self.check()
        return deepcopy(self._state)

    def store(self, state):
        self.check()
        _require(set(state) == set(self._state) and
                 all(state[key] == self._state[key] for key in
                     ("version", "manifest", "directory", "lock", "sequence", "state")),
                 "JOURNAL_IDENTITY_OR_GATE_CHANGE")
        self._write(state)

    def _control(self, controller, *, public=False):
        """Controller-only gate transition; generic store cannot open the gate."""
        self.check()
        value = deepcopy(self._state)
        if public:
            _require(controller.get("stage") == "finished" and
                     controller.get("pending") is None and
                     controller.get("public_verified") is True,
                     "PUBLIC_NOT_VERIFIED")
        value["controller"] = deepcopy(controller)
        value["state"] = "PUBLIC" if public else "MAINTENANCE"
        self._write(value)

    def _write(self, state):
        value = deepcopy(state)
        value["sequence"] += 1
        raw = _json(value)
        _require(len(raw) <= 1024 * 1024, "JOURNAL_TOO_LARGE")
        temporary = ".receipt-" + uuid.uuid4().hex
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=self.directory)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            self.check()
            os.replace(temporary, "receipt.json", src_dir_fd=self.directory,
                       dst_dir_fd=self.directory)
            os.fsync(self.directory)
            self._state = value
        except BaseException:
            self.failed = True
            raise

    def close(self):
        if self.lock is not None:
            os.close(self.lock)
            self.lock = None
        if self.directory is not None:
            os.close(self.directory)
            self.directory = None

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


def _timestamp_ns(value):
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z",
                         value if type(value) is str else "")
    _require(match is not None, "INVALID_INCARNATION_TIME")
    try:
        seconds = calendar.timegm(datetime.strptime(
            match[1], "%Y-%m-%dT%H:%M:%S").timetuple())
    except ValueError:
        raise OperatorRefusal("INVALID_INCARNATION_TIME") from None
    _require(seconds > 0, "NEVER_STARTED")
    return seconds * 1000000000 + int((match[2] or "").ljust(9, "0"))


def _container_id(value):
    _require(type(value) is str and re.fullmatch("[0-9a-f]{64}", value),
             "INVALID_CONTAINER_ID")
    return value


class _Docker:
    """Fixed local Docker read/stop surface; no user command or argv passthrough."""

    @staticmethod
    def _run(args):
        try:
            result = subprocess.run(
                ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock"] + args,
                shell=False, capture_output=True,
                timeout=75, env={"PATH": "/usr/bin:/bin", "LANG": "C"})
        except (OSError, subprocess.SubprocessError):
            raise OperatorRefusal("DOCKER_TRANSPORT_FAILED") from None
        _require(result.returncode == 0, "DOCKER_COMMAND_FAILED")
        _require(len(result.stdout) + len(result.stderr) <= 4 * 1024 * 1024,
                 "DOCKER_OUTPUT_TOO_LARGE")
        return result.stdout, result.stderr

    def inspect(self, container):
        raw, _ = self._run(["inspect", _container_id(container)])
        rows = _load(raw)
        _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict,
                 "INVALID_INSPECT")
        return rows[0]

    def pids(self, container):
        raw, _ = self._run(["top", _container_id(container), "-eo", "pid"])
        try:
            lines = raw.decode("ascii").splitlines()
            _require(lines and lines[0].strip() == "PID", "INVALID_PROCESS_INVENTORY")
            values = [int(line.strip()) for line in lines[1:]]
            _require(values and all(pid > 0 for pid in values) and
                     len(values) == len(set(values)), "INVALID_PROCESS_INVENTORY")
            return sorted(values)
        except (ValueError, UnicodeError):
            raise OperatorRefusal("INVALID_PROCESS_INVENTORY") from None

    def logs(self, container, started, finished):
        _timestamp_ns(started)
        _timestamp_ns(finished)
        # Settlement needs a shutdown marker, not an unbounded lifetime transcript.
        out, err = self._run(["logs", "--timestamps", "--tail", "2048",
                              "--since", started, "--until",
                              finished, _container_id(container)])
        try:
            return (out + b"\n" + err).decode("utf-8")
        except UnicodeError:
            raise OperatorRefusal("INVALID_LOG_ENCODING") from None

    def stop(self, container):
        self._run(["stop", "--time", "60", _container_id(container)])

    def start(self, container):
        self._run(["start", _container_id(container)])

    def daemon(self):
        out, _ = self._run(["info", "--format", "{{.ID}}"])
        return out.decode("ascii").strip()

    def image(self, image):
        _require(type(image) is str and re.fullmatch("sha256:[0-9a-f]{64}", image),
                 "INVALID_IMAGE_ID")
        out, _ = self._run(["image", "inspect", image])
        rows = _load(out)
        _require(type(rows) is list and len(rows) == 1 and rows[0]["Id"] == image,
                 "INVALID_IMAGE_INSPECT")
        return rows[0]["Config"]


def _process(pid):
    """Host PID identity; start ticks prevent PID-reuse confusion."""
    _require(type(pid) is int and pid > 0, "INVALID_PROCESS_ID")
    try:
        with open("/proc/{}/stat".format(pid), "rb") as stream:
            raw = stream.read()
        fields = raw.rsplit(b")", 1)[1].split()
        state, ticks = fields[0].decode("ascii"), int(fields[19])
        with open("/proc/{}/cmdline".format(pid), "rb") as stream:
            digest = hashlib.sha256(stream.read()).hexdigest()
        with open("/proc/{}/stat".format(pid), "rb") as stream:
            again = stream.read().rsplit(b")", 1)[1].split()
        _require(int(again[19]) == ticks, "PROCESS_IDENTITY_CHANGED")
        return {"pid": pid, "ticks": ticks, "state": state, "argv_sha256": digest}
    except FileNotFoundError:
        return None
    except (OSError, ValueError, IndexError, UnicodeError):
        raise OperatorRefusal("PROCESS_IDENTITY_UNAVAILABLE") from None


def _server_identity(row):
    """Projection pinned by the future fixed-recipe manifest validator.

    This is NOT sufficient runtime isolation evidence or a context constructor.
    It deliberately contains no environment, credentials or raw process args.
    """
    try:
        _container_id(row["Id"])
        _require(re.fullmatch("sha256:[0-9a-f]{64}", row["Image"]),
                 "INVALID_IMAGE_ID")
        host = row["HostConfig"]
        _require(host["RestartPolicy"] == {"Name": "no", "MaximumRetryCount": 0}
                 and type(host["RestartPolicy"]["MaximumRetryCount"]) is int
                 and host["Privileged"] is False and host["PidMode"] == ""
                 and not host["Devices"] and not host["CapAdd"],
                 "UNSAFE_CONTAINER_POLICY")
        value = {
            "id": row["Id"], "image": row["Image"],
            "mounts": sorted((m["Type"], m["Source"], m["Destination"], m["RW"])
                             for m in row["Mounts"]),
            "network": host["NetworkMode"], "ports": host["PortBindings"],
            "entrypoint": row["Config"]["Entrypoint"], "cmd": row["Config"]["Cmd"],
            "user": row["Config"]["User"],
        }
        return hashlib.sha256(_json(value)).hexdigest()
    except (KeyError, TypeError, ValueError):
        raise OperatorRefusal("INVALID_CONTAINER_IDENTITY") from None


def _boundary(docker, container, expected_identity):
    row = docker.inspect(container)
    _require(row.get("Id") == container and _server_identity(row) == expected_identity,
             "CONTAINER_IDENTITY_DRIFT")
    return row


def _inventory(docker, container, allowed_argv_sha256):
    rows = []
    for pid in docker.pids(container):
        row = _process(pid)
        _require(row is not None and row["state"] not in ("Z", "X", "T", "t") and
                 row["argv_sha256"] in allowed_argv_sha256, "UNKNOWN_CONTAINER_PROCESS")
        rows.append(row)
    return rows


def _old_processes_terminal(rows):
    _require(type(rows) is list and bool(rows), "MISSING_OLD_PROCESSES")
    for old in rows:
        _require(type(old) is dict and set(old) == {
            "pid", "ticks", "state", "argv_sha256"} and type(old["ticks"]) is int
            and old["ticks"] > 0, "INVALID_OLD_PROCESS_IDENTITY")
        current = _process(old["pid"])
        if current is not None and current["ticks"] == old["ticks"]:
            _require(current["state"] in ("Z", "X"), "OLD_PROCESS_SURVIVED")


def _clean_stop(docker, journal, *, container, expected_identity, allowed_argv_sha256):
    """Settle ONE known server incarnation, including already-stopped successors.

    Caller must first exclude ALL managed writers/launchers and bind host/daemon,
    payload and data identities. This primitive alone never authorizes SDK work.
    A never-started container is not settlement and is deliberately refused.
    """
    journal.check()
    row = _boundary(docker, container, expected_identity)
    try:
        terminal = row["State"]
        if terminal["Running"] is True:
            _require(terminal["Status"] == "running" and terminal["Restarting"] is False
                     and terminal["Dead"] is False and terminal["Paused"] is False and
                     terminal["Error"] == "" and type(terminal["Pid"]) is int and
                     terminal["Pid"] > 0, "UNSTABLE_RUNNING_SERVER")
            _timestamp_ns(terminal["StartedAt"])
            old = _inventory(docker, container, allowed_argv_sha256)
            _require(terminal["Pid"] in [p["pid"] for p in old], "MISSING_SERVER_PROCESS")
            state = journal.state
            state["stop_proofs"].pop(container, None)
            state["stop_intents"][container] = {
                "identity": expected_identity, "started": terminal["StartedAt"],
                "server_pid": terminal["Pid"], "old_processes": old}
            journal.store(state)
            current = _inventory(docker, container, allowed_argv_sha256)
            identity = lambda rows: sorted(
                (p["pid"], p["ticks"], p["argv_sha256"]) for p in rows)
            _require(_boundary(docker, container, expected_identity)["State"] == terminal
                     and identity(current) == identity(old),
                     "INCARNATION_CHANGED_BEFORE_STOP")
            journal.check()
            docker.stop(container)
            row = _boundary(docker, container, expected_identity)
            terminal = row["State"]
        _require(terminal["Status"] == "exited" and terminal["Running"] is False and
                 terminal["Restarting"] is False and terminal["Dead"] is False and
                 terminal["OOMKilled"] is False and terminal["Paused"] is False and
                 terminal["Error"] == "", "NOT_CLEAN_TERMINAL")
        _require(type(terminal["Pid"]) is int and terminal["Pid"] == 0 and
                 type(terminal["ExitCode"]) is int and terminal["ExitCode"] == 0,
                 "UNCLEAN_SERVER_EXIT")
        started, finished = terminal["StartedAt"], terminal["FinishedAt"]
        lower, upper = _timestamp_ns(started), _timestamp_ns(finished)
        _require(upper > lower, "INVALID_TERMINAL_INTERVAL")
        intent = journal.state["stop_intents"].get(container)
        _require(type(intent) is dict and set(intent) == {
            "identity", "started", "server_pid", "old_processes"} and
                 intent.get("identity") == expected_identity and
                 intent.get("started") == started and type(intent.get("server_pid")) is int
                 and intent["server_pid"] > 0, "MISSING_STOP_PROVENANCE")
        old = intent["old_processes"]
        _require(intent["server_pid"] in [p["pid"] for p in old], "MISSING_SERVER_PROCESS")
        _old_processes_terminal(old)
        log = docker.logs(container, started, finished)
        clean_times = []
        for line in log.splitlines():
            if line.endswith("finished clean shutdown - exiting"):
                stamp = _timestamp_ns(line.split(" ", 1)[0])
                _require(lower <= stamp <= upper, "STALE_OR_FUTURE_CLEAN_LOG")
                clean_times.append(stamp)
        _require(clean_times, "MISSING_CLEAN_LOG")
        _require(_boundary(docker, container, expected_identity)["State"] == terminal,
                 "INCARNATION_CHANGED_DURING_PROOF")
        _old_processes_terminal(old)
        journal.check()
        proof = {"identity": expected_identity, "started": started, "finished": finished,
                 "clean_times_ns": clean_times,
                 "log_sha256": hashlib.sha256(log.encode("utf-8")).hexdigest()}
        state = journal.state
        state["stop_proofs"][container] = proof
        journal.store(state)
        return proof
    except (KeyError, TypeError, ValueError):
        raise OperatorRefusal("INVALID_STOP_EVIDENCE") from None


def _keys(value, names):
    _require(type(value) is dict and set(value) == set(names.split()),
             "UNSUPPORTED_MANIFEST_SCHEMA")


def _sha(value):
    _require(type(value) is str and re.fullmatch("[0-9a-f]{64}", value),
             "INVALID_DIGEST")
    return value


def _text(value):
    _require(type(value) is str and 0 < len(value.encode("utf-8")) <= 4096 and
             not any(ord(c) < 32 or ord(c) == 127 for c in value), "INVALID_TEXT")
    return value


def _path_fd(path, directory=False, metadata=False):
    """No symlink component, including ancestors; no path normalization adoption."""
    _require(type(path) is str and path.startswith("/") and
             path == os.path.normpath(path) and "\x00" not in path,
             "NONCANONICAL_PATH")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        parts = path.split("/")[1:]
        for index, part in enumerate(parts):
            _require(bool(part), "NONCANONICAL_PATH")
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if metadata and index == len(parts) - 1 and not directory:
                flags = os.O_PATH | os.O_NOFOLLOW
            if index < len(parts) - 1 or directory:
                flags |= os.O_DIRECTORY
            new = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = new
        info = os.fstat(fd)
        _require(info.st_uid in (0, os.getuid()) and
                 not stat.S_IMODE(info.st_mode) & 0o022 and
                 (stat.S_ISDIR(info.st_mode) if directory else
                  stat.S_ISREG(info.st_mode) and info.st_nlink == 1),
                 "UNTRUSTED_PATH")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _bytes(path, limit=1024 * 1024, private=False):
    fd = _path_fd(path)
    try:
        if private:
            _private(os.fstat(fd))
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(limit + 1)
        _require(len(raw) <= limit, "FILE_TOO_LARGE")
        return raw
    finally:
        os.close(fd)


def _resource(value, directory=False):
    _keys(value, "path device inode" if directory else "path device inode sha256")
    for key in ("device", "inode"):
        _require(type(value[key]) is int and value[key] >= 0, "INVALID_RESOURCE")
    fd = _path_fd(value["path"], directory)
    try:
        _require(_identity(os.fstat(fd)) == [value["device"], value["inode"]],
                 "RESOURCE_IDENTITY_CHANGED")
    finally:
        os.close(fd)
    if not directory:
        _require(hashlib.sha256(_bytes(value["path"])).hexdigest() ==
                 _sha(value["sha256"]), "RESOURCE_HASH_CHANGED")


def _host_boot():
    with open("/proc/sys/kernel/random/boot_id", "r") as stream:
        return stream.read().strip()


def _host_id():
    with open("/etc/machine-id", "rb") as stream:
        return hashlib.sha256(stream.read().strip()).hexdigest()


def _config(raw, namespace):
    """Deliberately small file-storage recipe, not an arbitrary asd config loader."""
    try:
        lines = raw.decode("ascii").splitlines()
    except UnicodeError:
        raise OperatorRefusal("UNSUPPORTED_SERVER_CONFIG") from None
    stack, blocks, settings = [], set(), {}
    allowed = {
        ("service",): {"proto-fd-max": r"[1-9][0-9]*",
                       "cluster-name": r"[A-Za-z0-9_-]+", "node-id": r"[A-Fa-f0-9]+"},
        ("logging", "console"): {"context": r"any info"},
        ("network", "service"): {"address": r"any", "port": r"3000",
                                  "access-address": r"127\.0\.0\.1"},
        ("network", "heartbeat"): {"mode": r"mesh", "port": r"3002",
                                    "interval": r"[1-9][0-9]*", "timeout": r"[1-9][0-9]*"},
        ("network", "fabric"): {"address": r"127\.0\.0\.1", "port": r"3001"},
        ("network", "admin"): {"address": r"127\.0\.0\.1", "port": r"3003"},
        ("namespace " + namespace,): {
            "replication-factor": r"1", "default-ttl": r"[0-9]+",
            "nsup-period": r"[0-9]+", "default-read-touch-ttl-pct": r"0"},
        ("namespace " + namespace, "storage-engine device"): {
            "file": r"/opt/aerospike/data/operator\.dat",
            "filesize": r"[1-9][0-9]*[MG]", "flush-size": r"[1-9][0-9]*K"},
    }
    containers = {("logging",), ("network",)}
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith(" {"):
            stack.append(line[:-2])
            block = tuple(stack)
            _require(block in allowed or block in containers, "UNSUPPORTED_SERVER_CONFIG")
            _require(block not in blocks, "DUPLICATE_CONFIG_BLOCK")
            blocks.add(block)
        elif line == "}":
            _require(bool(stack), "UNSUPPORTED_SERVER_CONFIG")
            stack.pop()
        else:
            key, separator, value = line.partition(" ")
            identity = (tuple(stack), key)
            pattern = allowed.get(tuple(stack), {}).get(key)
            _require(separator and pattern and re.fullmatch(pattern, value) and
                     identity not in settings, "UNSUPPORTED_SERVER_CONFIG")
            settings[identity] = value
    _require(not stack and settings.get((("network", "service"), "address")) == "any"
             and settings.get((("network", "service"), "port")) == "3000" and
             settings.get((("namespace " + namespace,), "replication-factor")) == "1"
             and settings.get(((("namespace " + namespace), "storage-engine device"),
                               "file")) == "/opt/aerospike/data/operator.dat" and
             ((("namespace " + namespace), "storage-engine device"), "filesize") in settings,
             "UNSUPPORTED_SERVER_CONFIG")


def _manifest(path, journal_dir):
    raw = _bytes(os.fspath(path), private=True)
    value = _load(raw)
    _keys(value, "recipe host daemon namespace image containers data configs "
          "runtime_sha256 server_argv_sha256 launchers public_port")
    _require(value["recipe"] == "local-precreated-file-v1", "UNSUPPORTED_RECIPE")
    _sha(value["host"])
    _text(value["daemon"])
    _require(type(value["namespace"]) is str and
             re.fullmatch("[A-Za-z0-9_-]{1,31}", value["namespace"]),
             "UNSAFE_NAMESPACE")
    _require(type(value["image"]) is str and
             re.fullmatch("sha256:[0-9a-f]{64}", value["image"]), "INVALID_IMAGE_ID")
    _keys(value["containers"], "public maintenance")
    _require(_container_id(value["containers"]["public"]) !=
             _container_id(value["containers"]["maintenance"]), "SHARED_CONTAINER")
    _resource(value["data"], True)
    _keys(value["configs"], "public maintenance")
    configs = []
    for mode in ("public", "maintenance"):
        _resource(value["configs"][mode])
        configs.append(_bytes(value["configs"][mode]["path"]))
    # This recipe permits exactly the demonstrated service-address delta.
    public, maintenance = configs
    _config(public, value["namespace"])
    old = b"service {\n        address any\n        port 3000"
    new = b"service {\n        address 127.0.0.1\n        port 3000"
    _require(public.count(old) == 1 and public.replace(old, new) == maintenance and
             b"file /opt/aerospike/data/operator.dat" in public and
             ("namespace " + value["namespace"] + " {").encode() in public and
             b"replication-factor 1" in public and
             b"storage-engine device {" in public, "UNSUPPORTED_CONFIG_DELTA")
    _sha(value["runtime_sha256"])
    _require(type(value["server_argv_sha256"]) is list and
             0 < len(value["server_argv_sha256"]) <= 4, "INVALID_SERVER_PROCESSES")
    for digest in value["server_argv_sha256"]:
        _sha(digest)
    _require(type(value["public_port"]) is int and
             1024 <= value["public_port"] <= 65535, "INVALID_PUBLIC_PORT")
    launchers = value["launchers"]
    _keys(launchers, "recipe gate automatic_restart queues writer_argv_sha256 writers")
    _require(launchers["recipe"] == "trusted-manual-gate-v1" and
             launchers["gate"] == os.fspath(journal_dir) and
             launchers["automatic_restart"] is False and
             launchers["queues"] == "discard-old-admissions",
             "UNSUPPORTED_LAUNCHERS")
    _require(type(launchers["writer_argv_sha256"]) is list and
             type(launchers["writers"]) is list, "INVALID_WRITER_INVENTORY")
    for digest in launchers["writer_argv_sha256"]:
        _sha(digest)
    for writer in launchers["writers"]:
        _writer_schema(writer, launchers["writer_argv_sha256"])
    _require(len({w["pid"] for w in launchers["writers"]}) ==
             len(launchers["writers"]), "DUPLICATE_WRITER")
    return value, hashlib.sha256(raw).hexdigest()


def _writer_schema(writer, allowed):
    _keys(writer, "pid ticks argv_sha256 boot uid")
    _require(type(writer["pid"]) is int and writer["pid"] > 1 and
             type(writer["ticks"]) is int and writer["ticks"] > 0 and
             type(writer["uid"]) is int and writer["uid"] == os.getuid() == 1000 and
             type(writer["boot"]) is str and
             re.fullmatch("[0-9a-f-]{36}", writer["boot"]) and
             _sha(writer["argv_sha256"]) in allowed, "INVALID_WRITER_IDENTITY")


def _host_writers(allowed):
    """Discover every process of an enrolled writer version, including stopped."""
    result = {}
    for name in os.listdir("/proc"):
        if name.isdigit():
            row = _process(int(name))
            if row and row["state"] not in ("Z", "X") and row["argv_sha256"] in allowed:
                result[row["pid"]] = row
    return result


def _writer_owner(pid):
    with open("/proc/{}/status".format(pid), "r") as stream:
        uids = next(line for line in stream if line.startswith("Uid:")).split()[1:]
    _require(len(uids) == 4 and all(int(uid) == 1000 for uid in uids),
             "WRITER_OWNER_CHANGED")


def _terminate_writer(writer):
    row = _process(writer["pid"])
    if row is None or row["state"] in ("Z", "X"):
        return
    _require(writer["boot"] == _host_boot() and row["ticks"] == writer["ticks"] and
             row["argv_sha256"] == writer["argv_sha256"], "WRITER_IDENTITY_CHANGED")
    _writer_owner(writer["pid"])
    # Kill rather than resume paused admissions; writer termination is not server settlement.
    os.kill(writer["pid"], signal.SIGKILL)
    deadline = time.monotonic() + 10
    while True:
        row = _process(writer["pid"])
        if row is None or row["ticks"] != writer["ticks"] or row["state"] in ("Z", "X"):
            return
        _require(time.monotonic() < deadline, "WRITER_NOT_TERMINAL")
        time.sleep(0.05)


def _tree_digest(path, *, _journal_overlay=False):
    """Hash a sealed bundle, rejecting links, mutable files and import hooks."""
    root = _path_fd(path, True)
    os.close(root)
    rows = []
    def fail(error):
        raise OperatorRefusal("UNREADABLE_RUNTIME_TREE") from None

    for parent, directories, files in os.walk(path, followlinks=False, onerror=fail):
        if _journal_overlay and parent == "/op/journal":
            directories[:] = []
            continue
        for name in sorted(directories + files):
            full = os.path.join(parent, name)
            if _journal_overlay and full == "/op/journal":
                rows.append(["journal", "directory", 0o555])
                continue
            info = os.lstat(full)
            _require(not stat.S_ISLNK(info.st_mode) and
                     info.st_uid in (0, os.getuid()) and
                     not stat.S_IMODE(info.st_mode) & 0o022, "UNSAFE_RUNTIME_TREE")
            relative = os.path.relpath(full, path)
            _require(not name.endswith(".pth") and not name.startswith((
                "sitecustomize.", "usercustomize.")), "RUNTIME_IMPORT_HOOK")
            if stat.S_ISDIR(info.st_mode):
                rows.append([relative, "directory", stat.S_IMODE(info.st_mode)])
            else:
                _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                         "UNSAFE_RUNTIME_TREE")
                digest = hashlib.sha256()
                fd = _path_fd(full)
                with os.fdopen(fd, "rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                rows.append([relative, digest.hexdigest(), stat.S_IMODE(info.st_mode)])
    return hashlib.sha256(_json(sorted(rows))).hexdigest()


_RUNTIME_NAME = "_aerospike_operator_runtime.py"
_RUNTIME_RELATIVE = "source/gptcache/manager/scalar_data/" + _RUNTIME_NAME


class _FixedRuntime:
    """Fixed transport boundary; never constructs or accepts a provider context.

    Explicit offline preparation builds a relocatable member-pinned bundle.
    The fixed payload, not serialized controller proof, owns the in-process seal.
    """

    def __init__(self, journal_dir, expected):
        self.path = os.path.join(journal_dir, "runtime")
        self.expected = expected

    def prepare(self):
        source = os.path.join(os.path.dirname(__file__), _RUNTIME_NAME)
        _require(os.path.isfile(source),
                 "FIXED_RUNTIME_MISSING_IMPLEMENT_PRIVATE_RUNTIME_BEFORE_MAINTENANCE")
        _require(sys.version_info >= (3, 11), "SDK_RUNTIME_REQUIRES_PYTHON_311")
        _require(os.path.isdir(self.path),
                 "FIXED_BUNDLE_MISSING_PREPARE_TRUSTED_RUNTIME_WITH_SDK_NO_AUTO_INSTALL")
        self.check()
        _require(_bytes(source) == _bytes(os.path.join(self.path, _RUNTIME_RELATIVE)),
                 "FRAMEWORK_RUNTIME_SOURCE_MISMATCH")
        provider = os.path.join(os.path.dirname(__file__), "aerospike_storage.py")
        _require(_bytes(provider) == _bytes(os.path.join(
            self.path, "source/gptcache/manager/scalar_data/aerospike_storage.py")),
            "FRAMEWORK_PROVIDER_SOURCE_MISMATCH")
        _require(os.path.isfile(os.path.join(self.path, "source/aerospike_sdk/__init__.py")),
                 "SDK_ABSENT_FROM_FIXED_BUNDLE_PREPARE_DEPENDENCIES_NO_AUTO_INSTALL")
        # The bundle builder must preserve a relocatable Python prefix and SDK dependencies.
        for relative in ("lib/ld-linux-x86-64.so.2", "python/bin/python3",
                         "source/gptcache/manager/scalar_data/aerospike_storage.py"):
            fd = _path_fd(os.path.join(self.path, relative))
            os.close(fd)
        for relative in ("lib/ld-linux-x86-64.so.2", "python/bin/python3"):
            fd = _path_fd(os.path.join(self.path, relative))
            try:
                header = os.read(fd, 20)
                _require(header[:6] == b"\x7fELF\x02\x01" and header[18:20] == b"\x3e\x00",
                         "RUNTIME_REQUIRES_LINUX_X86_64_ELF")
            finally:
                os.close(fd)
        from gptcache.manager.scalar_data._aerospike_operator_runtime import _verify_bundle
        _verify_bundle(self.path)

    def check(self):
        _require(_tree_digest(self.path) == self.expected, "RUNTIME_PAYLOAD_CHANGED")

    def execute(self, container, request):
        self.check()
        _container_id(container)
        raw = _json(request)
        _require(len(raw) <= 512 * 1024, "REQUEST_TOO_LARGE")
        argv = ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock",
                "exec", "-i", "--user", "1000:1000",
                "--env", "PYTHONDONTWRITEBYTECODE=1", container,
                "/op/lib/ld-linux-x86-64.so.2", "--library-path", "/op/lib",
                "/op/python/bin/python3", "-I", "-B",
                "/op/" + _RUNTIME_RELATIVE]
        # Keep stdin OPEN through runtime exit. The fixed runtime must check EOF
        # before each mutation; a one-shot communicate() would close it too soon.
        process = None
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            try:
                process = subprocess.Popen(
                    argv, stdin=subprocess.PIPE, stdout=output, stderr=errors,
                    shell=False, env={"PATH": "/usr/bin:/bin", "LANG": "C"})
                process.stdin.write(raw + b"\n")
                process.stdin.flush()
                code = process.wait(timeout=75)
                _require(code == 0, "RUNTIME_FAILED_OUTCOME_UNKNOWN")
                output.seek(0)
                errors.seek(0)
                stdout, stderr = output.read(1024 * 1024 + 1), errors.read(65537)
                _require(len(stdout) <= 1024 * 1024 and len(stderr) <= 65536,
                         "RUNTIME_OUTPUT_TOO_LARGE")
                response = _load(stdout)
            except (OSError, subprocess.SubprocessError):
                raise OperatorRefusal("RUNTIME_OUTCOME_UNKNOWN_REENTER_MAINTENANCE") from None
            finally:
                if process is not None:
                    process.stdin.close()
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
        self.check()
        return response


class DockerMaintenanceOperator:
    """Controller for one precreated local persistent-file deployment.

    Enrollment is an explicit trusted provisioning step (_Journal.enroll); absent
    journals are NEVER auto-created. Only manual gate-compliant launchers and
    registered uid1000 host writers are supported, not systemd/cloud adapters.
    """

    @classmethod
    def from_manifest(cls, manifest_path, *, journal_dir):
        instance = cls()
        instance.manifest_path = os.fspath(manifest_path)
        instance.journal_dir = os.fspath(journal_dir)
        instance.manifest, instance.digest = _manifest(manifest_path, journal_dir)
        instance.docker = _Docker()
        instance.runtime = _FixedRuntime(instance.journal_dir,
                                         instance.manifest["runtime_sha256"])
        return instance

    def status(self):
        # No Docker, SDK, enrollment, gate changes, or directory creation.
        manifest, digest = _manifest(self.manifest_path, self.journal_dir)
        _require(manifest == self.manifest and digest == self.digest, "MANIFEST_CHANGED")
        result = _Journal.status(self.journal_dir)
        fd = _directory(self.journal_dir)
        try:
            receipt = _read_at(fd, "receipt.json")
            _require(receipt["manifest"] == self.digest, "MANIFEST_CHANGED")
            controller = receipt.get("controller", {})
            result.update(stage=controller.get("stage", "enrolled"),
                          intents=deepcopy(controller.get("intents", {})),
                          recovery=deepcopy(controller.get("recovery")),
                          recovery_history=deepcopy(controller.get("recovery_history", [])),
                          pending=deepcopy(controller.get("pending")))
            return result
        finally:
            os.close(fd)

    def _save(self, journal, control, stage=None):
        if stage:
            control["stage"] = stage
        control["public_verified"] = False
        control["deployment"] = deepcopy(self.manifest)
        journal._control(control)

    @contextmanager
    def _operation(self):
        # Fail before changing data/gate if the fixed implementation is absent.
        self.runtime.prepare()
        with _Journal(self.journal_dir, self.digest) as journal:
            try:
                self._deployment(journal)
            except (OperatorRefusal, OSError):
                if journal.state["state"] == "PUBLIC":
                    self._save(journal, journal.state["controller"], "deployment_refused")
                raise
            control = journal.state.get("controller")
            if control is None:
                control = {"stage": "enrolled", "intents": {}, "recovery": None,
                           "recovery_history": [],
                           "pending": None, "writers": deepcopy(
                               self.manifest["launchers"]["writers"]),
                           "nonce": None, "incarnation": None, "data_file": None,
                           "public_verified": False}
            yield journal, control

    def _deployment(self, journal):
        journal.check()
        manifest, digest = _manifest(self.manifest_path, self.journal_dir)
        _require(manifest == self.manifest and digest == self.digest, "MANIFEST_CHANGED")
        _require(_host_id() == manifest["host"] and
                 self.docker.daemon() == manifest["daemon"], "DEPLOYMENT_CHANGED")
        self.runtime.check()
        owners = [self._boundary(mode) for mode in ("public", "maintenance")]
        _require(sum(row["State"]["Running"] is True for row in owners) <= 1,
                 "CONCURRENT_DATA_OWNERS_HISTORY_UNSAFE")

    def _boundary(self, mode):
        manifest = self.manifest
        row = self.docker.inspect(manifest["containers"][mode])
        _server_identity(row)
        host, config = row["HostConfig"], row["Config"]
        image = self.docker.image(manifest["image"])
        _require(row["Id"] == manifest["containers"][mode] and
                 row["Image"] == manifest["image"] and
                 all(config.get(k) == image.get(k) or (
                     # Docker emits empty strings for these omitted image defaults.
                     k in ("User", "WorkingDir") and
                     config.get(k) in (None, "") and image.get(k) in (None, ""))
                     for k in (
                     "Entrypoint", "Cmd", "User", "Env", "WorkingDir", "Healthcheck",
                     "StopSignal", "Shell")) and
                 not host.get("Binds") and not host.get("VolumesFrom") and
                 not host.get("ExtraHosts") and not host.get("Links") and
                 not host.get("DeviceRequests") and not host.get("GroupAdd") and
                 not host.get("AutoRemove") and
                 host.get("SecurityOpt") in (None, [], ["no-new-privileges"],
                                             ["no-new-privileges:true"]) and
                 not host.get("CgroupParent") and
                 host.get("IpcMode", "private") == "private" and
                 host.get("UTSMode", "") == "" and
                 host.get("UsernsMode", "") == "",
                 "UNSUPPORTED_CONTAINER_OVERRIDES")
        expected = {
            ("bind", manifest["data"]["path"], "/opt/aerospike/data", True),
            ("bind", manifest["configs"][mode]["path"],
             "/etc/aerospike/aerospike.conf", False),
            ("bind", self.runtime.path, "/op", False),
            ("bind", self.journal_dir, "/op/journal", False)}
        actual = [(m["Type"], m["Source"], m["Destination"], m["RW"])
                  for m in row["Mounts"]]
        _require(len(actual) == len(expected) and set(actual) == expected,
                 "UNAUTHORIZED_MOUNTS")
        ports = {"3000/tcp": [{"HostIp": "127.0.0.1",
                              "HostPort": str(manifest["public_port"])}]}
        _require(host["NetworkMode"] == ("none" if mode == "maintenance" else "bridge")
                 and host["PortBindings"] == ({} if mode == "maintenance" else ports),
                 "UNSAFE_NETWORK")
        network = row["NetworkSettings"]
        if mode == "maintenance":
            _require(all(v is None for v in (network.get("Ports") or {}).values()) and
                     set(network.get("Networks", {})) <= {"none"},
                     "MAINTENANCE_NETWORK_EXPOSED")
        else:
            _require(set(network.get("Networks", {})) <= {"bridge"},
                     "PUBLIC_NETWORK_DRIFT")
        return row

    def _exclude(self, journal, control):
        launchers = self.manifest["launchers"]
        registered = {w["pid"]: w for w in control["writers"]}
        discovered = _host_writers(launchers["writer_argv_sha256"])
        _require(set(discovered) <= set(registered), "UNREGISTERED_MANAGED_WRITER")
        for writer in control["writers"]:
            _writer_schema(writer, launchers["writer_argv_sha256"])
        self._save(journal, control, "terminate_writers")
        for writer in control["writers"]:
            _terminate_writer(writer)
        _require(not _host_writers(launchers["writer_argv_sha256"]),
                 "WRITER_NOT_TERMINAL")
        self._save(journal, control, "writers_terminal_old_admissions_invalid")

    def _stop(self, journal, mode):
        row = self._boundary(mode)
        container = row["Id"]
        if row["State"]["Status"] == "created":
            _require(row["State"]["Running"] is False and
                     type(row["State"]["Pid"]) is int and row["State"]["Pid"] == 0 and
                     container not in journal.state["stop_intents"] and
                     mode == "maintenance", "UNPROVEN_PREVIOUS_OWNER")
            return None
        allowed = set(self.manifest["server_argv_sha256"])
        if journal.state.get("controller", {}).get("pending"):
            # A crashed fixed exec may still be in the server process inventory.
            allowed.update(self._runtime_argv_hashes())
        return _clean_stop(
            self.docker, journal, container=container,
            expected_identity=_server_identity(row),
            allowed_argv_sha256=allowed)

    @staticmethod
    def _runtime_argv_hash():
        args = ["/op/lib/ld-linux-x86-64.so.2", "--library-path", "/op/lib",
                "/op/python/bin/python3", "-I", "-B", "/op/" + _RUNTIME_RELATIVE]
        return hashlib.sha256(("\0".join(args) + "\0").encode()).hexdigest()

    @classmethod
    def _runtime_argv_hashes(cls):
        python = ["/op/python/bin/python3", "-I", "-B", "/op/" + _RUNTIME_RELATIVE]
        return {cls._runtime_argv_hash(),
                hashlib.sha256(("\0".join(python) + "\0").encode()).hexdigest()}

    def _data(self, journal, control):
        path = os.path.join(self.manifest["data"]["path"], "operator.dat")
        fd = _path_fd(path, metadata=True)
        try:
            info = os.fstat(fd)
            identity = _identity(info) + [info.st_size]
            _require(info.st_size > 0, "EMPTY_PERSISTENT_DATA_FILE")
            if control["data_file"] is not None:
                _require(control["data_file"] == identity, "DATA_FILE_CHANGED")
            else:
                control["data_file"] = identity
                self._save(journal, control)
        finally:
            os.close(fd)

    def _start(self, journal, control, mode):
        other = "maintenance" if mode == "public" else "public"
        other_row = self._boundary(other)
        _require(other_row["State"]["Running"] is False, "TWO_DATA_OWNERS")
        row = self._boundary(mode)
        _require(row["State"]["Running"] is False, "START_REQUIRES_STOP")
        # Verify prior owner settlement immediately before EVERY start.
        self._stop(journal, other)
        self._deployment(journal)
        self._data(journal, control)
        self.docker.start(row["Id"])
        row = self._boundary(mode)
        state = row["State"]
        _require(state["Running"] is True and state["Status"] == "running" and
                 state["Paused"] is False and state["Restarting"] is False and
                 state["Dead"] is False and state["Error"] == "" and
                 type(state["Pid"]) is int and state["Pid"] > 0,
                 "SERVER_NOT_RUNNING")
        stamp = _timestamp_ns(state["StartedAt"])
        previous = journal.state["stop_proofs"].get(row["Id"])
        if previous:
            _require(stamp > _timestamp_ns(previous["finished"]), "STALE_INCARNATION")
        processes = _inventory(self.docker, row["Id"],
                               set(self.manifest["server_argv_sha256"]))
        _require(state["Pid"] in [p["pid"] for p in processes], "MISSING_SERVER_PROCESS")
        control["incarnation"] = {
            "mode": mode, "container": row["Id"], "started": state["StartedAt"],
            "pid": state["Pid"], "identity": _server_identity(row),
            "processes": processes}
        self._save(journal, control, "isolated" if mode == "maintenance"
                   else "finish_public_verify")

    def _enter(self, journal, control):
        control["nonce"] = uuid.uuid4().hex
        self._save(journal, control, "exclude")
        self._exclude(journal, control)
        self._data(journal, control)
        # ALWAYS settle the old incarnation, including any abandoned fixed exec.
        for mode in ("public", "maintenance"):
            self._stop(journal, mode)
        control["pending"] = None
        self._save(journal, control, "new_clean_incarnation")
        self._start(journal, control, "maintenance")
        self._dispatch(journal, control, "ready")

    def _live(self, journal, control, public_writers=False):
        self._deployment(journal)
        self._data(journal, control)
        incarnation = control["incarnation"]
        _require(type(incarnation) is dict, "MISSING_INCARNATION")
        row = self._boundary(incarnation["mode"])
        state = row["State"]
        _require(row["Id"] == incarnation["container"] and state["Running"] is True and
                 state["Paused"] is False and state["Restarting"] is False and
                 state["Dead"] is False and state["Error"] == "" and
                 state["Pid"] == incarnation["pid"] and
                 state["StartedAt"] == incarnation["started"] and
                 _server_identity(row) == incarnation["identity"],
                 "RUNTIME_INCARNATION_CHANGED")
        processes = _inventory(self.docker, row["Id"],
                               set(self.manifest["server_argv_sha256"]))
        _require(all(p["state"] not in ("T", "t") for p in processes),
                 "RUNTIME_PROCESS_BOUNDARY_CHANGED")
        _require(sorted((p["pid"], p["ticks"], p["argv_sha256"]) for p in processes) ==
                 sorted((p["pid"], p["ticks"], p["argv_sha256"])
                        for p in incarnation["processes"]),
                 "RUNTIME_PROCESS_BOUNDARY_CHANGED")
        writers = _host_writers(self.manifest["launchers"]["writer_argv_sha256"])
        if public_writers:
            registered = {w["pid"]: w for w in control["writers"]}
            _require(set(writers) <= set(registered), "UNREGISTERED_MANAGED_WRITER")
            for pid, current in writers.items():
                old = registered[pid]
                _require(old["boot"] == _host_boot() and all(
                    current[k] == old[k] for k in ("pid", "ticks", "argv_sha256")),
                    "WRITER_IDENTITY_CHANGED")
        else:
            _require(not writers, "WRITER_REAPPEARED")

    def _dispatch(self, journal, control, operation, intent=None, qids=(),
                  historical=False):
        _require(control["pending"] is None, "OUTSTANDING_RUNTIME")
        self._live(journal, control)
        attempt = uuid.uuid4().hex
        controller_process = _process(os.getpid())
        _require(controller_process is not None and
                 controller_process["state"] not in ("Z", "X"),
                 "CONTROLLER_PROCESS_IDENTITY_UNAVAILABLE")
        control["pending"] = {"attempt": attempt, "operation": operation,
                              "intent_id": intent["intent_id"] if intent else None,
                              "qids": list(qids), "outcome": "UNKNOWN",
                              "historical_root_attempted": historical,
                              "controller_process": controller_process,
                              "host_boot": _host_boot()}
        if operation == "recover":
            control["recovery"] = {
                "namespace": intent["namespace"], "cache_scope": intent["cache_scope"],
                "status": "UNKNOWN",
                "results": [{"qid": qid, "status": "UNKNOWN"} for qid in qids]}
            control["recovery_history"].append({
                "attempt": attempt, "recovery": deepcopy(control["recovery"])})
        self._save(journal, control)
        state = journal.state
        request = {
            "version": 1, "operation": operation, "attempt": attempt,
            "nonce": control["nonce"], "namespace": self.manifest["namespace"],
            "intent": deepcopy(intent), "qids": list(qids),
            "historical_root_attempted": historical,
            "proof": {"manifest": self.digest, "sequence": state["sequence"],
                      "directory": state["directory"], "lock": state["lock"],
                      "controller_pid": os.getpid(), "host_boot": _host_boot(),
                      "controller_process": controller_process,
                      "deployment": deepcopy(self.manifest),
                      "incarnation": deepcopy(control["incarnation"]),
                      "stop_proofs": deepcopy(state["stop_proofs"]),
                      "data_file": control["data_file"],
                      "payload": self.manifest["runtime_sha256"]}}
        response = self.runtime.execute(control["incarnation"]["container"], request)
        self._live(journal, control)
        _keys(response, "version attempt nonce operation uid pid status results")
        _require(type(response["version"]) is int and response["version"] == 1 and
                 response["attempt"] == attempt and response["nonce"] == control["nonce"]
                 and response["operation"] == operation and
                 type(response["uid"]) is int and response["uid"] == 1000 and
                 type(response["pid"]) is int and response["pid"] > 0 and
                 response["status"] in ("OK", "PARTIAL", "REFUSED") and
                 type(response["results"]) is list, "INVALID_RUNTIME_RESPONSE")
        results = response["results"]
        if operation == "recover":
            _require(len(results) == len(qids), "INCOMPLETE_RECOVERY_RESPONSE")
            for expected, result in zip(qids, results):
                _keys(result, "qid status")
                _require(type(result["qid"]) is int and result["qid"] == expected and
                         result["status"] in (
                             "PURGED", "ABSENT", "FAILED", "UNKNOWN", "NOT_ATTEMPTED"),
                         "INVALID_RECOVERY_RESULT")
            known = all(r["status"] in ("PURGED", "ABSENT") for r in results)
            _require((response["status"] == "OK") == known,
                     "MISREPORTED_RECOVERY_SUCCESS")
            control["recovery"] = {"namespace": intent["namespace"],
                                   "cache_scope": intent["cache_scope"],
                                   "results": results, "status": response["status"]}
            control["recovery_history"][-1]["recovery"] = deepcopy(control["recovery"])
        else:
            _require(results == [], "UNEXPECTED_RUNTIME_RESULTS")
        control["pending"] = None
        self._save(journal, control)
        _require(response["status"] == "OK", "RUNTIME_REFUSED_OR_PARTIAL_SEE_STATUS")
        return response

    def _scope(self, namespace, cache_scope):
        _require(namespace == self.manifest["namespace"], "NAMESPACE_NOT_AUTHORIZED")
        return _text(cache_scope)

    def initialize_scope(self, *, namespace, cache_scope, intent_id, vector_binding):
        self._scope(namespace, cache_scope)
        _text(intent_id)
        _text(vector_binding)
        _require(re.fullmatch("[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", vector_binding),
                 "VECTOR_BINDING_MUST_BE_OPAQUE_ID_NOT_CREDENTIAL_URI")
        with self._operation() as (journal, control):
            intents = control["intents"]
            _require(intent_id not in intents and not any(
                i["namespace"] == namespace and i["cache_scope"] == cache_scope
                for i in intents.values()), "COMPETING_OR_EXISTING_INTENT_USE_RESUME")
            binding = hashlib.sha256(_json({
                "namespace": namespace, "cache_scope": cache_scope,
                "intent_id": intent_id, "vector_binding": vector_binding})).hexdigest()
            intent = {"namespace": namespace, "cache_scope": cache_scope,
                      "intent_id": intent_id, "vector_binding": vector_binding,
                      "boot": str(uuid.uuid4()), "binding": binding,
                      "root_attempted": False, "status": "INITIALIZING"}
            intents[intent_id] = intent
            self._save(journal, control, "new_intent_declared")
            return self._initialize(journal, control, intent)

    def _initialize(self, journal, control, intent):
        historical = intent["root_attempted"]
        self._enter(journal, control)
        # CURRENT durable attempt precedes dispatch; historical snapshot stays separate.
        intent["root_attempted"] = True
        self._save(journal, control, "root_attempt_durable")
        self._dispatch(journal, control, "initialize", intent, historical=historical)
        intent["status"] = "ACTIVE"
        self._save(journal, control, "isolated")
        return {"status": "ACTIVE", "intent_id": intent["intent_id"]}

    def resume_initialization(self, *, intent_id):
        _text(intent_id)
        with self._operation() as (journal, control):
            _require(intent_id in control["intents"], "UNKNOWN_INTENT")
            return self._initialize(journal, control, control["intents"][intent_id])

    def recover_session_purge(self, *, namespace, cache_scope, qids):
        self._scope(namespace, cache_scope)
        _require(type(qids) in (list, tuple) and all(
            type(qid) is int and 1 <= qid <= (1 << 63) - 1 for qid in qids),
            "INVALID_QIDS")
        qids = tuple(dict.fromkeys(qids))
        _require(len(_json(list(qids))) <= 256 * 1024, "TARGETS_TOO_LARGE")
        _require(len(_json([{"qid": qid, "status": "NOT_ATTEMPTED"} for qid in qids]))
                 <= 1024 * 1024 - 1024, "TARGET_RESULTS_TOO_LARGE_SPLIT_OPERATOR_REQUEST")
        with self._operation() as (journal, control):
            matches = [i for i in control["intents"].values()
                       if i["namespace"] == namespace and i["cache_scope"] == cache_scope
                       and i["status"] == "ACTIVE"]
            _require(len(matches) == 1, "NO_ACTIVE_INITIALIZATION_RECEIPT")
            self._enter(journal, control)
            self._dispatch(journal, control, "recover", matches[0], qids)
            return deepcopy(control["recovery"])

    def finish_maintenance(self):
        with self._operation() as (journal, control):
            _require(journal.state["state"] == "MAINTENANCE", "NOT_IN_MAINTENANCE")
            _require(control["intents"] and all(i["status"] == "ACTIVE"
                     for i in control["intents"].values()), "INITIALIZATION_UNFINISHED")
            # Even finish successors settle abandoned runtime and public incarnations.
            self._enter(journal, control)
            for intent in control["intents"].values():
                self._dispatch(journal, control, "validate", intent)
            self._save(journal, control, "finish_stop")
            self._stop(journal, "maintenance")
            self._start(journal, control, "public")
            for intent in control["intents"].values():
                self._dispatch(journal, control, "validate", intent)
            self._live(journal, control)
            control["stage"] = "finished"
            control["public_verified"] = True
            journal._control(control, public=True)
            return {"status": "PUBLIC", "recovery": deepcopy(control["recovery"]),
                    "recovery_history": deepcopy(control["recovery_history"])}

    @contextmanager
    def _public_writer_admission(self):
        """Trusted launcher integration: retain the SAME lock through registration.

        Yields a register(pid) function, not a command executor. Old queues must
        be discarded by the enrolled manual launcher. Unregistered launch/crash
        leaves a durable closed gate, never permission to resume old admissions.
        """
        with _Journal(self.journal_dir, self.digest) as journal:
            self._deployment(journal)
            _require(journal.state["state"] == "PUBLIC", "MAINTENANCE_GATE")
            control = journal.state["controller"]
            self._live(journal, control, public_writers=True)
            self._save(journal, control, "public_launch_admission")
            registered = []

            def register(pid):
                _require(not registered, "ONE_PROCESS_PER_ADMISSION")
                row = _process(pid)
                _require(row is not None and row["state"] not in ("Z", "X"),
                         "WRITER_NOT_LIVE")
                writer = {key: row[key] for key in ("pid", "ticks", "argv_sha256")}
                writer.update(boot=_host_boot(), uid=os.getuid())
                _writer_schema(writer, self.manifest["launchers"]["writer_argv_sha256"])
                _writer_owner(pid)
                control["writers"] = [w for w in control["writers"] if w["pid"] != pid]
                control["writers"].append(writer)
                self._save(journal, control)
                registered.append(pid)

            yield register
            _require(len(registered) == 1, "WRITER_REGISTRATION_REQUIRED")
            self._deployment(journal)
            control["stage"], control["public_verified"] = "finished", True
            journal._control(control, public=True)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's default includes untrusted argv (possibly credentials).
        super().error("invalid or missing allowlisted arguments")


def main(argv=None):
    parser = _Parser(
        description="Pre-enrolled local Docker maintenance; no automatic provisioning.")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "init-new", "resume-init", "recover-purge", "finish"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", required=True)
        command.add_argument("--journal-dir", required=True)
        if name in ("init-new", "recover-purge"):
            command.add_argument("--namespace", required=True)
            command.add_argument("--cache-scope", required=True)
        if name in ("init-new", "resume-init"):
            command.add_argument("--intent-id", required=True)
        if name == "init-new":
            command.add_argument("--vector-binding", required=True)
            command.add_argument("--declare-new-logical-cache", action="store_true",
                                 required=True)
        if name == "recover-purge":
            command.add_argument("--qids-file", required=True)
    args = parser.parse_args(argv)
    try:
        operator = DockerMaintenanceOperator.from_manifest(
            args.manifest, journal_dir=args.journal_dir)
        if args.command == "status":
            result = operator.status()
        elif args.command == "init-new":
            result = operator.initialize_scope(
                namespace=args.namespace, cache_scope=args.cache_scope,
                intent_id=args.intent_id, vector_binding=args.vector_binding)
        elif args.command == "resume-init":
            result = operator.resume_initialization(intent_id=args.intent_id)
        elif args.command == "recover-purge":
            result = operator.recover_session_purge(
                namespace=args.namespace, cache_scope=args.cache_scope,
                qids=_load(_bytes(args.qids_file, private=True)))
        else:
            result = operator.finish_maintenance()
        print(_json(result).decode("ascii"))
        return 0
    except OperatorRefusal as exc:
        print(_json({"status": "REFUSED", "reason": str(exc)}).decode("ascii"))
    except (OSError, KeyError, TypeError, ValueError, UnicodeError, subprocess.SubprocessError):
        # Never interpolate manifest, credentials, SDK/Docker output or paths.
        print('{"status":"REFUSED","reason":"INVALID_OR_UNAVAILABLE_LOCAL_RECIPE"}')
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
