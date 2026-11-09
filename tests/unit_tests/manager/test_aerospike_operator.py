"""Production stop/journal primitives; only Docker and /proc are boundary fakes."""

import ast
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from gptcache.manager.scalar_data import aerospike_operator as op


CONTAINER = "a" * 64
MANIFEST = "b" * 64
ARGV_HASH = hashlib.sha256(b"/usr/bin/asd\0--foreground\0").hexdigest()
START = "2026-11-06T01:00:00.000000001Z"
FINISH = "2026-11-06T01:00:01.000000002Z"


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "journal"
    path.mkdir(mode=0o700)
    op._Journal.enroll(path, MANIFEST)
    return path


def read(path):
    return json.loads((path / "receipt.json").read_bytes())


class DockerBoundary:
    def __init__(self, directory):
        self.directory = directory
        self.calls = []
        self.processes = {
            456: {"pid": 456, "ticks": 123, "state": "S", "argv_sha256": ARGV_HASH}}
        self.row = {
            "Id": CONTAINER, "Image": "sha256:" + "c" * 64,
            "HostConfig": {
                "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
                "Privileged": False, "PidMode": "", "Devices": [], "CapAdd": None,
                "NetworkMode": "none", "PortBindings": {}},
            "Mounts": [{"Type": "bind", "Source": "/owned/data",
                        "Destination": "/opt/aerospike/data", "RW": True}],
            "Config": {"Entrypoint": ["/usr/bin/asd"], "Cmd": ["--foreground"],
                       "User": ""},
            "State": {"Status": "running", "Running": True, "Restarting": False,
                      "Dead": False, "Paused": False, "Error": "", "OOMKilled": False,
                      "Pid": 456, "ExitCode": 0,
                      "StartedAt": START, "FinishedAt": "0001-01-01T00:00:00Z"}}
        self.identity = op._server_identity(self.row)
        self.log = FINISH + " INFO finished clean shutdown - exiting\n"
        self.after_stop = None
        self.after_logs = None

    def inspect(self, container):
        assert container == CONTAINER
        self.calls.append("inspect")
        return deepcopy(self.row)

    def pids(self, container):
        assert container == CONTAINER
        self.calls.append("top")
        return sorted(self.processes)

    def stop(self, container):
        assert container == CONTAINER
        self.calls.append("stop")
        saved = read(self.directory)
        assert saved["state"] == "MAINTENANCE"
        assert saved["stop_intents"][container]["started"] == START
        assert saved["stop_intents"][container]["old_processes"][0]["ticks"] == 123
        self.row["State"].update(Status="exited", Running=False, Pid=0,
                                 FinishedAt=FINISH)
        self.processes.clear()
        if self.after_stop:
            self.after_stop()

    def logs(self, container, started, finished):
        assert (container, started, finished) == (CONTAINER, START, FINISH)
        self.calls.append("logs")
        if self.after_logs:
            self.after_logs()
        return self.log

    def process(self, pid):
        return deepcopy(self.processes.get(pid))


@pytest.fixture
def docker(directory, monkeypatch):
    boundary = DockerBoundary(directory)
    monkeypatch.setattr(op, "_process", boundary.process)
    return boundary


def stop(docker, journal):
    return op._clean_stop(docker, journal, container=CONTAINER,
                          expected_identity=docker.identity,
                          allowed_argv_sha256={ARGV_HASH})


def test_clean_stop_positive_real_journal_and_proof(directory, docker):
    with op._Journal(directory, MANIFEST) as journal:
        proof = stop(docker, journal)
        assert proof["started"] == START
        assert proof["finished"] == FINISH
        assert proof["clean_times_ns"] == [op._timestamp_ns(FINISH)]
        assert read(directory)["stop_proofs"][CONTAINER] == proof
        assert read(directory)["sequence"] == 2
        assert docker.calls.count("stop") == 1
    assert op._Journal.status(directory)["launch_blocked"] is True
    assert "finished clean shutdown" not in (directory / "receipt.json").read_text()


@pytest.mark.parametrize("states", [("R", "S"), ("S", "R")])
def test_clean_stop_scheduler_state_is_not_incarnation(directory, docker, monkeypatch, states):
    original = docker.process
    observations = iter(states)

    def process(pid):
        row = original(pid)
        if row is not None:
            row["state"] = next(observations)
        return row

    monkeypatch.setattr(op, "_process", process)
    with op._Journal(directory, MANIFEST) as journal:
        assert stop(docker, journal)["finished"] == FINISH
    assert docker.calls.count("stop") == 1


@pytest.mark.parametrize("field,value", [
    ("ticks", 124), ("argv_sha256", "e" * 64), ("state", "T"), ("state", "t"),
])
def test_clean_stop_rechecks_stable_identity_and_stopped_state(
        directory, docker, monkeypatch, field, value):
    original = docker.process
    count = 0

    def process(pid):
        nonlocal count
        row = original(pid)
        count += 1
        if row is not None and count == 2:
            row[field] = value
        return row

    monkeypatch.setattr(op, "_process", process)
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal):
            stop(docker, journal)
    assert "stop" not in docker.calls


class ControllerCrash(BaseException):
    pass


def test_successor_after_stop_before_ack_uses_durable_intent(directory, docker):
    def crash():
        raise ControllerCrash()
    docker.after_stop = crash
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(ControllerCrash):
            stop(docker, journal)
    assert read(directory)["stop_proofs"] == {}
    assert read(directory)["stop_intents"][CONTAINER]["started"] == START
    with op._Journal(directory, MANIFEST) as successor:
        assert stop(docker, successor)["finished"] == FINISH
    assert docker.calls.count("stop") == 1
    assert op._Journal.status(directory)["launch_blocked"]


@pytest.mark.parametrize("field,value", [
    ("ExitCode", 137), ("ExitCode", False), ("Pid", False), ("Pid", 1),
    ("OOMKilled", True), ("OOMKilled", 0), ("Status", "created"),
    ("Restarting", True), ("Dead", True),
    ("Paused", True), ("Error", "secret daemon error"),
    ("FinishedAt", START), ("StartedAt", FINISH),
])
def test_unclean_terminal_refuses(directory, docker, field, value):
    def damage():
        docker.row["State"][field] = value
    docker.after_stop = damage
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal):
            stop(docker, journal)
    assert read(directory)["stop_proofs"] == {}
    assert read(directory)["state"] == "MAINTENANCE"


@pytest.mark.parametrize("log", [
    "", "finished clean shutdown - exiting",
    "2026-11-06T00:00:00Z finished clean shutdown - exiting",
    "2026-11-06T02:00:00Z finished clean shutdown - exiting",
    "0001-01-01T00:00:00Z finished clean shutdown - exiting",
    FINISH + " user payload finished clean shutdown - exiting, but not a clean marker",
])
def test_missing_stale_future_or_unbounded_log_refuses(directory, docker, log):
    docker.log = log
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal):
            stop(docker, journal)
    assert read(directory)["stop_proofs"] == {}


def test_stopped_without_intent_is_not_settlement(directory, docker):
    docker.row["State"].update(Status="exited", Running=False, Pid=0, FinishedAt=FINISH)
    docker.processes.clear()
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal, match="MISSING_STOP_PROVENANCE"):
            stop(docker, journal)
    assert "logs" not in docker.calls


@pytest.mark.parametrize("when", ["after_stop", "after_logs"])
def test_old_process_survived_refuses(directory, docker, when):
    old = deepcopy(docker.processes)
    def survive():
        docker.processes = old
    setattr(docker, when, survive)
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal, match="OLD_PROCESS_SURVIVED"):
            stop(docker, journal)
    assert read(directory)["stop_proofs"] == {}


def test_pid_reused_is_not_old_process(directory, docker):
    def reused():
        docker.processes[456] = {
            "pid": 456, "ticks": 124, "state": "S", "argv_sha256": "d" * 64}
    docker.after_stop = reused
    with op._Journal(directory, MANIFEST) as journal:
        assert stop(docker, journal)["finished"] == FINISH


def test_unknown_process_refuses_before_stop_or_intent(directory, docker):
    docker.processes[456]["argv_sha256"] = "d" * 64
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal, match="UNKNOWN_CONTAINER_PROCESS"):
            stop(docker, journal)
    assert "stop" not in docker.calls
    assert read(directory)["stop_intents"] == {}


def test_reinspect_after_logs_detects_restart_race(directory, docker):
    def restart():
        docker.row["State"]["StartedAt"] = FINISH
    docker.after_logs = restart
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal, match="INCARNATION_CHANGED_DURING_PROOF"):
            stop(docker, journal)
    assert read(directory)["stop_proofs"] == {}


@pytest.mark.parametrize("section,field,value", [
    ("HostConfig", "NetworkMode", "host"),
    ("HostConfig", "RestartPolicy", {"Name": "always", "MaximumRetryCount": 0}),
    ("HostConfig", "Privileged", True), ("HostConfig", "PidMode", "host"),
    ("HostConfig", "Devices", ["/dev/sda"]),
    ("HostConfig", "CapAdd", ["SYS_ADMIN"]),
    ("Config", "Entrypoint", ["/bin/sh"]),
])
def test_identity_drift_before_stop(directory, docker, section, field, value):
    docker.row[section][field] = value
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal):
            stop(docker, journal)
    assert "stop" not in docker.calls


def test_journal_competing_operator_and_gate_survives_close(directory):
    with op._Journal(directory, MANIFEST):
        with pytest.raises(BlockingIOError):
            op._Journal(directory, MANIFEST)
    with op._Journal(directory, MANIFEST) as successor:
        assert successor.state["state"] == "MAINTENANCE"
    assert (directory / "lock").exists()


@pytest.mark.parametrize("name", ["lock", "receipt.json"])
def test_missing_registered_file_never_recreated(directory, name):
    (directory / name).unlink()
    with pytest.raises(FileNotFoundError):
        op._Journal(directory, MANIFEST)
    assert not (directory / name).exists()
    with pytest.raises(FileNotFoundError):
        op._Journal.status(directory)


@pytest.mark.parametrize("name", ["lock", "receipt.json"])
def test_symlinked_registered_file_refused(directory, name):
    target = directory / "other"
    (directory / name).rename(target)
    (directory / name).symlink_to(target)
    with pytest.raises(OSError):
        op._Journal(directory, MANIFEST)


def test_parent_symlink_and_noncanonical_directory_refused(directory):
    link = directory.parent / "alias"
    link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(OSError):
        op._Journal.status(link)
    with pytest.raises(op.OperatorRefusal, match="NONCANONICAL"):
        op._Journal.status(str(directory) + "/../journal")


def test_hardlink_and_unsafe_modes_refused(directory):
    os.link(directory / "lock", directory / "alias")
    with pytest.raises(op.OperatorRefusal, match="PERMISSIONS"):
        op._Journal(directory, MANIFEST)
    (directory / "alias").unlink()
    (directory / "receipt.json").chmod(0o644)
    with pytest.raises(op.OperatorRefusal, match="PERMISSIONS"):
        op._Journal(directory, MANIFEST)


def test_wrong_manifest_refused(directory):
    with pytest.raises(op.OperatorRefusal, match="INVALID_JOURNAL"):
        op._Journal(directory, "d" * 64)


def test_stable_lock_replacement_detected_current_and_successor(directory):
    with op._Journal(directory, MANIFEST) as journal:
        (directory / "lock").rename(directory / "old")
        (directory / "lock").touch(mode=0o600)
        with pytest.raises(op.OperatorRefusal, match="LOCK_REPLACED"):
            journal.check()
    with pytest.raises(op.OperatorRefusal, match="LOCK_REPLACED"):
        op._Journal(directory, MANIFEST)
    with pytest.raises(op.OperatorRefusal, match="LOCK_REPLACED"):
        op._Journal.status(directory)


def test_receipt_edit_and_fork_rejected(directory, monkeypatch):
    with op._Journal(directory, MANIFEST) as journal:
        monkeypatch.setattr(op.os, "getpid", lambda: journal.pid + 1)
        with pytest.raises(op.OperatorRefusal, match="JOURNAL_NOT_HELD"):
            journal.check()
    monkeypatch.undo()
    with op._Journal(directory, MANIFEST) as journal:
        state = read(directory)
        state["sequence"] += 1
        (directory / "receipt.json").write_bytes(op._json(state))
        with pytest.raises(op.OperatorRefusal, match="JOURNAL_CHANGED"):
            journal.check()


def test_store_fsync_order_before_stop(directory, docker, monkeypatch):
    events = []
    fsync, replace, docker_stop = op.os.fsync, op.os.replace, docker.stop
    def sync(fd):
        events.append("directory-sync" if os.path.isdir("/proc/self/fd/" + str(fd))
                      else "file-sync")
        return fsync(fd)
    def rename(*args, **kwargs):
        events.append("replace")
        return replace(*args, **kwargs)
    def stop_after_durable_intent(container):
        assert events == ["file-sync", "replace", "directory-sync"]
        events.append("stop")
        docker_stop(container)
    monkeypatch.setattr(op.os, "fsync", sync)
    monkeypatch.setattr(op.os, "replace", rename)
    monkeypatch.setattr(docker, "stop", stop_after_durable_intent)
    with op._Journal(directory, MANIFEST) as journal:
        stop(docker, journal)
    assert events == ["file-sync", "replace", "directory-sync", "stop",
                      "file-sync", "replace", "directory-sync"]


@pytest.mark.parametrize("which", ["file", "directory"])
def test_fsync_failure_prevents_stop_and_poison_journal(directory, docker, monkeypatch,
                                                      which):
    fsync = op.os.fsync
    def fail(fd):
        is_dir = os.path.isdir("/proc/self/fd/" + str(fd))
        if is_dir == (which == "directory"):
            raise OSError("sensitive filesystem output")
        return fsync(fd)
    with op._Journal(directory, MANIFEST) as journal:
        monkeypatch.setattr(op.os, "fsync", fail)
        with pytest.raises(OSError):
            stop(docker, journal)
        with pytest.raises(op.OperatorRefusal, match="JOURNAL_NOT_HELD"):
            journal.check()
    assert "stop" not in docker.calls
    assert read(directory)["state"] == "MAINTENANCE"


def test_status_read_only_without_sdk_docker_or_writes(directory, monkeypatch):
    before = {p.name: (p.stat().st_mtime_ns, p.read_bytes()) for p in directory.iterdir()}
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected mutation or process launch")
    monkeypatch.setattr(op.subprocess, "run", forbidden)
    monkeypatch.setattr(op.os, "fsync", forbidden)
    assert op._Journal.status(directory) == {
        "state": "MAINTENANCE", "launch_blocked": True, "sequence": 0}
    assert before == {p.name: (p.stat().st_mtime_ns, p.read_bytes())
                      for p in directory.iterdir()}


def test_gate_cannot_be_cleared_by_generic_store(directory):
    with op._Journal(directory, MANIFEST) as journal:
        state = journal.state
        state["state"] = "PUBLIC"
        with pytest.raises(op.OperatorRefusal, match="GATE_CHANGE"):
            journal.store(state)
    assert op._Journal.status(directory)["launch_blocked"]


def test_duplicate_json_and_bad_time_rejected():
    with pytest.raises(op.OperatorRefusal, match="DUPLICATE"):
        op._load('{"state":"PUBLIC","state":"MAINTENANCE"}')
    for value in (True, None, "2026-02-30T00:00:00Z", "1970-01-01T00:00:00Z",
                  "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00.1234567890Z"):
        with pytest.raises(op.OperatorRefusal):
            op._timestamp_ns(value)
    assert op._timestamp_ns(FINISH) - op._timestamp_ns(START) == 1000000001


def test_process_identity_real_proc_handles_parentheses():
    row = op._process(os.getpid())
    assert row["pid"] == os.getpid()
    assert row["ticks"] > 0
    assert len(row["argv_sha256"]) == 64


def test_docker_fixed_argv_and_redacted_errors(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 1, b"secret user text", b"password=secret")
    monkeypatch.setattr(op.subprocess, "run", run)
    with pytest.raises(op.OperatorRefusal, match="^DOCKER_COMMAND_FAILED$"):
        op._Docker().stop(CONTAINER)
    assert calls[0][0] == ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock",
                           "stop", "--time", "60", CONTAINER]
    assert calls[0][1]["shell"] is False
    assert calls[0][1]["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C"}
    with pytest.raises(op.OperatorRefusal, match="INVALID_CONTAINER_ID"):
        op._Docker().stop("x; touch /tmp/unowned")
    assert len(calls) == 1


def test_docker_read_transport_positive_and_timestamp_bounded_argv(monkeypatch, docker):
    calls = []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        verb = argv[3]
        if verb == "inspect":
            out, err = json.dumps([docker.row]).encode(), b""
        elif verb == "top":
            out, err = b"PID\n456\n", b""
        else:
            assert verb == "logs"
            out, err = b"", docker.log.encode()
        return subprocess.CompletedProcess(argv, 0, out, err)
    monkeypatch.setattr(op.subprocess, "run", run)
    transport = op._Docker()
    assert transport.inspect(CONTAINER) == docker.row
    assert transport.pids(CONTAINER) == [456]
    assert docker.log in transport.logs(CONTAINER, START, FINISH)
    assert calls[-1][0][3:] == [
        "logs", "--timestamps", "--tail", "2048",
        "--since", START, "--until", FINISH, CONTAINER]
    assert all(call[1]["shell"] is False for call in calls)


def test_docker_bounded_log_tail_still_enforces_byte_cap(monkeypatch):
    def run(argv, **kwargs):
        assert argv[3:] == [
            "logs", "--timestamps", "--tail", "2048",
            "--since", START, "--until", FINISH, CONTAINER]
        return subprocess.CompletedProcess(argv, 0, b"x" * (4 * 1024 * 1024 + 1), b"")
    monkeypatch.setattr(op.subprocess, "run", run)
    with pytest.raises(op.OperatorRefusal, match="^DOCKER_OUTPUT_TOO_LARGE$"):
        op._Docker().logs(CONTAINER, START, FINISH)


@pytest.mark.parametrize("raw", [b"PID\n", b"PID\n1\n1\n", b"PID\n0\n",
                                b"PID\nsecret\n", b"COMMAND\n123\n"])
def test_docker_top_invalid_inventory_redacted(monkeypatch, raw):
    monkeypatch.setattr(op.subprocess, "run",
                        lambda argv, **kw: subprocess.CompletedProcess(argv, 0, raw, b""))
    with pytest.raises(op.OperatorRefusal, match="INVALID_PROCESS_INVENTORY"):
        op._Docker().pids(CONTAINER)


def test_journal_directory_replacement_detected(directory):
    with op._Journal(directory, MANIFEST) as journal:
        directory.rename(directory.with_name("old-journal"))
        directory.mkdir(mode=0o700)
        with pytest.raises(op.OperatorRefusal, match="JOURNAL_DIRECTORY_REPLACED"):
            journal.check()


def test_reinspect_race_before_stop_keeps_intent_but_never_stops(directory, docker,
                                                               monkeypatch):
    original = docker.inspect
    count = 0
    def drift(container):
        nonlocal count
        count += 1
        if count == 2:
            docker.row["State"]["StartedAt"] = FINISH
        return original(container)
    monkeypatch.setattr(docker, "inspect", drift)
    with op._Journal(directory, MANIFEST) as journal:
        with pytest.raises(op.OperatorRefusal, match="INCARNATION_CHANGED_BEFORE_STOP"):
            stop(docker, journal)
    assert "stop" not in docker.calls
    assert read(directory)["stop_proofs"] == {}
    assert CONTAINER in read(directory)["stop_intents"]


def test_source_python38_grammar_and_no_sdk_or_namespace_mutation():
    source = Path(op.__file__).read_text()
    ast.parse(source, feature_version=(3, 8))
    tree = ast.parse(source)
    imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    imports += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
                for alias in node.names]
    assert not any("aerospike_sdk" in name for name in imports)
    assert "set-config" not in source


def test_cli_cannot_silently_report_success_without_required_flags():
    result = subprocess.run([sys.executable, "-m",
                             "gptcache.manager.scalar_data.aerospike_operator",
                             "init-new"], capture_output=True, timeout=10)
    assert result.returncode != 0
    assert b"--declare-new-logical-cache" in result.stderr


MAINTENANCE = "d" * 64
BOOT = "6b3a6d06-6865-49f7-917c-e8ff46ed9d3a"
CONFIG = """service {
    proto-fd-max 1024
}
network {
    service {
        address any
        port 3000
    }
}
namespace test {
    replication-factor 1
    storage-engine device {
        file /opt/aerospike/data/operator.dat
        filesize 1G
    }
}
"""


def resource(path, directory=False):
    info = path.stat()
    value = {"path": str(path), "device": info.st_dev, "inode": info.st_ino}
    if not directory:
        value["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return value


class ControllerDocker:
    """OS/Docker command boundary only; all orchestration above is production."""

    def __init__(self, manifest, journal, runtime):
        self.manifest, self.journal = manifest, journal
        self.calls, self.processes, self.rows = [], {}, {}
        self.clock = 1
        self.next_pid = 456
        self.after_stop = self.after_start = None
        self.image_config = {"Entrypoint": ["/entrypoint.sh"], "Cmd": ["asd"],
                             "User": "", "Env": ["PATH=/usr/bin:/bin"], "WorkingDir": ""}
        for mode, container in manifest["containers"].items():
            row = deepcopy(DockerBoundary(journal).row)
            row["Id"], row["Config"] = container, deepcopy(self.image_config)
            row["HostConfig"]["NetworkMode"] = "none" if mode == "maintenance" else "bridge"
            row["HostConfig"]["PortBindings"] = {} if mode == "maintenance" else {
                "3000/tcp": [{"HostIp": "127.0.0.1",
                              "HostPort": str(manifest["public_port"])}]}
            row["NetworkSettings"] = {"Networks": {
                "none" if mode == "maintenance" else "bridge": {}}, "Ports": {}}
            row["Mounts"] = [
                {"Type": "bind", "Source": manifest["data"]["path"],
                 "Destination": "/opt/aerospike/data", "RW": True},
                {"Type": "bind", "Source": manifest["configs"][mode]["path"],
                 "Destination": "/etc/aerospike/aerospike.conf", "RW": False},
                {"Type": "bind", "Source": str(runtime), "Destination": "/op", "RW": False},
                {"Type": "bind", "Source": str(journal), "Destination": "/op/journal",
                 "RW": False}]
            row["State"].update(Status="created", Running=False, Pid=0,
                                StartedAt="0001-01-01T00:00:00Z")
            self.rows[container] = row
        self._run_server(manifest["containers"]["public"])

    def _stamp(self):
        self.clock += 1
        return "2026-11-06T01:00:{:02d}.000000001Z".format(self.clock)

    def _run_server(self, container):
        self.next_pid += 1
        self.rows[container]["State"].update(
            Status="running", Running=True, Pid=self.next_pid, StartedAt=self._stamp())
        self.processes[self.next_pid] = {
            "pid": self.next_pid, "ticks": self.next_pid * 10,
            "state": "S", "argv_sha256": ARGV_HASH}

    def daemon(self):
        return "owned-daemon"

    def image(self, image):
        assert image == self.manifest["image"]
        return deepcopy(self.image_config)

    def inspect(self, container):
        self.calls.append(("inspect", container))
        return deepcopy(self.rows[container])

    def pids(self, container):
        main = self.rows[container]["State"]["Pid"]
        return [main] + [pid for pid, row in self.processes.items()
                         if row["argv_sha256"] != ARGV_HASH] if main else []

    def stop(self, container):
        self.calls.append(("stop", container))
        receipt = read(self.journal)
        assert receipt["state"] == "MAINTENANCE"
        assert receipt["stop_intents"][container]["server_pid"] > 0
        main = self.rows[container]["State"]["Pid"]
        self.processes.pop(main)
        self.processes = {pid: row for pid, row in self.processes.items()
                          if row["argv_sha256"] == ARGV_HASH}
        self.rows[container]["State"].update(
            Status="exited", Running=False, Pid=0, FinishedAt=self._stamp())
        if self.after_stop:
            self.after_stop(container)

    def start(self, container):
        self.calls.append(("start", container))
        assert read(self.journal)["state"] == "MAINTENANCE"
        assert not any(row["State"]["Running"] for row in self.rows.values())
        other = next(key for key in self.rows if key != container)
        assert other in read(self.journal)["stop_proofs"]
        self._run_server(container)
        if self.after_start:
            self.after_start(container)

    def logs(self, container, started, finished):
        self.calls.append(("logs", container))
        assert self.rows[container]["State"]["StartedAt"] == started
        assert self.rows[container]["State"]["FinishedAt"] == finished
        return finished + " INFO finished clean shutdown - exiting\n"


@pytest.fixture
def controller(tmp_path, monkeypatch):
    journal = tmp_path / "journal"
    journal.mkdir(mode=0o700)
    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    data.chmod(0o755)
    (data / "operator.dat").write_bytes(b"persistent-existing-history")
    (data / "operator.dat").chmod(0o644)
    public, maintenance = tmp_path / "public.conf", tmp_path / "maintenance.conf"
    public.write_text(CONFIG)
    maintenance.write_text(CONFIG.replace("address any", "address 127.0.0.1"))
    public.chmod(0o644)
    maintenance.chmod(0o644)
    runtime = journal / "runtime"
    runtime.mkdir(mode=0o755)
    runtime.chmod(0o755)
    (runtime / "source-shaped-fixture.txt").write_text("No SDK mutation is simulated here.")
    (runtime / "source-shaped-fixture.txt").chmod(0o644)
    manifest = {
        "recipe": "local-precreated-file-v1", "host": MANIFEST, "daemon": "owned-daemon",
        "namespace": "test", "image": "sha256:" + "c" * 64,
        "containers": {"public": CONTAINER, "maintenance": MAINTENANCE},
        "data": resource(data, True),
        "configs": {"public": resource(public), "maintenance": resource(maintenance)},
        "runtime_sha256": op._tree_digest(str(runtime)),
        "server_argv_sha256": [ARGV_HASH], "public_port": 43200,
        "launchers": {"recipe": "trusted-manual-gate-v1", "gate": str(journal),
                      "automatic_restart": False, "queues": "discard-old-admissions",
                      "writer_argv_sha256": [], "writers": []}}
    path = tmp_path / "manifest.json"
    path.write_bytes(op._json(manifest))
    path.chmod(0o600)
    # Provisioning is separate, explicit, and happens before installing the RO bundle.
    runtime.rename(tmp_path / "fixture-runtime")
    op._Journal.enroll(journal, hashlib.sha256(path.read_bytes()).hexdigest())
    (tmp_path / "fixture-runtime").rename(runtime)
    instance = op.DockerMaintenanceOperator.from_manifest(path, journal_dir=journal)
    docker = ControllerDocker(manifest, journal, runtime)
    instance.docker = docker
    monkeypatch.setattr(op, "_host_id", lambda: MANIFEST)
    monkeypatch.setattr(op, "_host_boot", lambda: BOOT)
    monkeypatch.setattr(op, "_host_writers", lambda allowed: {})
    own_process = op._process(os.getpid())
    monkeypatch.setattr(op, "_process", lambda pid: deepcopy(
        own_process if pid == os.getpid() else docker.processes.get(pid)))
    monkeypatch.setattr(instance.runtime, "prepare", instance.runtime.check)
    requests = []

    def execute(container, request):
        assert request["proof"]["sequence"] == read(journal)["sequence"]
        assert read(journal)["controller"]["pending"]["attempt"] == request["attempt"]
        assert read(journal)["state"] == "MAINTENANCE"
        assert container == request["proof"]["incarnation"]["container"]
        if request["operation"] == "initialize":
            intent = read(journal)["controller"]["intents"][request["intent"]["intent_id"]]
            assert intent["root_attempted"] is True
        requests.append(deepcopy(request))
        return {"version": 1, "nonce": request["nonce"], "attempt": request["attempt"],
                "operation": request["operation"], "uid": 1000, "pid": 22, "status": "OK",
                "results": [{"qid": qid, "status": "PURGED"} for qid in request["qids"]]}

    monkeypatch.setattr(instance.runtime, "execute", execute)
    instance.test_requests = requests
    return instance, docker, journal, path


def initialize(instance):
    return instance.initialize_scope(namespace="test", cache_scope="scope",
                                     intent_id="new-cache", vector_binding="vector-store-v1")


@pytest.mark.parametrize("before,after", [("R", "S"), ("S", "R"), ("R", "D")])
def test_controller_live_scheduler_state_is_not_process_identity(
        controller, before, after):
    instance, docker, journal, _ = controller
    initialize(instance)
    with op._Journal(journal, instance.digest) as holder:
        control = holder.state["controller"]
        old = control["incarnation"]["processes"][0]
        old["state"] = before
        docker.processes[old["pid"]]["state"] = after
        instance._live(holder, control)


@pytest.mark.parametrize("change", [
    {"state": "T"}, {"state": "t"}, {"state": "Z"}, {"state": "X"},
    {"pid": 999}, {"ticks": 999}, {"argv_sha256": "e" * 64},
])
def test_controller_live_scheduler_fix_preserves_process_boundary(controller, change):
    instance, docker, journal, _ = controller
    initialize(instance)
    with op._Journal(journal, instance.digest) as holder:
        control = holder.state["controller"]
        pid = control["incarnation"]["pid"]
        docker.processes[pid].update(change)
        with pytest.raises(op.OperatorRefusal):
            instance._live(holder, control)


def test_controller_live_process_inventory_order_is_not_identity(controller, monkeypatch):
    instance, docker, journal, _ = controller
    initialize(instance)
    with op._Journal(journal, instance.digest) as holder:
        control = holder.state["controller"]
        second = dict(control["incarnation"]["processes"][0], pid=998, ticks=9980)
        control["incarnation"]["processes"].append(second)
        docker.processes[998] = dict(second, state="R")
        main = control["incarnation"]["pid"]
        monkeypatch.setattr(docker, "pids", lambda container: [998, main])
        instance._live(holder, control)
        docker.processes[999] = dict(second, pid=999)
        monkeypatch.setattr(docker, "pids", lambda container: [998, main, 999])
        with pytest.raises(op.OperatorRefusal, match="RUNTIME_PROCESS_BOUNDARY_CHANGED"):
            instance._live(holder, control)
        monkeypatch.setattr(docker, "pids", lambda container: [main])
        with pytest.raises(op.OperatorRefusal, match="RUNTIME_PROCESS_BOUNDARY_CHANGED"):
            instance._live(holder, control)


@pytest.mark.parametrize("mode", ["public", "maintenance"])
@pytest.mark.parametrize("image_default", ["omitted", None, ""])
def test_controller_boundary_docker_default_strings(controller, mode, image_default):
    instance, docker, _, _ = controller
    for key in ("User", "WorkingDir"):
        if image_default == "omitted":
            docker.image_config.pop(key)
        else:
            docker.image_config[key] = image_default
    container = instance.manifest["containers"][mode]
    assert instance._boundary(mode) == docker.rows[container]
    assert docker.calls == [("inspect", container)]


@pytest.mark.parametrize("mode", ["public", "maintenance"])
@pytest.mark.parametrize("key,value", [
    ("User", "root"), ("User", "1000"), ("WorkingDir", "/"), ("WorkingDir", "/tmp"),
    ("User", False), ("User", 0), ("User", []), ("User", {}),
    ("WorkingDir", False), ("WorkingDir", 0), ("WorkingDir", []), ("WorkingDir", {}),
    ("Entrypoint", []), ("Cmd", []), ("Env", []), ("Healthcheck", {}),
    ("StopSignal", ""), ("Shell", []),
])
def test_controller_boundary_default_strings_reject_overrides(controller, mode, key, value):
    instance, docker, _, _ = controller
    for default_key in ("User", "WorkingDir"):
        docker.image_config.pop(default_key)
    container = instance.manifest["containers"][mode]
    docker.rows[container]["Config"][key] = value
    with pytest.raises(op.OperatorRefusal, match="UNSUPPORTED_CONTAINER_OVERRIDES"):
        instance._boundary(mode)
    assert docker.calls == [("inspect", container)]


@pytest.mark.parametrize("mode", ["public", "maintenance"])
@pytest.mark.parametrize("key", ["User", "WorkingDir"])
@pytest.mark.parametrize("value", [False, 0, [], {}])
def test_controller_boundary_default_strings_reject_malformed_image(controller, mode, key, value):
    instance, docker, _, _ = controller
    docker.image_config[key] = value
    with pytest.raises(op.OperatorRefusal, match="UNSUPPORTED_CONTAINER_OVERRIDES"):
        instance._boundary(mode)


def test_controller_real_orchestration_init_recover_finish(controller):
    instance, docker, journal, _ = controller
    assert initialize(instance) == {"status": "ACTIVE", "intent_id": "new-cache"}
    first = read(journal)["controller"]["incarnation"]
    assert instance.test_requests[-1]["historical_root_attempted"] is False
    assert instance.test_requests[-1]["intent"]["root_attempted"] is True
    result = instance.recover_session_purge(
        namespace="test", cache_scope="scope", qids=[7, 7, 8])
    assert result["results"] == [{"qid": 7, "status": "PURGED"},
                                  {"qid": 8, "status": "PURGED"}]
    assert instance.test_requests[-1]["qids"] == [7, 8]
    second = read(journal)["controller"]["incarnation"]
    assert first["started"] != second["started"]
    assert instance.finish_maintenance()["status"] == "PUBLIC"
    assert instance.status()["launch_blocked"] is False
    assert instance.status()["recovery"] == result
    assert docker.rows[CONTAINER]["State"]["Running"] is True
    assert docker.rows[MAINTENANCE]["State"]["Running"] is False
    assert (Path(instance.manifest["data"]["path"]) / "operator.dat").read_bytes() == (
        b"persistent-existing-history")
    assert read(journal)["controller"]["pending"] is None


def test_controller_bootstrap_failure_resume_historical_attempt(controller, monkeypatch):
    instance, docker, journal, _ = controller
    execute = instance.runtime.execute

    def lost_reply(container, request):
        if request["operation"] == "initialize":
            docker.processes[999] = {
                "pid": 999, "ticks": 10000, "state": "S",
                "argv_sha256": instance._runtime_argv_hash()}
            raise op.OperatorRefusal("RUNTIME_FAILED_OUTCOME_UNKNOWN")
        return execute(container, request)
    monkeypatch.setattr(instance.runtime, "execute", lost_reply)
    with pytest.raises(op.OperatorRefusal, match="OUTCOME_UNKNOWN"):
        initialize(instance)
    assert instance.status()["pending"]["operation"] == "initialize"
    assert instance.status()["launch_blocked"] is True
    old_started = read(journal)["controller"]["incarnation"]["started"]
    monkeypatch.setattr(instance.runtime, "execute", execute)
    instance.resume_initialization(intent_id="new-cache")
    assert 999 not in docker.processes
    assert read(journal)["controller"]["incarnation"]["started"] != old_started
    assert instance.test_requests[-1]["historical_root_attempted"] is True
    assert instance.test_requests[-1]["intent"]["boot"] == (
        instance.test_requests[0]["intent"]["boot"] if instance.test_requests[0]["intent"]
        else read(journal)["controller"]["intents"]["new-cache"]["boot"])


def rewrite_manifest(controller, mutate):
    instance, _, _, path = controller
    value = json.loads(path.read_bytes())
    mutate(value)
    path.write_bytes(op._json(value))
    return value


@pytest.mark.parametrize("mutate", [
    lambda m: m.update(command=["sh", "-c", "secret"]),
    lambda m: m.update(runtime_path="/tmp/evil.py"),
    lambda m: m.update(import_hook="evil:factory"),
    lambda m: m.update(namespace="test;set-config"),
    lambda m: m.update(image="aerospike:latest"),
    lambda m: m.update(public_port=True),
    lambda m: m["containers"].update(maintenance=CONTAINER),
    lambda m: m["launchers"].update(automatic_restart=True),
    lambda m: m["launchers"].update(recipe="systemd"),
    lambda m: m["launchers"].update(queues="pause-then-resume"),
    lambda m: m["launchers"].update(gate="/other/gate"),
    lambda m: m["data"].update(inode=1),
    lambda m: m["data"].update(path="/tmp/../tmp"),
    lambda m: m["configs"]["public"].update(sha256="0" * 64),
])
def test_allowlisted_manifest_refusals(controller, mutate):
    _, _, journal, path = controller
    before = (journal / "receipt.json").read_bytes()
    rewrite_manifest(controller, mutate)
    with pytest.raises(op.OperatorRefusal):
        op.DockerMaintenanceOperator.from_manifest(path, journal_dir=journal)
    assert (journal / "receipt.json").read_bytes() == before


@pytest.mark.parametrize("addition", [
    "include /tmp/evil\n", "lua-user-path /tmp/evil\n", "xdr {\n}\n",
    "namespace other {\n}\n", "service {\n}\n",
])
def test_server_config_refuses_unreviewed_directives(controller, addition):
    instance, _, journal, path = controller
    public = Path(instance.manifest["configs"]["public"]["path"])
    maintenance = Path(instance.manifest["configs"]["maintenance"]["path"])
    public.write_text(CONFIG + addition)
    maintenance.write_text((CONFIG + addition).replace("address any", "address 127.0.0.1"))
    rewrite_manifest(controller, lambda m: m.update(configs={
        "public": resource(public), "maintenance": resource(maintenance)}))
    with pytest.raises(op.OperatorRefusal, match="CONFIG"):
        op.DockerMaintenanceOperator.from_manifest(path, journal_dir=journal)


@pytest.mark.parametrize("mutate,code", [
    (lambda row: row["HostConfig"].update(NetworkMode="host"), "UNSAFE_NETWORK"),
    (lambda row: row["HostConfig"].update(PortBindings={"3000/tcp": [{}]}), "UNSAFE_NETWORK"),
    (lambda row: row["HostConfig"].update(Privileged=True), "UNSAFE_CONTAINER_POLICY"),
    (lambda row: row["HostConfig"].update(PidMode="host"), "UNSAFE_CONTAINER_POLICY"),
    (lambda row: row["HostConfig"].update(CapAdd=["SYS_ADMIN"]), "UNSAFE_CONTAINER_POLICY"),
    (lambda row: row["HostConfig"].update(SecurityOpt=["seccomp=unconfined"]),
     "UNSUPPORTED_CONTAINER_OVERRIDES"),
    (lambda row: row["HostConfig"].update(AutoRemove=True), "UNSUPPORTED_CONTAINER_OVERRIDES"),
    (lambda row: row["Config"].update(Cmd=["sh"]), "UNSUPPORTED_CONTAINER_OVERRIDES"),
    (lambda row: row["Config"].update(Env=["PASSWORD=secret"]), "UNSUPPORTED_CONTAINER_OVERRIDES"),
    (lambda row: row["Mounts"][0].update(Source="/other/data"), "UNAUTHORIZED_MOUNTS"),
    (lambda row: row["Mounts"][2].update(RW=True), "UNAUTHORIZED_MOUNTS"),
    (lambda row: row["NetworkSettings"].update(Networks={"bridge": {}}),
     "MAINTENANCE_NETWORK_EXPOSED"),
    (lambda row: row["NetworkSettings"].update(Ports={"3000/tcp": [{}]}),
     "MAINTENANCE_NETWORK_EXPOSED"),
])
def test_container_refusals_before_any_stop(controller, mutate, code):
    instance, docker, journal, _ = controller
    mutate(docker.rows[MAINTENANCE])
    before = (journal / "receipt.json").read_bytes()
    with pytest.raises(op.OperatorRefusal, match=code):
        initialize(instance)
    assert not any(action in ("stop", "start") for action, _ in docker.calls)
    assert (journal / "receipt.json").read_bytes() == before


@pytest.mark.parametrize("fault", ["payload", "data", "config", "manifest", "daemon", "host"])
def test_pinned_resource_drift(controller, monkeypatch, fault):
    instance, docker, journal, path = controller
    initialize(instance)
    if fault == "payload":
        (Path(instance.runtime.path) / "source-shaped-fixture.txt").write_text("changed")
    elif fault == "data":
        data = Path(instance.manifest["data"]["path"]) / "operator.dat"
        data.rename(data.with_suffix(".old"))
        data.write_bytes(b"persistent-existing-history")
        data.chmod(0o644)
    elif fault == "config":
        Path(instance.manifest["configs"]["public"]["path"]).write_text(CONFIG + "\n")
    elif fault == "manifest":
        rewrite_manifest(controller, lambda m: m.update(public_port=43201))
    elif fault == "daemon":
        monkeypatch.setattr(docker, "daemon", lambda: "other")
    else:
        monkeypatch.setattr(op, "_host_id", lambda: "f" * 64)
    with pytest.raises(op.OperatorRefusal):
        instance.resume_initialization(intent_id="new-cache")
    assert op._Journal.status(journal)["launch_blocked"] is True


def test_missing_fixed_runtime_actionable_without_journal_mutation(controller, monkeypatch):
    instance, docker, journal, _ = controller
    monkeypatch.setattr(op, "__file__", str(journal / "missing-framework/operator.py"))
    monkeypatch.setattr(instance.runtime, "prepare",
                        lambda: op._FixedRuntime.prepare(instance.runtime))
    before = (journal / "receipt.json").read_bytes()
    with pytest.raises(op.OperatorRefusal, match="FIXED_RUNTIME_MISSING_IMPLEMENT_PRIVATE"):
        initialize(instance)
    assert (journal / "receipt.json").read_bytes() == before
    assert docker.calls == []


def test_status_and_help_readonly_without_sdk_or_runtime(controller, monkeypatch, capsys):
    instance, docker, journal, path = controller
    before = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, p.read_bytes())
              for p in journal.iterdir() if p.is_file()}
    monkeypatch.setattr(op._FixedRuntime, "prepare",
                        lambda self: pytest.fail("status must not prepare runtime"))
    assert op.main(["status", "--manifest", str(path), "--journal-dir", str(journal)]) == 0
    assert json.loads(capsys.readouterr().out)["launch_blocked"] is True
    with pytest.raises(SystemExit) as caught:
        op.main(["--help"])
    assert caught.value.code == 0
    assert docker.calls == []
    assert before == {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, p.read_bytes())
                      for p in journal.iterdir() if p.is_file()}


def test_controller_competing_operator_and_missing_journal_never_reenrolls(controller):
    instance, _, journal, _ = controller
    with op._Journal(journal, instance.digest):
        with pytest.raises(BlockingIOError):
            initialize(instance)
    (journal / "receipt.json").rename(journal / "lost-receipt")
    with pytest.raises(FileNotFoundError):
        initialize(instance)
    assert not (journal / "receipt.json").exists()


def test_competing_and_unknown_intents_refuse(controller):
    instance, docker, journal, _ = controller
    initialize(instance)
    before = (journal / "receipt.json").read_bytes()
    calls = len(docker.calls)
    with pytest.raises(op.OperatorRefusal, match="COMPETING"):
        initialize(instance)
    with pytest.raises(op.OperatorRefusal, match="UNKNOWN_INTENT"):
        instance.resume_initialization(intent_id="other")
    assert (journal / "receipt.json").read_bytes() == before
    assert not any(action in ("stop", "start") for action, _ in docker.calls[calls:])


@pytest.mark.parametrize("qids", [[True], [0], [-1], [1 << 63], ["1"], "1", None])
def test_qid_refusals_before_entry(controller, qids):
    instance, docker, _, _ = controller
    with pytest.raises(op.OperatorRefusal, match="INVALID_QIDS"):
        instance.recover_session_purge(namespace="test", cache_scope="scope", qids=qids)
    assert docker.calls == []


def test_unknown_managed_writer_refuses_before_public_clean_stop(controller, monkeypatch):
    instance, docker, journal, _ = controller
    monkeypatch.setattr(op, "_host_writers", lambda allowed: {777: {"pid": 777}})
    with pytest.raises(op.OperatorRefusal, match="UNREGISTERED_MANAGED_WRITER"):
        initialize(instance)
    assert not any(action == "stop" for action, _ in docker.calls)
    assert read(journal)["state"] == "MAINTENANCE"


def test_registered_writers_terminal_before_public_stop(controller, monkeypatch):
    instance, docker, journal, path = controller
    value = rewrite_manifest(controller, lambda m: m["launchers"].update(
        writer_argv_sha256=["e" * 64], writers=[{
            "pid": 777, "ticks": 77, "argv_sha256": "e" * 64, "uid": 1000, "boot": BOOT}]))
    instance.manifest, instance.digest = op._manifest(path, journal)
    # Test enrollment fixture binds the replacement manifest before any operator work.
    receipt = read(journal)
    receipt["manifest"] = instance.digest
    (journal / "receipt.json").write_bytes(op._json(receipt))
    live = {777: {"pid": 777, "ticks": 77, "state": "T", "argv_sha256": "e" * 64}}
    monkeypatch.setattr(op, "_host_writers", lambda allowed: deepcopy(live))
    observed = []

    def terminate(writer):
        assert read(journal)["controller"]["stage"] == "terminate_writers"
        assert not any(action == "stop" for action, _ in docker.calls)
        observed.append(writer["pid"])
        live.clear()
    monkeypatch.setattr(op, "_terminate_writer", terminate)
    initialize(instance)
    assert observed == [777]
    assert read(journal)["controller"]["writers"] == value["launchers"]["writers"]


def test_controller_successor_after_public_stop_before_ack(controller):
    instance, docker, journal, _ = controller

    def crash(container):
        if container == CONTAINER:
            raise ControllerCrash()
    docker.after_stop = crash
    with pytest.raises(ControllerCrash):
        initialize(instance)
    assert CONTAINER in read(journal)["stop_intents"]
    assert CONTAINER not in read(journal)["stop_proofs"]
    assert not docker.rows[MAINTENANCE]["State"]["Running"]
    docker.after_stop = None
    instance.resume_initialization(intent_id="new-cache")
    assert docker.calls.count(("stop", CONTAINER)) == 1
    assert read(journal)["controller"]["intents"]["new-cache"]["status"] == "ACTIVE"
    assert instance.test_requests[-1]["historical_root_attempted"] is False


def test_p3_prior_root_attempt_absent_refuses_without_zero_fill(controller, monkeypatch):
    instance, _, journal, _ = controller
    execute = instance.runtime.execute
    initialization_requests = []

    def runtime(container, request):
        if request["operation"] == "initialize":
            initialization_requests.append(request)
            if request["historical_root_attempted"]:
                raise op.OperatorRefusal("PRIOR_ATTEMPT_ROOT_ABSENT_NO_ZERO_FILL")
            raise op.OperatorRefusal("LOST_CREATE_REPLY")
        return execute(container, request)
    monkeypatch.setattr(instance.runtime, "execute", runtime)
    with pytest.raises(op.OperatorRefusal, match="LOST_CREATE_REPLY"):
        initialize(instance)
    boot = read(journal)["controller"]["intents"]["new-cache"]["boot"]
    with pytest.raises(op.OperatorRefusal, match="NO_ZERO_FILL"):
        instance.resume_initialization(intent_id="new-cache")
    assert [r["historical_root_attempted"] for r in initialization_requests] == [False, True]
    assert all(r["intent"]["root_attempted"] is True for r in initialization_requests)
    assert all(r["intent"]["boot"] == boot for r in initialization_requests)
    assert read(journal)["state"] == "MAINTENANCE"
    assert read(journal)["controller"]["intents"]["new-cache"]["status"] == "INITIALIZING"


def test_finish_interruption_public_reachable_but_gate_persists(controller, monkeypatch):
    instance, docker, journal, _ = controller
    initialize(instance)
    execute = instance.runtime.execute

    def crash(container, request):
        if container == CONTAINER and request["operation"] == "validate":
            raise ControllerCrash()
        return execute(container, request)
    monkeypatch.setattr(instance.runtime, "execute", crash)
    with pytest.raises(ControllerCrash):
        instance.finish_maintenance()
    assert docker.rows[CONTAINER]["State"]["Running"] is True
    assert instance.status()["launch_blocked"] is True
    assert read(journal)["controller"]["stage"] == "finish_public_verify"
    with pytest.raises(op.OperatorRefusal, match="MAINTENANCE_GATE"):
        with instance._public_writer_admission():
            pytest.fail("must not admit during interrupted finish")
    old_start = docker.rows[CONTAINER]["State"]["StartedAt"]
    monkeypatch.setattr(instance.runtime, "execute", execute)
    instance.finish_maintenance()
    assert instance.status()["launch_blocked"] is False
    assert docker.rows[CONTAINER]["State"]["StartedAt"] != old_start


@pytest.mark.parametrize("mutation,code", [
    (lambda r: r.update(attempt="stale"), "INVALID_RUNTIME_RESPONSE"),
    (lambda r: r.update(nonce="stale"), "INVALID_RUNTIME_RESPONSE"),
    (lambda r: r.update(uid=True), "INVALID_RUNTIME_RESPONSE"),
    (lambda r: r.update(pid=0), "INVALID_RUNTIME_RESPONSE"),
    (lambda r: r.update(status="SUCCESS"), "INVALID_RUNTIME_RESPONSE"),
    (lambda r: r.update(results=[]), "INCOMPLETE_RECOVERY_RESPONSE"),
    (lambda r: r["results"][0].update(qid=True), "INVALID_RECOVERY_RESULT"),
    (lambda r: r["results"][0].update(qid=99), "INVALID_RECOVERY_RESULT"),
    (lambda r: r["results"][0].update(status="SECRET"), "INVALID_RECOVERY_RESULT"),
    (lambda r: r["results"][0].update(status="UNKNOWN"), "MISREPORTED_RECOVERY_SUCCESS"),
    (lambda r: r.update(extra="secret"), "UNSUPPORTED_MANIFEST_SCHEMA"),
])
def test_runtime_response_refusals_preserve_unknown_targets(controller, monkeypatch,
                                                           mutation, code):
    instance, _, journal, _ = controller
    initialize(instance)
    execute = instance.runtime.execute

    def runtime(container, request):
        response = execute(container, request)
        if request["operation"] == "recover":
            mutation(response)
        return response
    monkeypatch.setattr(instance.runtime, "execute", runtime)
    with pytest.raises(op.OperatorRefusal, match=code):
        instance.recover_session_purge(namespace="test", cache_scope="scope", qids=[7])
    status = instance.status()
    assert status["launch_blocked"] is True
    assert status["pending"]["qids"] == [7]
    assert status["recovery"]["status"] == "UNKNOWN"
    assert status["recovery"]["results"] == [{"qid": 7, "status": "UNKNOWN"}]
    assert "SECRET" not in (journal / "receipt.json").read_text()


def test_partial_recovery_not_success_and_finish_discloses_outcomes(controller, monkeypatch):
    instance, _, journal, _ = controller
    initialize(instance)
    execute = instance.runtime.execute

    def partial(container, request):
        response = execute(container, request)
        if request["operation"] == "recover":
            response["status"] = "PARTIAL"
            response["results"] = [
                {"qid": 7, "status": "PURGED"}, {"qid": 8, "status": "FAILED"},
                {"qid": 9, "status": "NOT_ATTEMPTED"}]
        return response
    monkeypatch.setattr(instance.runtime, "execute", partial)
    with pytest.raises(op.OperatorRefusal, match="PARTIAL"):
        instance.recover_session_purge(namespace="test", cache_scope="scope", qids=[7, 8, 9])
    assert instance.status()["pending"] is None
    assert instance.status()["launch_blocked"] is True
    assert instance.status()["recovery"]["results"][1]["status"] == "FAILED"
    result = instance.finish_maintenance()
    assert result["status"] == "PUBLIC"
    assert result["recovery"]["status"] == "PARTIAL"
    assert result["recovery"]["results"][2]["status"] == "NOT_ATTEMPTED"
    assert read(journal)["controller"]["recovery"] == result["recovery"]


def test_unknown_recovery_survives_finish_settlement(controller, monkeypatch):
    instance, _, _, _ = controller
    initialize(instance)
    execute = instance.runtime.execute

    def lost(container, request):
        if request["operation"] == "recover":
            raise op.OperatorRefusal("LOST_REPLY")
        return execute(container, request)
    monkeypatch.setattr(instance.runtime, "execute", lost)
    with pytest.raises(op.OperatorRefusal, match="LOST_REPLY"):
        instance.recover_session_purge(namespace="test", cache_scope="scope", qids=[7, 8])
    result = instance.finish_maintenance()
    assert result["recovery"]["status"] == "UNKNOWN"
    assert all(row["status"] == "UNKNOWN" for row in result["recovery"]["results"])


@pytest.mark.parametrize("fault", ["pid", "started", "new_process", "data"])
def test_postdispatch_boundary_failure_never_opens_gate(controller, monkeypatch, fault):
    instance, docker, journal, _ = controller
    execute = instance.runtime.execute

    def runtime(container, request):
        response = execute(container, request)
        if request["operation"] == "initialize":
            if fault == "pid":
                docker.rows[container]["State"]["Pid"] += 1000
            elif fault == "started":
                docker.rows[container]["State"]["StartedAt"] = FINISH
            elif fault == "new_process":
                docker.processes[999] = {"pid": 999, "ticks": 999, "state": "S",
                                         "argv_sha256": "f" * 64}
            else:
                (Path(instance.manifest["data"]["path"]) / "operator.dat").write_bytes(b"x")
        return response
    monkeypatch.setattr(instance.runtime, "execute", runtime)
    with pytest.raises(op.OperatorRefusal):
        initialize(instance)
    assert read(journal)["state"] == "MAINTENANCE"
    assert read(journal)["controller"]["pending"]["operation"] == "initialize"
    assert read(journal)["controller"]["intents"]["new-cache"]["status"] == "INITIALIZING"


def test_cli_qids_file_and_argument_mapping(controller, monkeypatch, capsys):
    instance, _, journal, path = controller
    monkeypatch.setattr(op.DockerMaintenanceOperator, "from_manifest",
                        lambda *args, **kwargs: instance)
    flags = ["--manifest", str(path), "--journal-dir", str(journal)]
    assert op.main(["init-new"] + flags + [
        "--namespace", "test", "--cache-scope", "scope", "--intent-id", "new-cache",
        "--vector-binding", "vector-store-v1", "--declare-new-logical-cache"]) == 0
    qids = path.parent / "qids.json"
    qids.write_text("[7,7,8]")
    qids.chmod(0o600)
    assert op.main(["recover-purge"] + flags + [
        "--namespace", "test", "--cache-scope", "scope", "--qids-file", str(qids)]) == 0
    assert instance.test_requests[-1]["qids"] == [7, 8]
    assert op.main(["finish"] + flags) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "PUBLIC"


def test_cli_refusal_redacts_unknown_argv_and_credentials(controller, capsys, monkeypatch):
    _, _, journal, path = controller
    with pytest.raises(SystemExit):
        op.main(["status", "--manifest", str(path), "--journal-dir", str(journal),
                 "--password", "DO_NOT_LOG_CREDENTIAL"])
    output = capsys.readouterr()
    assert "DO_NOT_LOG_CREDENTIAL" not in output.err + output.out
    monkeypatch.setattr(op, "__file__", str(journal / "missing-framework/operator.py"))
    assert op.main(["init-new", "--manifest", str(path), "--journal-dir", str(journal),
                    "--namespace", "test", "--cache-scope", "scope", "--intent-id", "new",
                    "--vector-binding", "vector", "--declare-new-logical-cache"]) == 1
    assert "FIXED_RUNTIME_MISSING" in capsys.readouterr().out


@pytest.mark.parametrize("fault", ["symlink", "hardlink", "writable", "pth", "sitecustomize"])
def test_runtime_bundle_rejects_unsafe_payload_tree(controller, fault):
    instance, _, _, _ = controller
    runtime = Path(instance.runtime.path)
    original = runtime / "source-shaped-fixture.txt"
    if fault == "symlink":
        (runtime / "link").symlink_to(original)
    elif fault == "hardlink":
        os.link(original, runtime / "link")
    elif fault == "writable":
        original.chmod(0o666)
    else:
        path = runtime / ("evil.pth" if fault == "pth" else "sitecustomize.py")
        path.write_text("import arbitrary")
        path.chmod(0o644)
    with pytest.raises(op.OperatorRefusal):
        instance.runtime.check()


@pytest.mark.parametrize("failure", ["", "timeout", "nonzero", "oversize"])
def test_fixed_runtime_transport_retains_live_stdin_and_uses_fixed_argv(controller,
                                                                      monkeypatch, failure):
    instance, _, _, _ = controller
    calls = []

    class Child:
        def __init__(self, argv, **kwargs):
            self.stdin = io.BytesIO()
            self.returncode = None
            self.killed = False
            self.kwargs = kwargs
            calls.append((argv, self))
            self.out = kwargs["stdout"]

        def wait(self, timeout):
            if timeout == 75:
                assert not self.stdin.closed
                assert json.loads(self.stdin.getvalue()) == {"operation": "ready"}
                if failure == "timeout":
                    raise subprocess.TimeoutExpired("redacted", timeout)
                self.out.write(b"x" * (1024 * 1024 + 1) if failure == "oversize"
                               else b'{"fixed":"response"}')
                self.returncode = 3 if failure == "nonzero" else 0
            else:
                assert self.killed and timeout == 5
                self.returncode = -9
            return self.returncode

        def poll(self):
            return self.returncode

        def kill(self):
            self.killed = True

    monkeypatch.setattr(op.subprocess, "Popen", Child)
    if failure:
        with pytest.raises(op.OperatorRefusal):
            op._FixedRuntime.execute(instance.runtime, MAINTENANCE, {"operation": "ready"})
    else:
        assert op._FixedRuntime.execute(instance.runtime, MAINTENANCE, {"operation": "ready"}) == {
            "fixed": "response"}
    argv, child = calls[0]
    assert argv[:6] == ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock",
                       "exec", "-i", "--user"]
    assert "1000:1000" in argv and MAINTENANCE in argv
    assert argv[-4:] == ["/op/python/bin/python3", "-I", "-B",
                         "/op/" + op._RUNTIME_RELATIVE]
    assert child.kwargs["shell"] is False
    assert child.kwargs["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C"}
    assert child.stdin.closed
    assert child.killed == (failure == "timeout")


@pytest.mark.parametrize("fault", ["boot", "ticks", "argv", "owner"])
def test_writer_reuse_boot_and_owner_refusals_never_signal(monkeypatch, fault):
    writer = {"pid": 777, "ticks": 77, "argv_sha256": "e" * 64, "boot": BOOT, "uid": 1000}
    current = {"pid": 777, "ticks": 77, "argv_sha256": "e" * 64, "state": "T"}
    if fault == "boot":
        writer["boot"] = "old-boot"
    elif fault == "ticks":
        current["ticks"] += 1
    elif fault == "argv":
        current["argv_sha256"] = "f" * 64
    monkeypatch.setattr(op, "_host_boot", lambda: BOOT)
    monkeypatch.setattr(op, "_process", lambda pid: current)
    monkeypatch.setattr(op.os, "kill", lambda *args: pytest.fail("must not signal"))
    monkeypatch.setattr(op, "_writer_owner", lambda pid: op._require(
        fault != "owner", "WRITER_OWNER_CHANGED"))
    with pytest.raises(op.OperatorRefusal):
        op._terminate_writer(writer)


def test_paused_writer_terminal_kill_no_resume(monkeypatch):
    writer = {"pid": 777, "ticks": 77, "argv_sha256": "e" * 64, "boot": BOOT, "uid": 1000}
    live = [dict(writer, state="T")]
    signals = []
    monkeypatch.setattr(op, "_host_boot", lambda: BOOT)
    monkeypatch.setattr(op, "_process", lambda pid: live[0] if live else None)
    monkeypatch.setattr(op, "_writer_owner", lambda pid: None)

    def kill(pid, signum):
        signals.append((pid, signum))
        live.clear()
    monkeypatch.setattr(op.os, "kill", kill)
    op._terminate_writer(writer)
    assert signals == [(777, op.signal.SIGKILL)]


def test_public_admission_stable_gate_lock_registration_and_crash(controller, monkeypatch):
    instance, _, journal, path = controller
    rewrite_manifest(controller, lambda m: m["launchers"].update(
        writer_argv_sha256=["e" * 64]))
    instance.manifest, instance.digest = op._manifest(path, journal)
    receipt = read(journal)
    receipt["manifest"] = instance.digest
    (journal / "receipt.json").write_bytes(op._json(receipt))
    initialize(instance)
    instance.finish_maintenance()
    process = op._process
    host = {}
    monkeypatch.setattr(op, "_process", lambda pid: host.get(pid) or process(pid))
    monkeypatch.setattr(op, "_host_writers", lambda allowed: deepcopy(host))
    monkeypatch.setattr(op, "_writer_owner", lambda pid: None)
    with instance._public_writer_admission() as register:
        assert read(journal)["state"] == "MAINTENANCE"
        with pytest.raises(BlockingIOError):
            with op._Journal(journal, instance.digest):
                pytest.fail("launch admission must hold deployment lock")
        host[777] = {"pid": 777, "ticks": 77, "state": "T", "argv_sha256": "e" * 64}
        register(777)
    assert instance.status()["launch_blocked"] is False
    assert read(journal)["controller"]["writers"][0]["pid"] == 777
    with pytest.raises(ControllerCrash):
        with instance._public_writer_admission():
            raise ControllerCrash()
    assert instance.status()["launch_blocked"] is True


def test_concurrent_persistent_data_owners_refuse_before_settlement(controller):
    instance, docker, journal, _ = controller
    docker._run_server(MAINTENANCE)
    with pytest.raises(op.OperatorRefusal, match="CONCURRENT_DATA_OWNERS_HISTORY_UNSAFE"):
        initialize(instance)
    assert not any(action == "stop" for action, _ in docker.calls)
    assert read(journal)["state"] == "MAINTENANCE"


def test_known_deployment_drift_closes_previously_public_gate(controller, monkeypatch):
    instance, docker, journal, _ = controller
    initialize(instance)
    instance.finish_maintenance()
    monkeypatch.setattr(docker, "daemon", lambda: "wrong-daemon")
    with pytest.raises(op.OperatorRefusal, match="DEPLOYMENT_CHANGED"):
        instance.resume_initialization(intent_id="new-cache")
    assert read(journal)["state"] == "MAINTENANCE"
    assert read(journal)["controller"]["stage"] == "deployment_refused"


def test_recovery_history_retains_old_unknown_targets_on_new_call(controller, monkeypatch):
    instance, _, _, _ = controller
    initialize(instance)
    execute = instance.runtime.execute

    def lost(container, request):
        if request["operation"] == "recover":
            raise op.OperatorRefusal("LOST_REPLY")
        return execute(container, request)
    monkeypatch.setattr(instance.runtime, "execute", lost)
    with pytest.raises(op.OperatorRefusal, match="LOST_REPLY"):
        instance.recover_session_purge(namespace="test", cache_scope="scope", qids=[7])
    monkeypatch.setattr(instance.runtime, "execute", execute)
    instance.recover_session_purge(namespace="test", cache_scope="scope", qids=[8])
    history = instance.status()["recovery_history"]
    assert len(history) == 2
    assert history[0]["recovery"]["results"] == [{"qid": 7, "status": "UNKNOWN"}]
    assert history[1]["recovery"]["results"] == [{"qid": 8, "status": "PURGED"}]
