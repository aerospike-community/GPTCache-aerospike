"""Explicit-owned-deployment Aerospike fixture (never provision or adopt data).

Set GPTCACHE_AEROSPIKE_LIVE_ENV to an OWN JSON file with absolute ``manifest``,
``journal`` and ``sdk_config`` paths and two fresh operator ``scopes`` intents
(namespace/cache_scope/intent_id/vector_binding). The immutable enrolled
manifest must allow [sys.executable, "-B", <absolute aerospike_live_worker.py>].
No ignored planning script is a dependency of these versioned tests.
"""

import importlib.util
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import threading
import time

import pytest


def pytest_addoption(parser):
    parser.addoption("--aerospike-mandatory-live", action="store_true",
                     help="Missing optional SDK or unreachable server must fail, not skip.")
    parser.addoption("--aerospike-diagnostics", action="store_true",
                     help="Include explicit owned/frozen-fixture diagnostic experiments.")


def pytest_configure(config):
    config.addinivalue_line("markers", "aerospike_diagnostic: explicit owned-fixture experiments")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--aerospike-diagnostics"):
        return
    diagnostics = [item for item in items if item.get_closest_marker("aerospike_diagnostic")]
    if diagnostics:
        items[:] = [item for item in items if not item.get_closest_marker("aerospike_diagnostic")]
        config.hook.pytest_deselected(items=diagnostics)


@pytest.fixture(scope="session")
def aerospike_live_environment(request):
    mandatory = request.config.getoption("--aerospike-mandatory-live")
    if importlib.util.find_spec("aerospike_sdk") is None:
        if mandatory:
            pytest.fail("Mandatory live run requires the approved optional aerospike-sdk")
        pytest.skip("Optional aerospike-sdk is not installed")
    path = os.environ.get("GPTCACHE_AEROSPIKE_LIVE_ENV")
    if not path:
        pytest.fail("Supply GPTCACHE_AEROSPIKE_LIVE_ENV with explicit OWN deployment authority; "
                    "tests never adopt an arbitrary localhost service")
    config_path = Path(path)
    assert config_path.is_absolute() and config_path.is_file()
    config = json.loads(config_path.read_text())
    assert set(config) == {"manifest", "journal", "sdk_config", "scopes"}
    for name in ("manifest", "journal", "sdk_config"):
        value = Path(config[name])
        assert value.is_absolute() and value.exists(), "Missing explicit " + name
    assert len(config["scopes"]) == 2
    assert len({s["cache_scope"] for s in config["scopes"]}) == 2
    assert len({s["intent_id"] for s in config["scopes"]}) == 2
    from gptcache.manager.scalar_data.aerospike_operator import DockerMaintenanceOperator
    operator = DockerMaintenanceOperator.from_manifest(
        config["manifest"], journal_dir=config["journal"])
    operator.runtime.prepare()  # Whole-framework/source/provenance binding, no waiver.
    for mode in ("public", "maintenance"):
        operator._boundary(mode)
    config["operator"] = operator
    return config


@pytest.fixture(scope="session")
def aerospike_initialized(aerospike_live_environment):
    """Actual durable initialization and finish, before any ordinary client."""
    config = aerospike_live_environment
    operator = config["operator"]
    for intent in config["scopes"]:
        assert intent["namespace"] == operator.manifest["namespace"]
        status = operator.status()
        known = status["intents"].get(intent["intent_id"])
        if known is None:
            outcome = operator.initialize_scope(**intent)
        else:
            for key, value in intent.items():
                assert known[key] == value, "Existing intent does not match supplied authority"
            if known["status"] == "ACTIVE":
                continue
            outcome = operator.resume_initialization(intent_id=intent["intent_id"])
        assert outcome["status"] == "ACTIVE"
    if operator.status()["state"] != "PUBLIC":
        assert operator.finish_maintenance()["status"] == "PUBLIC"
    assert operator.status()["launch_blocked"] is False
    return config


@pytest.fixture
def aerospike_worker(aerospike_initialized):
    """Spawn, register the OWN child PID, release gate, THEN permit SDK connect."""
    config = aerospike_initialized
    operator = config["operator"]
    worker = Path(__file__).with_name("aerospike_live_worker.py").resolve()
    argv = [sys.executable, "-B", str(worker)]
    admission_lock = threading.Lock()

    def run(scope, scenario="smoke", *, expected_termination=None, **parameters):
        from gptcache.manager.scalar_data.aerospike_operator import _process
        child = None
        try:
            with admission_lock:
                with operator._public_writer_admission() as register:
                    child = subprocess.Popen(
                        argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True, env={**os.environ,
                                        "AEROSPIKE_SDK_CONFIG_URL": config["sdk_config"]})
                    deadline = time.monotonic() + 5
                    while True:
                        row = _process(child.pid)
                        assert child.poll() is None and time.monotonic() < deadline
                        if row and row["argv_sha256"] in operator.manifest["launchers"]["writer_argv_sha256"]:
                            break
                        time.sleep(.01)
                    register(child.pid)
                payload = dict(manifest=config["manifest"], journal=config["journal"],
                               scope=scope, scenario=scenario)
                payload.update(parameters)
                child.stdin.write(json.dumps(payload) + "\n")
                child.stdin.flush()
                assert select.select([child.stdout], [], [], 10)[0], "Child admission timed out"
                assert child.stdout.readline().strip() == "AEROSPIKE_LIVE_ADMITTED"
            stdout, stderr = child.communicate(
                timeout=180 if scenario in (
                    "stream_failure", "enum_diagnostic", "enum_minimal", "enum_wire", "enum_churn") else 45)
            observations = "\n".join(line for line in stdout.splitlines()
                                     if line.startswith("AEROSPIKE_LIVE_OBSERVATION "))
            if observations:
                print(observations, flush=True)
            if expected_termination is not None:
                assert expected_termination == -9
                assert child.returncode == expected_termination, (
                    "Controller did not terminate own admitted writer:\n" + stderr + "\n" + observations)
                return {"pid": child.pid, "terminated": True, "returncode": child.returncode}
            assert child.returncode == 0, "Actual worker failed:\n" + stderr + "\n" + observations
            prefix = "AEROSPIKE_LIVE_RESULT "
            rows = [line[len(prefix):] for line in stdout.splitlines() if line.startswith(prefix)]
            assert len(rows) == 1, "Missing or duplicate real-worker result"
            result = json.loads(rows[0])
            assert result["scenario"] == scenario and result["pid"] == child.pid
            return result
        finally:
            if child is not None and child.poll() is None:
                # Only this OWN spawned writer, never pytest/controller.
                child.kill()
                child.communicate(timeout=5)

    return run
