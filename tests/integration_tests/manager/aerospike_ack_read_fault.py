"""Real acknowledged touch followed by withheld real session-read traffic."""

from datetime import datetime
import hashlib
import socket
import threading
import time
import uuid

from aerospike_wire_observer import WireObserver, summarize


class SessionReadFaultProxy(WireObserver):
    def __init__(self, port):
        super().__init__(port)
        self.armed = False
        self.blocked = threading.Event()

    def arm(self):
        assert not self.armed
        self.armed = True
        self._log(session_read_fault_armed=True)

    def _pump(self, source, target, connection, direction):
        if direction != "client_to_server":
            return super()._pump(source, target, connection, direction)
        pending = bytearray()
        try:
            while not self.stop.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    if pending:
                        raise EOFError("Incomplete real request frame")
                    return
                pending.extend(chunk)
                while len(pending) >= 8:
                    size = int.from_bytes(pending[2:8], "big")
                    if pending[0] != 2 or size > 16 * 1024 * 1024:
                        raise ValueError("Unsupported real request frame")
                    if len(pending) < size + 8:
                        break
                    raw = bytes(pending[:size + 8])
                    del pending[:size + 8]
                    frame = summarize(raw, response=False)
                    decoded = frame.get("decompressed", frame)
                    session_scan = any(
                        any(field["type"] == 1 and field.get("text") == "gptc_s"
                            for field in message["fields"]) and
                        any(field["type"] == 11 for field in message["fields"])
                        for message in decoded.get("messages", ()))
                    if self.armed and session_scan:
                        self._log(connection=connection, direction=direction, forwarded=False,
                                  frame=frame, injected="withhold_session_scan")
                        self.blocked.set()
                        # Also withhold every resumed session read, if any.
                        self.stop.wait(12)
                        return
                    target.sendall(raw)
                    self._log(connection=connection, direction=direction, forwarded=True, frame=frame)
        except (OSError, EOFError) as exc:
            if not self.stop.is_set():
                self._log(connection=connection, direction=direction,
                          transport_error=type(exc).__name__)
        except Exception as exc:
            with self.lock:
                self.errors.append(dict(connection=connection, direction=direction,
                                        type=type(exc).__name__))
        finally:
            try:
                target.shutdown(socket.SHUT_WR)
            except OSError:
                pass


def run(provider, port, observe, inspect_point):
    from aerospike_sdk.policy import SystemSettings, TransactionSettings
    from aerospike_sdk.sync import ClusterDefinition
    from gptcache.manager.scalar_data.aerospike_storage import AerospikeStorageError
    from gptcache.manager.scalar_data.base import CacheData

    token = "ack-then-read-failure-" + uuid.uuid4().hex
    ids = provider.batch_insert([CacheData(token, ["synthetic"], session_id=token)])
    assert len(ids) == 1
    qid = ids[0]
    children = provider.list_sessions(None, qid)
    assert len(children) == 1
    before = inspect_point(provider, "q", qid)
    before_control = inspect_point(provider, "q", qid, controls=True)
    child_before = inspect_point(provider, "s", children[0].id)
    definition = ClusterDefinition("127.0.0.2", port).with_ip_map(
        {"127.0.0.1": "127.0.0.2"}).using_services_alternate(False).with_strict_config()
    definition.with_system_settings(SystemSettings(transactions=TransactionSettings(
        implicit_batch_write_transactions=False)))
    with SessionReadFaultProxy(port) as wire:
        doomed = type(provider)(
            cluster_definition=definition, namespace=provider.namespace,
            cache_scope=provider.cache_scope, behavior=provider._behavior)
        try:
            doomed.create()
            time.sleep(.01)
            start_mark = wire.mark()
            wire.arm()
            start = time.monotonic()
            errors = []
            try:
                doomed.get_data_by_id(qid)
            except AerospikeStorageError as exc:
                errors = [type(error).__name__ for error in exc.outcomes]
            elapsed = time.monotonic() - start
            evidence = wire.snapshot(start_mark)
            observe("ack_then_read_wire", evidence)
            assert wire.blocked.is_set() and not wire.errors
            assert any("Timeout" in name for name in errors), "ACK plus failed projection returned success"
            requests, acknowledgments = [], []
            for event in evidence["events"]:
                frame = event.get("frame", {}).get("decompressed", event.get("frame", {}))
                for message in frame.get("messages", ()):
                    if event.get("direction") == "client_to_server" and event.get("forwarded"):
                        if "last_access" in message["bins"] and message["info"][1] & 1:
                            requests.append(event["index"])
                    if event.get("direction") == "server_to_client" and event.get("forwarded"):
                        if (message["controls"].get("qid") == qid and
                                "last_access" in message["bins"] and message["result_code"] == 0):
                            acknowledgments.append(event["index"])
            blocked = [event["index"] for event in evidence["events"]
                       if event.get("injected") == "withhold_session_scan"]
            assert len(requests) == len(acknowledgments) == 1
            # Forwarding is logged after sendall, so cross-thread log indices
            # need not preserve wire order. The actual follow-up scan proves
            # get_data_by_id consumed the ACK; generation+1 proves no replay.
            assert blocked
        finally:
            doomed.close()
    after = inspect_point(provider, "q", qid)
    after_control = inspect_point(provider, "q", qid, controls=True)
    assert after_control["generation"] == before_control["generation"] + 1
    assert datetime.fromisoformat(after["last_access"]) > datetime.fromisoformat(before["last_access"])
    assert {k: v for k, v in after.items() if k != "last_access"} == {
        k: v for k, v in before.items() if k != "last_access"}
    assert inspect_point(provider, "s", children[0].id) == child_before
    observe("ack_then_read_proof", dict(
        qid=qid, touch_requests=len(requests), actual_acknowledgments=len(acknowledgments),
        session_reads_withheld=len(blocked), errors=errors, seconds=elapsed,
        before=before_control, after=after_control, access_changed=True,
        payload_sha256=hashlib.sha256(after["payload"]).hexdigest(),
        session_unchanged=True, touch_replayed=False))
    # Only this new synthetic fixture, and only after all evidence gates.
    provider.mark_deleted(ids)
    provider.clear_deleted_data()
    assert inspect_point(provider, "q", qid) is None
    assert inspect_point(provider, "s", children[0].id) is None
    return dict(cases=["ack_then_read_failure"], errors=errors,
                touch_requests=1, actual_acknowledgments=1, touch_replayed=False)
