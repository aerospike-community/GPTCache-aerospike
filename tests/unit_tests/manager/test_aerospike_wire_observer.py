"""Offline decoder tests only; manufactured frames are not live evidence."""

import importlib.util
import json
from pathlib import Path
import socket
import threading
from types import SimpleNamespace
from unittest.mock import Mock
import zlib

import pytest


SOURCE = Path(__file__).resolve().parents[3] / (
    "tests/integration_tests/manager/aerospike_wire_observer.py")
SPEC = importlib.util.spec_from_file_location("wire_observer_test_subject", SOURCE)
SUBJECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUBJECT)


def test_forward_log_order_can_lag_a_peer_response():
    """Synthetic unit schedule: sendall can let the peer log before we do."""
    relay = SUBJECT.WireObserver(3000)
    raw = frame(message(bins=(("qid", 1, (7).to_bytes(8, "big")),)))
    source, target = Mock(), Mock()
    source.recv.side_effect = [raw, b""]
    target.sendall.side_effect = lambda data: relay._log(
        connection=0, direction="server_to_client", forwarded=True,
        frame=SUBJECT.summarize(raw))
    relay._pump(source, target, 0, "client_to_server")
    assert not relay.errors
    assert [event["direction"] for event in relay.events] == [
        "server_to_client", "client_to_server"]
    assert all(event["forwarded"] for event in relay.events)
    target.sendall.assert_called_once_with(raw)


@pytest.mark.parametrize("mode", ["deadline", "eof"])
def test_fault_relay_forwards_one_real_frame_then_stalls_or_closes(monkeypatch, mode):
    """Manufactured protocol input exercises transport only, NOT live evidence."""
    monkeypatch.syspath_prepend(str(SOURCE.parent))
    spec = importlib.util.spec_from_file_location(
        "fault_relay_test_subject", SOURCE.with_name("aerospike_stream_fault.py"))
    fault = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fault)
    relay = fault.StreamFaultProxy(3000, mode)
    relay.arm()
    first = frame(message(bins=(("qid", 1, (7).to_bytes(8, "big")),)))
    second = frame(message(bins=(("qid", 1, (8).to_bytes(8, "big")),)))
    upstream, source = socket.socketpair()
    target, client = socket.socketpair()
    client.settimeout(.3)
    thread = threading.Thread(target=relay._pump, args=(
        source, target, 0, "server_to_client"), daemon=True)
    thread.start()
    try:
        upstream.sendall(first + second)
        received = bytearray()
        while len(received) < len(first):
            received.extend(client.recv(len(first) - len(received)))
        assert bytes(received) == first
        assert relay.prefix["qids"] == [7]
        if mode == "eof":
            assert client.recv(1) == b""
        else:
            with pytest.raises(socket.timeout):
                client.recv(1)
        assert not relay.errors
        assert len([row for row in relay.events if row.get("forwarded")]) == 1
    finally:
        relay.stop.set()
        for sock in (upstream, source, target, client):
            sock.close()
        thread.join(1)
    assert not thread.is_alive()


def test_persistent_eof_does_not_forward_data_on_resumed_connection(monkeypatch):
    """Synthetic socketpair regression: the retry cannot finish the fixture."""
    monkeypatch.syspath_prepend(str(SOURCE.parent))
    spec = importlib.util.spec_from_file_location(
        "persistent_fault_test_subject", SOURCE.with_name("aerospike_stream_fault.py"))
    fault = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fault)
    relay = fault.StreamFaultProxy(3000, "eof_persistent")
    relay.arm()
    for connection, qid in enumerate((7, 8)):
        raw = frame(message(bins=(("qid", 1, qid.to_bytes(8, "big")),)))
        upstream, source = socket.socketpair()
        target, client = socket.socketpair()
        client.settimeout(1)
        thread = threading.Thread(target=relay._pump, args=(
            source, target, connection, "server_to_client"), daemon=True)
        thread.start()
        try:
            upstream.sendall(raw)
            received = bytearray()
            while True:
                data = client.recv(4096)
                if not data:
                    break
                received.extend(data)
            assert bytes(received) == (raw if connection == 0 else b"")
        finally:
            for sock in (upstream, source, target, client):
                sock.close()
            thread.join(1)
        assert not thread.is_alive()
    assert relay.prefix["qids"] == [7]
    assert not relay.errors
    assert len([event for event in relay.events if event.get("forwarded")]) == 1
    assert len([event for event in relay.events if event.get("injected") == "eof_retry"]) == 1


def test_session_read_fault_passes_point_frame_and_withholds_scan(monkeypatch):
    """Manufactured unit frames only; actual ACK evidence is a separate live test."""
    monkeypatch.syspath_prepend(str(SOURCE.parent))
    spec = importlib.util.spec_from_file_location(
        "session_fault_test_subject", SOURCE.with_name("aerospike_ack_read_fault.py"))
    fault = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fault)
    relay = fault.SessionReadFaultProxy(3000)
    relay.arm()
    point = frame(message(fields=((1, b"gptc_q"),), bins=(("last_access", 3, b"stamp"),)))
    scan = frame(message(fields=((1, b"gptc_s"), (11, b"\0\0"))))
    client, source = socket.socketpair()
    target, server = socket.socketpair()
    server.settimeout(.2)
    thread = threading.Thread(target=relay._pump, args=(
        source, target, 0, "client_to_server"), daemon=True)
    thread.start()
    try:
        client.sendall(point)
        received = bytearray()
        while len(received) < len(point):
            received.extend(server.recv(len(point) - len(received)))
        assert bytes(received) == point
        client.sendall(scan)
        assert relay.blocked.wait(1)
        with pytest.raises(socket.timeout):
            server.recv(1)
        assert not relay.errors
        assert sum(event.get("forwarded") is True for event in relay.events) == 1
    finally:
        relay.stop.set()
        for sock in (client, source, target, server):
            sock.close()
        thread.join(1)
    assert not thread.is_alive()


@pytest.mark.parametrize("enabled", [False, True])
def test_frozen_fixture_diagnostics_are_explicit_not_ordinary_contract_tests(enabled):
    path = SOURCE.with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("diagnostic_collection_subject", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ordinary = SimpleNamespace(get_closest_marker=lambda name: None)
    diagnostic = SimpleNamespace(get_closest_marker=lambda name: object())
    items = [ordinary, diagnostic]
    hook = Mock()
    config = SimpleNamespace(getoption=lambda name: enabled,
                             hook=SimpleNamespace(pytest_deselected=hook))
    module.pytest_collection_modifyitems(config, items)
    if enabled:
        assert items == [ordinary, diagnostic]
        hook.assert_not_called()
    else:
        assert items == [ordinary]
        hook.assert_called_once_with(items=[diagnostic])


def frame(body, kind=3):
    return bytes((2, kind)) + len(body).to_bytes(6, "big") + body


def message(fields=(), bins=(), info3=0, generation=2, code=0):
    header = bytearray(22)
    header[0], header[3], header[5] = 22, info3, code
    header[6:10] = generation.to_bytes(4, "big")
    header[18:20] = len(fields).to_bytes(2, "big")
    header[20:22] = len(bins).to_bytes(2, "big")
    body = b"".join((len(value) + 1).to_bytes(4, "big") + bytes((kind,)) + value
                    for kind, value in fields)
    for name, particle, value in bins:
        name = name.encode()
        body += ((4 + len(name) + len(value)).to_bytes(4, "big") +
                 bytes((1, particle, 0, len(name))) + name + value)
    return bytes(header) + body


def test_response_preserves_controls_digests_and_terminal_not_payload():
    record = message(fields=((4, bytes(range(20))),), bins=(
        ("qid", 1, (522).to_bytes(8, "big", signed=True)),
        ("state", 1, (-1).to_bytes(8, "big", signed=True)),
        ("payload", 3, b"SENSITIVE-ANSWER")))
    decoded = SUBJECT.summarize(frame(record + message(info3=1)))
    assert decoded["message_count"] == 2
    first, last = decoded["messages"]
    assert first["fields"][0]["hex"] == bytes(range(20)).hex()
    assert first["controls"] == {"qid": 522, "state": -1}
    assert first["bins"] == ["qid", "state", "payload"]
    assert last["info"][2] == 1
    assert "SENSITIVE-ANSWER" not in json.dumps(decoded)


def test_complete_little_endian_pid_array_is_not_a_subset_or_resume():
    pids = b"".join(pid.to_bytes(2, "little") for pid in range(4096))
    decoded = SUBJECT.summarize(frame(message(fields=(
        (0, b"test"), (1, b"gptc_q"), (7, b"abcdefgh"), (11, pids)), info3=4)), response=False)
    assert decoded["partition_done_count"] == 0
    fields = {field["type"]: field for field in decoded["messages"][0]["fields"]}
    assert fields[11]["count"] == fields[11]["unique"] == 4096
    assert fields[11]["minimum"] == 0 and fields[11]["maximum"] == 4095
    assert fields[7]["hex"] == b"abcdefgh".hex()
    assert 12 not in fields and 13 not in fields and 43 not in fields


def test_partition_markers_preserve_error_counts_without_huge_record_lists():
    decoded = SUBJECT.summarize(frame(
        message(info3=4, generation=0) + message(info3=4, generation=4095, code=11) +
        message(info3=1)))
    assert decoded["message_count"] == 3 and len(decoded["messages"]) == 1
    assert decoded["partition_done_count"] == decoded["partition_done_unique"] == 2
    assert decoded["partition_done_codes"] == {"0": 1, "11": 1}
    assert decoded["partition_done_min"] == 0 and decoded["partition_done_max"] == 4095


def test_info_and_compressed_frames_are_hash_only_or_decoded():
    info = SUBJECT.summarize(frame(b"private-info-value", kind=1))
    assert "messages" not in info and "private-info-value" not in json.dumps(info)
    inner = frame(message(info3=1))
    compressed = frame(len(inner).to_bytes(8, "big") + zlib.compress(inner), kind=4)
    assert SUBJECT.summarize(compressed)["decompressed"] == SUBJECT.summarize(inner)


def test_zero_body_frame_is_retained_as_potential_stream_terminator():
    decoded = SUBJECT.summarize(frame(b""))
    assert decoded["empty_body"] and decoded["message_count"] == 0
    assert decoded["bytes"] == 8


@pytest.mark.parametrize("direction", ["client_to_server", "server_to_client"])
def test_relay_preserves_every_byte_despite_fragmented_reads(direction):
    class Source:
        def __init__(self, data):
            self.data = data

        def recv(self, size):
            chunk, self.data = self.data[:min(size, 3)], self.data[min(size, 3):]
            return chunk

    class Target:
        def __init__(self):
            self.frames = []
            self.shutdowns = []

        def sendall(self, raw):
            self.frames.append(raw)

        def shutdown(self, how):
            self.shutdowns.append(how)

    raw = frame(message(fields=((4, b"x" * 20),),
                        bins=(("payload", 3, b"SENSITIVE"),))) + frame(message(info3=1))
    observer, target = SUBJECT.WireObserver(3000), Target()
    observer._pump(Source(raw), target, 7, direction)
    snapshot = observer.snapshot()
    assert b"".join(target.frames) == raw and len(target.frames) > 2
    assert target.shutdowns == [SUBJECT.socket.SHUT_WR]
    assert not snapshot["decoder_errors"] and len(snapshot["events"]) == 2
    assert all(event["forwarded"] and event["direction"] == direction
               for event in snapshot["events"])
    assert "SENSITIVE" not in json.dumps(snapshot)


@pytest.mark.parametrize("raw", [
    b"",
    frame(b"\x16"), frame(message(fields=((11, b"\x00"),))),
    frame(message(fields=((4, b"abc"),))[:-1]),
    frame(message(bins=(("state", 1, b"\x00" * 8),))[:-1]),
])
def test_invalid_frames_raise_instead_of_looking_like_empty_success(raw):
    with pytest.raises(ValueError):
        SUBJECT.summarize(raw)
