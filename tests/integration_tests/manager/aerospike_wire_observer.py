"""Test-only byte-preserving loopback observer; never a backend/fallback.

Payloads stay in memory. Evidence contains hashes, headers, physical digests,
and whitelisted controls, not question/answer values. No delay, injection,
request replay, policy replacement, or server reconfiguration is performed.
"""

import hashlib
import socket
import threading
import time
import zlib


def summarize(frame, *, response=True):
    """Decode complete real protocol frames without retaining arbitrary values."""
    if len(frame) < 8 or frame[0] != 2 or int.from_bytes(frame[2:8], "big") != len(frame) - 8:
        raise ValueError("Invalid protocol frame")
    result = {"protocol": frame[1], "bytes": len(frame),
              "sha256": hashlib.sha256(frame).hexdigest()}
    if frame[1] == 4:
        decoded = zlib.decompress(frame[16:])
        if len(decoded) != int.from_bytes(frame[8:16], "big"):
            raise ValueError("Compressed frame length mismatch")
        result["decompressed"] = summarize(decoded, response=response)
        return result
    if frame[1] != 3:
        return result
    if len(frame) == 8:
        result.update(empty_body=True, messages=[], message_count=0)
        return result
    messages, partitions, partition_codes, offset, total = [], [], {}, 8, 0
    while offset < len(frame):
        start = offset
        header = frame[offset:offset + 22]
        if len(header) != 22 or header[0] != 22:
            raise ValueError("Unsupported message header")
        item = {"info": list(header[1:5]), "result_code": header[5],
                "generation": int.from_bytes(header[6:10], "big"),
                "expiration": int.from_bytes(header[10:14], "big"),
                "fields": [], "bins": [], "controls": {}}
        fields, ops = int.from_bytes(header[18:20], "big"), int.from_bytes(header[20:22], "big")
        offset += 22
        for _ in range(fields):
            size = int.from_bytes(frame[offset:offset + 4], "big")
            if size < 1 or offset + 4 + size > len(frame):
                raise ValueError("Truncated field")
            kind, value = frame[offset + 4], frame[offset + 5:offset + 4 + size]
            entry = {"type": kind, "bytes": len(value), "sha256": hashlib.sha256(value).hexdigest()}
            if kind in (0, 1):
                entry["text"] = value.decode("utf-8")
            elif kind in (4, 7):
                entry["hex"] = value.hex()
            elif kind == 11:
                if len(value) % 2:
                    raise ValueError("Odd PID array")
                ids = [int.from_bytes(value[i:i + 2], "little") for i in range(0, len(value), 2)]
                entry.update(count=len(ids), unique=len(set(ids)),
                             minimum=min(ids) if ids else None, maximum=max(ids) if ids else None)
            item["fields"].append(entry)
            offset += 4 + size
        for _ in range(ops):
            size = int.from_bytes(frame[offset:offset + 4], "big")
            if size < 4 or offset + 4 + size > len(frame):
                raise ValueError("Truncated operation")
            particle, name_size = frame[offset + 5], frame[offset + 7]
            if name_size > size - 4:
                raise ValueError("Truncated bin name")
            name = frame[offset + 8:offset + 8 + name_size].decode("utf-8")
            value = frame[offset + 8 + name_size:offset + 4 + size]
            item["bins"].append(name)
            if name in ("fmt", "qid", "state", "adm", "hi") and particle == 1 and len(value) == 8:
                item["controls"][name] = int.from_bytes(value, "big", signed=True)
            offset += 4 + size
        item["bytes"] = offset - start
        total += 1
        if response and header[3] & 4:
            partitions.append(item["generation"])
            code = str(item["result_code"])
            partition_codes[code] = partition_codes.get(code, 0) + 1
        else:
            messages.append(item)
    result.update(messages=messages, message_count=total,
                  partition_done_count=len(partitions),
                  partition_done_unique=len(set(partitions)),
                  partition_done_min=min(partitions) if partitions else None,
                  partition_done_max=max(partitions) if partitions else None,
                  partition_done_codes=partition_codes,
                  partition_done_sha256=hashlib.sha256(
                      b"".join(pid.to_bytes(4, "big") for pid in partitions)).hexdigest())
    return result


class WireObserver:
    """Relay only this registered child's client to its explicitly owned server."""

    def __init__(self, port):
        self.port = port
        self.events = []
        self.errors = []
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.sockets = []
        self.threads = []
        self.listener = None

    def _log(self, **event):
        with self.lock:
            self.events.append(dict(index=len(self.events), ns=time.monotonic_ns(), **event))

    def mark(self):
        with self.lock:
            return len(self.events)

    def snapshot(self, start=0):
        with self.lock:
            return {"events": list(self.events[start:]), "decoder_errors": list(self.errors)}

    def _pump(self, source, target, connection, direction):
        pending = bytearray()
        try:
            while not self.stop.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    if pending:
                        raise EOFError("Peer closed a partial frame")
                    break
                # Forward immediately, before decoding, rather than buffering a
                # complete frame and accidentally masking fragmented-read bugs.
                target.sendall(chunk)
                pending.extend(chunk)
                while len(pending) >= 8:
                    size = int.from_bytes(pending[2:8], "big")
                    if pending[0] != 2 or size > 16 * 1024 * 1024:
                        raise ValueError("Unsupported protocol version/frame size")
                    if len(pending) < size + 8:
                        break
                    frame = bytes(pending[:size + 8])
                    del pending[:size + 8]
                    summary = summarize(frame, response=direction == "server_to_client")
                    self._log(connection=connection, direction=direction, forwarded=True, frame=summary)
        except (OSError, EOFError) as exc:
            if not self.stop.is_set():
                self._log(connection=connection, direction=direction,
                          transport_error=type(exc).__name__, errno=getattr(exc, "errno", None))
        except Exception as exc:
            with self.lock:
                self.errors.append({"connection": connection, "direction": direction,
                                    "type": type(exc).__name__})
        finally:
            try:
                target.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def _accept(self):
        connection = 0
        while not self.stop.is_set():
            try:
                client, peer = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", self.port), timeout=3)
                upstream.settimeout(None)
            except OSError as exc:
                client.close()
                with self.lock:
                    self.errors.append({"type": type(exc).__name__, "phase": "upstream_connect"})
                continue
            self.sockets.extend((client, upstream))
            for sock in (client, upstream):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._log(connection=connection, accepted_peer=list(peer))
            for source, target, direction in (
                    (client, upstream, "client_to_server"), (upstream, client, "server_to_client")):
                thread = threading.Thread(target=self._pump, args=(
                    source, target, connection, direction), daemon=True)
                self.threads.append(thread)
                thread.start()
            connection += 1

    def __enter__(self):
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.listener.bind(("127.0.0.2", self.port))
            self.listener.listen(64)
            self.listener.settimeout(.1)
        except BaseException:
            self.listener.close()
            raise
        thread = threading.Thread(target=self._accept, daemon=True)
        self.threads.append(thread)
        thread.start()
        return self

    def __exit__(self, *unused):
        self.stop.set()
        self.listener.close()
        # Stop admission before closing the stable list of connected sockets.
        self.threads[0].join(timeout=2)
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        for thread in self.threads[1:]:
            thread.join(timeout=2)
