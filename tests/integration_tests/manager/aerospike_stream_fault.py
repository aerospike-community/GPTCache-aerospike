"""Real-server stream fault injection, not a protocol simulator or backend.

Forward one complete real question frame, then stall or close that connection.
All other traffic stays real. No reply generation, policy rewriting or replay.
"""

import socket

from aerospike_wire_observer import WireObserver, summarize


class StreamFaultProxy(WireObserver):
    def __init__(self, port, mode):
        super().__init__(port)
        assert mode in ("deadline", "eof", "eof_persistent")
        self.mode = mode
        self.armed = False
        self.prefix = None

    @staticmethod
    def _question_rows(frame):
        decoded = frame.get("decompressed", frame)
        return [message for message in decoded.get("messages", ())
                if "qid" in message.get("controls", {})]

    def arm(self):
        assert not self.armed and self.prefix is None
        self.armed = True
        self._log(fault_armed=self.mode)

    def _pump(self, source, target, connection, direction):
        if direction != "server_to_client":
            return super()._pump(source, target, connection, direction)
        pending = bytearray()
        try:
            while not self.stop.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    if pending:
                        raise EOFError("Incomplete real response frame")
                    return
                pending.extend(chunk)
                while len(pending) >= 8:
                    size = int.from_bytes(pending[2:8], "big")
                    if pending[0] != 2 or size > 16 * 1024 * 1024:
                        raise ValueError("Unsupported real response frame")
                    if len(pending) < size + 8:
                        break
                    raw = bytes(pending[:size + 8])
                    del pending[:size + 8]
                    frame = summarize(raw)
                    questions = self._question_rows(frame)
                    with self.lock:
                        inject = self.armed and self.prefix is None and bool(questions)
                        reject_retry = (self.armed and self.prefix is not None and
                                        self.mode == "eof_persistent" and bool(questions))
                        if inject:
                            self.prefix = dict(
                                connection=connection, mode=self.mode,
                                qids=[row["controls"]["qid"] for row in questions],
                                frame_sha256=frame["sha256"], bytes=len(raw))
                    if reject_retry:
                        self._log(connection=connection, direction=direction, forwarded=False,
                                  frame=frame, injected="eof_retry", delay_seconds=.1)
                        # Keep retries unable to advance beyond the first prefix.
                        # Bound reconnect pressure without changing caller policy.
                        self.stop.wait(.1)
                        target.shutdown(socket.SHUT_RDWR)
                        return
                    target.sendall(raw)
                    self._log(connection=connection, direction=direction,
                              forwarded=True, frame=frame, fault_prefix=inject)
                    if inject:
                        self._log(connection=connection, injected=self.mode,
                                  after_complete_real_frame=True)
                        if self.mode == "deadline":
                            # Longer than the unchanged 3s native query budget.
                            # __exit__ releases this wait after the REAL timeout.
                            self.stop.wait(12)
                        else:
                            target.shutdown(socket.SHUT_RDWR)
                        return
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
