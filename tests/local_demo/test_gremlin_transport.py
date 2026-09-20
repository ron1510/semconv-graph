from __future__ import annotations

import socket
import time
from threading import Event, Thread

import pytest

from tools.local_demo.gremlin_transport import DemoGremlinTransport


def test_stalled_websocket_handshake_is_bounded() -> None:
    release = Event()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()

        def stall() -> None:
            connection, _ = listener.accept()
            with connection:
                release.wait(2)

        server = Thread(target=stall)
        server.start()
        transport = DemoGremlinTransport(timeout_seconds=0.1)
        started = time.monotonic()
        try:
            with pytest.raises(TimeoutError):
                transport.connect(f"ws://127.0.0.1:{listener.getsockname()[1]}/gremlin")
            assert time.monotonic() - started < 2
        finally:
            transport.close()
            release.set()
            server.join(timeout=2)
