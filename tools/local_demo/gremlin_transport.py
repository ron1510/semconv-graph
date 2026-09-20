"""Bound local probe connections so a stalled port-forward cannot defeat wait_for."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import aiohttp
from gremlin_python.driver.aiohttp.transport import AiohttpTransport


class DemoGremlinTransport(AiohttpTransport):
    _loop: asyncio.AbstractEventLoop
    _client_session: aiohttp.ClientSession | None
    _websocket: aiohttp.ClientWebSocketResponse | None

    def __init__(self, timeout_seconds: float = 10) -> None:
        super().__init__(read_timeout=timeout_seconds, write_timeout=timeout_seconds)  # pyright: ignore[reportUnknownMemberType]
        self._timeout_seconds = timeout_seconds

    def connect(self, url: str, headers: Mapping[str, str] | None = None) -> None:
        async def connect() -> None:
            self._client_session = aiohttp.ClientSession(
                loop=self._loop, timeout=aiohttp.ClientTimeout(total=self._timeout_seconds)
            )
            try:
                self._websocket = await self._client_session.ws_connect(
                    url, headers=headers, max_msg_size=10 * 1024 * 1024
                )
            except BaseException:
                await self._client_session.close()
                self._client_session = None
                raise

        self._loop.run_until_complete(connect())
