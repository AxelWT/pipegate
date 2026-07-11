from __future__ import annotations

import asyncio
import base64
import contextlib
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import orjson

from pipegate.client import handle_request, main
from pipegate.schemas import BufferGateRequest, BufferGateResponse


class TestHandleRequest:
    async def test_successful_forward(self) -> None:
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([["key", "value"]]).decode(),
            method="GET",
            headers=orjson.dumps([["x-custom", "header"]]).decode(),
            body="",
        )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(200, text="ok", headers={"x-resp": "val"})
            )
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        ws.send.assert_called_once()
        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert resp.correlation_id == request.correlation_id
        assert resp.status_code == 200
        assert base64.b64decode(resp.body) == b"ok"
        assert any(k == "x-resp" and v == "val" for k, v in orjson.loads(resp.headers))

    async def test_strips_encoding_headers_from_response(self) -> None:
        """httpx decompresses gzip/deflate and de-chunks transfer encoding
        transparently. The forwarded headers must not claim content-encoding
        or content-length that misdescribe the already-decoded body, or the
        browser fails with ERR_CONTENT_DECODING_FAILED."""
        import gzip

        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        compressed = gzip.compress(b"plain-body")
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda req: httpx.Response(
                    200,
                    content=compressed,
                    headers={
                        "content-encoding": "gzip",
                        "content-length": str(len(compressed)),
                        "content-type": "text/plain",
                    },
                )
            )
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        forwarded = orjson.loads(resp.headers)
        forwarded_keys = {k for k, _ in forwarded}
        # Stripped: would misdescribe the decoded body
        assert "content-encoding" not in forwarded_keys
        assert "content-length" not in forwarded_keys
        # Preserved: unrelated headers survive
        assert any(k == "content-type" and v == "text/plain" for k, v in forwarded)
        # Body is the decompressed content, not the compressed bytes
        assert base64.b64decode(resp.body) == b"plain-body"

    async def test_trailing_slash_in_target_does_not_double(self) -> None:
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )
        captured: list[str] = []

        def handler(req: httpx.Request) -> httpx.Response:
            captured.append(str(req.url))
            return httpx.Response(200, text="ok")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000/", request, http_client, ws)

        assert captured[0] == "http://localhost:9000/api/data"

    async def test_post_with_binary_body(self) -> None:
        ws = AsyncMock()
        binary = bytes(range(256))
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="upload",
            url_query=orjson.dumps([]).decode(),
            method="POST",
            headers=orjson.dumps([]).decode(),
            body=base64.b64encode(binary).decode(),
        )

        captured: list[bytes] = []

        def handler(req: httpx.Request) -> httpx.Response:
            captured.append(req.content)
            return httpx.Response(201, content=binary)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        assert captured[0] == binary
        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert resp.status_code == 201
        assert base64.b64decode(resp.body) == binary

    async def test_target_error_returns_504(self) -> None:
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert resp.status_code == 504
        assert resp.body == ""

    async def test_error_response_headers_is_valid_json(self) -> None:
        """PR #24: client error path must emit parseable headers.

        headers="" fails orjson.loads("") → JSONDecodeError if any consumer
        calls orjson.loads without a falsy guard.
        headers="{}" is valid JSON and the correct fix.

        This test FAILS on the original code (headers="") and PASSES on the fix.
        """
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert resp.status_code == 504
        # Must be parseable JSON — orjson.loads("") raises JSONDecodeError
        parsed = orjson.loads(resp.headers)
        assert parsed == []

    async def test_send_failure_does_not_raise(self) -> None:
        """If the WebSocket is closed when sending the response, handle_request
        must not raise — the server-side disconnect handling takes over."""
        ws = AsyncMock()
        ws.send = AsyncMock(side_effect=RuntimeError("connection closed"))
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda req: httpx.Response(200, text="ok"))
        ) as http_client:
            # Must not raise
            await handle_request("http://localhost:9000", request, http_client, ws)

        ws.send.assert_awaited_once()


class TestMainReconnect:
    async def test_retries_on_connection_refused(self) -> None:
        call_count = 0
        connect_cm = AsyncMock()

        async def side_effect(*a: object, **kw: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionRefusedError("refused")
            raise asyncio.CancelledError()

        connect_cm.__aenter__ = side_effect
        connect_cm.__aexit__ = AsyncMock(return_value=False)

        with (
            contextlib.suppress(asyncio.CancelledError),
            patch("pipegate.client.connect", return_value=connect_cm),
            patch("pipegate.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await main("http://localhost:9000", "ws://fake:8000/conn")

        assert call_count >= 2

    async def test_backoff_starts_at_one_second(self) -> None:
        """First reconnect waits 1s (2^0), not 2s — matches README's 1s..60s."""
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        call_count = 0
        connect_cm = AsyncMock()

        async def side_effect(*a: object, **kw: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise ConnectionRefusedError("refused")
            raise asyncio.CancelledError()

        connect_cm.__aenter__ = side_effect
        connect_cm.__aexit__ = AsyncMock(return_value=False)

        with (
            contextlib.suppress(asyncio.CancelledError),
            patch("pipegate.client.connect", return_value=connect_cm),
            patch("pipegate.client.asyncio.sleep", new=fake_sleep),
        ):
            await main("http://localhost:9000", "ws://fake:8000/conn")

        assert sleeps[0] == 1.0
        assert sleeps[1] == 2.0
        assert sleeps[2] == 4.0

    async def test_reconnects_when_established_connection_drops(self) -> None:
        """A dropped established connection must trigger reconnection,
        not a busy-loop of repeated recv() failures."""
        from websockets.exceptions import ConnectionClosed

        call_count = 0
        ws_mock = AsyncMock()

        async def side_effect(*a: object, **kw: object) -> AsyncMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                ws_mock.recv = AsyncMock(side_effect=ConnectionClosed(None, None))
                return ws_mock
            raise asyncio.CancelledError()

        connect_cm = AsyncMock()
        connect_cm.__aenter__ = side_effect
        connect_cm.__aexit__ = AsyncMock(return_value=False)

        with (
            contextlib.suppress(asyncio.CancelledError),
            patch("pipegate.client.connect", return_value=connect_cm),
            patch("pipegate.client.asyncio.sleep", new_callable=AsyncMock),
        ):
            await main("http://localhost:9000", "ws://fake:8000/conn")

        assert call_count >= 2
