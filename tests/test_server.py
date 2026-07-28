from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import uuid
from typing import cast

import orjson
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from pipegate.schemas import (
    BufferGateRequest,
    ResponseChunk,
    ResponseEnd,
    ResponseHeaders,
    Settings,
)
from pipegate.server import create_app

from .conftest import make_token

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app() -> FastAPI:
    app = create_app()
    app.extra["settings"] = Settings()
    return app


async def _send_streaming_response(
    inbox: asyncio.Queue[dict[str, object]],
    fwd: dict[str, object],
    *,
    response_body: bytes = b"tunnel-response",
    response_status: int = 200,
    response_headers: str | None = None,
    error: str | None = None,
    skip_headers: bool = False,
) -> None:
    """Push the streaming-response sequence (ResponseHeaders -> optional
    ResponseChunk -> ResponseEnd) for the given forwarded request. If
    ``skip_headers`` is set, only ResponseEnd is sent (simulating a client
    error before headers arrived). If ``error`` is set, ResponseEnd carries
    it (simulating mid-stream abnormal termination)."""
    cid = fwd["correlation_id"]
    if not skip_headers:
        await inbox.put(
            {
                "type": "websocket.receive",
                "text": ResponseHeaders(
                    correlation_id=cid,
                    status_code=response_status,
                    headers=response_headers
                    if response_headers is not None
                    else orjson.dumps([["x-tunnel", "ok"]]).decode(),
                ).model_dump_json(),
            }
        )
        if response_body:
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseChunk(
                        correlation_id=cid,
                        body=base64.b64encode(response_body).decode(),
                    ).model_dump_json(),
                }
            )
    await inbox.put(
        {
            "type": "websocket.receive",
            "text": ResponseEnd(correlation_id=cid, error=error).model_dump_json(),
        }
    )


async def _ws_roundtrip(
    app: FastAPI,
    connection_id: str,
    token: str,
    *,
    method: str = "GET",
    path: str = "test-path",
    body: str = "",
    query: str = "",
    response_body: bytes = b"tunnel-response",
    response_status: int = 200,
    response_headers: str | None = None,
) -> tuple[Response, dict[str, str]]:
    """Full tunnel round-trip: HTTP -> WS forward -> WS response -> HTTP."""
    transport = ASGITransport(app=app)

    url = f"/{connection_id}/{path}"
    if query:
        url += f"?{query}"

    forwarded_request: dict[str, str] = {}

    async with AsyncClient(transport=transport, base_url="http://test") as client:

        async def http_request() -> Response:
            return await client.request(method, url, content=body)

        async def ws_client() -> None:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put),  # type: ignore[arg-type]
            )

            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.accept"

            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.send"

            fwd = json.loads(cast(str, msg["text"]))
            forwarded_request.update(fwd)

            await _send_streaming_response(
                inbox,
                fwd,
                response_body=response_body,
                response_status=response_status,
                response_headers=response_headers,
            )

            await asyncio.sleep(0.05)
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        ws_task = asyncio.create_task(ws_client())
        await asyncio.sleep(0.01)
        http_task = asyncio.create_task(http_request())

        await ws_task
        resp = await asyncio.wait_for(http_task, timeout=10)

    return resp, forwarded_request


async def _connect_ws(
    app: FastAPI,
    *,
    token: str | None = None,
) -> str:
    """Attempt a WS connection, return the first server message type."""
    query = f"token={token}" if token else ""
    scope: dict[str, object] = {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "path": "/",
        "query_string": query.encode(),
        "headers": [],
    }
    inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    await inbox.put({"type": "websocket.connect"})
    app_task = asyncio.create_task(
        app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
    )
    msg = await asyncio.wait_for(outbox.get(), timeout=3)
    await inbox.put({"type": "websocket.disconnect"})
    with contextlib.suppress(Exception):
        await asyncio.wait_for(app_task, timeout=2)
    return cast(str, msg["type"])


# ---------------------------------------------------------------------------
# Tunnel round-trip
# ---------------------------------------------------------------------------


class TestTunnelRoundTrip:
    async def test_get(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip(
            _make_app(), connection_id, make_token(connection_id)
        )
        assert resp.status_code == 200
        assert resp.text == "tunnel-response"
        assert fwd["method"] == "GET"
        assert fwd["url_path"] == "test-path"

    async def test_post_with_body(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            method="POST",
            body="hello",
        )
        assert resp.status_code == 200
        assert fwd["method"] == "POST"
        assert base64.b64decode(fwd["body"]) == b"hello"

    async def test_preserves_query_params(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            query="a=1&a=2&b=3",
        )
        assert resp.status_code == 200
        query = json.loads(fwd["url_query"])
        assert isinstance(query, list)
        a_vals = sorted(v for k, v in query if k == "a")
        assert a_vals == ["1", "2"]
        assert [v for k, v in query if k == "b"] == ["3"]

    async def test_custom_response_status(self, connection_id: str) -> None:
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            response_status=404,
            response_body=b"not found",
        )
        assert resp.status_code == 404
        assert resp.text == "not found"

    async def test_binary_body_roundtrip(self, connection_id: str) -> None:
        """Binary (non-UTF-8) bodies survive the tunnel."""
        binary = bytes(range(256))
        resp, fwd = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            method="POST",
            body=binary,  # type: ignore[arg-type]
            response_body=binary,
        )
        assert resp.status_code == 200
        assert resp.content == binary
        assert base64.b64decode(fwd["body"]) == binary

    async def test_head_response_has_no_body(self, connection_id: str) -> None:
        # The real client uses httpx.stream() which yields no chunks for HEAD
        # (httpx knows HEAD has no body), so the tunnel emits headers + end
        # only. Simulate that: response_body=b"" means no ResponseChunk.
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            method="HEAD",
            response_body=b"",
        )
        assert resp.status_code == 200
        assert resp.content == b""

    async def test_hop_by_hop_headers_not_forwarded(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            method="POST",
            body="hello",
        )
        assert resp.status_code == 200
        forwarded_headers = orjson.loads(fwd["headers"])
        forwarded_keys = {k for k, _ in forwarded_headers}
        for h in ("connection", "transfer-encoding", "keep-alive", "upgrade"):
            assert h not in forwarded_keys, f"{h} should not be forwarded"

    async def test_response_strips_encoding_headers(self, connection_id: str) -> None:
        """The server must not forward content-encoding/content-length from
        the tunnel response — httpx has already decoded the body, so those
        headers would misdescribe it and cause ERR_CONTENT_DECODING_FAILED.
        Starlette re-adds content-length with the correct (decoded) size."""
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            response_headers=orjson.dumps(
                [
                    ["content-encoding", "gzip"],
                    ["content-length", "999"],
                    ["transfer-encoding", "chunked"],
                    ["x-tunnel", "ok"],
                ]
            ).decode(),
        )
        assert resp.status_code == 200
        # Stripped: would misdescribe the decoded body
        assert "content-encoding" not in resp.headers
        assert "transfer-encoding" not in resp.headers
        # content-length is re-added by Starlette with the correct size
        # (not the fake "999" from the tunnel response)
        assert resp.headers.get("content-length") != "999"
        # Preserved: unrelated headers survive
        assert resp.headers["x-tunnel"] == "ok"

    async def test_duplicate_set_cookie_headers_preserved(
        self, connection_id: str
    ) -> None:
        """Multiple Set-Cookie headers must survive the tunnel.

        A dict-based serialization silently drops duplicates (last wins),
        which breaks the CSRF Double Submit pattern: login sets both
        access_token and csrf_token via Set-Cookie, and losing either
        cookie causes 403 on the next state-changing request. The
        list-of-pairs wire format preserves both.
        """
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            response_headers=orjson.dumps(
                [
                    ["set-cookie", "access_token=eyJabc; Path=/; HttpOnly"],
                    ["set-cookie", "csrf_token=def456; Path=/"],
                ]
            ).decode(),
        )
        assert resp.status_code == 200
        set_cookies = resp.headers.get_list("set-cookie")
        assert len(set_cookies) == 2
        values = " ".join(set_cookies)
        assert "access_token=" in values
        assert "csrf_token=" in values


# ---------------------------------------------------------------------------
# Disconnect behaviour
# ---------------------------------------------------------------------------


class TestDisconnect:
    async def test_future_fails_on_ws_disconnect_during_send(
        self, connection_id: str
    ) -> None:
        from fastapi.websockets import WebSocketDisconnect as _WSD

        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            send_attempted = asyncio.Event()

            async def ws_side() -> None:
                scope: dict[str, object] = {
                    "type": "websocket",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "path": "/",
                    "query_string": f"token={token}".encode(),
                    "headers": [],
                }
                inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                await inbox.put({"type": "websocket.connect"})

                async def patched_send(message: dict[str, object]) -> None:
                    if message.get("type") == "websocket.send":
                        send_attempted.set()
                        raise _WSD(code=1006, reason="simulated disconnect")
                    await outbox.put(message)

                app_task = asyncio.create_task(
                    app(scope, inbox.get, patched_send),  # type: ignore[arg-type]
                )
                msg = await asyncio.wait_for(outbox.get(), timeout=5)
                assert msg["type"] == "websocket.accept"
                await asyncio.wait_for(send_attempted.wait(), timeout=5)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(app_task, timeout=3)

            ws_task = asyncio.create_task(ws_side())
            await asyncio.sleep(0.01)

            http_resp = await asyncio.wait_for(
                client.get(f"/{connection_id}/ping"), timeout=5
            )
            await ws_task

        assert http_resp.status_code == 502

    async def test_buffer_cleaned_up_after_disconnect(self, connection_id: str) -> None:
        app = _make_app()
        token = make_token(connection_id)

        scope: dict[str, object] = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "path": "/",
            "query_string": f"token={token}".encode(),
            "headers": [],
        }
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

        await inbox.put({"type": "websocket.connect"})
        app_task = asyncio.create_task(
            app(scope, inbox.get, outbox.put),  # type: ignore[arg-type]
        )
        msg = await asyncio.wait_for(outbox.get(), timeout=5)
        assert msg["type"] == "websocket.accept"

        await inbox.put({"type": "websocket.disconnect"})
        with contextlib.suppress(Exception):
            await asyncio.wait_for(app_task, timeout=2)

        buffers: dict[str, object] = app.extra.get("buffers", {})
        assert connection_id not in buffers

    async def test_pending_future_fails_with_502_on_disconnect(
        self, connection_id: str
    ) -> None:
        """A pending request must resolve with 502 (not hang for 300s -> 504)
        when the tunnel client disconnects via the receive path."""
        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )

            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.accept"

            await asyncio.sleep(0.01)

            http_task = asyncio.create_task(client.get(f"/{connection_id}/pending"))
            await asyncio.sleep(0.05)

            # Disconnect without ever sending a response. The receive() loop
            # exits; the pending future must be failed with 502 promptly.
            await inbox.put({"type": "websocket.disconnect"})

            resp = await asyncio.wait_for(http_task, timeout=5)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# WebSocket authentication
# ---------------------------------------------------------------------------


class TestWebSocketAuth:
    async def test_rejected_without_token(self) -> None:
        assert await _connect_ws(_make_app()) == "websocket.close"

    async def test_rejected_with_expired_token(self, connection_id: str) -> None:
        from datetime import timedelta

        expired = make_token(connection_id, expires_in=timedelta(seconds=-1))
        assert await _connect_ws(_make_app(), token=expired) == "websocket.close"

    async def test_accepted_with_valid_token(self, connection_id: str) -> None:
        valid = make_token(connection_id)
        assert await _connect_ws(_make_app(), token=valid) == "websocket.accept"


# ---------------------------------------------------------------------------
# Body size limit
# ---------------------------------------------------------------------------


class TestBodySizeLimit:
    async def test_oversized_body_rejected_with_413(self, connection_id: str) -> None:
        app = _make_app()
        app.extra["settings"].max_body_bytes = 10
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(f"/{connection_id}/upload", content=b"x" * 100)

        assert resp.status_code == 413

    async def test_oversized_body_rejected_early_via_content_length(
        self, connection_id: str
    ) -> None:
        """Content-Length exceeding the limit must be rejected before reading
        the body, so a huge upload doesn't consume memory."""
        app = _make_app()
        app.extra["settings"].max_body_bytes = 10
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/{connection_id}/upload",
                content=b"x" * 10000,
                headers={"content-length": "10000"},
            )

        assert resp.status_code == 413


# ---------------------------------------------------------------------------
# Queue backpressure
# ---------------------------------------------------------------------------


class TestQueueBackpressure:
    async def test_503_when_queue_full(self, connection_id: str) -> None:
        app = create_app()
        settings = Settings()
        settings.max_queue_depth = 1
        app.extra["settings"] = settings

        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = asyncio.create_task(
                client.post(f"/{connection_id}/req1", content=b"a")
            )
            await asyncio.sleep(0.05)
            second = await asyncio.wait_for(
                client.post(f"/{connection_id}/req2", content=b"b"), timeout=2
            )
            first.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await first

        assert second.status_code == 503


# ---------------------------------------------------------------------------
# Memory cleanup for unconnected connection_ids
# ---------------------------------------------------------------------------


class TestMemoryCleanup:
    async def test_buffers_cleaned_up_for_unconnected_cid(
        self, connection_id: str
    ) -> None:
        """When a request targets a cid with no tunnel connected, the
        queue and pending set must be cleaned up after the request completes
        — not leaked forever."""
        app = _make_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            http_task = asyncio.create_task(client.get(f"/{connection_id}/probe"))
            await asyncio.sleep(0.05)
            http_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await http_task

        buffers: dict[str, object] = app.extra["buffers"]
        assert connection_id not in buffers


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class TestHealth:
    async def test_healthz(self, client: AsyncClient) -> None:
        resp = await client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    async def test_healthz_on_base_domain_in_subdomain_mode(self) -> None:
        """In subdomain mode, /healthz on the base domain itself still works."""
        app = _make_subdomain_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(
            transport=transport, base_url=f"http://{BASE_DOMAIN}"
        ) as client:
            resp = await client.get("/healthz")

        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    async def test_healthz_forwarded_on_subdomain(self, connection_id: str) -> None:
        """In subdomain mode, /healthz on a subdomain must be forwarded to the
        tunnel, not intercepted by PipeGate."""
        resp, fwd = await _ws_roundtrip_subdomain(
            _make_subdomain_app(),
            connection_id,
            make_token(connection_id),
            path="healthz",
        )
        assert resp.status_code == 200
        assert fwd["url_path"] == "healthz"


class TestLifespanShutdown:
    async def test_shutdown_tolerates_concurrent_future_pop(self) -> None:
        """Lifespan shutdown iterates futures; a concurrent request finally-block
        popping its correlation_id must not raise RuntimeError about dict
        changing size during iteration."""
        app = create_app()
        settings = Settings()
        app.extra["settings"] = settings

        # Access the closure-captured dicts via app.extra where exposed, but
        # futures is not exposed — drive a real pending request instead.
        transport = ASGITransport(app=app)
        token = make_token("shutdown-test")

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.accept"

            # Start a pending HTTP request (no response will come)
            http_task = asyncio.create_task(client.get("/shutdown-test/pending"))
            await asyncio.sleep(0.05)

            # Disconnect the tunnel — pending future resolves to 502 quickly,
            # which pops the futures dict. The http finally-block runs.
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)
            resp = await asyncio.wait_for(http_task, timeout=5)

        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# PR #24: empty headers in WS error response
# ---------------------------------------------------------------------------

#
# Server guard: orjson.loads(response.headers) if response.headers else {}
# headers="" is falsy → safe today. headers="{}" is valid JSON either way.
# orjson.loads("") raises JSONDecodeError, so "{}" is the correct value for
# any consumer without the guard.


class TestEmptyHeadersHandling:
    async def test_empty_string_headers_does_not_crash_server(
        self, connection_id: str
    ) -> None:
        # server guard makes "" safe — passes on both old and new code
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            response_headers="",
            response_body=b"",
            response_status=504,
        )
        assert resp.status_code == 504

    async def test_empty_json_array_headers_works(self, connection_id: str) -> None:
        resp, _ = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            response_headers="[]",
            response_body=b"",
            response_status=504,
        )
        assert resp.status_code == 504

    def test_empty_string_is_not_valid_json(self) -> None:
        # Guard is load-bearing: removing it would crash on headers=""
        import pytest

        with pytest.raises(ValueError):
            orjson.loads("")
        assert orjson.loads("[]") == []


# ---------------------------------------------------------------------------
# PR #24: send() must use the queue captured at connect, not dict re-lookup
# ---------------------------------------------------------------------------

#
# If buffers[connection_id] is replaced while a WS is active, the original
# send() (dict lookup each iteration) drains the replacement queue silently.
# The fix captures `queue` as a local variable at connect time.


class TestQueueStability:
    async def test_send_uses_captured_queue_not_dict_lookup(
        self, connection_id: str
    ) -> None:
        """Replace buffers[connection_id] while send() is blocked; verify send()
        ignores the replacement (fixed) rather than draining it (original bug)."""
        app = _make_app()
        token = make_token(connection_id)
        scope: dict[str, object] = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "path": "/",
            "query_string": f"token={token}".encode(),
            "headers": [],
        }
        inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

        await inbox.put({"type": "websocket.connect"})
        app_task = asyncio.create_task(app(scope, inbox.get, outbox.put))  # type: ignore[arg-type]

        msg = await asyncio.wait_for(outbox.get(), timeout=5)
        assert msg["type"] == "websocket.accept"

        buffers: dict[str, asyncio.Queue[BufferGateRequest]] = app.extra["buffers"]
        replacement: asyncio.Queue[BufferGateRequest] = asyncio.Queue(maxsize=100)
        buffers[connection_id] = replacement

        replacement.put_nowait(
            BufferGateRequest(
                correlation_id=uuid.uuid4(),
                url_path="injected",
                url_query="[]",
                method="GET",
                headers="[]",
                body="",
            )
        )

        await asyncio.sleep(0.05)
        received_from_replacement = not outbox.empty()

        await inbox.put({"type": "websocket.disconnect", "code": 1000})
        with contextlib.suppress(Exception):
            await asyncio.wait_for(app_task, timeout=2)

        assert not received_from_replacement, (
            "send() read from a replacement queue — it must use the captured queue ref"
        )


# ---------------------------------------------------------------------------
# Subdomain routing (PIPEGATE_BASE_DOMAIN)
# ---------------------------------------------------------------------------


BASE_DOMAIN = "tunnel.example.com"


def _make_subdomain_app() -> FastAPI:
    app = create_app()
    settings = Settings()
    settings.base_domain = BASE_DOMAIN
    app.extra["settings"] = settings
    return app


async def _ws_roundtrip_subdomain(
    app: FastAPI,
    connection_id: str,
    token: str,
    *,
    method: str = "GET",
    path: str = "test-path",
    body: str = "",
    query: str = "",
    response_body: bytes = b"tunnel-response",
    response_status: int = 200,
    response_headers: str | None = None,
) -> tuple[Response, dict[str, str]]:
    """Full tunnel round-trip in subdomain mode: connection_id comes from
    the Host header, the full path is forwarded as-is."""
    transport = ASGITransport(app=app)
    base_url = f"http://{connection_id}.{BASE_DOMAIN}"

    url = f"/{path}" if path else "/"
    if query:
        url += f"?{query}"

    forwarded_request: dict[str, str] = {}

    async with AsyncClient(transport=transport, base_url=base_url) as client:

        async def http_request() -> Response:
            return await client.request(method, url, content=body)

        async def ws_client() -> None:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put),  # type: ignore[arg-type]
            )

            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.accept"

            msg = await asyncio.wait_for(outbox.get(), timeout=5)
            assert msg["type"] == "websocket.send"

            fwd = json.loads(cast(str, msg["text"]))
            forwarded_request.update(fwd)

            await _send_streaming_response(
                inbox,
                fwd,
                response_body=response_body,
                response_status=response_status,
                response_headers=response_headers,
            )

            await asyncio.sleep(0.05)
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        ws_task = asyncio.create_task(ws_client())
        await asyncio.sleep(0.01)
        http_task = asyncio.create_task(http_request())

        await ws_task
        resp = await asyncio.wait_for(http_task, timeout=10)

    return resp, forwarded_request


class TestSubdomainRouting:
    async def test_get_forwards_full_path(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip_subdomain(
            _make_subdomain_app(),
            connection_id,
            make_token(connection_id),
            path="api/data",
        )
        assert resp.status_code == 200
        assert fwd["method"] == "GET"
        # connection_id must NOT appear in forwarded path
        assert fwd["url_path"] == "api/data"

    async def test_absolute_asset_path_works(self, connection_id: str) -> None:
        """The whole point: /static/main.js reaches the tunnel without a cid prefix."""
        resp, fwd = await _ws_roundtrip_subdomain(
            _make_subdomain_app(),
            connection_id,
            make_token(connection_id),
            path="static/js/main.js",
        )
        assert resp.status_code == 200
        assert fwd["url_path"] == "static/js/main.js"

    async def test_root_path(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip_subdomain(
            _make_subdomain_app(),
            connection_id,
            make_token(connection_id),
            path="",
        )
        assert resp.status_code == 200
        assert fwd["url_path"] == ""

    async def test_host_with_port_stripped(self, connection_id: str) -> None:
        """Host: cid.tunnel.example.com:8000 must still resolve the cid."""
        app = _make_subdomain_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)
        # base_url with port -> Host header includes :8000
        base_url = f"http://{connection_id}.{BASE_DOMAIN}:8000"

        async with AsyncClient(transport=transport, base_url=base_url) as client:

            async def ws_client() -> None:
                scope: dict[str, object] = {
                    "type": "websocket",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "path": "/",
                    "query_string": f"token={token}".encode(),
                    "headers": [],
                }
                inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                await inbox.put({"type": "websocket.connect"})
                app_task = asyncio.create_task(
                    app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
                )
                msg = await asyncio.wait_for(outbox.get(), timeout=5)
                assert msg["type"] == "websocket.accept"
                fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
                fwd = json.loads(cast(str, fwd_msg["text"]))
                await _send_streaming_response(inbox, fwd, response_body=b"ok")
                await asyncio.sleep(0.05)
                await inbox.put({"type": "websocket.disconnect"})
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(app_task, timeout=2)

            ws_task = asyncio.create_task(ws_client())
            await asyncio.sleep(0.01)
            http_task = asyncio.create_task(client.get("/api/data"))

            await ws_task
            resp = await asyncio.wait_for(http_task, timeout=5)

        assert resp.status_code == 200
        assert resp.text == "ok"

    async def test_non_subdomain_host_rejected(self, connection_id: str) -> None:
        """When base_domain is set, a host that isn't a subdomain returns 400."""
        app = _make_subdomain_app()
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/data")

        assert resp.status_code == 400

    async def test_subdomain_case_insensitive(self, connection_id: str) -> None:
        """DNS is case-insensitive; a mixed-case Host must still resolve the
        connection_id registered via JWT sub (which may be lowercase)."""
        app = _make_subdomain_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)
        upper_host = f"http://{connection_id.upper()}.{BASE_DOMAIN}"

        async with AsyncClient(transport=transport, base_url=upper_host) as client:

            async def ws_client() -> None:
                scope: dict[str, object] = {
                    "type": "websocket",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "path": "/",
                    "query_string": f"token={token}".encode(),
                    "headers": [],
                }
                inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                await inbox.put({"type": "websocket.connect"})
                app_task = asyncio.create_task(
                    app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
                )
                msg = await asyncio.wait_for(outbox.get(), timeout=5)
                assert msg["type"] == "websocket.accept"
                fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
                fwd = json.loads(cast(str, fwd_msg["text"]))
                await _send_streaming_response(inbox, fwd, response_body=b"ok")
                await asyncio.sleep(0.05)
                await inbox.put({"type": "websocket.disconnect"})
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(app_task, timeout=2)

            ws_task = asyncio.create_task(ws_client())
            await asyncio.sleep(0.01)
            http_task = asyncio.create_task(client.get("/api/data"))

            await ws_task
            resp = await asyncio.wait_for(http_task, timeout=5)

        assert resp.status_code == 200
        assert resp.text == "ok"

    async def test_query_params_preserved(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip_subdomain(
            _make_subdomain_app(),
            connection_id,
            make_token(connection_id),
            query="a=1&a=2&b=3",
        )
        assert resp.status_code == 200
        query = json.loads(fwd["url_query"])
        a_vals = sorted(v for k, v in query if k == "a")
        assert a_vals == ["1", "2"]

    async def test_base_domain_with_leading_dot(self, connection_id: str) -> None:
        """A common mistake is setting PIPEGATE_BASE_DOMAIN='.tunnel.example.com'.
        The leading dot must be tolerated, not cause all requests to 400."""
        app = create_app()
        settings = Settings()
        settings.base_domain = "." + BASE_DOMAIN
        app.extra["settings"] = settings
        token = make_token(connection_id)
        transport = ASGITransport(app=app)
        base_url = f"http://{connection_id}.{BASE_DOMAIN}"

        async with AsyncClient(transport=transport, base_url=base_url) as client:

            async def ws_client() -> None:
                scope: dict[str, object] = {
                    "type": "websocket",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "path": "/",
                    "query_string": f"token={token}".encode(),
                    "headers": [],
                }
                inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                await inbox.put({"type": "websocket.connect"})
                app_task = asyncio.create_task(
                    app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
                )
                msg = await asyncio.wait_for(outbox.get(), timeout=5)
                assert msg["type"] == "websocket.accept"
                fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
                fwd = json.loads(cast(str, fwd_msg["text"]))
                await _send_streaming_response(inbox, fwd, response_body=b"ok")
                await asyncio.sleep(0.05)
                await inbox.put({"type": "websocket.disconnect"})
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(app_task, timeout=2)

            ws_task = asyncio.create_task(ws_client())
            await asyncio.sleep(0.01)
            http_task = asyncio.create_task(client.get("/api/data"))

            await ws_task
            resp = await asyncio.wait_for(http_task, timeout=5)

        assert resp.status_code == 200

    async def test_multi_level_subdomain_takes_leftmost_label(
        self, connection_id: str
    ) -> None:
        """For host a.b.tunnel.example.com, the connection_id must be 'a'
        (the leftmost label), not 'a.b'."""
        app = _make_subdomain_app()
        token = make_token("a")
        transport = ASGITransport(app=app)
        multi_host = f"http://a.b.{BASE_DOMAIN}"

        async with AsyncClient(transport=transport, base_url=multi_host) as client:

            async def ws_client() -> None:
                scope: dict[str, object] = {
                    "type": "websocket",
                    "asgi": {"version": "3.0"},
                    "http_version": "1.1",
                    "path": "/",
                    "query_string": f"token={token}".encode(),
                    "headers": [],
                }
                inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
                await inbox.put({"type": "websocket.connect"})
                app_task = asyncio.create_task(
                    app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
                )
                msg = await asyncio.wait_for(outbox.get(), timeout=5)
                assert msg["type"] == "websocket.accept"
                fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
                fwd = json.loads(cast(str, fwd_msg["text"]))
                await _send_streaming_response(inbox, fwd, response_body=b"ok")
                await asyncio.sleep(0.05)
                await inbox.put({"type": "websocket.disconnect"})
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(app_task, timeout=2)

            ws_task = asyncio.create_task(ws_client())
            await asyncio.sleep(0.01)
            http_task = asyncio.create_task(client.get("/api/data"))

            await ws_task
            resp = await asyncio.wait_for(http_task, timeout=5)

        # The WS registered connection_id (lowercased), the HTTP request
        # used host a.b.tunnel.example.com -> leftmost label 'a' must match
        assert resp.status_code == 200


class TestPathModeBackwardCompat:
    """Without PIPEGATE_BASE_DOMAIN, path-based routing works as before."""

    async def test_path_mode_still_works(self, connection_id: str) -> None:
        # _make_app uses default Settings (base_domain=None)
        resp, fwd = await _ws_roundtrip(
            _make_app(), connection_id, make_token(connection_id)
        )
        assert resp.status_code == 200
        assert fwd["url_path"] == "test-path"

    async def test_path_mode_static_asset(self, connection_id: str) -> None:
        resp, fwd = await _ws_roundtrip(
            _make_app(),
            connection_id,
            make_token(connection_id),
            path="static/main.js",
        )
        assert resp.status_code == 200
        assert fwd["url_path"] == "static/main.js"


# ---------------------------------------------------------------------------
# Streaming response protocol
# ---------------------------------------------------------------------------


class TestStreamingProtocol:
    """The tunnel forwards responses as ResponseHeaders -> N ResponseChunk ->
    ResponseEnd. This unifies SSE/streaming and buffered responses under one
    protocol and is what enables pipegate to proxy LangGraph /runs/stream
    endpoints without the caller timing out before any event arrives."""

    async def test_multiple_chunks_concatenated(self, connection_id: str) -> None:
        """SSE-style: many small chunks must arrive at the HTTP caller in
        order, concatenated into the full body."""
        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        sse_events = [
            b'data: {"chunk": 1}\n\n',
            b'data: {"chunk": 2}\n\n',
            b'data: {"chunk": 3}\n\n',
            b"data: [DONE]\n\n",
        ]

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            # Start HTTP first — the WS handler forwards it via outbox.
            http_task = asyncio.create_task(client.get(f"/{connection_id}/stream"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseHeaders(
                        correlation_id=cid,
                        status_code=200,
                        headers=orjson.dumps(
                            [["content-type", "text/event-stream"]]
                        ).decode(),
                    ).model_dump_json(),
                }
            )
            for ev in sse_events:
                await inbox.put(
                    {
                        "type": "websocket.receive",
                        "text": ResponseChunk(
                            correlation_id=cid,
                            body=base64.b64encode(ev).decode(),
                        ).model_dump_json(),
                    }
                )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseEnd(correlation_id=cid).model_dump_json(),
                }
            )

            resp = await asyncio.wait_for(http_task, timeout=5)

            await asyncio.sleep(0.05)
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 200
        assert resp.headers["content-type"] == "text/event-stream"
        assert resp.content == b"".join(sse_events)

    async def test_error_before_headers_returns_502(self, connection_id: str) -> None:
        """If the tunnel client sends only ResponseEnd(error=...) with no
        preceding ResponseHeaders (e.g. upstream ConnectError), the caller
        must receive 502 — not a hang, not a 200 with empty body."""
        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/x"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            # No ResponseHeaders — just an error end.
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseEnd(
                        correlation_id=cid, error="ConnectError: refused"
                    ).model_dump_json(),
                }
            )

            resp = await asyncio.wait_for(http_task, timeout=5)

            await asyncio.sleep(0.05)
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 502
        assert "ConnectError" in resp.json()["detail"]

    async def test_caller_disconnect_propagates_cancel(
        self, connection_id: str
    ) -> None:
        """When the HTTP caller disconnects mid-stream, the server must push
        a CancelRequest onto the tunnel's outbound queue so the client aborts
        the upstream request (preventing dangling LangGraph runs that would
        otherwise 409 on the next request).

        We call the ASGI app directly with a custom ``receive`` callable that
        returns ``http.disconnect`` on demand — httpx's ASGITransport doesn't
        propagate caller cancellation to the server, so this is the only way
        to trigger Starlette's listen_for_disconnect path in a unit test.

        The WS send task immediately drains the outbound queue and emits the
        CancelRequest as a websocket.send frame, so we observe it on the WS
        outbox (not the in-memory queue).
        """
        from pipegate.schemas import CancelRequest as _Cancel

        app = _make_app()
        token = make_token(connection_id)

        # --- WS side: accept, forward the request, deliver response frames ---
        ws_scope: dict[str, object] = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "path": "/",
            "query_string": f"token={token}".encode(),
            "headers": [],
        }
        ws_inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        ws_outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        await ws_inbox.put({"type": "websocket.connect"})
        ws_task = asyncio.create_task(
            app(ws_scope, ws_inbox.get, ws_outbox.put)  # type: ignore[arg-type]
        )
        await asyncio.wait_for(ws_outbox.get(), timeout=5)  # accept

        # --- HTTP side: call the ASGI app directly with custom receive/send ---
        http_scope: dict[str, object] = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": f"/{connection_id}/stream",
            "raw_path": f"/{connection_id}/stream".encode(),
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("test", 80),
        }

        request_received = asyncio.Event()
        response_started = asyncio.Event()
        first_chunk_sent = asyncio.Event()
        disconnect_signalled = asyncio.Event()

        async def http_receive() -> dict[str, object]:
            if not request_received.is_set():
                request_received.set()
                return {"type": "http.request", "body": b"", "more_body": False}
            await disconnect_signalled.wait()
            return {"type": "http.disconnect"}

        async def http_send(message: dict[str, object]) -> None:
            if message["type"] == "http.response.start":
                response_started.set()
            elif message["type"] == "http.response.body" and message.get("body"):
                first_chunk_sent.set()

        http_task = asyncio.create_task(
            app(http_scope, http_receive, http_send)  # type: ignore[arg-type]
        )
        await asyncio.wait_for(request_received.wait(), timeout=5)

        # forwarded request (BufferGateRequest) lands on the WS outbox.
        fwd_msg = await asyncio.wait_for(ws_outbox.get(), timeout=5)
        fwd = json.loads(cast(str, fwd_msg["text"]))
        cid = fwd["correlation_id"]

        # Send headers + one chunk, but NO ResponseEnd — body_iter stays
        # suspended on stream_queue.get() after yielding the first chunk.
        await ws_inbox.put(
            {
                "type": "websocket.receive",
                "text": ResponseHeaders(
                    correlation_id=cid,
                    status_code=200,
                    headers=orjson.dumps(
                        [["content-type", "text/event-stream"]]
                    ).decode(),
                ).model_dump_json(),
            }
        )
        await ws_inbox.put(
            {
                "type": "websocket.receive",
                "text": ResponseChunk(
                    correlation_id=cid,
                    body=base64.b64encode(b"data: 1\n\n").decode(),
                ).model_dump_json(),
            }
        )

        # Wait for the server to start streaming (response.start + first chunk).
        await asyncio.wait_for(response_started.wait(), timeout=5)
        await asyncio.wait_for(first_chunk_sent.wait(), timeout=5)

        # Caller disconnects — Starlette's listen_for_disconnect picks this up
        # and cancels body_iter, whose finally must push a CancelRequest.
        disconnect_signalled.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(http_task, timeout=5)

        # The WS send task drains the outbound queue and emits the
        # CancelRequest as a websocket.send frame. Observe it here.
        cancel_msg: dict[str, object] | None = None
        try:
            cancel_msg = await asyncio.wait_for(ws_outbox.get(), timeout=2)
        except TimeoutError:
            cancel_msg = None

        await ws_inbox.put({"type": "websocket.disconnect"})
        with contextlib.suppress(Exception):
            await asyncio.wait_for(ws_task, timeout=2)

        assert cancel_msg is not None, "no CancelRequest was emitted on the WS"
        assert cancel_msg.get("type") == "websocket.send"
        payload = json.loads(cast(str, cancel_msg.get("text", "")))
        assert payload.get("type") == "cancel"
        cancel = _Cancel.model_validate(payload)
        assert cancel.correlation_id == uuid.UUID(cid)

    async def test_tunnel_disconnect_mid_stream_terminates_response(
        self, connection_id: str
    ) -> None:
        """If the WS drops while the HTTP caller is mid-stream, the server
        pushes ResponseEnd(error=...) onto the stream queue so body_iter
        aborts the HTTP connection — the caller sees a connection error
        rather than a silently truncated 200 body (which would otherwise
        surface as e.g. "Unterminated string in JSON" in the caller's
        JSON parser)."""
        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/stream"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            # Send headers + one chunk, then drop the WS without ResponseEnd.
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseHeaders(
                        correlation_id=cid,
                        status_code=200,
                        headers="[]",
                    ).model_dump_json(),
                }
            )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseChunk(
                        correlation_id=cid,
                        body=base64.b64encode(b"partial").decode(),
                    ).model_dump_json(),
                }
            )

            # Wait for the first chunk to be consumed, then disconnect.
            await asyncio.sleep(0.1)
            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

            # The HTTP caller must now see a connection error — not a
            # silently truncated 200 response. The ConnectionError is
            # raised by body_iter when it receives the injected
            # ResponseEnd(error="Tunnel client disconnected").
            with contextlib.suppress(Exception):
                await asyncio.wait_for(http_task, timeout=5)

            # If httpx returned a response at all (it may surface the
            # error as a RemoteProtocolError on .aread()), the body must
            # NOT be a clean, complete response.
            if http_task.done() and not http_task.cancelled():
                exc = http_task.exception()
                if exc is not None:
                    # Connection aborted — exactly what we want.
                    assert isinstance(exc, Exception)

    async def test_error_mid_stream_aborts_connection(self, connection_id: str) -> None:
        """When the tunnel client sends ResponseEnd(error=...) after
        ResponseHeaders + chunks have already been delivered (e.g. httpx
        ReadTimeout, upstream crash, or WS send failure mid-stream), the
        server must abort the HTTP connection — not let body_iter exit
        cleanly and leave the caller with a silently truncated 200 body.

        This is the regression guard for the "Unterminated string in JSON
        at position N" bug: without the abort, the caller receives a
        partial JSON body with HTTP 200 and no error indication, causing
        the caller's JSON parser to fail with a confusing syntax error
        instead of a clear connection error.
        """
        app = _make_app()
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/x"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            # Send headers + partial body, then ResponseEnd with an error
            # (simulates httpx ReadTimeout mid-stream).
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseHeaders(
                        correlation_id=cid,
                        status_code=200,
                        headers=orjson.dumps(
                            [["content-type", "application/json"]]
                        ).decode(),
                    ).model_dump_json(),
                }
            )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseChunk(
                        correlation_id=cid,
                        body=base64.b64encode(b'{"key": "val').decode(),
                    ).model_dump_json(),
                }
            )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseEnd(
                        correlation_id=cid,
                        error="read timed out",
                    ).model_dump_json(),
                }
            )

            # The HTTP caller must see a connection error — NOT a 200
            # with the partial body '{"key": "val' (which would cause
            # "Unterminated string in JSON at position 14" in the
            # caller's JSON parser).
            error_seen = False
            try:
                await asyncio.wait_for(http_task, timeout=5)
            except Exception:
                error_seen = True

            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

            # If httpx surfaced a Response despite the error (ASGITransport
            # may deliver partial content before the exception), the body
            # must not be a valid complete response.
            if not error_seen and http_task.done() and not http_task.cancelled():
                exc = http_task.exception()
                if exc is None:
                    resp = http_task.result()
                    # The body must be incomplete — not a valid JSON.
                    assert not resp.content.endswith(b"}"), (
                        "caller received a complete-looking response body "
                        "despite ResponseEnd(error=...) — silent truncation"
                    )
                else:
                    error_seen = True

            assert error_seen or (
                http_task.done()
                and not http_task.cancelled()
                and http_task.exception() is not None
            ), (
                "caller did not see a connection error after "
                "ResponseEnd(error=...) — body was silently truncated"
            )

    async def test_stream_idle_timeout_is_configurable(
        self, connection_id: str
    ) -> None:
        """``stream_idle_timeout`` (env ``PIPEGATE_STREAM_IDLE_TIMEOUT``)
        replaces the hardcoded 300s. With a 1s value, a stream that goes
        idle after the first chunk must terminate within ~2s — not 300s —
        and emit a CancelRequest so the tunnel client aborts the upstream.

        This is the regression guard for the long-task streaming fix: it
        proves the timeout is now configurable (operators of Agent/SSE
        upstreams can raise or disable it) rather than a magic constant.
        """
        from pipegate.schemas import CancelRequest as _Cancel

        app = create_app()
        app.extra["settings"] = Settings(stream_idle_timeout=1)
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/stream"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            # Send headers + one chunk, then NO ResponseEnd. body_iter yields
            # the chunk, loops back, and blocks on stream_queue.get() under
            # asyncio.timeout(1). After ~1s the idle timeout fires.
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseHeaders(
                        correlation_id=cid,
                        status_code=200,
                        headers="[]",
                    ).model_dump_json(),
                }
            )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseChunk(
                        correlation_id=cid,
                        body=base64.b64encode(b"partial").decode(),
                    ).model_dump_json(),
                }
            )

            # The HTTP response must resolve within 5s — proving the idle
            # timeout fired (under the old 300s default this would hang).
            resp = await asyncio.wait_for(http_task, timeout=5)

            # The CancelRequest must be drained by the WS send task and
            # appear on the WS outbox.
            cancel_msg = await asyncio.wait_for(outbox.get(), timeout=2)

            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 200
        assert b"partial" in resp.content
        assert cancel_msg.get("type") == "websocket.send"
        payload = json.loads(cast(str, cancel_msg.get("text", "")))
        cancel = _Cancel.model_validate(payload)
        assert cancel.correlation_id == uuid.UUID(cid)

    async def test_stream_header_timeout_is_configurable(
        self, connection_id: str
    ) -> None:
        """``stream_header_timeout`` (env ``PIPEGATE_STREAM_HEADER_TIMEOUT``)
        governs the wait for the first response frame. With a 1s value and
        no frames sent, the caller must receive 504 within ~2s — not 300s."""
        app = create_app()
        app.extra["settings"] = Settings(stream_header_timeout=1)
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/stream"))
            # Receive the forwarded request, but send NO response frames.
            await asyncio.wait_for(outbox.get(), timeout=5)

            # Caller must get 504 within 5s — proving the header timeout
            # fired (under the old 300s default this would hang for minutes).
            resp = await asyncio.wait_for(http_task, timeout=5)

            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 504

    async def test_stream_idle_timeout_zero_disables_timeout(
        self, connection_id: str
    ) -> None:
        """``stream_idle_timeout=0`` disables the idle timeout entirely —
        body_iter must wait on stream_queue.get() without any asyncio.timeout
        wrapper. Verified by sending no ResponseEnd and asserting the stream
        does NOT terminate within a window well past the default 300s would
        be impractical, so instead we assert the stream is still pending after
        1.5s (which would have terminated at 1s under
        ``stream_idle_timeout=1`` as proven by the test above)."""
        app = create_app()
        app.extra["settings"] = Settings(stream_idle_timeout=0)
        token = make_token(connection_id)
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            scope: dict[str, object] = {
                "type": "websocket",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "path": "/",
                "query_string": f"token={token}".encode(),
                "headers": [],
            }
            inbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            outbox: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            await inbox.put({"type": "websocket.connect"})
            app_task = asyncio.create_task(
                app(scope, inbox.get, outbox.put)  # type: ignore[arg-type]
            )
            await asyncio.wait_for(outbox.get(), timeout=5)  # accept

            http_task = asyncio.create_task(client.get(f"/{connection_id}/stream"))
            fwd_msg = await asyncio.wait_for(outbox.get(), timeout=5)
            fwd = json.loads(cast(str, fwd_msg["text"]))
            cid = fwd["correlation_id"]

            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseHeaders(
                        correlation_id=cid,
                        status_code=200,
                        headers="[]",
                    ).model_dump_json(),
                }
            )
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseChunk(
                        correlation_id=cid,
                        body=base64.b64encode(b"partial").decode(),
                    ).model_dump_json(),
                }
            )

            # After 1.5s the stream must still be pending — under
            # stream_idle_timeout=1 it would have terminated at ~1s.
            await asyncio.sleep(1.5)
            assert not http_task.done(), (
                "stream terminated despite stream_idle_timeout=0 (disabled)"
            )

            # Clean up: send ResponseEnd to let the stream complete, then
            # tear down the WS.
            await inbox.put(
                {
                    "type": "websocket.receive",
                    "text": ResponseEnd(correlation_id=cid).model_dump_json(),
                }
            )
            resp = await asyncio.wait_for(http_task, timeout=5)

            await inbox.put({"type": "websocket.disconnect"})
            with contextlib.suppress(Exception):
                await asyncio.wait_for(app_task, timeout=2)

        assert resp.status_code == 200
        assert b"partial" in resp.content
