from __future__ import annotations

import asyncio
import base64
import contextlib
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import orjson

from pipegate.client import handle_request, main, rewrite_html_paths
from pipegate.schemas import BufferGateRequest, BufferGateResponse


class TestRewriteHtmlPaths:
    def test_rewrites_absolute_paths(self) -> None:
        html = '<link href="/static/style.css"><script src="/js/app.js"></script>'
        result = rewrite_html_paths(html, "my-conn-id")
        assert result == '<link href="/my-conn-id/static/style.css"><script src="/my-conn-id/js/app.js"></script>'

    def test_preserves_relative_paths(self) -> None:
        html = '<link href="style.css"><script src="./app.js"></script>'
        result = rewrite_html_paths(html, "my-conn-id")
        assert result == '<link href="style.css"><script src="./app.js"></script>'

    def test_preserves_protocol_relative_urls(self) -> None:
        html = '<script src="//cdn.example.com/lib.js"></script>'
        result = rewrite_html_paths(html, "my-conn-id")
        assert result == '<script src="//cdn.example.com/lib.js"></script>'

    def test_preserves_already_rewritten_paths(self) -> None:
        html = '<link href="/my-conn-id/static/style.css">'
        result = rewrite_html_paths(html, "my-conn-id")
        assert result == '<link href="/my-conn-id/static/style.css">'

    def test_handles_action_attribute(self) -> None:
        html = '<form action="/submit"><img data-src="/images/logo.png">'
        result = rewrite_html_paths(html, "my-conn-id")
        assert result == '<form action="/my-conn-id/submit"><img data-src="/my-conn-id/images/logo.png">'


class TestHandleRequest:
    async def test_successful_forward(self) -> None:
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn",
            url_path="api/data",
            url_query=orjson.dumps([["key", "value"]]).decode(),
            method="GET",
            headers=orjson.dumps({"x-custom": "header"}).decode(),
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
        assert orjson.loads(resp.headers)["x-resp"] == "val"

    async def test_post_with_binary_body(self) -> None:
        ws = AsyncMock()
        binary = bytes(range(256))
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn",
            url_path="upload",
            url_query=orjson.dumps([]).decode(),
            method="POST",
            headers=orjson.dumps({}).decode(),
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
            connection_id="test-conn",
            url_path="api/data",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps({}).decode(),
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

    async def test_html_response_path_rewrite(self) -> None:
        """测试 HTML 响应中的路径被正确重写。"""
        ws = AsyncMock()
        html_content = '<html><head><link href="/static/style.css"></head><body></body></html>'
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn-123",
            url_path="index.html",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps({}).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=html_content.encode("utf-8"),
                headers={
                    "content-type": "text/html; charset=utf-8",
                    "content-length": str(len(html_content.encode("utf-8"))),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        decoded_body = base64.b64decode(resp.body).decode("utf-8")
        assert "/test-conn-123/static/style.css" in decoded_body
        assert 'href="/static/style.css"' not in decoded_body
        # 验证 Content-Length header 被移除
        resp_headers = orjson.loads(resp.headers)
        assert "content-length" not in resp_headers

    async def test_non_html_response_unchanged(self) -> None:
        """测试非 HTML 响应保持原样传递。"""
        ws = AsyncMock()
        css_content = "body { color: red; }"
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn-123",
            url_path="static/style.css",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps({}).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=css_content.encode("utf-8"),
                headers={
                    "content-type": "text/css",
                    "content-length": str(len(css_content.encode("utf-8"))),
                    "cache-control": "max-age=3600",
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        # 验证内容不变
        assert base64.b64decode(resp.body).decode("utf-8") == css_content
        # 验证 headers 保留（包括 content-length）
        resp_headers = orjson.loads(resp.headers)
        assert resp_headers.get("content-type") == "text/css"
        assert resp_headers.get("content-length") == str(len(css_content.encode("utf-8")))
        assert resp_headers.get("cache-control") == "max-age=3600"

    async def test_javascript_response_unchanged(self) -> None:
        """测试 JS 响应保持原样传递。"""
        ws = AsyncMock()
        js_content = "console.log('hello');"
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn-123",
            url_path="static/app.js",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps({}).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=js_content.encode("utf-8"),
                headers={
                    "content-type": "application/javascript",
                    "content-length": str(len(js_content.encode("utf-8"))),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert base64.b64decode(resp.body).decode("utf-8") == js_content
        resp_headers = orjson.loads(resp.headers)
        assert resp_headers.get("content-type") == "application/javascript"
        assert resp_headers.get("content-length") == str(len(js_content.encode("utf-8")))

    async def test_json_response_unchanged(self) -> None:
        """测试 JSON API 响应保持原样传递。"""
        ws = AsyncMock()
        json_content = '{"status": "ok"}'
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            connection_id="test-conn-123",
            url_path="api/status",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps({}).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=json_content.encode("utf-8"),
                headers={
                    "content-type": "application/json",
                    "content-length": str(len(json_content.encode("utf-8"))),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request("http://localhost:9000", request, http_client, ws)

        resp = BufferGateResponse.model_validate_json(ws.send.call_args[0][0])
        assert base64.b64decode(resp.body).decode("utf-8") == json_content
        resp_headers = orjson.loads(resp.headers)
        assert "content-length" in resp_headers


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
