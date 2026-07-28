from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import uuid
from collections.abc import AsyncIterator
from typing import cast
from unittest.mock import AsyncMock, patch

import httpx
import orjson
from httpx import AsyncByteStream

from pipegate.client import _HTTP_TIMEOUT, _WS_KWARGS, handle_request, main
from pipegate.schemas import (
    BufferGateRequest,
    CancelRequest,
)


def _parse_sent(payload_json: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads(payload_json))


def _sent_messages(ws: AsyncMock) -> list[dict[str, object]]:
    """All messages the client sent over the WS, as parsed dicts in order."""
    return [_parse_sent(cast(str, call.args[0])) for call in ws.send.call_args_list]


class _ListStream(AsyncByteStream):
    """A simple AsyncByteStream backed by a list of byte chunks."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for c in self._chunks:
            yield c


class _BlockingStream(AsyncByteStream):
    """Streams one chunk, then blocks until ``aclose()`` is called. On
    ``aclose()``, ``__aiter__`` raises to simulate a real socket being
    closed mid-stream (httpx's MockTransport doesn't do this automatically
    — without the raise, ``aiter_bytes()`` would return normally on cancel,
    hiding bugs in the cancel error-handling path)."""

    def __init__(self, first_chunk: bytes = b"data: first\n\n") -> None:
        self._first = first_chunk
        self.yielded_first = asyncio.Event()
        self.aclosed = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._first
        self.yielded_first.set()
        # Block until aclose() — simulates waiting for more upstream data.
        await self.aclosed.wait()
        # aclose() was called: simulate the socket close that would cause
        # aiter_bytes() to raise (not a clean end-of-stream).
        raise RuntimeError("stream closed by aclose()")

    async def aclose(self) -> None:
        self.aclosed.set()


def _streaming_response(
    status: int = 200,
    chunks: list[bytes] | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    """Build an httpx Response whose body is streamed from ``chunks``."""
    return httpx.Response(
        status,
        stream=_ListStream(chunks or []),
        headers=headers or {},
    )


class TestHandleRequest:
    async def test_successful_forward_emits_headers_chunk_end(self) -> None:
        """A buffered 200 response is forwarded as:
        ResponseHeaders -> ResponseChunk (whole body) -> ResponseEnd."""
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="api/data",
            url_query=orjson.dumps([["key", "value"]]).decode(),
            method="GET",
            headers=orjson.dumps([["x-custom", "header"]]).decode(),
            body="",
        )

        def handler(req: httpx.Request) -> httpx.Response:
            return _streaming_response(200, chunks=[b"ok"], headers={"x-resp": "val"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000",
                request,
                http_client,
                ws,
                asyncio.Event(),
            )

        sent = _sent_messages(ws)
        assert len(sent) == 3
        assert sent[0]["type"] == "headers"
        assert sent[0]["status_code"] == 200
        headers = orjson.loads(cast(str, sent[0]["headers"]))
        assert any(k == "x-resp" and v == "val" for k, v in headers)
        assert sent[1]["type"] == "chunk"
        assert base64.b64decode(cast(str, sent[1]["body"])) == b"ok"
        assert sent[2]["type"] == "end"
        assert sent[2]["error"] is None

    async def test_strips_encoding_headers_and_decompresses_body(self) -> None:
        """httpx decompresses gzip/deflate and de-chunks transfer encoding
        transparently via aiter_bytes(). The forwarded headers must not claim
        content-encoding or content-length that misdescribe the already-decoded
        body, or the browser fails with ERR_CONTENT_DECODING_FAILED. The
        forwarded body must be the decompressed plaintext, not the raw
        compressed bytes — otherwise, with content-encoding stripped, the
        browser renders gzip bytes as mojibake.

        Regression: previously this used aiter_raw(), which skips httpx's
        decoder, so the compressed bytes were forwarded verbatim while the
        content-encoding header was stripped — producing garbled pages."""
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

        plaintext = b"hello world, this is plain text that should be readable"
        gz_body = gzip.compress(plaintext)

        def handler(req: httpx.Request) -> httpx.Response:
            return _streaming_response(
                200,
                chunks=[gz_body],
                headers={
                    "content-encoding": "gzip",
                    "content-length": str(len(gz_body)),
                    "content-type": "text/plain",
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000", request, http_client, ws, asyncio.Event()
            )

        sent = _sent_messages(ws)
        headers_msg = next(m for m in sent if m["type"] == "headers")
        forwarded = orjson.loads(cast(str, headers_msg["headers"]))
        forwarded_keys = {k for k, _ in forwarded}
        assert "content-encoding" not in forwarded_keys
        assert "content-length" not in forwarded_keys
        assert any(k == "content-type" and v == "text/plain" for k, v in forwarded)
        chunk_msg = next(m for m in sent if m["type"] == "chunk")
        forwarded_body = base64.b64decode(cast(str, chunk_msg["body"]))
        assert forwarded_body == plaintext, (
            f"expected decompressed plaintext, got {forwarded_body!r}"
        )
        assert forwarded_body != gz_body, "body was forwarded still compressed"

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
            return _streaming_response(200, chunks=[b"ok"])

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000/", request, http_client, ws, asyncio.Event()
            )

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
            return _streaming_response(201, chunks=[binary])

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000", request, http_client, ws, asyncio.Event()
            )

        assert captured[0] == binary
        sent = _sent_messages(ws)
        headers_msg = next(m for m in sent if m["type"] == "headers")
        assert headers_msg["status_code"] == 201
        chunk_msg = next(m for m in sent if m["type"] == "chunk")
        assert base64.b64decode(cast(str, chunk_msg["body"])) == binary

    async def test_target_error_emits_end_with_error(self) -> None:
        """Upstream ConnectError must produce a single ResponseEnd(error=...)
        with no preceding ResponseHeaders — the server maps this to 502."""
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
            await handle_request(
                "http://localhost:9000", request, http_client, ws, asyncio.Event()
            )

        sent = _sent_messages(ws)
        assert len(sent) == 1
        assert sent[0]["type"] == "end"
        assert sent[0]["error"] is not None
        assert "connection refused" in cast(str, sent[0]["error"])

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

        def handler(req: httpx.Request) -> httpx.Response:
            return _streaming_response(200, chunks=[b"ok"])

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000", request, http_client, ws, asyncio.Event()
            )

        ws.send.assert_awaited_once()

    async def test_multiple_body_chunks_streamed_separately(self) -> None:
        """When the upstream service streams the body in multiple chunks
        (e.g. SSE), each chunk must be forwarded as a separate ResponseChunk
        rather than buffered into one. This is what lets LangGraph
        /runs/stream work through the tunnel without the caller timing out
        before any event arrives."""
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="stream",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        sse_events = [b"data: 1\n\n", b"data: 2\n\n", b"data: [DONE]\n\n"]

        def handler(req: httpx.Request) -> httpx.Response:
            return _streaming_response(
                200,
                chunks=sse_events,
                headers={"content-type": "text/event-stream"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            await handle_request(
                "http://localhost:9000", request, http_client, ws, asyncio.Event()
            )

        sent = _sent_messages(ws)
        chunks = [m for m in sent if m["type"] == "chunk"]
        assert len(chunks) == 3, "each upstream chunk must be a separate ResponseChunk"
        reassembled = b"".join(base64.b64decode(cast(str, c["body"])) for c in chunks)
        assert reassembled == b"".join(sse_events)
        assert sent[-1]["type"] == "end"
        assert sent[-1]["error"] is None


class TestCancellation:
    """The cancel_event is set by main() when the server signals
    CancelRequest. handle_request must close the upstream httpx stream in
    response, so the local service (e.g. LangGraph) observes client-side
    disconnect and aborts any active run instead of leaving it dangling."""

    async def test_cancel_event_closes_upstream_stream(self) -> None:
        """When cancel_event fires mid-stream, handle_request must call
        response.aclose() on the upstream httpx stream, which causes aiter_bytes
        to stop. The final ResponseEnd must carry error='cancelled'.

        Note: httpx's MockTransport doesn't abort a blocked generator on
        aclose() the way a real socket would — the test's _BlockingStream
        overrides aclose() to release its own block, simulating the socket
        close that would unblock a real upstream's response iterator.
        """
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="stream",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        blocking_stream = _BlockingStream(first_chunk=b"data: first\n\n")

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                stream=blocking_stream,
                headers={"content-type": "text/event-stream"},
            )

        cancel_event = asyncio.Event()

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as http_client:
            task = asyncio.create_task(
                handle_request(
                    "http://localhost:9000",
                    request,
                    http_client,
                    ws,
                    cancel_event,
                )
            )
            # Wait for the upstream to yield its first chunk — by then
            # handle_request has sent ResponseHeaders + ResponseChunk and is
            # blocked in aiter_bytes() waiting for more.
            await asyncio.wait_for(blocking_stream.yielded_first.wait(), timeout=5)
            # Give the watcher a moment to be armed.
            await asyncio.sleep(0.1)
            # Fire the cancel — the watcher aclose()s the upstream stream,
            # which (for a real socket) would abort aiter_bytes(). For the
            # mock, _BlockingStream.aclose() releases the block.
            cancel_event.set()
            await asyncio.wait_for(task, timeout=5)

        # The watcher must have called aclose() on the upstream response.
        assert blocking_stream.aclosed.is_set(), (
            "cancel_event did not trigger response.aclose()"
        )

        sent = _sent_messages(ws)
        # Headers + first chunk were sent before cancel; then ResponseEnd.
        end_msg = next(m for m in sent if m["type"] == "end")
        assert end_msg["error"] == "cancelled"

    async def test_cancel_request_in_main_sets_event(self) -> None:
        """When main() receives a CancelRequest WS frame, it must set the
        corresponding inflight cancel_event so handle_request aborts the
        upstream request."""
        cid = uuid.uuid4()
        request_msg = BufferGateRequest(
            correlation_id=cid,
            url_path="api",
            url_query="[]",
            method="GET",
            headers="[]",
            body="",
        )
        cancel_msg = CancelRequest(correlation_id=cid)

        # The fake WS yields: request, cancel, then blocks forever.
        messages = [
            request_msg.model_dump_json(),
            cancel_msg.model_dump_json(),
        ]

        class FakeWS:
            def __init__(self) -> None:
                self.sent: list[str] = []

            async def recv(self) -> str:
                if messages:
                    return messages.pop(0)
                await asyncio.Event().wait()
                raise RuntimeError("unreachable")

            async def send(self, payload: str) -> None:
                self.sent.append(payload)

        class FakeConnect:
            def __init__(self, ws: FakeWS) -> None:
                self._ws = ws

            async def __aenter__(self) -> FakeWS:
                return self._ws

            async def __aexit__(self, *a: object) -> bool:
                return False

        fake_ws = FakeWS()
        captured_events: dict[uuid.UUID, asyncio.Event] = {}

        async def fake_handle_request(
            target: str,
            req: BufferGateRequest,
            http_client: httpx.AsyncClient,
            ws_client: object,
            cancel_event: asyncio.Event,
        ) -> None:
            captured_events[req.correlation_id] = cancel_event
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(cancel_event.wait(), timeout=2)

        # httpx.AsyncClient() is an async context manager. Replace it with a
        # minimal fake that yields a throwaway client — the patched
        # handle_request ignores it. (Without this, httpx tries to use the
        # ambient HTTP_PROXY/ALL_PROXY env vars and fails in CI/sandboxes.)
        class _FakeHttpClientCM:
            async def __aenter__(self) -> object:
                return object()

            async def __aexit__(self, *a: object) -> bool:
                return False

        with (
            patch("pipegate.client.connect", return_value=FakeConnect(fake_ws)),
            patch("pipegate.client.handle_request", new=fake_handle_request),
            patch(
                "pipegate.client.httpx.AsyncClient", return_value=_FakeHttpClientCM()
            ),
            patch("pipegate.client._asyncio_sleep", new_callable=AsyncMock),
        ):
            main_task = asyncio.create_task(
                main("http://localhost:9000", "ws://fake:8000/")
            )
            # Wait for the cancel to propagate to handle_request's event.
            await asyncio.sleep(0.5)
            main_task.cancel()
            try:
                await main_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                raise AssertionError(f"main() raised unexpectedly: {e!r}") from e

        assert cid in captured_events, "handle_request was never invoked"
        assert captured_events[cid].is_set(), (
            "CancelRequest did not set the cancel_event"
        )


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
            patch("pipegate.client._asyncio_sleep", new_callable=AsyncMock),
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
            patch("pipegate.client._asyncio_sleep", new=fake_sleep),
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
            patch("pipegate.client._asyncio_sleep", new_callable=AsyncMock),
        ):
            await main("http://localhost:9000", "ws://fake:8000/conn")

        assert call_count >= 2


class TestStreamingTimeoutConfig:
    """Regression guards for the long-task streaming fix.

    The original bug: ``httpx.AsyncClient()`` was constructed without an
    explicit ``timeout``, so httpx's default ``Timeout(5.0)`` applied a 5s
    *read* timeout to every upstream chunk. SSE/Agent streams legitimately go
    silent for tens of seconds between events (model reasoning, tool calls),
    so ``aiter_bytes()`` raised ``ReadTimeout`` mid-stream, ``handle_request``
    emitted ``ResponseEnd(error="read timed out")``, and the caller saw the
    stream cut off abruptly.

    These tests pin the fix at three layers:
      1. The ``_HTTP_TIMEOUT`` constant has ``read=None``.
      2. ``main()`` actually passes ``_HTTP_TIMEOUT`` to ``httpx.AsyncClient``
         and ``_WS_KWARGS`` to ``connect`` (guards against someone reverting
         the call sites while leaving the constants in place).
      3. ``_WS_KWARGS`` carries explicit ping/max_size knobs (reverse-proxy
         deployment guard).
    """

    def test_httpx_read_timeout_is_disabled(self) -> None:
        """read=None is the whole point — SSE idle gaps must not trip a
        client-side read timeout. The server-side ``stream_idle_timeout``
        remains the only arbiter of when an idle stream is dead."""
        assert _HTTP_TIMEOUT.read is None
        # Connect/write/pool stay tight so dead upstreams are still detected.
        assert _HTTP_TIMEOUT.connect == 10.0
        assert _HTTP_TIMEOUT.write == 10.0
        assert _HTTP_TIMEOUT.pool == 10.0

    def test_websocket_keepalive_kwargs_are_explicit(self) -> None:
        """Explicit ping_interval/ping_timeout/max_size protect the WS from
        reverse-proxy idle drops and large-frame rejection."""
        assert _WS_KWARGS["ping_interval"] == 20
        assert _WS_KWARGS["ping_timeout"] == 20
        assert _WS_KWARGS["max_size"] is None
        assert _WS_KWARGS["close_timeout"] == 10

    async def test_main_passes_timeout_and_ws_kwargs_to_call_sites(
        self,
    ) -> None:
        """``main()`` must construct ``httpx.AsyncClient(timeout=_HTTP_TIMEOUT)``
        and ``connect(server_url, **_WS_KWARGS)``. A regression that drops
        either argument (reverting to the defaults) would re-introduce the
        silent mid-stream cutoff."""

        class _FakeWS:
            async def recv(self) -> str:
                # Block forever — main() will be cancelled below.
                await asyncio.Event().wait()
                raise RuntimeError("unreachable")

            async def send(self, payload: str) -> None:
                pass

        class _FakeConnect:
            def __init__(self, ws: _FakeWS) -> None:
                self._ws = ws

            async def __aenter__(self) -> _FakeWS:
                return self._ws

            async def __aexit__(self, *a: object) -> bool:
                return False

        class _FakeHttpClientCM:
            async def __aenter__(self) -> object:
                return object()

            async def __aexit__(self, *a: object) -> bool:
                return False

        fake_ws = _FakeWS()
        with (
            patch(
                "pipegate.client.connect", return_value=_FakeConnect(fake_ws)
            ) as connect_mock,
            patch(
                "pipegate.client.httpx.AsyncClient",
                return_value=_FakeHttpClientCM(),
            ) as client_mock,
            patch("pipegate.client._asyncio_sleep", new_callable=AsyncMock),
        ):
            main_task = asyncio.create_task(
                main("http://localhost:9000", "ws://fake:8000/")
            )
            # Let main() enter the async with blocks (constructing the WS and
            # the http client) before cancelling.
            await asyncio.sleep(0.3)
            main_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await main_task

        # connect() must be invoked with the keepalive kwargs.
        assert connect_mock.call_args is not None
        connect_kwargs = connect_mock.call_args.kwargs
        for key in ("ping_interval", "ping_timeout", "max_size", "close_timeout"):
            assert key in connect_kwargs, (
                f"connect() missing explicit kwarg {key!r} — reverse-proxy "
                "keepalive regression"
            )
        assert connect_kwargs["max_size"] is None

        # httpx.AsyncClient() must be invoked with timeout=_HTTP_TIMEOUT.
        assert client_mock.call_args is not None
        passed_timeout = client_mock.call_args.kwargs.get("timeout")
        assert passed_timeout is _HTTP_TIMEOUT, (
            "httpx.AsyncClient() not constructed with _HTTP_TIMEOUT — "
            "default 5s read timeout will silently cut off SSE streams"
        )

    async def test_long_idle_gap_between_chunks_does_not_error(self) -> None:
        """Behavioral smoke test: an upstream SSE stream that yields a chunk,
        goes silent for a beat, then yields another chunk must complete with
        ``ResponseEnd(error=None)`` — not a ``ReadTimeout`` error.

        Note: httpx's MockTransport does not enforce read timeouts on its
        synthetic streams, so this test does not by itself catch a regression
        to the default 5s timeout (that's what the constant test above is
        for). It does, however, exercise the ``handle_request`` chunk-forward
        path end-to-end with a delayed stream and asserts the final
        ``ResponseEnd`` is clean.
        """
        ws = AsyncMock()
        request = BufferGateRequest(
            correlation_id=uuid.uuid4(),
            url_path="stream",
            url_query=orjson.dumps([]).decode(),
            method="GET",
            headers=orjson.dumps([]).decode(),
            body="",
        )

        class _DelayedStream(AsyncByteStream):
            """Yields chunk1, sleeps past what a 5s read timeout would have
            killed, then yields chunk2 and ends."""

            def __init__(self, delay: float) -> None:
                self._delay = delay

            async def __aiter__(self) -> AsyncIterator[bytes]:
                yield b"data: first\n\n"
                await asyncio.sleep(self._delay)
                yield b"data: second\n\n"

            async def aclose(self) -> None:
                pass

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                stream=_DelayedStream(delay=0.2),
                headers={"content-type": "text/event-stream"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as http_client:
            await handle_request(
                "http://localhost:9000",
                request,
                http_client,
                ws,
                asyncio.Event(),
            )

        sent = _sent_messages(ws)
        end_msg = next(m for m in sent if m["type"] == "end")
        assert end_msg is not None
        # Under our fix (read timeout disabled), the stream completes cleanly
        # with both chunks forwarded and error=None.
        assert end_msg["error"] is None, (
            "ResponseEnd carried an error — stream did not complete cleanly"
        )
        bodies = [
            base64.b64decode(cast(str, m["body"])) for m in sent if m["type"] == "chunk"
        ]
        assert b"data: first\n\n" in bodies
        assert b"data: second\n\n" in bodies
