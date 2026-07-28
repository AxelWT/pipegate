from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import cast, get_args

import orjson
from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from .auth import verify_token
from .schemas import (
    BufferGateRequest,
    CancelRequest,
    Methods,
    OutboundMessage,
    ResponseChunk,
    ResponseEnd,
    ResponseHeaders,
    ResponseMessage,
    ResponseMessagePayload,
    Settings,
)

logger = logging.getLogger(__name__)

_HOP_BY_HOP: frozenset[str] = frozenset(
    {
        "connection",
        "content-length",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)

# Per in-flight HTTP request, the server holds an asyncio.Queue of
# ResponseMessage items. Unbounded (maxsize=0) matches the original whole-
# body buffering semantics (the full response was held in memory anyway);
# bounding it would require recovery handling for QueueFull mid-stream,
# which is hard to do cleanly. If memory pressure becomes a real issue,
# promote this to a Settings field.
_STREAM_QUEUE_MAXSIZE: int = 0


def _resolve_target(
    full_path: str, host: str | None, settings: Settings
) -> tuple[str, str]:
    """Resolve (connection_id, path) for an inbound HTTP request.

    Subdomain mode (PIPEGATE_BASE_DOMAIN set): the connection_id is taken
    from the leftmost label of the Host header (port stripped), and the
    full request path is forwarded as-is — so absolute asset paths like
    ``/static/main.js`` work without a prefix.

    Path mode (default, backwards compatible): the first path segment is
    the connection_id and the remainder is forwarded.
    """
    if settings.base_domain:
        base = "." + settings.base_domain.lstrip(".")
        host_no_port = (host or "").split(":", 1)[0].lower()
        if host_no_port.endswith(base) and len(host_no_port) > len(base):
            prefix = host_no_port[: -len(base)]
            connection_id = prefix.split(".", 1)[0]
            return connection_id, full_path
        raise HTTPException(
            status_code=400,
            detail=f"Host must be a subdomain of {settings.base_domain}",
        )

    parts = full_path.split("/", 1)
    connection_id = parts[0]
    path = parts[1] if len(parts) > 1 else ""
    if not connection_id:
        raise HTTPException(status_code=404, detail="Connection id required")
    return connection_id, path


def _build_response_headers(
    raw_headers_json: str,
) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """Parse client-provided response headers and split them into a unique
    dict (for Response construction) plus a list of duplicate-key pairs
    (e.g. multiple Set-Cookie) to append afterwards. Strips content-encoding
    / content-length / transfer-encoding defensively — even if the client
    failed to strip them — so they don't misdescribe the streamed body."""
    raw = orjson.loads(raw_headers_json) if raw_headers_json else []
    _stripped = {"content-encoding", "content-length", "transfer-encoding"}
    unique_headers: dict[str, str] = {}
    duplicate_headers: list[tuple[str, str]] = []
    for k, v in raw:
        kl = k.lower()
        if kl in _stripped:
            continue
        if kl in unique_headers:
            duplicate_headers.append((k, v))
        else:
            unique_headers[kl] = v
    return unique_headers, duplicate_headers


def create_app() -> FastAPI:
    buffers: dict[str, asyncio.Queue[OutboundMessage]] = {}
    streams: dict[uuid.UUID, asyncio.Queue[ResponseMessagePayload]] = {}
    pending_by_conn: dict[str, set[uuid.UUID]] = {}
    connected_conns: set[str] = set()

    def _cleanup_request(connection_id: str, correlation_id: uuid.UUID) -> None:
        """Drop per-request state. The connection-level buffer is only
        removed when no pending requests remain AND no WS is currently
        connected (a freshly-reconnected WS may be about to consume it)."""
        streams.pop(correlation_id, None)
        conn_pending = pending_by_conn.get(connection_id)
        if conn_pending is not None:
            conn_pending.discard(correlation_id)
            if not conn_pending and connection_id not in connected_conns:
                pending_by_conn.pop(connection_id, None)
                buffers.pop(connection_id, None)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.extra["settings"] = Settings()
        app.extra["buffers"] = buffers
        try:
            yield
        finally:
            # On shutdown, fail every in-flight request promptly so callers
            # don't hang for 300s. Pushing ResponseEnd (rather than raising
            # HTTPException into a future, as the old code did) works for
            # both the pre-header and mid-stream cases.
            for cid, q in list(streams.items()):
                q.put_nowait(ResponseEnd(correlation_id=cid, error="server shutdown"))

    app = FastAPI(lifespan=lifespan)
    app.extra["buffers"] = buffers

    @app.api_route(
        "/{full_path:path}",
        methods=list(get_args(Methods)),
    )
    async def handle_http_request(
        request: Request,
        full_path: str = "",
    ) -> Response:
        settings: Settings = request.app.extra["settings"]
        host = request.headers.get("host")

        if full_path == "healthz":
            host_no_port = (host or "").split(":", 1)[0].lower()
            if not settings.base_domain or host_no_port == settings.base_domain:
                return Response(
                    content=orjson.dumps({"status": "ok"}),
                    media_type="application/json",
                )

        connection_id, path_slug = _resolve_target(full_path, host, settings)
        correlation_id = uuid.uuid4()

        body_buf = bytearray()
        limit = settings.max_body_bytes
        async for chunk in request.stream():
            body_buf.extend(chunk)
            if len(body_buf) > limit:
                raise HTTPException(
                    status_code=413,
                    detail=f"Request body exceeds limit of {limit} bytes",
                )
        raw_body = bytes(body_buf)

        stream_queue: asyncio.Queue[ResponseMessagePayload] = asyncio.Queue(
            maxsize=_STREAM_QUEUE_MAXSIZE
        )
        streams[correlation_id] = stream_queue
        pending_by_conn.setdefault(connection_id, set()).add(correlation_id)

        if connection_id not in buffers:
            buffers[connection_id] = asyncio.Queue(maxsize=settings.max_queue_depth)
        queue = buffers[connection_id]

        try:
            queue.put_nowait(
                BufferGateRequest(
                    correlation_id=correlation_id,
                    method=cast(Methods, request.method),
                    url_path=path_slug,
                    url_query=orjson.dumps(
                        list(request.query_params.multi_items())
                    ).decode(),
                    headers=orjson.dumps(
                        [
                            [k.decode("latin-1"), v.decode("latin-1")]
                            for k, v in request.headers.raw
                            if k.decode("latin-1").lower() not in _HOP_BY_HOP
                        ]
                        + [["x-pipegate-correlation-id", correlation_id.hex]]
                    ).decode(),
                    body=base64.b64encode(raw_body).decode(),
                )
            )
        except asyncio.QueueFull:
            _cleanup_request(connection_id, correlation_id)
            raise HTTPException(
                status_code=503,
                detail="Queue full — tunnel client is too slow or not connected",
            ) from None

        # Wait for the first frame (ResponseHeaders, or an early ResponseEnd
        # if the client errored before reading the upstream response). Any
        # exception here (timeout, caller cancellation, error frame) must
        # clean up; the success path falls through and delegates cleanup
        # to body_iter's finally block.
        try:
            try:
                if settings.stream_header_timeout > 0:
                    async with asyncio.timeout(settings.stream_header_timeout):
                        first = await stream_queue.get()
                else:
                    first = await stream_queue.get()
            except TimeoutError as e:
                raise HTTPException(status_code=504, detail="Gateway Timeout") from e

            if isinstance(first, ResponseEnd):
                raise HTTPException(
                    status_code=502,
                    detail=first.error or "Tunnel client error",
                )
            if not isinstance(first, ResponseHeaders):
                raise HTTPException(
                    status_code=502,
                    detail=f"Unexpected first frame: {type(first).__name__}",
                )

            # Parse headers inside the try block so a malformed headers JSON
            # triggers cleanup instead of leaking the stream queue.
            unique_headers, duplicate_headers = _build_response_headers(first.headers)
        except BaseException:
            _cleanup_request(connection_id, correlation_id)
            raise

        async def body_iter() -> AsyncIterator[bytes]:
            cancelled = False
            timed_out = False
            try:
                while True:
                    # Idle timeout: if no frame arrives within
                    # stream_idle_timeout (default 300s), the upstream is
                    # stuck. Send CancelRequest to abort the tunnel client's
                    # in-flight request and terminate the response so the
                    # caller doesn't hang forever. A value of 0 disables the
                    # timeout — appropriate for SSE/Agent streams with long
                    # gaps between events (the upstream itself remains the
                    # only arbiter of when the stream ends).
                    try:
                        if settings.stream_idle_timeout > 0:
                            async with asyncio.timeout(settings.stream_idle_timeout):
                                msg = await stream_queue.get()
                        else:
                            msg = await stream_queue.get()
                    except TimeoutError:
                        timed_out = True
                        break
                    if isinstance(msg, ResponseChunk):
                        yield base64.b64decode(msg.body) if msg.body else b""
                    elif isinstance(msg, ResponseEnd):
                        break
                    # A second ResponseHeaders would be a protocol bug;
                    # ignore defensively.
            except (asyncio.CancelledError, GeneratorExit):
                # Caller disconnected. Starlette cancels the streaming task
                # and calls aclose() on the generator, surfacing as either
                # CancelledError or GeneratorExit at the await point. Both
                # mean: tell the tunnel client to abort the upstream
                # request so the local service (e.g. LangGraph) tears down
                # its active run instead of leaving it dangling for the
                # next request to collide with as a 409.
                cancelled = True
                raise
            finally:
                if cancelled or timed_out:
                    cancel = CancelRequest(correlation_id=correlation_id)
                    with contextlib.suppress(asyncio.QueueFull, KeyError):
                        # Best-effort: if the tunnel buffer is gone or full,
                        # the client will tear down on the next WS failure
                        # anyway. Drop the cancel.
                        buffers[connection_id].put_nowait(cancel)
                _cleanup_request(connection_id, correlation_id)

        resp = StreamingResponse(
            body_iter(),
            status_code=first.status_code,
            headers=unique_headers,
        )
        for k, v in duplicate_headers:
            resp.headers.append(k, v)
        # NOTE: If the caller disconnects in the narrow window between this
        # return and Starlette's first iteration of body_iter, the generator's
        # finally won't execute. CPython's reference-counting GC will close
        # the generator (injecting GeneratorExit) when the StreamingResponse
        # is collected after the send() failure propagates. The WS disconnect
        # handler is the ultimate backstop.
        return resp

    @app.websocket("/")
    async def handle_websocket(
        websocket: WebSocket,
    ) -> None:
        settings: Settings = websocket.app.extra["settings"]
        token: str | None = websocket.query_params.get("token")
        if not token:
            await websocket.close(code=1008, reason="Missing token")
            return
        try:
            payload = verify_token(token, settings)
        except Exception as exc:
            logger.warning("WebSocket auth failed: %s", exc)
            await websocket.close(code=1008, reason="Invalid token")
            return

        connection_id = payload.sub.lower()

        await websocket.accept()
        logger.info("WebSocket connected: %s", connection_id)
        connected_conns.add(connection_id)

        if connection_id not in buffers:
            buffers[connection_id] = asyncio.Queue(maxsize=settings.max_queue_depth)
        queue = buffers[connection_id]

        async def receive() -> None:
            try:
                while True:
                    message_text = await websocket.receive_text()
                    try:
                        message = ResponseMessage.model_validate_json(message_text).root
                        sq = streams.get(message.correlation_id)
                        if sq is not None:
                            sq.put_nowait(message)
                        else:
                            logger.warning(
                                "No pending stream for: %s",
                                message.correlation_id,
                            )
                    except ValidationError as ve:
                        logger.warning("Invalid message format: %s", ve)
            except WebSocketDisconnect:
                pass

        async def send() -> None:
            while True:
                outbound = await queue.get()
                try:
                    await websocket.send_text(outbound.model_dump_json())
                except WebSocketDisconnect:
                    # The request whose send just failed will never get a
                    # response now. Fail its stream so the caller doesn't
                    # hang for 300s. CancelRequests are fire-and-forget.
                    if isinstance(outbound, BufferGateRequest):
                        sq = streams.get(outbound.correlation_id)
                        if sq is not None:
                            sq.put_nowait(
                                ResponseEnd(
                                    correlation_id=outbound.correlation_id,
                                    error="Tunnel client disconnected",
                                )
                            )
                    break

        receive_task = asyncio.create_task(receive())
        send_task = asyncio.create_task(send())

        _done, pending = await asyncio.wait(
            {receive_task, send_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in _done:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                task.result()
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        logger.info("WebSocket disconnected: %s", connection_id)
        connected_conns.discard(connection_id)
        buffers.pop(connection_id, None)
        # Fail every in-flight request for this connection so callers don't
        # hang waiting for headers that will never arrive.
        for cid in pending_by_conn.pop(connection_id, set()):
            sq = streams.get(cid)
            if sq is not None:
                sq.put_nowait(
                    ResponseEnd(
                        correlation_id=cid,
                        error="Tunnel client disconnected",
                    )
                )

    return app
