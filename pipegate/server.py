from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import uuid
from collections.abc import AsyncGenerator
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
from pydantic import ValidationError

from .auth import verify_token
from .schemas import (
    BufferGateRequest,
    BufferGateResponse,
    Methods,
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


def create_app() -> FastAPI:
    buffers: dict[str, asyncio.Queue[BufferGateRequest]] = {}
    futures: dict[uuid.UUID, asyncio.Future[BufferGateResponse]] = {}
    pending_by_conn: dict[str, set[uuid.UUID]] = {}
    connected_conns: set[str] = set()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.extra["settings"] = Settings()
        app.extra["buffers"] = buffers
        try:
            yield
        finally:
            for fut in list(futures.values()):
                if not fut.done():
                    fut.set_exception(
                        HTTPException(status_code=504, detail="Gateway Timeout")
                    )

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

        future: asyncio.Future[BufferGateResponse] = asyncio.Future()
        futures[correlation_id] = future
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
                        {
                            k: v
                            for k, v in request.headers.items()
                            if k.lower() not in _HOP_BY_HOP
                        }
                        | {"x-pipegate-correlation-id": correlation_id.hex}
                    ).decode(),
                    body=base64.b64encode(raw_body).decode(),
                )
            )
        except asyncio.QueueFull:
            futures.pop(correlation_id, None)
            pending_by_conn.get(connection_id, set()).discard(correlation_id)
            raise HTTPException(
                status_code=503,
                detail="Queue full — tunnel client is too slow or not connected",
            ) from None

        try:
            async with asyncio.timeout(300):
                response = await future
        except TimeoutError as e:
            raise HTTPException(status_code=504, detail="Gateway Timeout") from e
        finally:
            futures.pop(correlation_id, None)
            conn_pending = pending_by_conn.get(connection_id)
            if conn_pending is not None:
                conn_pending.discard(correlation_id)
                if not conn_pending and connection_id not in connected_conns:
                    pending_by_conn.pop(connection_id, None)
                    buffers.pop(connection_id, None)

        response_content = (
            b""
            if request.method == "HEAD"
            else (base64.b64decode(response.body) if response.body else b"")
        )
        return Response(
            content=response_content,
            headers=orjson.loads(response.headers) if response.headers else {},
            status_code=response.status_code,
        )

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
                        message = BufferGateResponse.model_validate_json(message_text)
                        future = futures.get(message.correlation_id)
                        if future and not future.done():
                            future.set_result(message)
                        else:
                            logger.warning(
                                "No pending future for: %s", message.correlation_id
                            )
                    except ValidationError as ve:
                        logger.warning("Invalid message format: %s", ve)
            except WebSocketDisconnect:
                pass

        async def send() -> None:
            while True:
                request = await queue.get()
                try:
                    await websocket.send_text(request.model_dump_json())
                except WebSocketDisconnect:
                    fut = futures.get(request.correlation_id)
                    if fut and not fut.done():
                        fut.set_exception(
                            HTTPException(
                                status_code=502,
                                detail="Tunnel client disconnected",
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
        for cid in pending_by_conn.pop(connection_id, set()):
            fut = futures.pop(cid, None)
            if fut and not fut.done():
                fut.set_exception(
                    HTTPException(
                        status_code=502,
                        detail="Tunnel client disconnected",
                    )
                )

    return app
