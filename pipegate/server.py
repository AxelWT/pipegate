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
import uvicorn
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


def create_app() -> FastAPI:
    """创建并配置 FastAPI 应用实例。

    原理：工厂函数模式，创建独立的 buffers（请求队列）和 futures（响应等待器）
    两个共享字典。注册三个核心端点：
    - /healthz：健康检查，供监控和负载均衡器探测
    - /{connection_id}/{path}：HTTP 请求入口，将请求入队并等待隧道响应
    - /：WebSocket 端点，隧道客户端连接后建立双向通信

    buffers 按 connection_id 分组，每个连接有独立的请求队列，实现多隧道隔离。
    futures 按 correlation_id 存储等待响应的 Future 对象，实现请求-响应匹配。
    """
    buffers: dict[str, asyncio.Queue[BufferGateRequest]] = {}
    futures: dict[uuid.UUID, asyncio.Future[BufferGateResponse]] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """应用生命周期管理器。

        原理：使用 async context manager 管理应用启动和关闭时的资源。
        启动时初始化 settings 和 buffers 到 app.extra 中，供路由访问。
        关闭时遍历所有未完成的 Future，设置 Gateway Timeout 异常，
        确保等待中的 HTTP 请求不会因服务关闭而无限挂起，而是返回明确的错误。
        """
        app.extra["settings"] = Settings(_cli_parse_args=False)
        app.extra["buffers"] = buffers
        try:
            yield
        finally:
            for fut in futures.values():
                if not fut.done():
                    fut.set_exception(
                        HTTPException(status_code=504, detail="Gateway Timeout")
                    )

    app = FastAPI(lifespan=lifespan)
    app.extra["buffers"] = buffers

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """健康检查端点。

        原理：返回固定 JSON 响应 {"status": "ok"}，供 Kubernetes、
        负载均衡器或监控系统探测服务存活状态。不检查依赖服务状态，
        仅表示进程存活且 FastAPI 路由可用。
        """
        return {"status": "ok"}

    @app.api_route(
        "/{connection_id}/{path_slug:path}",
        methods=list(get_args(Methods)),
    )
    async def handle_http_request(
        connection_id: str,
        request: Request,
        path_slug: str = "",
    ) -> Response:
        """处理 HTTP 请求，通过 WebSocket 隧道转发并等待响应。

        原理：
        1. 检查请求体大小，超过限制返回 413 错误
        2. 创建 correlation_id（唯一请求标识）和 Future 对象（用于等待响应）
        3. 使用 setdefault 原子操作获取或创建该 connection_id 的请求队列
        4. 将请求信息（方法、路径、查询参数、headers、base64编码的body）封装入队
        5. 等待 Future 完成（最长 300 秒），WebSocket 客户端收到请求后转发到目标，
           目标响应后 WebSocket 发回响应，由 receive() 函数匹配 Future 并设置结果
        6. 解码响应 body（HEAD 请求返回空 body），返回 HTTP Response

        队列满时返回 503，超时返回 504，WebSocket 断开返回 502。
        """
        settings: Settings = request.app.extra["settings"]
        correlation_id = uuid.uuid4()

        raw_body = await request.body()
        if len(raw_body) > settings.max_body_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Request body exceeds limit of {settings.max_body_bytes} bytes",
            )

        running_loop = asyncio.get_running_loop()
        future: asyncio.Future[BufferGateResponse] = running_loop.create_future()
        futures[correlation_id] = future

        queue = buffers.setdefault(
            connection_id, asyncio.Queue(maxsize=settings.max_queue_depth)
        )

        try:
            queue.put_nowait(
                BufferGateRequest(
                    correlation_id=correlation_id,
                    connection_id=connection_id,
                    method=cast(Methods, request.method),
                    url_path=path_slug,
                    url_query=orjson.dumps(
                        list(request.query_params.multi_items())
                    ).decode(),
                    headers=orjson.dumps(
                        {
                            **dict(request.headers),
                            "x-pipegate-correlation-id": correlation_id.hex,
                        }
                    ).decode(),
                    body=base64.b64encode(raw_body).decode(),
                )
            )
        except asyncio.QueueFull:
            futures.pop(correlation_id, None)
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
        """处理 WebSocket 连接，建立双向隧道通信。

        原理：
        1. 从查询参数获取 JWT token，验证有效性（签名、过期时间）
        2. 从 payload.sub 提取 connection_id，标识该隧道连接
        3. 接受 WebSocket 连接，获取或创建该 connection_id 的请求队列
        4. 启动两个并行协程：
           - receive()：监听 WebSocket 消息，匹配 correlation_id 设置 Future 结果
           - send()：从队列取出请求发送给 WebSocket 客户端
        5. 任一协程结束（断开/错误）时取消另一个，清理队列

        JWT 验证失败返回 1008 关闭码，connection_id 用于多隧道隔离。
        """
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

        connection_id = payload.sub

        await websocket.accept()
        logger.info("WebSocket connected: %s", connection_id)

        queue = buffers.setdefault(
            connection_id, asyncio.Queue(maxsize=settings.max_queue_depth)
        )

        async def receive() -> None:
            """接收 WebSocket 消息并匹配等待中的请求。

            原理：循环接收 WebSocket 文本消息，解析为 BufferGateResponse。
            通过 correlation_id 在 futures 字典中查找对应的 Future 对象。
            如果 Future 存在且未完成，设置结果完成等待中的 HTTP 请求。
            如果 Future 不存在或已完成（重复响应/超时已清理），记录警告。
            WebSocket 断开时退出循环，由外层清理。
            """
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
            """从队列获取请求并发送给 WebSocket 客户端。

            原理：循环从队列中获取等待发送的请求，序列化为 JSON 发送。
            如果 WebSocket 在发送时断开，说明隧道客户端已离线，
            将该请求的 Future 设置为 502 异常，让等待中的 HTTP 请求
            返回明确的错误而非无限等待。队列由 handle_http_request 入队。
            """
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
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        logger.info("WebSocket disconnected: %s", connection_id)
        buffers.pop(connection_id, None)

    return app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8000)
