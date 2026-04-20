from __future__ import annotations

import asyncio
import base64
import re

import httpx
import orjson
import typer
from websockets.asyncio.client import ClientConnection, connect

from .schemas import BufferGateRequest, BufferGateResponse

app = typer.Typer()

_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0

# 匹配 HTML 中绝对路径的正则表达式
# 匹配 href="/xxx", src="/xxx", action="/xxx" 等属性
_ABSOLUTE_PATH_PATTERN = re.compile(
    r'''(href|src|action|data-src|data-href|poster|background|content)=["'](/[^"']*)["']''',
    re.IGNORECASE,
)


def rewrite_html_paths(html_content: str, connection_id: str) -> str:
    """重写 HTML 内容中的绝对路径。

    将 href="/static/style.css" 等绝对路径转换为
    href="/connection_id/static/style.css"，使静态文件能通过隧道正确加载。

    Args:
        html_content: HTML 内容字符串
        connection_id: 连接标识符

    Returns:
        重写后的 HTML 内容
    """
    def replace_path(match: re.Match[str]) -> str:
        attr = match.group(1)
        path = match.group(2)
        # 排除特殊路径：以 // 开头的协议相对URL、已经包含 connection_id 的路径
        if path.startswith("//") or path.startswith(f"/{connection_id}"):
            return match.group(0)
        return f'{attr}="/{connection_id}{path}"'

    return _ABSOLUTE_PATH_PATTERN.sub(replace_path, html_content)


@app.command()
def start_client(target_url: str, server_url: str) -> None:
    """启动 PipeGate 客户端的 CLI 入口。

    原理：使用 Typer CLI 框架，接收两个参数：
    - target_url：本地服务的地址（如 http://localhost:9000）
    - server_url：PipeGate 服务器的 WebSocket 地址

    调用 asyncio.run() 启动异步主循环，建立到服务器的 WebSocket 连接，
    并持续转发请求到本地服务。
    """
    asyncio.run(main(target_url, server_url))


async def handle_request(
    target: str,
    request: BufferGateRequest,
    http_client: httpx.AsyncClient,
    ws_client: ClientConnection,
) -> None:
    """处理从服务器收到的请求，转发到本地目标并返回响应。

    原理：
    1. 解析请求中的方法、路径、headers、查询参数和 base64 编码的 body
    2. 使用 httpx 将请求转发到本地目标服务（target + url_path）
    3. 将响应的 headers、body（base64编码）和状态码封装为 BufferGateResponse
    4. 如果响应是 HTML 内容，重写其中的绝对路径为包含 connection_id 的路径
    5. 通过 WebSocket 发回给 PipeGate 服务器，由 correlation_id 匹配原始请求

    异常处理：任何错误（连接失败、超时等）都返回 504 Gateway Timeout，
    确保服务器端的等待请求能收到明确的错误响应而非无限挂起。
    """
    try:
        response = await http_client.request(
            method=request.method,
            url=f"{target}/{request.url_path}",
            headers=orjson.loads(request.headers),
            params=orjson.loads(request.url_query),
            content=base64.b64decode(request.body) if request.body else b"",
        )

        # DEBUG: 打印请求和响应信息
        typer.echo(f"[DEBUG] Request: {request.method} {target}/{request.url_path}")
        typer.echo(f"[DEBUG] Response status: {response.status_code}")
        typer.echo(f"[DEBUG] Response content-type: {response.headers.get('content-type', 'N/A')}")
        typer.echo(f"[DEBUG] Response body length: {len(response.content)} bytes")
        if len(response.content) < 500:
            typer.echo(f"[DEBUG] Response body: {response.content[:500]}")

        # 处理响应 body：如果是 HTML 则重写路径
        response_body = response.content
        response_headers = dict(response.headers)
        content_type = response.headers.get("content-type", "")

        # httpx 会自动解压 gzip/deflate 内容，但响应头仍保留 Content-Encoding
        # 必须移除，否则浏览器会尝试再次解压导致 ERR_CONTENT_DECODING_FAILED
        response_headers.pop("content-encoding", None)

        if "text/html" in content_type or "application/xhtml+xml" in content_type:
            try:
                html_content = response_body.decode("utf-8")
                rewritten_html = rewrite_html_paths(html_content, request.connection_id)
                response_body = rewritten_html.encode("utf-8")
                # 移除 Content-Length 和 Transfer-Encoding，因为 body 大小已改变
                response_headers.pop("content-length", None)
                response_headers.pop("transfer-encoding", None)
            except UnicodeDecodeError:
                # 如果无法解码，保持原始内容不变
                pass

        payload = BufferGateResponse(
            correlation_id=request.correlation_id,
            headers=orjson.dumps(response_headers).decode(),
            body=base64.b64encode(response_body).decode(),
            status_code=response.status_code,
        )
    except Exception as e:
        typer.echo(f"Error processing request {request.correlation_id}: {e}", err=True)
        payload = BufferGateResponse(
            correlation_id=request.correlation_id,
            headers="{}",
            body="",
            status_code=504,
        )

    await ws_client.send(payload.model_dump_json())


async def main(target_url: str, server_url: str) -> None:
    """客户端主循环：管理 WebSocket 连接和请求转发。

    原理：
    1. 使用指数退避策略重连：初始 1 秒，每次失败翻倍，最大 60 秒
    2. 成功连接后重置退避计数器，进入请求转发循环
    3. 使用 TaskGroup 并发处理多个请求，每个请求独立异步处理
    4. 持续接收 WebSocket 消息，解析为 BufferGateRequest 并转发

    连接管理：ConnectionRefusedError/OSError 触发重连，
    其他异常也触发重连（防御性处理）。
    asyncio.CancelledError 直接退出（用户终止）。
    """
    attempt = 0

    while True:
        delay = min(_BACKOFF_BASE * (2**attempt), _BACKOFF_MAX)

        if attempt > 0:
            typer.echo(f"Reconnecting in {delay:.0f}s (attempt {attempt + 1})...")
            await asyncio.sleep(delay)

        typer.echo(f"Connecting to {server_url}...")

        try:
            async with (
                connect(server_url) as ws_client,
                httpx.AsyncClient() as http_client,
            ):
                typer.echo("Connected.")
                attempt = 0
                async with asyncio.TaskGroup() as tg:
                    while True:
                        try:
                            message = await ws_client.recv()
                            request = BufferGateRequest.model_validate_json(message)
                            tg.create_task(
                                handle_request(
                                    target_url, request, http_client, ws_client
                                )
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            typer.echo(f"Error receiving message: {e}", err=True)
        except asyncio.CancelledError:
            raise
        except (ConnectionRefusedError, OSError) as e:
            typer.echo(f"Connection failed: {e}", err=True)
        except Exception as e:
            typer.echo(f"Unexpected error: {e}", err=True)

        attempt += 1


if __name__ == "__main__":
    app()
