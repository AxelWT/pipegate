"""PipeGate 启动脚本：统一入口启动客户端或服务端。

使用方式：
    # 启动服务端
    python run_client.py server [--port 8000]

    # 启动客户端（自动生成 token）
    python run_client.py client http://localhost:3000 [--server ws://localhost:8000]

环境变量：
    PIPEGATE_JWT_SECRET: JWT 签名密钥（必须）
    PIPEGATE_JWT_ALGORITHMS: JWT 算法列表（必须，如 ["HS256"])
    PIPEGATE_TOKEN_EXPIRY_DAYS: Token 过期天数（可选，默认 36500 天）
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import jwt
import typer
import uvicorn

from pipegate.client import main as client_main
from pipegate.schemas import JWTPayload, Settings
from pipegate.server import create_app

app = typer.Typer(help="PipeGate 启动脚本")


@app.command()
def server(port: int = 8000) -> None:
    """启动 PipeGate 服务端。

    参数：
    - port：服务端监听端口，默认 8000
    """
    typer.echo(f"Starting PipeGate server on port {port}...")
    fastapi_app = create_app()
    uvicorn.run(fastapi_app, host="0.0.0.0", port=port)


@app.command()
def client(
    target_url: str,
    server_url: str = "ws://localhost:8000",
) -> None:
    """自动生成 token 并启动 PipeGate 客户端。

    参数：
    - target_url：本地服务的地址（如 http://localhost:3000）
    - server_url：PipeGate 服务器地址（如 ws://localhost:8000），不含 token
    """
    settings = Settings(_cli_parse_args=False)

    # 生成 connection_id 和 token
    connection_id = settings.connection_id or uuid.uuid4().hex
    jwt_payload = JWTPayload(
        sub=connection_id,
        exp=int((datetime.now(UTC) + timedelta(days=settings.token_expiry_days)).timestamp()),
    )
    token = jwt.encode(
        jwt_payload.model_dump(mode="json"),
        key=settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithms[0],
    )

    # 构建完整 URL
    full_url = f"{server_url}/?token={token}"

    typer.echo(f"Connection-id: {connection_id}")
    typer.echo(f"Target:        {target_url}")
    typer.echo(f"Server:        {server_url}")
    typer.echo(f"Token expiry:  {settings.token_expiry_days} days")

    # 启动客户端
    asyncio.run(client_main(target_url, full_url))


if __name__ == "__main__":
    app()