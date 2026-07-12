from __future__ import annotations

import asyncio

import typer
import uvicorn

from .auth import generate_token
from .client import main as run_client
from .config import ConfigError, load_profile, to_ws_url
from .schemas import Settings
from .server import create_app

app = typer.Typer(help="PipeGate — self-hosted HTTP tunnel.")


@app.command("token")
def token_cmd(
    connection_id: str | None = typer.Option(
        None, "--connection-id", "-c", help="Pin a connection ID (overrides env var)."
    ),
) -> None:
    """Generate a JWT bearer token for a tunnel connection."""
    settings = Settings()
    result = generate_token(settings, connection_id)
    typer.echo(f"Connection-id: {result.connection_id}")
    typer.echo(f"JWT Bearer:    {result.bearer}")


@app.command("client")
def client_cmd(
    target_url: str = typer.Argument(..., help="Local server to forward requests to."),
    server_url: str = typer.Argument(
        ..., help="PipeGate server WebSocket URL (include ?token=…)"
    ),
) -> None:
    """Start the tunnel client."""
    asyncio.run(run_client(target_url, server_url))


@app.command("server")
def server_cmd(
    host: str = typer.Option("0.0.0.0", "--host", help="Host to bind to."),
    port: int = typer.Option(8000, "--port", "-p", help="Port to listen on."),
) -> None:
    """Start the PipeGate server."""
    uvicorn.run(create_app(), host=host, port=port)


@app.command("connect")
def connect_cmd(
    profile: str | None = typer.Argument(
        None, help="Profile name (default: 'default')."
    ),
    target: str | None = typer.Option(
        None, "--target", help="Override profile target URL."
    ),
    server: str | None = typer.Option(
        None, "--server", help="Override profile server URL."
    ),
    cid: str | None = typer.Option(
        None, "--cid", help="Override profile connection_id."
    ),
    secret: str | None = typer.Option(
        None,
        "--secret",
        help="Override profile JWT secret (appears in process list — prefer config).",
    ),
) -> None:
    """Start the tunnel via a config profile (one command, no copy-paste).

    Reads a profile from ~/.config/pipegate/config.toml or ./.pipegate.toml,
    signs a JWT locally with the profile's secret, and connects. The token
    never expires unless the profile sets ``ttl_days``.
    """
    try:
        prof = load_profile(profile)
    except ConfigError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=2) from e

    target_url = target or prof.target
    server_url = server or prof.server
    conn_id = cid or prof.cid
    secret_val = secret or prof.secret

    settings = Settings(
        jwt_secret=secret_val,
        jwt_ttl_days=prof.ttl_days,
    )
    token_result = generate_token(settings, conn_id)
    ws_url = to_ws_url(server_url, token_result.bearer)

    typer.echo(f"Connection-id: {token_result.connection_id}")
    typer.echo(f"Connecting to {server_url} ...")
    asyncio.run(run_client(target_url, ws_url))


if __name__ == "__main__":
    app()
