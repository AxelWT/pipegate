from __future__ import annotations

import asyncio
import base64
import sys

import httpx
import orjson
from websockets.asyncio.client import ClientConnection, connect

from .schemas import BufferGateRequest, BufferGateResponse

_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0

# httpx transparently decompresses gzip/br/deflate and de-chunks transfer
# encoding. These headers would misdescribe the already-decoded body, so
# they must be stripped before forwarding (otherwise the browser sees
# content-encoding: gzip on a plaintext body and fails with
# ERR_CONTENT_DECODING_FAILED).
_RESP_STRIP: frozenset[str] = frozenset(
    {
        "content-encoding",
        "content-length",
        "transfer-encoding",
    }
)


async def handle_request(
    target: str,
    request: BufferGateRequest,
    http_client: httpx.AsyncClient,
    ws_client: ClientConnection,
) -> None:
    try:
        response = await http_client.request(
            method=request.method,
            url=f"{target.rstrip('/')}/{request.url_path}",
            headers=orjson.loads(request.headers),
            params=orjson.loads(request.url_query),
            content=base64.b64decode(request.body) if request.body else b"",
        )
        payload = BufferGateResponse(
            correlation_id=request.correlation_id,
            headers=orjson.dumps(
                {
                    k: v
                    for k, v in response.headers.items()
                    if k.lower() not in _RESP_STRIP
                }
            ).decode(),
            body=base64.b64encode(response.content).decode(),
            status_code=response.status_code,
        )
    except Exception as e:
        print(
            f"Error processing request {request.correlation_id}: {e}",
            file=sys.stderr,
        )
        payload = BufferGateResponse(
            correlation_id=request.correlation_id,
            headers="{}",
            body="",
            status_code=504,
        )

    try:
        await ws_client.send(payload.model_dump_json())
    except Exception as e:
        print(
            f"Failed to send response for {request.correlation_id}: {e}",
            file=sys.stderr,
        )


async def main(target_url: str, server_url: str) -> None:
    attempt = 0

    while True:
        if attempt > 0:
            delay = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
            print(
                f"Reconnecting in {delay:.0f}s (attempt {attempt + 1})...",
                file=sys.stderr,
            )
            await asyncio.sleep(delay)

        print(f"Connecting to {server_url}...")

        try:
            async with (
                connect(server_url) as ws_client,
                httpx.AsyncClient() as http_client,
            ):
                print("Connected.")
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
                            print(f"Connection lost: {e}", file=sys.stderr)
                            break
        except asyncio.CancelledError:
            raise
        except (ConnectionRefusedError, OSError) as e:
            print(f"Connection failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)

        attempt += 1
