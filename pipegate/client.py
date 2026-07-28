from __future__ import annotations

import asyncio
import base64
import contextlib
import sys
import uuid
from typing import TypedDict

import httpx
import orjson
from websockets.asyncio.client import ClientConnection, connect

from .schemas import (
    BufferGateRequest,
    CancelRequest,
    ResponseChunk,
    ResponseEnd,
    ResponseHeaders,
)

_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0

# httpx default timeout is Timeout(5.0) — i.e. a 5s *read* timeout applied to
# every chunk. SSE/Agent streams legitimately go silent for tens of seconds
# (model reasoning, tool calls, sub-agent delegation) between events; under
# the default, httpx raises ReadTimeout mid-stream, handle_request emits
# ResponseEnd(error="read timed out"), and the caller sees the stream cut off
# abruptly. Disable the read timeout (None) so SSE idle gaps are tolerated;
# keep tight connect/write/pool timeouts so dead upstreams are still detected.
# The server-side stream_idle_timeout remains the ultimate backstop.
_HTTP_TIMEOUT: httpx.Timeout = httpx.Timeout(
    connect=10.0, read=None, write=10.0, pool=10.0
)


# Explicit WebSocket keepalive. websockets' defaults (ping_interval=20,
# ping_timeout=20) are reasonable, but when pipegate server sits behind a
# reverse proxy (nginx/Cloudflare/ALB/Caddy) the proxy's own idle timeout is
# often shorter than 20s and silently drops the WS — surfacing as a stream
# cut off mid-task. Declaring these explicitly makes the intent visible and
# gives operators one obvious knob to align with proxy_read_timeout.
# max_size=None lifts the default 1MiB frame cap so large base64-encoded
# upstream bodies (Agent responses, file outputs) aren't rejected.
#
# TypedDict (rather than dict[str, object]) so mypy can verify the splat
# against connect()'s signature — guards against typos / renamed params.
class _WSConnectKwargs(TypedDict):
    ping_interval: float
    ping_timeout: float
    max_size: int | None
    close_timeout: float


_WS_KWARGS: _WSConnectKwargs = {
    "ping_interval": 20,
    "ping_timeout": 20,
    "max_size": None,
    "close_timeout": 10,
}

# Indirection so tests can patch the sleep used by main()'s reconnect loop
# without disturbing the event loop's own asyncio.sleep (which would break
# asyncio internals — asyncio.sleep is a module-level attribute, so patching
# pipegate.client.asyncio.sleep actually mutates the global).
_asyncio_sleep = asyncio.sleep

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


async def _send(ws_client: ClientConnection, payload: str) -> bool:
    """Best-effort send. Returns True on success, False on failure (the WS
    is gone; the enclosing TaskGroup will tear down on the next recv)."""
    try:
        await ws_client.send(payload)
        return True
    except Exception as e:
        print(f"Failed to send response message: {e}", file=sys.stderr)
        return False


async def handle_request(
    target: str,
    request: BufferGateRequest,
    http_client: httpx.AsyncClient,
    ws_client: ClientConnection,
    cancel_event: asyncio.Event,
) -> None:
    """Forward ``request`` to ``target`` and stream the response back over
    ``ws_client`` as ``ResponseHeaders`` -> 0..N ``ResponseChunk`` ->
    ``ResponseEnd``.

    If ``cancel_event`` is set (caller disconnected mid-stream), the upstream
    httpx stream is ``aclose()``-d so the local service (e.g. LangGraph)
    observes client-side disconnect and aborts any active run instead of
    leaving it dangling for the next request to collide with as a 409.
    """
    cid = request.correlation_id

    try:
        async with http_client.stream(
            method=request.method,
            url=f"{target.rstrip('/')}/{request.url_path}",
            headers=orjson.loads(request.headers),
            params=orjson.loads(request.url_query),
            content=base64.b64decode(request.body) if request.body else b"",
        ) as response:
            headers_json = orjson.dumps(
                [
                    [k, v]
                    for k, v in response.headers.multi_items()
                    if k.lower() not in _RESP_STRIP
                ]
            ).decode()
            if not await _send(
                ws_client,
                ResponseHeaders(
                    correlation_id=cid,
                    status_code=response.status_code,
                    headers=headers_json,
                ).model_dump_json(),
            ):
                return

            # Watcher: when the server signals caller-disconnect, close the
            # upstream stream. aiter_bytes() will then raise, breaking the loop.
            async def _watch_cancel() -> None:
                await cancel_event.wait()
                try:
                    await response.aclose()
                except Exception as e:
                    print(
                        f"Failed to close upstream stream on cancel: {e}",
                        file=sys.stderr,
                    )

            watcher = asyncio.create_task(_watch_cancel())
            stream_error: str | None = None
            try:
                async for chunk in response.aiter_bytes():
                    if not chunk:
                        continue
                    if not await _send(
                        ws_client,
                        ResponseChunk(
                            correlation_id=cid,
                            body=base64.b64encode(chunk).decode(),
                        ).model_dump_json(),
                    ):
                        return
            except Exception as e:
                # aiter_bytes() raised — either because the watcher closed the
                # stream (cancel) or a genuine upstream/network error.
                if cancel_event.is_set():
                    stream_error = "cancelled"
                else:
                    stream_error = str(e) or "stream error"
            finally:
                if not watcher.done():
                    watcher.cancel()
                # Retrieve any pending exception to avoid
                # "Task exception was never retrieved" warnings.
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await watcher

            await _send(
                ws_client,
                ResponseEnd(
                    correlation_id=cid,
                    error=stream_error,
                ).model_dump_json(),
            )
    except Exception as e:
        print(f"Error processing request {cid}: {e}", file=sys.stderr)
        # Error before headers were sent — emit ResponseEnd with error so
        # the server can map it to a 502 for the caller.
        await _send(
            ws_client,
            ResponseEnd(
                correlation_id=cid, error=str(e) or "upstream error"
            ).model_dump_json(),
        )


def _decode_outbound(raw: str) -> BufferGateRequest | CancelRequest | None:
    """Parse a WS text frame from the server into either a request or a
    cancel signal. Returns None on unparseable input (logged + skipped)."""
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as e:
        print(f"Malformed outbound message: {e}", file=sys.stderr)
        return None
    if not isinstance(data, dict):
        print(f"Outbound message is not a JSON object: {data!r}", file=sys.stderr)
        return None
    msg_type = data.get("type")
    if msg_type == "cancel":
        try:
            return CancelRequest.model_validate(data)
        except Exception as e:
            print(f"Invalid cancel message: {e}", file=sys.stderr)
            return None
    # BufferGateRequest has no `type` discriminator — validate by shape.
    try:
        return BufferGateRequest.model_validate(data)
    except Exception as e:
        print(f"Invalid request message: {e}", file=sys.stderr)
        return None


async def main(target_url: str, server_url: str) -> None:
    attempt = 0
    inflight: dict[uuid.UUID, asyncio.Event] = {}

    while True:
        if attempt > 0:
            delay = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
            print(
                f"Reconnecting in {delay:.0f}s (attempt {attempt + 1})...",
                file=sys.stderr,
            )
            await _asyncio_sleep(delay)

        print(f"Connecting to {server_url}...")

        try:
            async with (
                connect(server_url, **_WS_KWARGS) as ws_client,
                httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as http_client,
            ):
                print("Connected.")
                attempt = 0
                async with asyncio.TaskGroup() as tg:
                    while True:
                        try:
                            message = await ws_client.recv()
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            print(f"Connection lost: {e}", file=sys.stderr)
                            break

                        if isinstance(message, (bytes, bytearray)):
                            message = message.decode("utf-8", "replace")

                        parsed = _decode_outbound(message)
                        if parsed is None:
                            continue

                        if isinstance(parsed, CancelRequest):
                            ev = inflight.get(parsed.correlation_id)
                            if ev is not None:
                                ev.set()
                            else:
                                print(
                                    f"Cancel for unknown cid {parsed.correlation_id}",
                                    file=sys.stderr,
                                )
                            continue

                        # parsed is now narrowed to BufferGateRequest.
                        request_msg: BufferGateRequest = parsed
                        cancel_event = asyncio.Event()
                        inflight[request_msg.correlation_id] = cancel_event

                        async def _run(
                            req: BufferGateRequest,
                            ev: asyncio.Event,
                        ) -> None:
                            try:
                                await handle_request(
                                    target_url, req, http_client, ws_client, ev
                                )
                            finally:
                                inflight.pop(req.correlation_id, None)

                        tg.create_task(_run(request_msg, cancel_event))
        except asyncio.CancelledError:
            raise
        except (ConnectionRefusedError, OSError) as e:
            print(f"Connection failed: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)

        attempt += 1
