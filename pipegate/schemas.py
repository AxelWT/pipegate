from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel, Field, RootModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

Methods = Literal[
    "GET",
    "POST",
    "PUT",
    "DELETE",
    "PATCH",
    "OPTIONS",
    "HEAD",
]


class BufferGateRequest(BaseModel):
    """Server -> client: an HTTP request to forward to the local service."""

    correlation_id: uuid.UUID
    url_path: str
    url_query: str
    method: Methods
    headers: str
    body: str


class CancelRequest(BaseModel):
    """Server -> client: caller disconnected; abort the in-flight upstream
    request identified by ``correlation_id``. Best-effort — if the queue is
    full the cancel is dropped (the upstream request will then terminate
    naturally on the next WS send failure or upstream completion)."""

    type: Literal["cancel"] = "cancel"
    correlation_id: uuid.UUID


class ResponseHeaders(BaseModel):
    """Client -> server: first message of a streaming response, carrying the
    status code and headers. Exactly one per correlation_id (unless an error
    pre-empts it, in which case only ``ResponseEnd`` with ``error`` is sent)."""

    type: Literal["headers"] = "headers"
    correlation_id: uuid.UUID
    status_code: int
    headers: str  # JSON: [[k, v], ...]


class ResponseChunk(BaseModel):
    """Client -> server: zero or more raw body bytes (base64-encoded) for the
    streaming response identified by ``correlation_id``."""

    type: Literal["chunk"] = "chunk"
    correlation_id: uuid.UUID
    body: str  # base64


class ResponseEnd(BaseModel):
    """Client -> server: terminal message for a correlation_id. ``error`` is
    None on a clean end-of-stream; a non-empty string signals an abnormal
    termination (upstream error, cancel, send failure) which the server maps
    to an appropriate status for the caller."""

    type: Literal["end"] = "end"
    correlation_id: uuid.UUID
    error: str | None = None


ResponseMessagePayload = ResponseHeaders | ResponseChunk | ResponseEnd
"""The discriminated-union payload carried by any client -> server WS frame.
Use this as the queue element type; use ``ResponseMessage`` for parsing."""

ResponseMessage = RootModel[ResponseMessagePayload]
"""Wrapper model used to parse a raw WS text frame into a
``ResponseMessagePayload`` via ``model_validate_json``; access ``.root`` to
get the typed payload."""

OutboundMessage = BufferGateRequest | CancelRequest
"""Server -> client WS frame: either a new HTTP request or a cancel for an
in-flight one. Items placed in the per-connection outbound queue."""


class BufferGateResponse(BaseModel):
    """Legacy single-message response. Retained only so existing tests that
    construct responses directly can be migrated incrementally. Not emitted
    by the client at runtime anymore."""

    correlation_id: uuid.UUID
    headers: str
    body: str
    status_code: int


class JWTPayload(BaseModel):
    sub: str  # connection_id
    exp: int | None = None  # expiry (unix); None = never expires
    nbf: int  # not before (unix)
    iat: int  # issued at (unix)
    iss: str  # issuer
    aud: str  # audience
    jti: str  # unique token id


class Settings(BaseSettings):
    model_config = SettingsConfigDict(cli_parse_args=False, populate_by_name=True)

    connection_id: str | None = Field(alias="PIPEGATE_CONNECTION_ID", default=None)
    jwt_secret: SecretStr = Field(alias="PIPEGATE_JWT_SECRET")
    jwt_algorithms: list[str] = Field(
        alias="PIPEGATE_JWT_ALGORITHMS", default=["HS256"], min_length=1
    )
    jwt_issuer: str = Field(alias="PIPEGATE_JWT_ISSUER", default="pipegate")
    jwt_audience: str = Field(alias="PIPEGATE_JWT_AUDIENCE", default="pipegate")
    jwt_ttl_days: int | None = Field(alias="PIPEGATE_JWT_TTL_DAYS", default=None)
    max_body_bytes: int = Field(
        alias="PIPEGATE_MAX_BODY_BYTES",
        default=10 * 1024 * 1024,
    )
    max_queue_depth: int = Field(
        alias="PIPEGATE_MAX_QUEUE_DEPTH",
        default=100,
    )
    base_domain: str | None = Field(alias="PIPEGATE_BASE_DOMAIN", default=None)
    stream_header_timeout: int = Field(
        alias="PIPEGATE_STREAM_HEADER_TIMEOUT",
        default=300,
        description=(
            "Seconds to wait for the first response frame (ResponseHeaders or "
            "early ResponseEnd) from the tunnel client before returning 504 to "
            "the caller. 0 disables the timeout (wait forever). Long-task "
            "upstreams (Agent services that may think for many minutes before "
            "emitting any bytes) should raise this or set it to 0."
        ),
    )
    stream_idle_timeout: int = Field(
        alias="PIPEGATE_STREAM_IDLE_TIMEOUT",
        default=300,
        description=(
            "Seconds of idle (no chunks) allowed between response frames before "
            "the stream is terminated and a CancelRequest is sent to the tunnel "
            "client. 0 disables the timeout. SSE/Agent streams with long gaps "
            "between events should raise this or set it to 0."
        ),
    )
