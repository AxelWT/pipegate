from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import jwt

from .schemas import JWTPayload, Settings


class TokenResult(NamedTuple):
    connection_id: str
    bearer: str


def generate_token(settings: Settings, connection_id: str | None = None) -> TokenResult:
    """Create a connection ID and signed JWT.

    When ``settings.jwt_ttl_days`` is ``None`` (the default), the token never
    expires — the ``exp`` claim is omitted entirely. pyjwt only enforces
    expiry when ``exp`` is present, so such tokens remain valid indefinitely.
    """
    cid = connection_id or settings.connection_id or uuid.uuid4().hex
    now = datetime.now(UTC)
    ts = int(now.timestamp())
    exp = (
        int((now + timedelta(days=settings.jwt_ttl_days)).timestamp())
        if settings.jwt_ttl_days is not None
        else None
    )
    payload = JWTPayload(
        sub=cid,
        exp=exp,
        nbf=ts,
        iat=ts,
        iss=settings.jwt_issuer,
        aud=settings.jwt_audience,
        jti=uuid.uuid4().hex,
    )
    token = jwt.encode(
        payload.model_dump(mode="json", exclude_none=True),
        key=settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithms[0],
    )
    return TokenResult(connection_id=cid, bearer=token)


def verify_token(token: str, settings: Settings) -> JWTPayload:
    """Decode and verify a JWT, validating all standard claims."""
    decoded = jwt.decode(
        token,
        settings.jwt_secret.get_secret_value(),
        algorithms=settings.jwt_algorithms,
        audience=settings.jwt_audience,
        issuer=settings.jwt_issuer,
    )
    return JWTPayload.model_validate(decoded)
