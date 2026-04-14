from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import jwt

from .schemas import JWTPayload, Settings


def verify_token(token: str, settings: Settings) -> JWTPayload:
    """解码并验证 JWT 令牌，返回 payload（connection ID 存储在 ``sub`` 字段中）。"""
    decoded = jwt.decode(
        token,
        settings.jwt_secret.get_secret_value(),
        algorithms=settings.jwt_algorithms,
    )
    return JWTPayload.model_validate(decoded)


def make_jwt_bearer() -> None:
    """CLI 工具：生成 connection ID 和 JWT 令牌。"""
    settings = Settings(_cli_parse_args=True)  # #10: only here do we want CLI parsing
    connection_id = settings.connection_id or uuid.uuid4().hex

    jwt_payload = JWTPayload(
        sub=connection_id,
        exp=int((datetime.now(UTC) + timedelta(days=21)).timestamp()),
    )

    jwt_bearer = jwt.encode(
        jwt_payload.model_dump(mode="json"),
        key=settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithms[0],
    )

    print(f"Connection-id: {connection_id}")
    print(f"JWT Bearer:    {jwt_bearer}")


if __name__ == "__main__":
    make_jwt_bearer()
