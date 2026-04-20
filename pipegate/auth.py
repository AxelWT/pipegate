from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import jwt

from .schemas import JWTPayload, Settings


def verify_token(token: str, settings: Settings) -> JWTPayload:
    """解码并验证 JWT 令牌。

    原理：使用配置的密钥和算法解码 JWT，验证签名有效性，
    确保令牌未被篡改且未过期。验证成功后返回 payload，
    其中 ``sub`` 字段包含 connection ID，用于标识 WebSocket 连接。
    """
    decoded = jwt.decode(
        token,
        settings.jwt_secret.get_secret_value(),
        algorithms=settings.jwt_algorithms,
    )
    return JWTPayload.model_validate(decoded)


def make_jwt_bearer() -> None:
    """CLI 工具：生成 connection ID 和 JWT 令牌。

    原理：创建一个包含 connection ID（存储在 ``sub`` 字段）和
    过期时间（存储在 ``exp`` 字段）的 payload，使用配置的密钥
    进行签名生成 JWT。主要用于测试、调试或手动创建隧道凭证。
    """
    settings = Settings(_cli_parse_args=True)  # #10: only here do we want CLI parsing
    connection_id = settings.connection_id or uuid.uuid4().hex

    jwt_payload = JWTPayload(
        sub=connection_id,
        exp=int((datetime.now(UTC) + timedelta(days=settings.token_expiry_days)).timestamp()),
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
