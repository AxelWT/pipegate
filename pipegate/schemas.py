from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
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
    """隧道请求模型，封装从服务器到客户端的请求信息。

    字段说明：
    - correlation_id：唯一请求标识符，用于匹配响应
    - url_path：请求路径（不含 connection_id）
    - url_query：查询参数，JSON 编码的键值对列表
    - method：HTTP 方法（GET/POST/PUT/DELETE/PATCH/OPTIONS/HEAD）
    - headers：请求头，JSON 编码的字典
    - body：请求体，base64 编码（支持二进制数据）
    """
    correlation_id: uuid.UUID
    url_path: str
    url_query: str
    method: Methods
    headers: str
    body: str


class BufferGateResponse(BaseModel):
    """隧道响应模型，封装从客户端到服务器的响应信息。

    字段说明：
    - correlation_id：与请求匹配的唯一标识符
    - headers：响应头，JSON 编码的字典
    - body：响应体，base64 编码（支持二进制数据）
    - status_code：HTTP 状态码
    """
    correlation_id: uuid.UUID
    headers: str
    body: str
    status_code: int


class JWTPayload(BaseModel):
    """JWT payload 模型，定义令牌中的有效数据。

    字段说明：
    - sub：主体标识，存储 connection_id，用于标识 WebSocket 连接
    - exp：过期时间戳（Unix epoch），用于验证令牌有效期
    """
    sub: str
    exp: int


class Settings(BaseSettings):
    """应用配置模型，从环境变量加载设置。

    配置项：
    - connection_id：可选的连接标识符（仅用于 CLI 生成令牌）
    - jwt_secret：JWT 签名密钥，必须配置
    - jwt_algorithms：允许的 JWT 算法列表，必须配置
    - max_body_bytes：请求体大小上限，默认 10MB
    - max_queue_depth：请求队列深度上限，默认 100
    - token_expiry_days：Token 过期天数，默认 36500（约 100 年，相当于不过期）

    使用 pydantic-settings 从环境变量自动加载，环境变量名通过 alias 定义。
    """
    model_config = SettingsConfigDict(cli_parse_args=False)
    connection_id: str | None = Field(alias="PIPEGATE_CONNECTION_ID", default=None)
    jwt_secret: SecretStr = Field(alias="PIPEGATE_JWT_SECRET")
    jwt_algorithms: list[str] = Field(alias="PIPEGATE_JWT_ALGORITHMS")
    max_body_bytes: int = Field(
        alias="PIPEGATE_MAX_BODY_BYTES",
        default=10 * 1024 * 1024,
    )
    max_queue_depth: int = Field(
        alias="PIPEGATE_MAX_QUEUE_DEPTH",
        default=100,
    )
    token_expiry_days: int = Field(
        alias="PIPEGATE_TOKEN_EXPIRY_DAYS",
        default=36500,  # 约 100 年，相当于不过期
    )
