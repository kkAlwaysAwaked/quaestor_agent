"""Agent 到 Retrieval 的短期服务凭据，与浏览器登录凭据使用不同密钥。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import jwt

from core.config import ServiceTokenSettings
from core.retrieval_contracts import RetrievalError


ISSUER = "agentic-rag-internal"
AUDIENCE = "retrieval"
SUBJECT = "agent-worker"


@dataclass(frozen=True)
class ServicePrincipal:
    user_id: str
    request_id: str


# 作用：为已从数据库领取的聊天任务签发绑定用户和请求的短期检索凭据。
def issue_retrieval_token(settings: ServiceTokenSettings, *, user_id: str, request_id: str) -> str:
    now = int(datetime.now(UTC).timestamp())
    return jwt.encode({
        "iss": ISSUER, "aud": AUDIENCE, "sub": SUBJECT, "scope": "retrieve",
        "user_id": str(UUID(user_id)), "request_id": str(UUID(request_id)),
        "iat": now, "exp": now + settings.token_ttl_seconds, "jti": str(uuid4()),
    }, settings.service_secret, algorithm="HS256")


# 作用：验证固定签名算法、签发者、接收方、有效期及请求绑定，拒绝不完整凭据。
def verify_retrieval_token(settings: ServiceTokenSettings, token: str) -> ServicePrincipal:
    try:
        claims = jwt.decode(
            token, settings.service_secret, algorithms=["HS256"],
            audience=AUDIENCE, issuer=ISSUER,
            options={"require": ["iss", "aud", "sub", "scope", "user_id", "request_id", "iat", "exp", "jti"]},
        )
        if claims["sub"] != SUBJECT or claims["scope"] != "retrieve":
            raise ValueError("invalid service purpose")
        if not isinstance(claims["iat"], int) or not isinstance(claims["exp"], int):
            raise ValueError("invalid token times")
        if not 0 < claims["exp"] - claims["iat"] <= settings.token_ttl_seconds:
            raise ValueError("invalid token lifetime")
        return ServicePrincipal(str(UUID(claims["user_id"])), str(UUID(claims["request_id"])))
    except (jwt.InvalidTokenError, ValueError, TypeError, AttributeError) as exc:
        raise RetrievalError(401, "invalid_service_token", "服务凭据无效或已过期") from exc
