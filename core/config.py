"""Read process-specific settings without connecting to services at import time."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ConfigError(ValueError):
    """A required setting is missing or invalid."""


# 作用：读取项目根目录的 .env 文件并解析为配置字典。
def _dotenv_values() -> dict[str, str]:
    path = PROJECT_ROOT / ".env"
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


# 作用：按环境变量、.env、默认值的优先级读取单项配置。
def setting(name: str, default: str | None = None) -> str | None:
    """Environment variables override the local .env file."""
    if name in os.environ:
        return os.environ[name]
    return _dotenv_values().get(name, default)


# 作用：读取必填配置，并在缺失或为空时抛出配置错误。
def _required(name: str) -> str:
    value = setting(name)
    if not value or not value.strip():
        raise ConfigError(f"Missing required setting: {name}")
    return value


# 作用：读取并校验端口配置，确保其处于有效端口范围内。
def _port(name: str, default: int) -> int:
    value = setting(name, str(default))
    try:
        port = int(value or "")
    except ValueError as exc:
        raise ConfigError(f"{name} must be a valid port") from exc
    if not 1 <= port <= 65535:
        raise ConfigError(f"{name} must be between 1 and 65535")
    return port


@dataclass(frozen=True)
class InfrastructureSettings:
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_user: str
    mysql_password: str
    redis_host: str
    redis_port: int
    redis_password: str
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_management_port: int
    rabbitmq_user: str
    rabbitmq_password: str
    qdrant_url: str
    qdrant_api_key: str


@dataclass(frozen=True)
class RabbitRuntimeSettings:
    host: str
    port: int
    user: str
    password: str


@dataclass(frozen=True)
class QdrantRuntimeSettings:
    url: str
    api_key: str


# 作用：仅加载 RabbitMQ 消费者和发布者实际需要的连接配置。
def load_rabbit_runtime_settings() -> RabbitRuntimeSettings:
    return RabbitRuntimeSettings(
        host=setting("RABBITMQ_HOST", "127.0.0.1") or "127.0.0.1",
        port=_port("RABBITMQ_PORT", 5673),
        user=_required("RABBITMQ_USER"),
        password=_required("RABBITMQ_PASSWORD"),
    )


# 作用：仅加载 Qdrant Server 客户端需要的地址与 API Key。
def load_qdrant_runtime_settings() -> QdrantRuntimeSettings:
    host = setting("QDRANT_HOST", "127.0.0.1") or "127.0.0.1"
    port = _port("QDRANT_PORT", 6333)
    return QdrantRuntimeSettings(
        url=f"http://{host}:{port}", api_key=_required("QDRANT_API_KEY")
    )


# 数据库运行时配置独立加载，避免使用 MySQL 时强制要求其他中间件密钥。
@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int
    max_overflow: int
    pool_recycle_seconds: int


@dataclass(frozen=True)
class TaskLeaseSettings:
    lease_seconds: int
    recovery_batch_size: int
    outbox_batch_size: int


# 作用：读取非负整数配置，并拒绝无法转换或低于下限的值。
def _nonnegative_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = setting(name, str(default))
    try:
        value = int(raw or "")
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be at least {minimum}")
    return value


# 作用：仅加载 MySQL 运行时连接与连接池配置。
def load_database_settings() -> DatabaseSettings:
    return DatabaseSettings(
        host=setting("MYSQL_HOST", "127.0.0.1") or "127.0.0.1",
        port=_port("MYSQL_PORT", 3307),
        database=_required("MYSQL_DATABASE"),
        user=_required("MYSQL_USER"),
        password=_required("MYSQL_PASSWORD"),
        pool_size=_nonnegative_int("MYSQL_POOL_SIZE", 5, minimum=1),
        max_overflow=_nonnegative_int("MYSQL_MAX_OVERFLOW", 5),
        pool_recycle_seconds=_nonnegative_int("MYSQL_POOL_RECYCLE_SECONDS", 1800, minimum=1),
    )


# 作用：读取任务租约以及数据库批量领取的基础配置。
def load_task_lease_settings() -> TaskLeaseSettings:
    return TaskLeaseSettings(
        lease_seconds=_nonnegative_int("TASK_LEASE_SECONDS", 120, minimum=1),
        recovery_batch_size=_nonnegative_int("TASK_RECOVERY_BATCH_SIZE", 50, minimum=1),
        outbox_batch_size=_nonnegative_int("OUTBOX_BATCH_SIZE", 50, minimum=1),
    )


# 作用：加载并校验 MySQL、Redis、RabbitMQ 和 Qdrant 的连接配置。
def load_infrastructure_settings() -> InfrastructureSettings:
    """Validate infrastructure settings when a bootstrap or service starts."""
    qdrant_host = setting("QDRANT_HOST", "127.0.0.1")
    qdrant_port = _port("QDRANT_PORT", 6333)
    return InfrastructureSettings(
        mysql_host=setting("MYSQL_HOST", "127.0.0.1") or "127.0.0.1",
        mysql_port=_port("MYSQL_PORT", 3307),
        mysql_database=_required("MYSQL_DATABASE"),
        mysql_user=_required("MYSQL_USER"),
        mysql_password=_required("MYSQL_PASSWORD"),
        redis_host=setting("REDIS_HOST", "127.0.0.1") or "127.0.0.1",
        redis_port=_port("REDIS_PORT", 6379),
        redis_password=_required("REDIS_PASSWORD"),
        rabbitmq_host=setting("RABBITMQ_HOST", "127.0.0.1") or "127.0.0.1",
        rabbitmq_port=_port("RABBITMQ_PORT", 5673),
        rabbitmq_management_port=_port("RABBITMQ_MANAGEMENT_PORT", 15673),
        rabbitmq_user=_required("RABBITMQ_USER"),
        rabbitmq_password=_required("RABBITMQ_PASSWORD"),
        qdrant_url=f"http://{qdrant_host}:{qdrant_port}",
        qdrant_api_key=_required("QDRANT_API_KEY"),
    )


@dataclass(frozen=True)
class ModelSettings:
    api_key: str
    base_url: str


@dataclass(frozen=True)
class ServiceTokenSettings:
    service_secret: str
    token_ttl_seconds: int = 120


@dataclass(frozen=True)
class RetrievalSettings(ServiceTokenSettings):
    request_timeout_seconds: int = 60
    run_lease_seconds: int = 90
    max_inflight: int = 8
    pipeline_concurrency: int = 2
    query_mode: str = "llm"
    qdrant_timeout_seconds: int = 10


@dataclass(frozen=True)
class RedisRuntimeSettings:
    host: str
    port: int
    password: str


@dataclass(frozen=True)
class AgentSettings:
    model_name: str = "deepseek-v4-flash"
    thinking_mode: str = "disabled"
    concurrency: int = 2
    prefetch_count: int = 2
    max_attempts: int = 3
    max_steps: int = 5
    model_max_tokens: int = 4096
    model_timeout_seconds: int = 60
    generation_timeout_seconds: int = 180
    retrieval_url: str = "http://127.0.0.1:8001"
    retrieval_timeout_seconds: int = 70
    max_output_chars: int = 20000
    active_stream_ttl_seconds: int = 3600
    terminal_stream_ttl_seconds: int = 86400
    io_timeout_seconds: int = 5
    database_timeout_seconds: int = 10
    shutdown_grace_seconds: int = 20


# 作用：独立读取 Redis 运行时配置，避免 Agent 被迫提供 Qdrant 连接参数。
def load_redis_runtime_settings() -> RedisRuntimeSettings:
    return RedisRuntimeSettings(
        host=setting("REDIS_HOST", "127.0.0.1") or "127.0.0.1",
        port=_port("REDIS_PORT", 6379), password=_required("REDIS_PASSWORD"),
    )


# 作用：读取 Agent 的模型、消费并发、生成时限和事件保留配置，并约束恢复时间窗口。
def load_agent_settings() -> AgentSettings:
    concurrency = _nonnegative_int("AGENT_CONCURRENCY", 2, minimum=1)
    prefetch = _nonnegative_int("AGENT_PREFETCH_COUNT", concurrency, minimum=1)
    if concurrency > 2 or prefetch > concurrency:
        raise ConfigError("first release requires AGENT_CONCURRENCY <= 2 and PREFETCH_COUNT <= concurrency")
    thinking = setting("AGENT_THINKING_MODE", "disabled")
    if thinking not in ("enabled", "disabled"):
        raise ConfigError("AGENT_THINKING_MODE must be enabled or disabled")
    duration = _nonnegative_int("AGENT_GENERATION_TIMEOUT_SECONDS", 180, minimum=1)
    if duration > 900:
        raise ConfigError("AGENT_GENERATION_TIMEOUT_SECONDS must be <= 900 to leave room before RabbitMQ acknowledgement timeout")
    active_ttl = _nonnegative_int("CHAT_ACTIVE_TTL_SECONDS", 3600, minimum=1)
    lease = load_task_lease_settings()
    if lease.lease_seconds < 30:
        raise ConfigError("TASK_LEASE_SECONDS must be at least 30 for Agent heartbeat and Outbox publishing")
    if active_ttl < duration + lease.lease_seconds + 360:
        raise ConfigError("CHAT_ACTIVE_TTL_SECONDS must cover generation, lease recovery and retry delay")
    retrieval_url = setting("RETRIEVAL_BASE_URL", "http://127.0.0.1:8001") or ""
    if not retrieval_url.startswith(("http://", "https://")):
        raise ConfigError("RETRIEVAL_BASE_URL must be an HTTP URL")
    return AgentSettings(
        model_name=setting("AGENT_MODEL_NAME", "deepseek-v4-flash") or "deepseek-v4-flash",
        thinking_mode=thinking, concurrency=concurrency, prefetch_count=prefetch,
        max_attempts=_nonnegative_int("AGENT_MAX_ATTEMPTS", 3, minimum=1),
        max_steps=_nonnegative_int("AGENT_MAX_STEPS", 5, minimum=1),
        model_max_tokens=_nonnegative_int("AGENT_MODEL_MAX_TOKENS", 4096, minimum=1),
        model_timeout_seconds=_nonnegative_int("AGENT_MODEL_TIMEOUT_SECONDS", 60, minimum=1),
        generation_timeout_seconds=duration, retrieval_url=retrieval_url.rstrip("/"),
        retrieval_timeout_seconds=_nonnegative_int("AGENT_RETRIEVAL_TIMEOUT_SECONDS", 70, minimum=1),
        max_output_chars=_nonnegative_int("AGENT_MAX_OUTPUT_CHARS", 20000, minimum=1),
        active_stream_ttl_seconds=active_ttl,
        terminal_stream_ttl_seconds=_nonnegative_int("CHAT_TERMINAL_TTL_SECONDS", 86400, minimum=1),
        shutdown_grace_seconds=_nonnegative_int("AGENT_SHUTDOWN_GRACE_SECONDS", 20, minimum=1),
    )


# 作用：加载检索服务的认证、超时和并发配置，并确保租约覆盖请求时限。
def load_retrieval_settings() -> RetrievalSettings:
    credentials = load_retrieval_token_settings()
    timeout = _nonnegative_int("RETRIEVAL_TIMEOUT_SECONDS", 60, minimum=1)
    lease = _nonnegative_int("RETRIEVAL_RUN_LEASE_SECONDS", 90, minimum=1)
    if lease < timeout + 10:
        raise ConfigError("RETRIEVAL_RUN_LEASE_SECONDS must exceed timeout by at least 10 seconds")
    mode = setting("RETRIEVAL_QUERY_MODE", "llm")
    if mode not in ("llm", "fixed"):
        raise ConfigError("RETRIEVAL_QUERY_MODE must be llm or fixed")
    return RetrievalSettings(
        service_secret=credentials.service_secret, token_ttl_seconds=credentials.token_ttl_seconds,
        request_timeout_seconds=timeout, run_lease_seconds=lease,
        max_inflight=_nonnegative_int("RETRIEVAL_MAX_INFLIGHT", 8, minimum=1),
        pipeline_concurrency=_nonnegative_int("RETRIEVAL_PIPELINE_CONCURRENCY", 2, minimum=1),
        query_mode=mode,
        qdrant_timeout_seconds=_nonnegative_int("RETRIEVAL_QDRANT_TIMEOUT_SECONDS", 10, minimum=1),
    )


# 作用：只读取 Agent 与 Retrieval 共用的内部签名凭据，不校验检索服务自身的运行参数。
def load_retrieval_token_settings() -> ServiceTokenSettings:
    secret = _required("RETRIEVAL_SERVICE_SECRET")
    if len(secret.encode("utf-8")) < 32 or secret.startswith("replace_"):
        raise ConfigError("RETRIEVAL_SERVICE_SECRET must be a dedicated secret of at least 32 bytes")
    return ServiceTokenSettings(
        service_secret=secret,
        token_ttl_seconds=_nonnegative_int("RETRIEVAL_TOKEN_TTL_SECONDS", 120, minimum=1),
    )


# 作用：加载 Agent 与检索服务使用的模型 API 配置。
def load_model_settings() -> ModelSettings:
    """Agent and Retrieval call this at startup; Ingest does not need it."""
    return ModelSettings(
        api_key=_required("DEEPSEEK_API_KEY"),
        base_url=setting("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        or "https://api.deepseek.com",
    )


# 作用：解析模型缓存目录，并将相对路径转换为项目内的绝对路径。
def model_cache_dir() -> Path:
    value = setting("MODEL_CACHE_DIR")
    if not value:
        return PROJECT_ROOT / "models"
    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


# 作用：解析上传文件目录，并将相对路径转换为项目内的绝对路径。
def upload_root() -> Path:
    value = setting("UPLOAD_ROOT")
    if not value:
        return PROJECT_ROOT / "uploads"
    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()
