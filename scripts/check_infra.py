"""Read-only checks for the four services; run with python -m scripts.check_infra."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from core.config import ConfigError, InfrastructureSettings, load_infrastructure_settings
from infra.mysql.health import check_mysql
from infra.qdrant import check_qdrant
from infra.rabbitmq import check_rabbitmq


# 作用：连接 Redis 并通过 PING 验证服务可用性。
def check_redis(settings: InfrastructureSettings) -> None:
    import redis

    client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    try:
        if not client.ping():
            raise RuntimeError("Redis did not respond to PING")
    finally:
        client.close()


# 作用：依次检查四项基础设施服务并返回整体检查结果。
def main() -> int:
    try:
        settings = load_infrastructure_settings()
    except ConfigError as exc:
        print(f"Configuration: {exc}")
        return 2

    checks: tuple[tuple[str, Callable[[], None]], ...] = (
        ("MySQL", lambda: check_mysql(settings)),
        ("Redis", lambda: check_redis(settings)),
        ("RabbitMQ", lambda: asyncio.run(check_rabbitmq(settings))),
        ("Qdrant", lambda: check_qdrant(settings)),
    )
    failed = False
    for name, check in checks:
        try:
            check()
            print(f"[OK] {name}")
        except Exception as exc:
            failed = True
            print(f"[FAIL] {name}: {type(exc).__name__}: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
