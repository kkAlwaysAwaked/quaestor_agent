"""Initialize RabbitMQ and Qdrant after Compose services are healthy.

Run from the project root: python -m scripts.bootstrap_infra
"""

from __future__ import annotations

import asyncio
import time

from aio_pika.exceptions import AMQPConnectionError

from core.config import ConfigError, load_infrastructure_settings
from infra.qdrant import bootstrap_qdrant
from infra.rabbitmq import bootstrap_rabbitmq


# 作用：加载配置并重试初始化 RabbitMQ 拓扑与 Qdrant 集合。
def main() -> int:
    try:
        settings = load_infrastructure_settings()
    except ConfigError as exc:
        print(f"Configuration: {exc}")
        return 2

    for attempt in range(1, 16):
        try:
            asyncio.run(bootstrap_rabbitmq(settings))
            bootstrap_qdrant(settings)
            print("RabbitMQ topology and Qdrant collection are ready.")
            return 0
        except (AMQPConnectionError, ConnectionError, OSError, TimeoutError) as exc:
            if attempt == 15:
                print(f"Infrastructure unavailable after {attempt} attempts: {exc}")
                return 1
            time.sleep(2)
        except Exception as exc:
            print(f"Infrastructure initialization failed: {type(exc).__name__}: {exc}")
            return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
