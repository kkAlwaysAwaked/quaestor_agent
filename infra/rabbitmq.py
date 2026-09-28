"""Declare or inspect RabbitMQ topology; never publish business jobs here."""

from __future__ import annotations

import base64
import json
from urllib.parse import quote
from urllib.request import Request, urlopen

import aio_pika

from core.config import InfrastructureSettings
from infra import topology as t


# 作用：使用基础设施配置建立 RabbitMQ 的可靠 AMQP 连接。AMQP 是消息通信协议。
async def _connect(settings: InfrastructureSettings) -> aio_pika.RobustConnection:
    return await aio_pika.connect_robust(
        host=settings.rabbitmq_host,
        port=settings.rabbitmq_port,
        login=settings.rabbitmq_user,
        password=settings.rabbitmq_password,
        virtualhost="/",
        timeout=5,
    )

# 作用：声明业务交换机、工作队列及其死信队列和路由绑定。
async def bootstrap_rabbitmq(settings: InfrastructureSettings) -> None:
    connection = await _connect(settings)
    try:
        channel = await connection.channel()
        jobs = await channel.declare_exchange(
            t.JOBS_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
        )
        dead = await channel.declare_exchange(
            t.DEAD_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
        )
        for live_name, dead_name in (
            (t.CHAT_QUEUE, t.CHAT_DEAD_QUEUE),
            (t.INGEST_QUEUE, t.INGEST_DEAD_QUEUE),
        ):
            dead_queue = await channel.declare_queue(dead_name, durable=True)
            await dead_queue.bind(dead, routing_key=dead_name)
            live_queue = await channel.declare_queue(
                live_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": t.DEAD_EXCHANGE,
                    "x-dead-letter-routing-key": dead_name,
                },
            )
            await live_queue.bind(jobs, routing_key=live_name)
    finally:
        await connection.close()


# 作用：检查 RabbitMQ 拓扑是否已创建且配置符合预期。
async def check_rabbitmq(settings: InfrastructureSettings) -> None:
    """Verify existence via AMQP and arguments/bindings via management API."""
    connection = await _connect(settings)
    try:
        channel = await connection.channel()
        for name in (t.JOBS_EXCHANGE, t.DEAD_EXCHANGE):
            await channel.declare_exchange(
                name, aio_pika.ExchangeType.DIRECT, passive=True
            )
        for name in (
            t.CHAT_QUEUE,
            t.INGEST_QUEUE,
            t.CHAT_DEAD_QUEUE,
            t.INGEST_DEAD_QUEUE,
        ):
            await channel.declare_queue(name, passive=True)
    finally:
        await connection.close()
    _check_topology_api(settings)


# 作用：通过管理 API 核对交换机、队列及绑定的详细属性。
def _check_topology_api(settings: InfrastructureSettings) -> None:
    credentials = f"{settings.rabbitmq_user}:{settings.rabbitmq_password}"
    token = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
    base = f"http://{settings.rabbitmq_host}:{settings.rabbitmq_management_port}/api"
    vhost = quote("/", safe="")

    # 作用：携带管理接口认证信息获取指定路径的 JSON 数据。
    def fetch(path: str) -> object:
        request = Request(
            base + path,
            headers={"Authorization": f"Basic {token}"},
        )
        with urlopen(request, timeout=5) as response:
            return json.load(response)

    for name in (t.JOBS_EXCHANGE, t.DEAD_EXCHANGE):
        exchange = fetch(f"/exchanges/{vhost}/{quote(name, safe='')}")
        if exchange["type"] != "direct" or not exchange["durable"]:
            raise RuntimeError(f"RabbitMQ exchange {name} has an unexpected type or durability")

    for live_name, dead_name in (
        (t.CHAT_QUEUE, t.CHAT_DEAD_QUEUE),
        (t.INGEST_QUEUE, t.INGEST_DEAD_QUEUE),
    ):
        for name in (live_name, dead_name):
            queue = fetch(f"/queues/{vhost}/{quote(name, safe='')}")
            if not queue["durable"]:
                raise RuntimeError(f"RabbitMQ queue {name} is not durable")
        live_queue = fetch(f"/queues/{vhost}/{quote(live_name, safe='')}")
        arguments = live_queue.get("arguments") or {}
        if (
            arguments.get("x-dead-letter-exchange") != t.DEAD_EXCHANGE
            or arguments.get("x-dead-letter-routing-key") != dead_name
        ):
            raise RuntimeError(f"RabbitMQ dead-letter routing for {live_name} differs")
        for exchange_name, queue_name, route in (
            (t.JOBS_EXCHANGE, live_name, live_name),
            (t.DEAD_EXCHANGE, dead_name, dead_name),
        ):
            bindings = fetch(
                f"/bindings/{vhost}/e/{quote(exchange_name, safe='')}"
                f"/q/{quote(queue_name, safe='')}"
            )
            if not any(item.get("routing_key") == route for item in bindings):
                raise RuntimeError(f"RabbitMQ binding {exchange_name} -> {queue_name} is missing")
