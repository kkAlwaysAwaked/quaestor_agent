"""Agent 进程入口：连接生命周期、小并发消费、可靠发布、恢复和有界停机。"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from dataclasses import dataclass

import aio_pika
import httpx
from openai import AsyncOpenAI
from sqlalchemy import text

from core.config import (
    AgentSettings, ConfigError, load_agent_settings, load_database_settings, load_model_settings,
    load_rabbit_runtime_settings, load_redis_runtime_settings, load_retrieval_token_settings, load_task_lease_settings,
)
from infra.mysql.base import new_id
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.mysql.status import OutboxDestination
from infra.redis_streams import ChatStreams, create_redis_client
from infra.topology import CHAT_QUEUE, JOBS_EXCHANGE
from services.agent.delivery import handle_message
from services.agent.outbox import publish_loop, recovery_loop
from services.agent.retrieval_client import RetrievalClient


LOG = logging.getLogger(__name__)


@dataclass
class AgentRuntime:
    settings: AgentSettings
    lease: object
    sessions: object
    engine: object
    model: object
    retrieval: object
    streams: object
    http: object
    redis: object
    connection: object = None
    exchange: object = None
    queue: object = None

    # 作用：有界释放 AMQP、模型 SDK、HTTP、Redis 和 MySQL，单个资源关闭失败不阻断其他清理。
    async def close(self) -> None:
        operations = []
        if self.connection is not None:
            operations.append(("RabbitMQ", self.connection.close))
        operations.extend([
            ("模型 SDK", self.model.close), ("HTTP", self.http.aclose),
            ("Redis", self.redis.aclose), ("MySQL", self.engine.dispose),
        ])
        for name, close in operations:
            try:
                async with asyncio.timeout(self.settings.io_timeout_seconds):
                    await close()
            except Exception as exc:
                LOG.warning("资源关闭未完成: %s error=%s", name, type(exc).__name__)


# 作用：启动时检查自己的依赖并构建可复用客户端，不初始化 Embedding 或 Qdrant 模型。
async def create_runtime() -> AgentRuntime:
    settings, lease = load_agent_settings(), load_task_lease_settings()
    rabbit, redis_settings = load_rabbit_runtime_settings(), load_redis_runtime_settings()
    credentials, model_settings = load_retrieval_token_settings(), load_model_settings()
    if model_settings.api_key.startswith("replace_"):
        raise ConfigError("Agent requires an actual DEEPSEEK_API_KEY")
    engine = create_mysql_engine(load_database_settings())
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=6, max_keepalive_connections=4))
    redis = create_redis_client(redis_settings, timeout_seconds=settings.io_timeout_seconds)
    model = AsyncOpenAI(
        api_key=model_settings.api_key, base_url=model_settings.base_url,
        timeout=settings.model_timeout_seconds, max_retries=0,
    )
    runtime = AgentRuntime(
        settings, lease, create_session_factory(engine), engine, model,
        RetrievalClient(http, settings=settings, credentials=credentials), ChatStreams(redis, settings), http, redis,
    )
    try:
        async with asyncio.timeout(settings.database_timeout_seconds):
            async with engine.connect() as connection:
                await connection.execute(text("SELECT user_message_id, history_until_sequence, lease_expires_at FROM chat_requests LIMIT 1"))
                await connection.execute(text("SELECT input_data, status FROM retrieval_runs LIMIT 1"))
                await connection.execute(text("SELECT destination, payload FROM outbox_events LIMIT 1"))
        async with asyncio.timeout(settings.io_timeout_seconds):
            await redis.ping()
            response = await http.get(f"{settings.retrieval_url}/health", timeout=settings.io_timeout_seconds)
            response.raise_for_status()
        async with asyncio.timeout(settings.io_timeout_seconds):
            runtime.connection = await aio_pika.connect_robust(
                host=rabbit.host, port=rabbit.port, login=rabbit.user, password=rabbit.password,
                timeout=settings.io_timeout_seconds, heartbeat=30,
            )
            publish_channel = await runtime.connection.channel(publisher_confirms=True, on_return_raises=True)
            runtime.exchange = await publish_channel.declare_exchange(JOBS_EXCHANGE, aio_pika.ExchangeType.DIRECT, passive=True)
            consume_channel = await runtime.connection.channel()
            await consume_channel.set_qos(prefetch_count=settings.prefetch_count)
            runtime.queue = await consume_channel.declare_queue(CHAT_QUEUE, passive=True)
        return runtime
    except BaseException:
        await runtime.close()
        raise


# 作用：以独立信号量限制实际任务数，停止领取后让已有任务继续完成。
async def consume_messages(runtime, *, active: set, stop: asyncio.Event) -> None:
    slots = asyncio.Semaphore(runtime.settings.concurrency)

    # 作用：在一个任务处理结束后归还业务并发名额。
    async def dispatch(message) -> None:
        try:
            await handle_message(message, runtime, owner=f"agent-worker-{new_id()}")
        finally:
            slots.release()

    # 作用：移除已结束任务并消费异常，避免未处理异常隐藏在后台。
    def completed(task: asyncio.Task) -> None:
        active.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                LOG.warning("聊天处理任务异常退出: %s", type(error).__name__)

    async with runtime.queue.iterator() as messages:
        while not stop.is_set():
            await slots.acquire()
            if stop.is_set():
                slots.release()
                break
            try:
                message = await messages.__anext__()
            except BaseException:
                slots.release()
                raise
            task = asyncio.create_task(dispatch(message))
            active.add(task)
            task.add_done_callback(completed)


# 作用：先等待在途任务，超过停机宽限后安全取消并触发持久化重试或消息重投。
async def drain_tasks(active: set, *, grace_seconds: int) -> None:
    if not active:
        return
    _, pending = await asyncio.wait(tuple(active), timeout=grace_seconds)
    for task in pending:
        task.cancel()
    await asyncio.gather(*tuple(active), return_exceptions=True)


# 作用：注册 Windows 兼容的退出信号，只设置停止事件而不直接关闭在途任务连接。
def install_stop_signals(stop: asyncio.Event) -> dict:
    previous = {}
    loop = asyncio.get_running_loop()

    # 作用：把操作系统退出信号转成事件循环中的停止请求。
    def request_stop(signum, frame) -> None:
        loop.call_soon_threadsafe(stop.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)
    return previous


# 作用：运行消费、发布和恢复循环，停机先停止领取，再排空任务，最后关闭资源。
async def run_worker() -> None:
    runtime = await create_runtime()
    stop, background_stop, active = asyncio.Event(), asyncio.Event(), set()
    previous = install_stop_signals(stop)
    consumer = asyncio.create_task(consume_messages(runtime, active=active, stop=stop))
    background = [
        asyncio.create_task(publish_loop(runtime, destination=OutboxDestination.RABBITMQ.value, stop=background_stop)),
        asyncio.create_task(publish_loop(runtime, destination=OutboxDestination.REDIS_STREAM.value, stop=background_stop)),
        asyncio.create_task(recovery_loop(runtime, stop=background_stop)),
    ]
    stopper = asyncio.create_task(stop.wait())
    try:
        finished, _ = await asyncio.wait([consumer, stopper, *background], return_when=asyncio.FIRST_COMPLETED)
        if not stop.is_set():
            for task in finished:
                task.result()
            raise RuntimeError("Agent 的关键循环意外退出")
    finally:
        stop.set()
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await drain_tasks(active, grace_seconds=runtime.settings.shutdown_grace_seconds)
        background_stop.set()
        for task in [stopper, *background]:
            task.cancel()
        await asyncio.gather(stopper, *background, return_exceptions=True)
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        await runtime.close()


# 作用：配置命令行日志并启动 Agent Worker，Ctrl+C 按有界停机顺序退出。
def main() -> None:
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
