"""聊天任务发布、终态事件补发与过期租约恢复，各网络调用都在数据库事务外。"""

from __future__ import annotations

import asyncio
import json
import logging

import aio_pika

from core.chat_contracts import ChatJob, ChatStreamEvent
from infra.mysql.base import new_id, utc_now
from infra.mysql.models import ChatRequest, Message
from infra.mysql.repositories.chat import recover_expired_chats, retry_delay
from infra.mysql.repositories.outbox import claim_due_events, mark_event_sent, reschedule_event
from infra.mysql.status import OutboxDestination, TaskStatus
from infra.topology import CHAT_QUEUE, CHAT_STREAM_ROUTE


LOG = logging.getLogger(__name__)


# 作用：核对持久化 Outbox 与任务字段，防止错误通知被发布到其他用户或请求。
async def validate_event(runtime, *, event_id: str, aggregate_id: str, payload: dict, destination: str):
    parsed = ChatJob.model_validate(payload) if destination == OutboxDestination.RABBITMQ.value else ChatStreamEvent.model_validate(payload)
    request_id = str(parsed.request_id) if isinstance(parsed, ChatJob) else parsed.data["request_id"]
    if request_id != aggregate_id:
        raise ValueError("Outbox aggregate_id 与通知任务不一致")
    async with asyncio.timeout(runtime.settings.database_timeout_seconds):
        async with runtime.sessions() as session:
            task = await session.get(ChatRequest, request_id)
            if task is None:
                raise ValueError("Outbox 指向不存在的聊天任务")
            if isinstance(parsed, ChatJob):
                if str(parsed.user_id) != task.user_id or str(parsed.conversation_id) != task.conversation_id:
                    raise ValueError("Outbox 用户或会话与任务不一致")
            else:
                expected = "done" if task.status == TaskStatus.SUCCEEDED.value else "error" if task.status == TaskStatus.FAILED.value else None
                if parsed.event != expected or parsed.data["attempt"] != task.attempt or parsed.data.get("event_id") != event_id:
                    raise ValueError("终态 Outbox 与数据库事实不一致")
                if parsed.event == "done":
                    answer = await session.get(Message, task.result_message_id) if task.result_message_id else None
                    if answer is None or answer.request_id != task.id or answer.conversation_id != task.conversation_id or answer.role != "assistant" or parsed.data.get("result_message_id") != answer.id:
                        raise ValueError("done 对应的完整回答尚未保存")
    return parsed


# 作用：领取一批聊天通知，确认外部送达后标记 sent，失败时保存退避时间。
async def publish_once(runtime, *, destination: str, request_id: str | None = None) -> int:
    owner = f"chat-publisher-{new_id()}"
    route = CHAT_QUEUE if destination == OutboxDestination.RABBITMQ.value else CHAT_STREAM_ROUTE
    limit = 1 if request_id else min(10, runtime.lease.outbox_batch_size)
    async with asyncio.timeout(runtime.settings.database_timeout_seconds):
        async with runtime.sessions() as session, session.begin():
            events = await claim_due_events(
                session, owner=owner, lease_seconds=runtime.lease.lease_seconds, limit=limit,
                routing_key=route, destination=destination, aggregate_id=request_id,
            )
            snapshots = [(event.id, event.aggregate_id, event.payload, event.attempt_count) for event in events]
    for event_id, aggregate_id, payload, attempt in snapshots:
        try:
            parsed = await validate_event(runtime, event_id=event_id, aggregate_id=aggregate_id, payload=payload, destination=destination)
            if destination == OutboxDestination.RABBITMQ.value:
                confirmed = await runtime.exchange.publish(aio_pika.Message(
                    body=json.dumps(parsed.model_dump(mode="json"), ensure_ascii=False).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json", message_id=event_id,
                ), routing_key=CHAT_QUEUE, mandatory=True, timeout=runtime.settings.io_timeout_seconds)
                if confirmed is None or confirmed is False:
                    raise RuntimeError("RabbitMQ 未确认任务通知")
            else:
                await runtime.streams.emit_terminal(parsed)
            async with asyncio.timeout(runtime.settings.database_timeout_seconds):
                async with runtime.sessions() as session, session.begin():
                    await mark_event_sent(session, event_id=event_id, owner=owner)
        except Exception as exc:
            LOG.warning("聊天 Outbox 暂未送达: event_id=%s error=%s", event_id, type(exc).__name__)
            async with asyncio.timeout(runtime.settings.database_timeout_seconds):
                async with runtime.sessions() as session, session.begin():
                    await reschedule_event(
                        session, event_id=event_id, owner=owner,
                        available_at=utc_now() + retry_delay(attempt), error=type(exc).__name__,
                    )
    return len(snapshots)


# 作用：正常终态提交后尝试立即通知；失败时保留 Outbox，让 ACK 不依赖 Redis 可用性。
async def try_publish_terminal(runtime, request_id: str) -> None:
    try:
        await publish_once(runtime, destination=OutboxDestination.REDIS_STREAM.value, request_id=request_id)
    except Exception:
        LOG.warning("终态留待 Outbox 补发: request_id=%s", request_id)


# 作用：等待停止信号或短轮询间隔，避免依赖故障造成忙等。
async def wait_for_stop(stop: asyncio.Event, seconds: int) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


# 作用：持续发布聊天 RabbitMQ 或 Redis Outbox，运行期间依赖恢复后自动继续。
async def publish_loop(runtime, *, destination: str, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            count = await publish_once(runtime, destination=destination)
        except Exception as exc:
            LOG.warning("聊天发布循环等待恢复: %s", type(exc).__name__)
            count = 0
        await wait_for_stop(stop, 1 if count else 3)


# 作用：周期性恢复过期聊天租约，数据库暂不可用时等待下一轮重试。
async def recovery_loop(runtime, *, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            async with asyncio.timeout(runtime.settings.database_timeout_seconds):
                async with runtime.sessions() as session, session.begin():
                    await recover_expired_chats(session, limit=runtime.lease.recovery_batch_size, max_attempts=runtime.settings.max_attempts)
        except Exception as exc:
            LOG.warning("聊天恢复循环等待数据库: %s", type(exc).__name__)
        await wait_for_stop(stop, 10)
