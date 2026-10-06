"""聊天受理、固定历史、结果提交与租约恢复；事务统一由应用层控制。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.chat_contracts import AgentError, ChatJob, ChatStreamEvent, LeaseLost
from core.retrieval_contracts import RetrieveRequest
from infra.mysql.base import new_id, utc_now
from infra.mysql.models import ChatRequest, Conversation, Message, OutboxEvent, RetrievalRun
from infra.mysql.repositories.conversations import lock_conversation, reserve_message_sequence
from infra.mysql.repositories.tasks import find_chat_by_idempotency_key, lock_expired_tasks, transition_task
from infra.mysql.status import OutboxDestination, RetrievalStatus, TaskStatus
from infra.topology import CHAT_QUEUE, CHAT_STREAM_ROUTE


@dataclass(frozen=True)
class ChatContext:
    request_id: str
    user_id: str
    conversation_id: str
    attempt: int
    lease_expires_at: datetime
    messages: list[dict]
    saved_retrieval_input: RetrieveRequest | None
    retrieval_succeeded: bool


# 作用：按当前执行次数计算有上限的延迟重试时间。
def retry_delay(attempt: int) -> timedelta:
    return timedelta(seconds=min(300, 10 * 2 ** max(0, attempt - 1)))


# 作用：在当前事务内添加只携带可信任务标识的 RabbitMQ 通知。
def add_chat_job_event(session: AsyncSession, task: ChatRequest, *, available_at=None) -> None:
    job = ChatJob(schema_version=1, request_id=task.id, user_id=task.user_id, conversation_id=task.conversation_id)
    session.add(OutboxEvent(
        event_type="chat.requested", destination=OutboxDestination.RABBITMQ.value,
        routing_key=CHAT_QUEUE, aggregate_type="chat_request", aggregate_id=task.id,
        payload=job.model_dump(mode="json"), available_at=available_at or utc_now(),
    ))


# 作用：在终态事务中添加待发布的 done/error 事件，使 Redis 失败也能补发通知。
def add_terminal_event(session: AsyncSession, task: ChatRequest, *, event: str, details: dict) -> None:
    event_id = new_id()
    payload = ChatStreamEvent(event=event, data={
        **details, "request_id": task.id, "attempt": task.attempt, "event_id": event_id,
    })
    session.add(OutboxEvent(
        id=event_id, event_type=f"chat.{event}", destination=OutboxDestination.REDIS_STREAM.value,
        routing_key=CHAT_STREAM_ROUTE, aggregate_type="chat_request", aggregate_id=task.id,
        payload=payload.model_dump(mode="json"),
    ))


# 作用：锁定归属会话，在同一事务内创建用户消息、聊天任务和可靠投递 Outbox。
async def accept_chat(
    session: AsyncSession, *, user_id: str, conversation_id: str,
    content: str, idempotency_key: str,
) -> ChatRequest:
    if not content.strip() or len(content) > 12000 or not 1 <= len(idempotency_key) <= 128:
        raise AgentError("invalid_chat_input", "提问或幂等键不符合长度要求", retryable=False)
    conversation = await lock_conversation(session, user_id=user_id, conversation_id=conversation_id)
    if conversation is None:
        raise AgentError("conversation_not_found", "会话不存在或无权访问", retryable=False)
    fingerprint = hashlib.sha256(json.dumps(
        {"conversation_id": conversation_id, "content": content}, ensure_ascii=False, sort_keys=True,
    ).encode("utf-8")).hexdigest()
    existing = await find_chat_by_idempotency_key(session, user_id=user_id, idempotency_key=idempotency_key)
    if existing is not None:
        if existing.request_fingerprint != fingerprint:
            raise AgentError("chat_input_conflict", "同一幂等键不能改变提问", retryable=False)
        return existing
    if conversation.active_request_id is not None:
        raise AgentError("conversation_busy", "会话中已有未完成提问", retryable=False)
    request_id = new_id()
    sequence = reserve_message_sequence(conversation)
    message = Message(
        id=new_id(), conversation_id=conversation.id, request_id=request_id,
        sequence=sequence, role="user", content=content,
    )
    session.add(message)
    await session.flush()
    task = ChatRequest(
        id=request_id, user_id=user_id, conversation_id=conversation.id,
        user_message_id=message.id, history_until_sequence=sequence,
        idempotency_key=idempotency_key, request_fingerprint=fingerprint,
    )
    session.add(task)
    await session.flush()
    conversation.active_request_id = task.id
    add_chat_job_event(session, task)
    return task


# 作用：保留未经改写的连续历史后缀，并限制消息数量和上下文总长度。
def trim_history(messages: list[dict], *, max_chars: int = 60000, max_messages: int = 100) -> list[dict]:
    kept, size = [], 0
    for message in reversed(messages):
        content = message["content"]
        if len(content) > 12000:
            if not kept:
                raise AgentError("history_too_large", "当前提问超过模型上下文限制", retryable=False)
            break
        if len(kept) >= max_messages or size + len(content) > max_chars:
            break
        kept.append(message)
        size += len(content)
    kept.reverse()
    return kept


# 作用：核对当前租约并读取固定截止历史，最近失败轮次之前的上下文整体截断。
async def load_chat_context(session: AsyncSession, *, request_id: str, owner: str, attempt: int) -> ChatContext:
    task = await session.get(ChatRequest, request_id)
    if (
        task is None or task.status != TaskStatus.RUNNING.value or task.attempt != attempt
        or task.lease_owner != owner or not task.lease_expires_at or task.lease_expires_at <= utc_now()
    ):
        raise AgentError("task_lease_lost", "任务执行权已变化")
    conversation = await session.get(Conversation, task.conversation_id)
    if conversation is None or conversation.user_id != task.user_id or conversation.active_request_id != task.id:
        raise AgentError("invalid_task_context", "任务与会话预留关系不一致", retryable=False)
    failed_sequence = await session.scalar(select(func.max(Message.sequence)).join(
        ChatRequest, Message.request_id == ChatRequest.id,
    ).where(
        Message.conversation_id == task.conversation_id, Message.role == "user",
        Message.sequence < task.history_until_sequence, ChatRequest.status == TaskStatus.FAILED.value,
    ))
    rows = (await session.scalars(select(Message).where(
        Message.conversation_id == task.conversation_id,
        Message.sequence <= task.history_until_sequence,
        Message.sequence > (failed_sequence or 0),
    ).order_by(Message.sequence.desc()).limit(100))).all()
    rows.reverse()
    if not rows or rows[-1].id != task.user_message_id or rows[-1].role != "user" or rows[-1].request_id != task.id:
        raise AgentError("invalid_task_history", "任务历史截止点与本次提问不一致", retryable=False)
    messages = trim_history([{"role": row.role, "content": row.content} for row in rows])
    run = await session.scalar(select(RetrievalRun).where(RetrievalRun.request_id == task.id))
    saved = None
    if run is not None:
        if run.user_id != task.user_id:
            raise AgentError("invalid_retrieval_owner", "检索记录归属不一致", retryable=False)
        try:
            saved = RetrieveRequest.model_validate(run.input_data)
        except ValueError as exc:
            raise AgentError("invalid_retrieval_input", "已保存检索输入不完整", retryable=False) from exc
        if str(saved.request_id) != task.id or saved.messages[-1].model_dump() != messages[-1]:
            raise AgentError("invalid_retrieval_input", "已保存检索输入不属于本次提问", retryable=False)
    return ChatContext(
        task.id, task.user_id, task.conversation_id, task.attempt, task.lease_expires_at,
        messages, saved, run is not None and run.status == RetrievalStatus.SUCCEEDED.value,
    )


# 作用：在写入事务内锁定仍有效的任务租约，拒绝旧执行者修改状态和答案。
async def lock_owned_task(session: AsyncSession, *, request_id: str, owner: str, attempt: int) -> ChatRequest | None:
    return await session.scalar(select(ChatRequest).where(
        ChatRequest.id == request_id, ChatRequest.status == TaskStatus.RUNNING.value,
        ChatRequest.attempt == attempt, ChatRequest.lease_owner == owner,
        ChatRequest.lease_expires_at > utc_now(),
    ).with_for_update())


# 作用：在已锁定任务上写最终失败、释放会话并创建可靠 error 通知。
async def finalize_failure(session: AsyncSession, task: ChatRequest, *, code: str, message: str, owner: str | None = None) -> None:
    conversation = await session.get(Conversation, task.conversation_id, with_for_update=True)
    if owner is not None and not await transition_task(
        session, ChatRequest, task_id=task.id, owner=owner, attempt=task.attempt,
        target=TaskStatus.FAILED, error=code,
    ):
        raise LeaseLost()
    task.status = TaskStatus.FAILED.value
    task.last_error = code
    task.lease_owner = None
    task.lease_expires_at = None
    if conversation is not None and conversation.active_request_id == task.id:
        conversation.active_request_id = None
    add_terminal_event(session, task, event="error", details={"code": code, "message": message})


# 作用：把完整答案、聊天成功、会话释放和 done Outbox 作为一个事务提交。
async def finish_chat(session: AsyncSession, *, request_id: str, owner: str, attempt: int, answer: str) -> bool:
    task = await lock_owned_task(session, request_id=request_id, owner=owner, attempt=attempt)
    if task is None:
        return False
    conversation = await session.get(Conversation, task.conversation_id, with_for_update=True)
    if conversation is None or conversation.user_id != task.user_id or conversation.active_request_id != task.id:
        raise AgentError("conversation_reservation_lost", "会话预留状态不一致", retryable=False)
    message = Message(
        id=new_id(), conversation_id=conversation.id, request_id=task.id,
        sequence=reserve_message_sequence(conversation), role="assistant", content=answer,
    )
    session.add(message)
    await session.flush()
    if not await transition_task(
        session, ChatRequest, task_id=task.id, owner=owner, attempt=attempt,
        target=TaskStatus.SUCCEEDED,
    ):
        raise LeaseLost()
    task.result_message_id = message.id
    task.status = TaskStatus.SUCCEEDED.value
    task.lease_owner = None
    task.lease_expires_at = None
    task.last_error = None
    conversation.active_request_id = None
    add_terminal_event(session, task, event="done", details={"result_message_id": message.id})
    return True


# 作用：按错误与重试上限安排持久化重试或最终失败，失去租约则不写任何新状态。
async def fail_chat(
    session: AsyncSession, *, request_id: str, owner: str, attempt: int,
    error: AgentError, max_attempts: int,
) -> str:
    task = await lock_owned_task(session, request_id=request_id, owner=owner, attempt=attempt)
    if task is None:
        return "lease_lost"
    if not error.retryable or attempt >= max_attempts:
        await finalize_failure(session, task, code=error.code, message="回答未能完成，请稍后重新提问。", owner=owner)
        return "failed"
    due = utc_now() + retry_delay(attempt)
    if not await transition_task(
        session, ChatRequest, task_id=task.id, owner=owner, attempt=attempt,
        target=TaskStatus.RETRY_WAIT, available_at=due, error=error.code,
    ):
        raise LeaseLost()
    add_chat_job_event(session, task, available_at=due)
    return "retry"


# 作用：在当前事务中恢复过期聊天任务，重试通知或最终失败与状态一起保存。
async def recover_expired_chats(session: AsyncSession, *, limit: int, max_attempts: int) -> int:
    tasks = await lock_expired_tasks(session, ChatRequest, limit=limit)
    for task in tasks:
        if task.attempt >= max_attempts:
            await finalize_failure(session, task, code="task_lease_expired", message="回答执行中断，请重新提问。")
        else:
            task.status = TaskStatus.RETRY_WAIT.value
            task.last_error = "task_lease_expired"
            task.available_at = utc_now() + retry_delay(task.attempt)
            task.lease_owner = None
            task.lease_expires_at = None
            add_chat_job_event(session, task, available_at=task.available_at)
    return len(tasks)
