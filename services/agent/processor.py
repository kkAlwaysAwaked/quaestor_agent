"""一次已领取聊天任务的租约维护、流式生成与原子答案提交。"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from core.chat_contracts import AgentError, ChatStreamEvent, LeaseLost
from infra.mysql.base import utc_now
from infra.mysql.models import ChatRequest
from infra.mysql.repositories.chat import finish_chat, load_chat_context
from infra.mysql.repositories.tasks import renew_task_lease
from services.agent.engine import run_agent_async
from services.agent.retrieval_client import RetrievalContext


class LeaseGuard:
    # 作用：保存本次已确认的租约截止时间，防止暂停后恢复的旧执行者继续输出。
    def __init__(self, expires_at) -> None:
        self.expires_at = expires_at

    # 作用：每次输出前检查本机已确认的租约是否仍有效，最终写入另由 MySQL 条件更新兜底。
    def ensure_valid(self) -> None:
        if self.expires_at <= utc_now():
            raise LeaseLost()


# 作用：定期提交 MySQL 续租并延长活动 Stream 保留期，任一依赖故障都停止当前生成。
async def keep_lease(runtime, context, *, owner: str, guard: LeaseGuard, started: asyncio.Event, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(1, runtime.lease.lease_seconds // 3))
            return
        except TimeoutError:
            pass
        now = utc_now()
        async with asyncio.timeout(runtime.settings.database_timeout_seconds):
            async with runtime.sessions() as session, session.begin():
                valid = await renew_task_lease(
                    session, ChatRequest, task_id=context.request_id, owner=owner,
                    attempt=context.attempt, lease_seconds=runtime.lease.lease_seconds, now=now,
                )
        if not valid:
            raise LeaseLost()
        guard.expires_at = now + timedelta(seconds=runtime.lease.lease_seconds)
        if started.is_set():
            await runtime.streams.touch_active(request_id=context.request_id, attempt=context.attempt, owner=owner)


# 作用：先写 reset，再回流真正模型增量；正常结束时把累计答案与终态 Outbox 一起提交。
async def generate_and_commit(runtime, context, *, owner: str, guard: LeaseGuard, started: asyncio.Event) -> None:
    async with asyncio.timeout(runtime.settings.generation_timeout_seconds):
        guard.ensure_valid()
        await runtime.streams.emit_active(ChatStreamEvent(event="status", data={
            "request_id": context.request_id, "attempt": context.attempt,
            "phase": "started", "reset": True, "message": "开始准备回答",
        }), owner=owner)
        started.set()
        retrieval = RetrievalContext(
            runtime.retrieval, request_id=context.request_id, user_id=context.user_id,
            messages=context.messages, saved_input=context.saved_retrieval_input,
            previously_succeeded=context.retrieval_succeeded,
        )
        pieces, chars, byte_count = [], 0, 0
        async for event in run_agent_async(
            context.messages, runtime.model, retrieval_context=retrieval, settings=runtime.settings,
        ):
            guard.ensure_valid()
            if event.event == "token":
                token = event.data.get("token")
                if not isinstance(token, str):
                    raise AgentError("invalid_model_token", "回答片段格式不正确", retryable=False)
                chars += len(token)
                byte_count += len(token.encode("utf-8"))
                if chars > runtime.settings.max_output_chars or byte_count > 60000:
                    raise AgentError("answer_too_large", "回答超过保存长度限制", retryable=False)
            await runtime.streams.emit_active(ChatStreamEvent(event=event.event, data={
                **event.data, "request_id": context.request_id, "attempt": context.attempt,
            }), owner=owner)
            if event.event == "token":
                pieces.append(token)
        answer = "".join(pieces)
        if not answer.strip():
            raise AgentError("model_empty_answer", "回答服务返回空文本")
        guard.ensure_valid()
        async with asyncio.timeout(runtime.settings.database_timeout_seconds):
            async with runtime.sessions() as session, session.begin():
                if not await finish_chat(
                    session, request_id=context.request_id, owner=owner, attempt=context.attempt, answer=answer,
                ):
                    raise LeaseLost()


# 作用：同时监督生成与心跳；续租失败立即取消模型流，取消处理不会提交半段答案。
async def execute_attempt(runtime, *, request_id: str, owner: str, attempt: int) -> None:
    async with asyncio.timeout(runtime.settings.database_timeout_seconds):
        async with runtime.sessions() as session:
            context = await load_chat_context(session, request_id=request_id, owner=owner, attempt=attempt)
    guard, started, stop = LeaseGuard(context.lease_expires_at), asyncio.Event(), asyncio.Event()
    heartbeat = asyncio.create_task(keep_lease(runtime, context, owner=owner, guard=guard, started=started, stop=stop))
    generation = asyncio.create_task(generate_and_commit(runtime, context, owner=owner, guard=guard, started=started))
    try:
        finished, _ = await asyncio.wait((heartbeat, generation), return_when=asyncio.FIRST_COMPLETED)
        if generation in finished:
            generation.result()
            return
        heartbeat.result()
        raise LeaseLost()
    except TimeoutError as exc:
        raise AgentError("generation_timeout", "回答生成超过时限") from exc
    finally:
        stop.set()
        for task in (generation, heartbeat):
            if not task.done():
                task.cancel()
        await asyncio.gather(generation, heartbeat, return_exceptions=True)
