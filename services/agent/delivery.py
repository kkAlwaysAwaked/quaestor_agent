"""聊天通知校验与手动确认；业务状态持久化之后才 ACK 或进入死信。"""

from __future__ import annotations

import asyncio
import logging

from core.chat_contracts import AgentError, ChatJob, ChatStreamEvent
from infra.mysql.models import ChatRequest
from infra.mysql.repositories.chat import fail_chat
from infra.mysql.repositories.tasks import claim_task
from infra.mysql.status import TaskStatus
from services.agent.outbox import try_publish_terminal
from services.agent.processor import execute_attempt


LOG = logging.getLogger(__name__)


# 作用：限制消息确认的等待时间；确认失败仍依靠数据库幂等和 RabbitMQ 重投恢复。
async def confirm_message(message, *, rejected: bool = False) -> None:
    async with asyncio.timeout(5):
        if rejected:
            await message.reject(requeue=False)
        else:
            await message.ack()


# 作用：在新事务中记录当前尝试的重试或最终失败，不让旧租约覆盖新的任务状态。
async def record_failure(runtime, *, request_id: str, owner: str, attempt: int, error: AgentError) -> str:
    async with asyncio.timeout(runtime.settings.database_timeout_seconds):
        async with runtime.sessions() as session, session.begin():
            return await fail_chat(
                session, request_id=request_id, owner=owner, attempt=attempt,
                error=error, max_attempts=runtime.settings.max_attempts,
            )


# 作用：业务结果可靠保存后决定 ACK/死信，租约变化时以最新数据库状态为准。
async def settle_failure(message, runtime, *, request_id: str, owner: str, attempt: int, outcome: str) -> None:
    if outcome == "lease_lost":
        async with asyncio.timeout(runtime.settings.database_timeout_seconds):
            async with runtime.sessions() as session:
                task = await session.get(ChatRequest, request_id)
                outcome = "failed" if task and task.status == TaskStatus.FAILED.value else "completed" if task and task.status == TaskStatus.SUCCEEDED.value else "duplicate"
    if outcome in ("failed", "completed"):
        await try_publish_terminal(runtime, request_id)
    if outcome == "retry":
        try:
            await runtime.streams.emit_active(ChatStreamEvent(event="status", data={
                "request_id": request_id, "attempt": attempt,
                "phase": "retry_scheduled", "message": "回答暂时中断，正在安排重试",
            }), owner=owner)
        except Exception:
            # Redis 故障不能撤销已经提交到 MySQL 的重试安排，也不能伪造最终 error。
            pass
    if outcome == "failed":
        await confirm_message(message, rejected=True)
    else:
        await confirm_message(message)


# 作用：无法证明业务提交时不提前确认，短暂等待后让 RabbitMQ 重投原通知。
async def requeue_uncommitted(message) -> None:
    if not message.processed:
        await asyncio.sleep(1)
        try:
            async with asyncio.timeout(5):
                await message.nack(requeue=True)
        except Exception:
            LOG.warning("消息通道不可用，未确认通知将由 RabbitMQ 回收")


# 作用：核对队列标识、原子领取任务并执行，处理失败或停机后先持久化再确认通知。
async def handle_message(message, runtime, *, owner: str) -> None:
    try:
        job = ChatJob.model_validate_json(message.body)
    except (ValueError, TypeError):
        LOG.warning("非法 ChatJob 进入死信队列")
        try:
            await confirm_message(message, rejected=True)
        except Exception:
            await requeue_uncommitted(message)
        return
    request_id = str(job.request_id)
    attempt = None
    try:
        async with asyncio.timeout(runtime.settings.database_timeout_seconds):
            async with runtime.sessions() as session, session.begin():
                task = await session.get(ChatRequest, request_id)
                if task is None or task.user_id != str(job.user_id) or task.conversation_id != str(job.conversation_id):
                    invalid = True
                    claimed = False
                    status = None
                else:
                    invalid = False
                    claimed = await claim_task(session, ChatRequest, task_id=request_id, owner=owner, lease_seconds=runtime.lease.lease_seconds)
                    await session.refresh(task)
                    status = task.status
                    attempt = task.attempt if claimed else None
        if invalid:
            await confirm_message(message, rejected=True)
            return
        if not claimed:
            if status in (TaskStatus.SUCCEEDED.value, TaskStatus.FAILED.value):
                await try_publish_terminal(runtime, request_id)
            if status == TaskStatus.FAILED.value:
                await confirm_message(message, rejected=True)
            else:
                await confirm_message(message)
            return
        try:
            await execute_attempt(runtime, request_id=request_id, owner=owner, attempt=attempt)
        except AgentError as exc:
            outcome = await record_failure(runtime, request_id=request_id, owner=owner, attempt=attempt, error=exc)
            await settle_failure(message, runtime, request_id=request_id, owner=owner, attempt=attempt, outcome=outcome)
            return
        except Exception as exc:
            LOG.warning("聊天执行暂时失败: request_id=%s error=%s", request_id, type(exc).__name__)
            error = AgentError("agent_backend_failed", "回答依赖暂不可用")
            outcome = await record_failure(runtime, request_id=request_id, owner=owner, attempt=attempt, error=error)
            await settle_failure(message, runtime, request_id=request_id, owner=owner, attempt=attempt, outcome=outcome)
            return
        await try_publish_terminal(runtime, request_id)
        await confirm_message(message)
    except asyncio.CancelledError:
        if attempt is not None and not message.processed:
            try:
                outcome = await asyncio.shield(record_failure(
                    runtime, request_id=request_id, owner=owner, attempt=attempt,
                    error=AgentError("worker_shutdown", "执行进程正在退出"),
                ))
                await settle_failure(message, runtime, request_id=request_id, owner=owner, attempt=attempt, outcome=outcome)
                return
            except Exception:
                pass
        await requeue_uncommitted(message)
        raise
    except Exception as exc:
        LOG.warning("聊天通知尚未可靠确认: request_id=%s error=%s", request_id, type(exc).__name__)
        await requeue_uncommitted(message)
