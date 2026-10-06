"""聊天与入库任务共用的原子领取、续租和状态转换操作。"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from infra.mysql.base import utc_now
from infra.mysql.models import ChatRequest, IngestJob
from infra.mysql.status import TaskStatus


TaskModel = type[ChatRequest] | type[IngestJob]


# 作用：根据用户和幂等键查找已有聊天任务，供重复提交时复用。
async def find_chat_by_idempotency_key(
    session: AsyncSession, *, user_id: str, idempotency_key: str
) -> ChatRequest | None:
    result = await session.execute(
        select(ChatRequest).where(
            ChatRequest.user_id == user_id,
            ChatRequest.idempotency_key == idempotency_key,
        )
    )
    return result.scalar_one_or_none()


# 作用：用一条条件更新原子领取到期任务，并返回是否取得执行权。
async def claim_task(
    session: AsyncSession,
    model: TaskModel,
    *,
    task_id: str,
    owner: str,
    lease_seconds: int,
    now: datetime | None = None,
) -> bool:
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    current = now or utc_now()
    result = await session.execute(
        update(model)
        .where(
            model.id == task_id,
            model.status.in_((TaskStatus.PENDING.value, TaskStatus.RETRY_WAIT.value)),
            model.available_at <= current,
        )
        .values(
            status=TaskStatus.RUNNING.value,
            attempt=model.attempt + 1,
            lease_owner=owner,
            lease_expires_at=current + timedelta(seconds=lease_seconds),
        )
    )
    return result.rowcount == 1


# 作用：仅让当前 attempt 的有效租约持有者延长执行时间。
async def renew_task_lease(
    session: AsyncSession,
    model: TaskModel,
    *,
    task_id: str,
    owner: str,
    attempt: int,
    lease_seconds: int,
    now: datetime | None = None,
) -> bool:
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    current = now or utc_now()
    result = await session.execute(
        update(model)
        .where(
            model.id == task_id,
            model.status == TaskStatus.RUNNING.value,
            model.attempt == attempt,
            model.lease_owner == owner,
            model.lease_expires_at > current,
        )
        .values(lease_expires_at=current + timedelta(seconds=lease_seconds))
    )
    return result.rowcount == 1


# 作用：仅允许有效租约持有者将任务转为成功、重试等待或最终失败。
async def transition_task(
    session: AsyncSession,
    model: TaskModel,
    *,
    task_id: str,
    owner: str,
    attempt: int,
    target: TaskStatus,
    available_at: datetime | None = None,
    error: str | None = None,
    now: datetime | None = None,
) -> bool:
    if target not in (TaskStatus.RETRY_WAIT, TaskStatus.SUCCEEDED, TaskStatus.FAILED):
        raise ValueError("target must be retry_wait, succeeded or failed")
    if target == TaskStatus.RETRY_WAIT and available_at is None:
        raise ValueError("retry_wait requires available_at")
    current = now or utc_now()
    result = await session.execute(
        update(model)
        .where(
            model.id == task_id,
            model.status == TaskStatus.RUNNING.value,
            model.attempt == attempt,
            model.lease_owner == owner,
            model.lease_expires_at > current,
        )
        .values(
            status=target.value,
            available_at=available_at or current,
            lease_owner=None,
            lease_expires_at=None,
            last_error=error,
        )
    )
    return result.rowcount == 1


# 作用：锁定一批租约已过期的运行中任务，供恢复流程重新安排。
async def lock_expired_tasks(
    session: AsyncSession,
    model: TaskModel,
    *,
    limit: int,
    now: datetime | None = None,
) -> list[ChatRequest] | list[IngestJob]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    current = now or utc_now()
    result = await session.execute(
        select(model)
        .where(model.status == TaskStatus.RUNNING.value, model.lease_expires_at <= current)
        .order_by(model.lease_expires_at, model.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars().all())
