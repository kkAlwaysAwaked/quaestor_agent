"""Outbox 事件的并发领取、发布确认和失败回退。"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from infra.mysql.base import utc_now
from infra.mysql.models import OutboxEvent
from infra.mysql.status import OutboxStatus


# 作用：使用行锁跳过其他发布者已领取的事件，并设置发布租约。
async def claim_due_events(
    session: AsyncSession,
    *,
    owner: str,
    lease_seconds: int,
    limit: int,
    now: datetime | None = None,
) -> list[OutboxEvent]:
    if lease_seconds <= 0 or limit <= 0:
        raise ValueError("lease_seconds and limit must be positive")
    current = now or utc_now()
    result = await session.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.available_at <= current,
            or_(
                OutboxEvent.status == OutboxStatus.PENDING.value,
                and_(
                    OutboxEvent.status == OutboxStatus.PUBLISHING.value,
                    OutboxEvent.lease_expires_at <= current,
                ),
            ),
        )
        .order_by(OutboxEvent.available_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    events = list(result.scalars().all())
    for event in events:
        event.status = OutboxStatus.PUBLISHING.value
        event.lease_owner = owner
        event.lease_expires_at = current + timedelta(seconds=lease_seconds)
        event.attempt_count += 1
    await session.flush()
    return events


# 作用：仅在发布租约仍有效时，将已确认送达的事件标记为已发送。
async def mark_event_sent(
    session: AsyncSession,
    *,
    event_id: str,
    owner: str,
    now: datetime | None = None,
) -> bool:
    current = now or utc_now()
    result = await session.execute(
        update(OutboxEvent)
        .where(
            OutboxEvent.id == event_id,
            OutboxEvent.status == OutboxStatus.PUBLISHING.value,
            OutboxEvent.lease_owner == owner,
            OutboxEvent.lease_expires_at > current,
        )
        .values(
            status=OutboxStatus.SENT.value,
            sent_at=current,
            lease_owner=None,
            lease_expires_at=None,
            last_error=None,
        )
    )
    return result.rowcount == 1


# 作用：发布失败时清除租约、记录错误并安排下一次发送时间。
async def reschedule_event(
    session: AsyncSession,
    *,
    event_id: str,
    owner: str,
    available_at: datetime,
    error: str,
    now: datetime | None = None,
) -> bool:
    current = now or utc_now()
    result = await session.execute(
        update(OutboxEvent)
        .where(
            OutboxEvent.id == event_id,
            OutboxEvent.status == OutboxStatus.PUBLISHING.value,
            OutboxEvent.lease_owner == owner,
            OutboxEvent.lease_expires_at > current,
        )
        .values(
            status=OutboxStatus.PENDING.value,
            available_at=available_at,
            lease_owner=None,
            lease_expires_at=None,
            last_error=error,
        )
    )
    return result.rowcount == 1
