"""会话行锁与消息序号分配。"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infra.mysql.models import Conversation


# 作用：按用户归属锁定会话，供受理任务时检查活动请求并分配序号。
async def lock_conversation(
    session: AsyncSession, *, user_id: str, conversation_id: str
) -> Conversation | None:
    result = await session.execute(
        select(Conversation)
        .where(Conversation.id == conversation_id, Conversation.user_id == user_id)
        .with_for_update()
    )
    return result.scalar_one_or_none()


# 作用：在已锁定的会话行上预留一个消息序号，由外层事务统一提交。
def reserve_message_sequence(conversation: Conversation) -> int:
    sequence = conversation.next_message_sequence
    conversation.next_message_sequence += 1
    return sequence
