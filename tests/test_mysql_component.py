"""验证 MySQL 组件的表约束与条件更新；SQLite 不模拟 MySQL 行锁并发。"""

from __future__ import annotations

import unittest
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from infra.mysql.base import Base, new_id, utc_now
from infra.mysql.models import ChatRequest, Conversation, Message, OutboxEvent, User
from infra.mysql.repositories.conversations import lock_conversation, reserve_message_sequence
from infra.mysql.repositories.outbox import claim_due_events, mark_event_sent, reschedule_event
from infra.mysql.repositories.tasks import claim_task, renew_task_lease, transition_task
from infra.mysql.session import create_session_factory
from infra.mysql.status import OutboxDestination, TaskStatus


class MysqlComponentTests(unittest.IsolatedAsyncioTestCase):
    # 作用：为每个测试创建独立的内存数据库及异步会话工厂。
    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = create_session_factory(self.engine)
        self.user_id = new_id()
        self.conversation_id = new_id()
        self.request_id = new_id()
        self.message_id = new_id()
        async with self.sessions() as session, session.begin():
            session.add(User(id=self.user_id, email="learner@example.com", password_hash="hash"))
            session.add(Conversation(
                id=self.conversation_id, user_id=self.user_id, next_message_sequence=2
            ))
            session.add(Message(
                id=self.message_id,
                conversation_id=self.conversation_id,
                request_id=self.request_id,
                sequence=1,
                role="user",
                content="今年有多少天年假？",
            ))
            session.add(ChatRequest(
                id=self.request_id,
                user_id=self.user_id,
                conversation_id=self.conversation_id,
                user_message_id=self.message_id,
                history_until_sequence=1,
                idempotency_key="submit-1",
                request_fingerprint="a" * 64,
            ))

    # 作用：测试结束后关闭连接并释放内存数据库。
    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    # 作用：确认会话按用户过滤，并在同一事务内推进消息序号。
    async def test_conversation_lock_and_sequence(self) -> None:
        async with self.sessions() as session, session.begin():
            self.assertIsNone(await lock_conversation(
                session, user_id=new_id(), conversation_id=self.conversation_id
            ))
            conversation = await lock_conversation(
                session, user_id=self.user_id, conversation_id=self.conversation_id
            )
            self.assertIsNotNone(conversation)
            self.assertEqual(reserve_message_sequence(conversation), 2)
            self.assertEqual(reserve_message_sequence(conversation), 3)
        async with self.sessions() as session:
            conversation = await session.get(Conversation, self.conversation_id)
            self.assertEqual(conversation.next_message_sequence, 4)

    # 作用：验证用户范围内的提交幂等键由数据库唯一约束保护。
    async def test_chat_idempotency_unique_constraint(self) -> None:
        async with self.sessions() as session:
            with self.assertRaises(IntegrityError):
                async with session.begin():
                    session.add(ChatRequest(
                        id=new_id(),
                        user_id=self.user_id,
                        conversation_id=self.conversation_id,
                        user_message_id=self.message_id,
                        history_until_sequence=1,
                        idempotency_key="submit-1",
                        request_fingerprint="b" * 64,
                    ))
        async with self.sessions() as session:
            result = await session.execute(select(ChatRequest).where(ChatRequest.user_id == self.user_id))
            self.assertEqual(len(result.scalars().all()), 1)

    # 作用：验证业务写入失败会回滚同一事务中已插入的 Outbox 事件。
    async def test_failed_task_insert_rolls_back_outbox(self) -> None:
        event_id = new_id()
        async with self.sessions() as session:
            with self.assertRaises(IntegrityError):
                async with session.begin():
                    session.add(OutboxEvent(
                        id=event_id,
                        event_type="chat.requested",
                        destination=OutboxDestination.RABBITMQ.value,
                        routing_key="chat.requests",
                        aggregate_type="chat_request",
                        aggregate_id=self.request_id,
                        payload={"request_id": self.request_id},
                    ))
                    await session.flush()
                    session.add(ChatRequest(
                        id=new_id(),
                        user_id=self.user_id,
                        conversation_id=self.conversation_id,
                        user_message_id=self.message_id,
                        history_until_sequence=1,
                        idempotency_key="submit-1",
                        request_fingerprint="b" * 64,
                    ))
        async with self.sessions() as session:
            self.assertIsNone(await session.get(OutboxEvent, event_id))

    # 作用：验证重复领取、过早重试和旧 attempt 写入均被条件更新拒绝。
    async def test_task_lease_fences_previous_attempt(self) -> None:
        now = utc_now()
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-a",
                lease_seconds=30, now=now,
            ))
        async with self.sessions() as session, session.begin():
            self.assertFalse(await claim_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-b",
                lease_seconds=30, now=now,
            ))
            self.assertFalse(await renew_task_lease(
                session, ChatRequest, task_id=self.request_id, owner="worker-b",
                attempt=1, lease_seconds=30, now=now,
            ))
            self.assertTrue(await transition_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-a",
                attempt=1, target=TaskStatus.RETRY_WAIT,
                available_at=now + timedelta(seconds=10), now=now,
            ))
        async with self.sessions() as session, session.begin():
            self.assertFalse(await claim_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-b",
                lease_seconds=30, now=now + timedelta(seconds=5),
            ))
            self.assertTrue(await claim_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-b",
                lease_seconds=30, now=now + timedelta(seconds=11),
            ))
            self.assertFalse(await transition_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-a",
                attempt=1, target=TaskStatus.SUCCEEDED, now=now + timedelta(seconds=12),
            ))
            self.assertTrue(await transition_task(
                session, ChatRequest, task_id=self.request_id, owner="worker-b",
                attempt=2, target=TaskStatus.SUCCEEDED, now=now + timedelta(seconds=12),
            ))
        async with self.sessions() as session:
            task = await session.get(ChatRequest, self.request_id)
            self.assertEqual((task.status, task.attempt), (TaskStatus.SUCCEEDED.value, 2))

    # 作用：验证 Outbox 领取、延迟重试和租约持有者校验。
    async def test_outbox_claim_retry_and_confirmation(self) -> None:
        now = utc_now()
        event_id = new_id()
        async with self.sessions() as session, session.begin():
            session.add(OutboxEvent(
                id=event_id,
                event_type="chat.requested",
                destination=OutboxDestination.RABBITMQ.value,
                routing_key="chat.requests",
                aggregate_type="chat_request",
                aggregate_id=self.request_id,
                payload={"request_id": self.request_id},
                available_at=now,
            ))
        async with self.sessions() as session, session.begin():
            events = await claim_due_events(
                session, owner="publisher-a", lease_seconds=30, limit=10, now=now
            )
            self.assertEqual([event.id for event in events], [event_id])
        async with self.sessions() as session, session.begin():
            self.assertEqual(await claim_due_events(
                session, owner="publisher-b", lease_seconds=30, limit=10, now=now
            ), [])
            self.assertTrue(await reschedule_event(
                session, event_id=event_id, owner="publisher-a",
                available_at=now + timedelta(seconds=10), error="broker unavailable", now=now,
            ))
        async with self.sessions() as session, session.begin():
            self.assertEqual(await claim_due_events(
                session, owner="publisher-b", lease_seconds=30, limit=10,
                now=now + timedelta(seconds=5),
            ), [])
            events = await claim_due_events(
                session, owner="publisher-b", lease_seconds=30, limit=10,
                now=now + timedelta(seconds=11),
            )
            self.assertEqual(len(events), 1)
            self.assertFalse(await mark_event_sent(
                session, event_id=event_id, owner="publisher-a", now=now + timedelta(seconds=12)
            ))
            self.assertTrue(await mark_event_sent(
                session, event_id=event_id, owner="publisher-b", now=now + timedelta(seconds=12)
            ))
        async with self.sessions() as session:
            event = await session.get(OutboxEvent, event_id)
            self.assertEqual(event.status, "sent")
            self.assertEqual(event.attempt_count, 2)


if __name__ == "__main__":
    unittest.main()
