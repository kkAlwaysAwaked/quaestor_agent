"""验证入库任务的确定性、幂等提交和 MySQL 发布边界。"""

from __future__ import annotations

import tempfile
import unittest
import os
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from core.config import PROJECT_ROOT, load_qdrant_runtime_settings, load_rabbit_runtime_settings
from infra.mysql.base import Base, new_id, utc_now
from infra.mysql.models import Document, DocumentVersion, IngestJob, OutboxEvent, ParentChunk, User
from infra.mysql.repositories.tasks import claim_task
from infra.mysql.session import create_session_factory
from infra.mysql.status import DocumentVersionStatus, TaskStatus
from services.ingest.create_database import split_document
from services.ingest.submission import resolve_storage_key, submit_markdown
from services.ingest.vector_store import IngestVectorStore
from services.ingest.worker import execute_ingest, handle_message, record_failure, recover_expired_once


class FakeEncoder:
    # 作用：为测试返回稳定点 ID，避免下载真实模型。
    def encode(self, plan, *, user_id: str, document_id: str, version_id: str):
        return [SimpleNamespace(id=child.id) for child in plan.children]


class FakeVectorStore:
    # 作用：建立内存向量点集合，模拟 Qdrant 的写入和校验。
    def __init__(self):
        self.points = {}

    # 作用：按点 ID 覆盖写入，模拟幂等 upsert。
    async def upsert(self, points):
        self.points.update({point.id: point for point in points})

    # 作用：确认所有预期点均存在于内存向量集合中。
    async def verify(self, *, version_id: str, user_id: str, document_id: str, point_to_parent: dict[str, str]):
        if set(point_to_parent) != set(self.points):
            raise RuntimeError("向量点缺失")


class FailingVectorStore(FakeVectorStore):
    # 作用：模拟 Qdrant 部分写入后校验失败。
    async def verify(self, **kwargs):
        raise RuntimeError("Qdrant 仅写入部分点")


class FakeQdrantClient:
    # 作用：准备一个包含指定归属载荷的假 Qdrant 点。
    def __init__(self, point_id: str, payload: dict):
        self.point_id = point_id
        self.payload = payload

    # 作用：按假存储中的点数响应精确计数请求。
    def count(self, **kwargs):
        return SimpleNamespace(count=1)

    # 作用：返回带载荷的假点以供入库校验逻辑检查。
    def retrieve(self, **kwargs):
        return [SimpleNamespace(id=self.point_id, payload=self.payload)]


class FakeMessage:
    # 作用：构造无需 RabbitMQ 连接的消息替身。
    def __init__(self, body: bytes):
        self.body = body
        self.rejected = False

    # 作用：记录坏消息是否被拒绝且不再重入队列。
    async def reject(self, *, requeue: bool):
        self.rejected = not requeue


class IngestSettingsTests(unittest.TestCase):
    # 作用：确认入库运行时配置只要求 RabbitMQ 与 Qdrant 密钥，不依赖 Redis。
    def test_settings_are_component_scoped(self):
        with patch.dict(os.environ, {
            "RABBITMQ_USER": "worker", "RABBITMQ_PASSWORD": "rabbit-secret",
            "QDRANT_API_KEY": "qdrant-secret",
        }, clear=True), patch("core.config._dotenv_values", return_value={}):
            self.assertEqual(load_rabbit_runtime_settings().user, "worker")
            self.assertEqual(load_qdrant_runtime_settings().api_key, "qdrant-secret")


class IngestComponentTests(unittest.IsolatedAsyncioTestCase):
    # 作用：创建隔离的 SQLite 数据库和临时上传目录。
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.upload_patch = patch("services.ingest.submission.upload_root", return_value=Path(self.temp.name))
        self.upload_patch.start()
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = create_session_factory(self.engine)
        self.user_id = new_id()
        async with self.sessions() as session, session.begin():
            session.add(User(id=self.user_id, email="ingest@example.com", password_hash="!"))
        self.fixture = PROJECT_ROOT / "tests" / "fixtures" / "employee_handbook.md"

    # 作用：关闭测试数据库并移除临时文件。
    async def asyncTearDown(self):
        await self.engine.dispose()
        self.upload_patch.stop()
        self.temp.cleanup()

    # 作用：确认相同版本重试得到完全相同的父块和子块 ID。
    async def test_split_ids_are_stable(self):
        text = self.fixture.read_text(encoding="utf-8")
        first = split_document(text, version_id=self.user_id)
        second = split_document(text, version_id=self.user_id)
        other = split_document(text, version_id=new_id())
        self.assertEqual(first, second)
        self.assertNotEqual(first.parents[0].id, other.parents[0].id)
        self.assertTrue(first.children)

    # 作用：确认上传文件标识不能跳出统一上传根目录。
    async def test_storage_key_rejects_traversal(self):
        with self.assertRaises(ValueError):
            resolve_storage_key("../outside.md")

    # 作用：确认非对象或坏 UUID 的队列消息直接死信，不拖垮 Worker。
    async def test_malformed_queue_message_is_rejected(self):
        for body in (b"[]", b'{"schema_version":1,"job_id":"broken"}'):
            message = FakeMessage(body)
            await handle_message(
                message, None, None, None, owner="test", lease_seconds=1,
            )
            self.assertTrue(message.rejected)

    # 作用：确认 Qdrant 完整性检查会拒绝用户或父块归属不匹配的点。
    async def test_vector_verification_checks_payload_ownership(self):
        point_id = new_id()
        parent_id = new_id()
        client = FakeQdrantClient(point_id, {
            "version_id": self.user_id, "user_id": self.user_id,
            "document_id": self.user_id, "parent_id": parent_id,
        })
        store = IngestVectorStore.__new__(IngestVectorStore)
        store.client = client
        await store.verify(
            version_id=self.user_id, user_id=self.user_id,
            document_id=self.user_id, point_to_parent={point_id: parent_id},
        )
        client.payload["user_id"] = new_id()
        with self.assertRaises(RuntimeError):
            await store.verify(
                version_id=self.user_id, user_id=self.user_id,
                document_id=self.user_id, point_to_parent={point_id: parent_id},
            )

    # 作用：确认重复提交只产生一份任务和事件，并拒绝同键不同内容。
    async def test_submit_idempotency(self):
        job_id = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="same"
        )
        repeat = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="same"
        )
        self.assertEqual(job_id, repeat)
        async with self.sessions() as session:
            jobs = (await session.scalars(select(IngestJob))).all()
            events = (await session.scalars(select(OutboxEvent))).all()
            self.assertEqual((len(jobs), len(events)), (1, 1))
            self.assertTrue(resolve_storage_key(
                (await session.get(DocumentVersion, jobs[0].version_id)).storage_key
            ).is_file())
        changed = Path(self.temp.name) / "changed.md"
        changed.write_text("另一个文件", encoding="utf-8")
        with self.assertRaises(ValueError):
            await submit_markdown(
                self.sessions, user_id=self.user_id, source=changed, idempotency_key="same"
            )

    # 作用：模拟完整入库，确认向量校验后才切换文档当前版本并完成任务。
    async def test_execute_publishes_only_after_vector_verification(self):
        job_id = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="publish"
        )
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, IngestJob, task_id=job_id, owner="worker-a", lease_seconds=30
            ))
            job = await session.get(IngestJob, job_id)
            await session.refresh(job)
            attempt = job.attempt
        vectors = FakeVectorStore()
        self.assertTrue(await execute_ingest(
            self.sessions, vectors, FakeEncoder(), job_id=job_id,
            owner="worker-a", attempt=attempt, lease_seconds=30,
        ))
        async with self.sessions() as session:
            job = await session.get(IngestJob, job_id)
            document = await session.get(Document, job.document_id)
            version = await session.get(DocumentVersion, job.version_id)
            parents = (await session.scalars(select(ParentChunk))).all()
            self.assertEqual(job.status, TaskStatus.SUCCEEDED.value)
            self.assertEqual(version.status, DocumentVersionStatus.PUBLISHED.value)
            self.assertEqual(document.current_version_id, version.id)
            self.assertEqual(len(parents), 2)
            self.assertTrue(vectors.points)

    # 作用：确认向量库部分写入时父块可保留供重试，但版本不可见。
    async def test_partial_vector_failure_never_publishes(self):
        job_id = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="partial"
        )
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, IngestJob, task_id=job_id, owner="worker-a", lease_seconds=30
            ))
            job = await session.get(IngestJob, job_id)
            await session.refresh(job)
            attempt = job.attempt
        vectors = FailingVectorStore()
        with self.assertRaises(RuntimeError):
            await execute_ingest(
                self.sessions, vectors, FakeEncoder(), job_id=job_id,
                owner="worker-a", attempt=attempt, lease_seconds=30,
            )
        async with self.sessions() as session:
            job = await session.get(IngestJob, job_id)
            version = await session.get(DocumentVersion, job.version_id)
            document = await session.get(Document, job.document_id)
            self.assertEqual(version.status, DocumentVersionStatus.PROCESSING.value)
            self.assertIsNone(document.current_version_id)
            self.assertTrue((await session.scalars(select(ParentChunk))).all())
        self.assertFalse(await record_failure(
            self.sessions, job_id=job_id, owner="worker-a",
            attempt=attempt, error=RuntimeError("Qdrant 部分写入"),
        ))

    # 作用：验证暂时错误产生延迟 Outbox，坏文件最终失败且版本始终不可见。
    async def test_failure_retry_and_final_failure(self):
        job_id = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="failure"
        )
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, IngestJob, task_id=job_id, owner="worker-a", lease_seconds=30
            ))
        self.assertFalse(await record_failure(
            self.sessions, job_id=job_id, owner="worker-a",
            attempt=1, error=RuntimeError("Qdrant 暂不可用"),
        ))
        async with self.sessions() as session:
            job = await session.get(IngestJob, job_id)
            version = await session.get(DocumentVersion, job.version_id)
            events = (await session.scalars(select(OutboxEvent))).all()
            self.assertEqual(job.status, TaskStatus.RETRY_WAIT.value)
            self.assertEqual(version.status, DocumentVersionStatus.PROCESSING.value)
            self.assertEqual(len(events), 2)
            due = job.available_at
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, IngestJob, task_id=job_id, owner="worker-b",
                lease_seconds=30, now=due,
            ))
        self.assertTrue(await record_failure(
            self.sessions, job_id=job_id, owner="worker-b",
            attempt=2, error=ValueError("坏文档"),
        ))
        async with self.sessions() as session:
            job = await session.get(IngestJob, job_id)
            version = await session.get(DocumentVersion, job.version_id)
            document = await session.get(Document, job.document_id)
            self.assertEqual(job.status, TaskStatus.FAILED.value)
            self.assertEqual(version.status, DocumentVersionStatus.FAILED.value)
            self.assertIsNone(document.current_version_id)

    # 作用：模拟 Worker 退出后租约过期，验证恢复事务写入重试任务和 Outbox。
    async def test_expired_lease_is_recovered(self):
        job_id = await submit_markdown(
            self.sessions, user_id=self.user_id, source=self.fixture, idempotency_key="recovery"
        )
        now = utc_now()
        async with self.sessions() as session, session.begin():
            self.assertTrue(await claim_task(
                session, IngestJob, task_id=job_id, owner="crashed-worker",
                lease_seconds=1, now=now,
            ))
            job = await session.get(IngestJob, job_id)
            await session.refresh(job)
            job.lease_expires_at = now - timedelta(seconds=5)
        async with self.sessions() as session, session.begin():
            self.assertEqual(await recover_expired_once(session), 1)
        async with self.sessions() as session:
            job = await session.get(IngestJob, job_id)
            events = (await session.scalars(select(OutboxEvent))).all()
            self.assertEqual(job.status, TaskStatus.RETRY_WAIT.value)
            self.assertIsNone(job.lease_owner)
            self.assertEqual(len(events), 2)


if __name__ == "__main__":
    unittest.main()
