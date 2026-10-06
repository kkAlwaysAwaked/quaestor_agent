"""Ingest 消费者：领取任务、入库、版本发布、可靠投递与租约恢复。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sys
from datetime import timedelta
from uuid import UUID, uuid4

import aio_pika
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import (
    load_database_settings, load_qdrant_runtime_settings,
    load_rabbit_runtime_settings, load_task_lease_settings,
)
from infra.mysql.base import utc_now
from infra.mysql.models import Document, DocumentVersion, IngestJob, OutboxEvent
from infra.mysql.repositories.documents import fail_version, publish_version, save_parent_chunks
from infra.mysql.repositories.outbox import claim_due_events, mark_event_sent, reschedule_event
from infra.mysql.repositories.tasks import claim_task, lock_expired_tasks, renew_task_lease, transition_task
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.mysql.status import DocumentVersionStatus, OutboxDestination, TaskStatus
from infra.topology import INGEST_QUEUE, JOBS_EXCHANGE
from services.ingest.create_database import EmbeddingEncoder, PROCESSING_CONFIG, split_document
from services.ingest.submission import resolve_storage_key
from services.ingest.vector_store import IngestVectorStore


LOG = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


# 作用：按执行次数计算有上限的重试退避时间。
def retry_delay(attempt: int) -> timedelta:
    return timedelta(seconds=min(300, 10 * 2 ** max(0, attempt - 1)))


# 作用：在当前数据库事务中写入一次待发布的入库通知。
def add_ingest_event(session: AsyncSession, *, job_id: str, available_at=None) -> None:
    session.add(OutboxEvent(
        event_type="ingest.requested", destination=OutboxDestination.RABBITMQ.value,
        routing_key=INGEST_QUEUE, aggregate_type="ingest_job", aggregate_id=job_id,
        payload={"schema_version": 1, "job_id": job_id},
        available_at=available_at or utc_now(),
    ))


# 作用：定期续租；续租失败时通知主处理流程停止发布结果。
async def keep_lease(
    sessions: async_sessionmaker[AsyncSession], *, job_id: str, owner: str,
    attempt: int, lease_seconds: int, stop: asyncio.Event, lost: asyncio.Event,
) -> None:
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(1, lease_seconds // 3))
            return
        except TimeoutError:
            pass
        try:
            async with sessions() as session, session.begin():
                valid = await renew_task_lease(
                    session, IngestJob, task_id=job_id, owner=owner,
                    attempt=attempt, lease_seconds=lease_seconds,
                )
            if not valid:
                lost.set()
                return
        except Exception:
            LOG.exception("入库任务续租失败，停止当前执行: %s", job_id)
            lost.set()
            return


# 作用：核对任务、文件哈希和切分配置，写入父块及向量后原子发布版本。
async def execute_ingest(
    sessions: async_sessionmaker[AsyncSession], vector_store: IngestVectorStore,
    encoder: EmbeddingEncoder, *, job_id: str, owner: str, attempt: int,
    lease_seconds: int,
) -> bool:
    stop = asyncio.Event()
    lost = asyncio.Event()
    heartbeat = asyncio.create_task(keep_lease(
        sessions, job_id=job_id, owner=owner, attempt=attempt,
        lease_seconds=lease_seconds, stop=stop, lost=lost,
    ))
    try:
        async with sessions() as session:
            job = await session.get(IngestJob, job_id)
            if job is None:
                raise ValueError("入库任务不存在")
            version = await session.get(DocumentVersion, job.version_id)
            document = await session.get(Document, job.document_id)
            if (
                version is None or document is None or version.document_id != document.id
                or document.user_id != job.user_id
                or version.status != DocumentVersionStatus.PROCESSING.value
            ):
                raise ValueError("入库任务的用户、文档或处理中版本不一致")
            if version.processing_config != PROCESSING_CONFIG:
                raise ValueError("文档版本的切分或模型配置与当前 Worker 不一致")
            storage_key = version.storage_key
            expected_sha = version.file_sha256
            user_id, document_id, version_id = job.user_id, job.document_id, job.version_id
            source_name = document.original_filename
        file_data = await asyncio.to_thread(resolve_storage_key(storage_key).read_bytes)
        if hashlib.sha256(file_data).hexdigest() != expected_sha:
            raise ValueError("上传文件内容与文档版本哈希不一致")
        plan = await asyncio.to_thread(split_document, file_data.decode("utf-8"), version_id=version_id)
        if lost.is_set():
            return False
        async with sessions() as session, session.begin():
            if not await renew_task_lease(
                session, IngestJob, task_id=job_id, owner=owner,
                attempt=attempt, lease_seconds=lease_seconds,
            ):
                return False
            await save_parent_chunks(
                session, document_id=document_id, version_id=version_id,
                source=source_name, parents=plan.parents,
            )
        points = await asyncio.to_thread(
            encoder.encode, plan, user_id=user_id,
            document_id=document_id, version_id=version_id,
        )
        if lost.is_set():
            return False
        await vector_store.upsert(points)
        await vector_store.verify(
            version_id=version_id, user_id=user_id, document_id=document_id,
            point_to_parent={child.id: child.parent_id for child in plan.children},
        )
        if lost.is_set():
            return False
        async with sessions() as session, session.begin():
            job = await session.get(IngestJob, job_id)
            if job is None:
                raise ValueError("入库任务不存在")
            return await publish_version(
                session, job=job, owner=owner, attempt=attempt,
                expected_parent_count=len(plan.parents),
            )
    finally:
        stop.set()
        await heartbeat


# 作用：把处理错误持久化为延迟重试或最终失败，返回是否应进入死信队列。
async def record_failure(
    sessions: async_sessionmaker[AsyncSession], *, job_id: str, owner: str,
    attempt: int, error: Exception,
) -> bool:
    final = attempt >= MAX_ATTEMPTS or isinstance(
        error, (ValueError, UnicodeError, FileNotFoundError)
    )
    message = f"{type(error).__name__}: {error}"[:2000]
    async with sessions() as session, session.begin():
        job = await session.get(IngestJob, job_id)
        if job is None:
            return True
        if final:
            changed = await fail_version(
                session, job=job, owner=owner, attempt=attempt, error=message
            )
        else:
            due = utc_now() + retry_delay(attempt)
            changed = await transition_task(
                session, IngestJob, task_id=job_id, owner=owner, attempt=attempt,
                target=TaskStatus.RETRY_WAIT, available_at=due, error=message,
            )
            if changed:
                add_ingest_event(session, job_id=job_id, available_at=due)
        if not changed:
            LOG.warning("旧租约不能写入失败状态: %s attempt=%s", job_id, attempt)
            return False
    return final


# 作用：校验队列通知，领取数据库任务并在结果持久化后 ACK 或死信。
async def handle_message(
    message: aio_pika.IncomingMessage, sessions: async_sessionmaker[AsyncSession],
    vector_store: IngestVectorStore, encoder: EmbeddingEncoder,
    *, owner: str, lease_seconds: int,
) -> None:
    try:
        payload = json.loads(message.body)
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise ValueError("不支持的 IngestJob schema_version")
        if not isinstance(payload.get("job_id"), str):
            raise ValueError("IngestJob job_id 必须是 UUID 字符串")
        job_id = str(UUID(payload["job_id"]))
    except (ValueError, KeyError, TypeError, UnicodeError) as exc:
        LOG.warning("非法入库消息进入死信队列: %s", exc)
        await message.reject(requeue=False)
        return
    try:
        async with sessions() as session, session.begin():
            job = await session.get(IngestJob, job_id)
            if job is None:
                await message.reject(requeue=False)
                return
            claimed = await claim_task(
                session, IngestJob, task_id=job_id, owner=owner,
                lease_seconds=lease_seconds,
            )
            if claimed:
                await session.refresh(job)
                attempt = job.attempt
        if not claimed:
            await message.ack()
            return
        try:
            completed = await execute_ingest(
                sessions, vector_store, encoder, job_id=job_id,
                owner=owner, attempt=attempt, lease_seconds=lease_seconds,
            )
        except Exception as exc:
            LOG.exception("入库任务执行失败: %s", job_id)
            dead_letter = await record_failure(
                sessions, job_id=job_id, owner=owner, attempt=attempt, error=exc,
            )
            if dead_letter:
                await message.reject(requeue=False)
            else:
                await message.ack()
            return
        if not completed:
            LOG.warning("入库执行权已丢失: %s attempt=%s", job_id, attempt)
        await message.ack()
    except Exception:
        LOG.exception("入库消息尚未可靠提交，交由 RabbitMQ 重投: %s", job_id)
        if not message.processed:
            await message.nack(requeue=True)


# 作用：领取已到期的入库 Outbox，获得 RabbitMQ 发布确认后标记已发送。
async def publish_outbox(
    sessions: async_sessionmaker[AsyncSession], exchange: aio_pika.Exchange,
    *, owner: str, lease_seconds: int,
) -> None:
    while True:
        async with sessions() as session, session.begin():
            events = await claim_due_events(
                session, owner=owner, lease_seconds=lease_seconds,
                limit=20, routing_key=INGEST_QUEUE,
                destination=OutboxDestination.RABBITMQ.value,
            )
            snapshots = [(event.id, event.payload, event.attempt_count) for event in events]
        for event_id, payload, attempt in snapshots:
            try:
                confirmed = await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode("utf-8"),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type="application/json", message_id=event_id,
                    ), routing_key=INGEST_QUEUE, mandatory=True, timeout=10,
                )
                if confirmed is False:
                    raise RuntimeError("RabbitMQ 未确认持久化发布")
                async with sessions() as session, session.begin():
                    await mark_event_sent(session, event_id=event_id, owner=owner)
            except Exception as exc:
                LOG.exception("入库 Outbox 发布失败: %s", event_id)
                async with sessions() as session, session.begin():
                    await reschedule_event(
                        session, event_id=event_id, owner=owner,
                        available_at=utc_now() + retry_delay(attempt), error=str(exc)[:2000],
                    )
        await asyncio.sleep(1 if snapshots else 3)


# 作用：在调用方事务内恢复一批过期任务，并同步写入重试 Outbox 或最终失败状态。
async def recover_expired_once(session: AsyncSession, *, limit: int = 50) -> int:
    expired = await lock_expired_tasks(session, IngestJob, limit=limit)
    for job in expired:
        job.lease_owner = None
        job.lease_expires_at = None
        job.last_error = "执行租约过期"
        if job.attempt >= MAX_ATTEMPTS:
            job.status = TaskStatus.FAILED.value
            version = await session.get(DocumentVersion, job.version_id)
            if version and version.status == DocumentVersionStatus.PROCESSING.value:
                version.status = DocumentVersionStatus.FAILED.value
        else:
            job.status = TaskStatus.RETRY_WAIT.value
            job.available_at = utc_now() + retry_delay(job.attempt)
            add_ingest_event(session, job_id=job.id, available_at=job.available_at)
    return len(expired)


# 作用：周期性运行过期租约恢复事务，使 Worker 退出后的任务重新得到通知。
async def recover_expired(
    sessions: async_sessionmaker[AsyncSession], *, interval_seconds: int = 10,
) -> None:
    while True:
        async with sessions() as session, session.begin():
            await recover_expired_once(session)
        await asyncio.sleep(interval_seconds)


# 作用：以每次一条的并发度消费入库队列，交由处理函数决定 ACK 或死信。
async def consume_messages(
    queue: aio_pika.Queue, sessions: async_sessionmaker[AsyncSession],
    vector_store: IngestVectorStore, encoder: EmbeddingEncoder, *, lease_seconds: int,
) -> None:
    async with queue.iterator() as messages:
        async for message in messages:
            await handle_message(
                message, sessions, vector_store, encoder,
                owner=f"ingest-worker-{uuid4()}", lease_seconds=lease_seconds,
            )


# 作用：启动入库专用发布循环、恢复循环和手动确认的 RabbitMQ 消费者。
async def run_worker() -> None:
    rabbit = load_rabbit_runtime_settings()
    qdrant = load_qdrant_runtime_settings()
    lease = load_task_lease_settings()
    engine = create_mysql_engine(load_database_settings())
    sessions = create_session_factory(engine)
    vector_store = None
    connection = None
    background: list[asyncio.Task] = []
    try:
        vector_store = IngestVectorStore(url=qdrant.url, api_key=qdrant.api_key)
        connection = await aio_pika.connect_robust(
            host=rabbit.host, port=rabbit.port,
            login=rabbit.user, password=rabbit.password,
        )
        encoder = await asyncio.to_thread(EmbeddingEncoder)
        publish_channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        exchange = await publish_channel.declare_exchange(
            JOBS_EXCHANGE, aio_pika.ExchangeType.DIRECT, passive=True,
        )
        consume_channel = await connection.channel()
        await consume_channel.set_qos(prefetch_count=1)
        queue = await consume_channel.declare_queue(INGEST_QUEUE, passive=True)
        background = [
            asyncio.create_task(publish_outbox(
                sessions, exchange, owner=f"ingest-publisher-{uuid4()}",
                lease_seconds=lease.lease_seconds,
            )),
            asyncio.create_task(recover_expired(sessions)),
            asyncio.create_task(consume_messages(
                queue, sessions, vector_store, encoder,
                lease_seconds=lease.lease_seconds,
            )),
        ]
        finished, _ = await asyncio.wait(background, return_when=asyncio.FIRST_COMPLETED)
        for task in finished:
            task.result()
        raise RuntimeError("入库 Worker 的一个关键循环意外退出")
    finally:
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        if connection is not None:
            await connection.close()
        if vector_store is not None:
            await vector_store.close()
        await engine.dispose()


# 作用：从命令行启动入库 Worker 并开启可读日志。
def main() -> None:
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
