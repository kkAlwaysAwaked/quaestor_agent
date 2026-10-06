"""创建单份 Markdown 入库任务：文件落盘后原子写入版本、任务和 Outbox。"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from core.config import upload_root
from infra.mysql.base import new_id
from infra.mysql.models import Document, DocumentVersion, IngestJob, OutboxEvent, User
from infra.mysql.status import DocumentVersionStatus, OutboxDestination
from infra.topology import INGEST_QUEUE
from services.ingest.create_database import PROCESSING_CONFIG


# 作用：把数据库保存的相对文件标识解析到上传目录，并拒绝路径越界。
def resolve_storage_key(storage_key: str) -> Path:
    root = upload_root().resolve()
    path = (root / storage_key).resolve()
    if not path.is_relative_to(root) or path == root:
        raise ValueError("非法上传文件标识：路径越界")
    return path


# 作用：为指定用户创建可重试的 Markdown 入库任务，重复幂等键返回原任务。
async def submit_markdown(
    sessions: async_sessionmaker[AsyncSession], *, user_id: str, source: Path,
    idempotency_key: str, document_id: str | None = None,
) -> str:
    source = source.resolve()
    if source.suffix.lower() != ".md" or not source.is_file():
        raise ValueError("第一版入库只接受存在的 Markdown 文件")
    contents = source.read_bytes()
    if not contents or len(contents) > 10 * 1024 * 1024:
        raise ValueError("Markdown 文件必须非空且不超过 10 MiB")
    contents.decode("utf-8")
    digest = hashlib.sha256(contents).hexdigest()
    fingerprint = hashlib.sha256(
        f"{user_id}:{document_id or 'new'}:{source.name}:{digest}".encode("utf-8")
    ).hexdigest()
    file_path: Path | None = None
    try:
        async with sessions() as session, session.begin():
            existing = await session.scalar(select(IngestJob).where(
                IngestJob.user_id == user_id, IngestJob.idempotency_key == idempotency_key
            ))
            if existing:
                if existing.request_fingerprint != fingerprint:
                    raise ValueError("同一幂等键对应不同文件或文档")
                return existing.id
            if await session.get(User, user_id) is None:
                raise ValueError("用户不存在")
            document = await session.get(Document, document_id, with_for_update=True) if document_id else None
            if document_id and (document is None or document.user_id != user_id):
                raise ValueError("文档不存在或不属于当前用户")
            if document:
                if document.original_filename != source.name:
                    raise ValueError("新版本的来源文件名必须与文档原文件名一致")
                active = await session.scalar(select(DocumentVersion.id).where(
                    DocumentVersion.document_id == document.id,
                    DocumentVersion.status == DocumentVersionStatus.PROCESSING.value,
                ))
                if active:
                    raise ValueError("该文档已有处理中版本")
            doc_id = document.id if document else new_id()
            version_id = new_id()
            job_id = new_id()
            storage_key = f"{user_id}/{doc_id}/{version_id}.md"
            file_path = resolve_storage_key(storage_key)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = file_path.with_suffix(".tmp")
            try:
                temporary.write_bytes(contents)
                os.replace(temporary, file_path)
            finally:
                temporary.unlink(missing_ok=True)
            if document is None:
                session.add(Document(
                    id=doc_id, user_id=user_id, original_filename=source.name,
                    storage_key=storage_key,
                ))
                await session.flush()
            session.add(DocumentVersion(
                id=version_id, document_id=doc_id, storage_key=storage_key,
                file_sha256=digest, processing_config=dict(PROCESSING_CONFIG),
            ))
            await session.flush()
            session.add(IngestJob(
                id=job_id, user_id=user_id, document_id=doc_id, version_id=version_id,
                idempotency_key=idempotency_key, request_fingerprint=fingerprint,
            ))
            session.add(OutboxEvent(
                event_type="ingest.requested", destination=OutboxDestination.RABBITMQ.value,
                routing_key=INGEST_QUEUE, aggregate_type="ingest_job", aggregate_id=job_id,
                payload={"schema_version": 1, "job_id": job_id},
            ))
        return job_id
    except Exception:
        if file_path is not None:
            file_path.unlink(missing_ok=True)
        raise
