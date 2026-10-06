"""入库父块与版本发布的 MySQL 操作；事务始终由调用方控制。"""

from __future__ import annotations

from typing import Protocol, Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from infra.mysql.base import utc_now
from infra.mysql.models import Document, DocumentVersion, IngestJob, ParentChunk
from infra.mysql.repositories.tasks import transition_task
from infra.mysql.status import DocumentVersionStatus, TaskStatus


class ParentPayload(Protocol):
    id: str
    index: int
    content: str


# 作用：在处理中版本下幂等保存父块，重复执行时核对 ID 和正文完全一致。
async def save_parent_chunks(
    session: AsyncSession, *, document_id: str, version_id: str, source: str,
    parents: Sequence[ParentPayload],
) -> None:
    result = await session.execute(select(ParentChunk).where(ParentChunk.version_id == version_id))
    existing = {chunk.chunk_index: chunk for chunk in result.scalars()}
    if existing and len(existing) != len(parents):
        raise ValueError("同一文档版本的父块数量发生变化；拒绝用不同切分结果覆盖")
    for parent in parents:
        old = existing.get(parent.index)
        if old:
            if old.id != parent.id or old.content != parent.content or old.document_id != document_id:
                raise ValueError("同一文档版本的父块内容发生变化；拒绝覆盖")
        else:
            session.add(ParentChunk(
                id=parent.id, document_id=document_id, version_id=version_id,
                chunk_index=parent.index, content=parent.content, source=source,
            ))
    await session.flush()


# 作用：仅允许当前有效租约的执行者原子发布版本、切换文档当前版本并完成任务。
async def publish_version(
    session: AsyncSession, *, job: IngestJob, owner: str, attempt: int,
    expected_parent_count: int,
) -> bool:
    document = await session.get(Document, job.document_id, with_for_update=True)
    version = await session.get(DocumentVersion, job.version_id, with_for_update=True)
    if document is None or version is None or version.document_id != document.id:
        raise ValueError("入库任务引用的文档或版本不存在")
    if version.status != DocumentVersionStatus.PROCESSING.value:
        return False
    parent_count = await session.scalar(select(func.count(ParentChunk.id)).where(
        ParentChunk.version_id == version.id
    ))
    if parent_count != expected_parent_count:
        raise RuntimeError("MySQL 父块数量与本次切分结果不一致")
    if document.current_version_id and document.current_version_id != version.id:
        current = await session.get(DocumentVersion, document.current_version_id)
        if current and current.created_at > version.created_at:
            raise ValueError("较新的文档版本已发布；拒绝旧版本覆盖")
    if not await transition_task(
        session, IngestJob, task_id=job.id, owner=owner,
        attempt=attempt, target=TaskStatus.SUCCEEDED,
    ):
        return False
    version.status = DocumentVersionStatus.PUBLISHED.value
    version.published_at = utc_now()
    document.current_version_id = version.id
    document.storage_key = version.storage_key
    await session.flush()
    return True


# 作用：在最终失败时把不可检索的版本标记失败，并由任务状态保留具体错误。
async def fail_version(session: AsyncSession, *, job: IngestJob, owner: str, attempt: int, error: str) -> bool:
    if not await transition_task(
        session, IngestJob, task_id=job.id, owner=owner,
        attempt=attempt, target=TaskStatus.FAILED, error=error,
    ):
        return False
    version = await session.get(DocumentVersion, job.version_id, with_for_update=True)
    if version and version.status == DocumentVersionStatus.PROCESSING.value:
        version.status = DocumentVersionStatus.FAILED.value
    await session.flush()
    return True
