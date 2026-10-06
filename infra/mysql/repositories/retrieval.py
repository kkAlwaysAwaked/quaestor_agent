"""检索请求授权、版本范围和检索结果租约；所有事务由应用层控制。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.index_config import DENSE_MODEL, SPARSE_MODEL
from core.retrieval_contracts import RetrieveRequest, RetrievalError, RetrievalScope
from infra.mysql.base import new_id, utc_now
from infra.mysql.models import ChatRequest, Conversation, Document, DocumentVersion, Message, ParentChunk, RetrievalRun
from infra.mysql.status import DocumentVersionStatus, RetrievalStatus, TaskStatus


@dataclass(frozen=True)
class RetrievalClaim:
    run_id: str
    attempt: int
    scope: RetrievalScope | None = None
    cached_result: dict | None = None


# 作用：读取用户当前已发布版本，并拒绝入库与查询使用不同向量模型的情况。
async def load_published_scope(session: AsyncSession, *, user_id: str) -> RetrievalScope:
    versions = (await session.scalars(
        select(DocumentVersion).join(Document, Document.id == DocumentVersion.document_id).where(
            Document.user_id == user_id,
            Document.current_version_id == DocumentVersion.id,
            DocumentVersion.status == DocumentVersionStatus.PUBLISHED.value,
        ).order_by(DocumentVersion.id)
    )).all()
    for version in versions:
        config = version.processing_config
        if config.get("dense_model") != DENSE_MODEL or config.get("sparse_model") != SPARSE_MODEL:
            raise RetrievalError(409, "index_model_mismatch", "已发布版本的向量模型与检索配置不一致")
    return RetrievalScope(user_id, tuple(version.id for version in versions))


# 作用：核对任务归属及历史截止点，并在短事务中领取一次检索或读取已保存成功结果。
async def claim_retrieval(
    session: AsyncSession, *, payload: RetrieveRequest, user_id: str,
    owner: str, lease_seconds: int,
) -> RetrievalClaim:
    task = await session.scalar(select(ChatRequest).where(
        ChatRequest.id == str(payload.request_id), ChatRequest.user_id == user_id,
    ).with_for_update())
    if task is None:
        raise RetrievalError(404, "request_not_found", "聊天任务不存在或无权访问")
    conversation = await session.get(Conversation, task.conversation_id)
    if conversation is None or conversation.user_id != user_id:
        raise RetrievalError(404, "request_not_found", "聊天任务不存在或无权访问")
    history = (await session.scalars(select(Message).where(
        Message.conversation_id == task.conversation_id,
        Message.sequence <= task.history_until_sequence,
    ).order_by(Message.sequence.desc()).limit(len(payload.messages)))).all()
    history.reverse()
    incoming = [message.model_dump() for message in payload.messages]
    if (
        not history or history[-1].id != task.user_message_id
        or history[-1].request_id != task.id or history[-1].role != "user"
        or incoming != [{"role": message.role, "content": message.content} for message in history]
    ):
        raise RetrievalError(409, "history_mismatch", "检索上下文必须是截至本次提问的数据库历史后缀")
    input_data = payload.model_dump(mode="json")
    run = await session.scalar(select(RetrievalRun).where(
        RetrievalRun.request_id == task.id
    ).with_for_update())
    if run is not None:
        if run.user_id != user_id or run.input_data != input_data:
            raise RetrievalError(409, "retrieval_input_conflict", "同一 request_id 的检索输入不能改变")
        if run.status == RetrievalStatus.SUCCEEDED.value:
            if run.result_data is None:
                raise RetrievalError(409, "retrieval_snapshot_unavailable", "成功检索的结果快照缺失")
            return RetrievalClaim(run.id, run.attempt, cached_result=run.result_data)
        if run.status == RetrievalStatus.RUNNING.value and run.lease_expires_at and run.lease_expires_at > utc_now():
            raise RetrievalError(409, "retrieval_in_progress", "该请求正在检索，请稍后重试")
    current = utc_now()
    if task.status != TaskStatus.RUNNING.value or not task.lease_expires_at or task.lease_expires_at <= current:
        raise RetrievalError(409, "chat_task_not_running", "只有持有有效执行租约的聊天任务可以发起新检索")
    scope = await load_published_scope(session, user_id=user_id)
    if run is None:
        run = RetrievalRun(
            id=new_id(), request_id=task.id, user_id=user_id,
            search_query=payload.search_query, input_data=input_data, attempt=0,
        )
        session.add(run)
    run.status = RetrievalStatus.RUNNING.value
    run.attempt += 1
    run.lease_owner = owner
    run.lease_expires_at = current + timedelta(seconds=lease_seconds)
    run.last_error = None
    await session.flush()
    return RetrievalClaim(run.id, run.attempt, scope=scope)


# 作用：按用户、版本及发布状态批量读取父块正文，并保持传入的 RRF 排序。
async def fetch_parent_documents(
    session: AsyncSession, *, ranked_parents: list[tuple[str, float]],
    scope: RetrievalScope, require_current: bool = True,
) -> list[dict]:
    if not ranked_parents or not scope.version_ids:
        return []
    statement = select(ParentChunk).join(
        DocumentVersion, ParentChunk.version_id == DocumentVersion.id
    ).join(Document, ParentChunk.document_id == Document.id).where(
        ParentChunk.id.in_([parent_id for parent_id, _ in ranked_parents]),
        ParentChunk.version_id.in_(scope.version_ids),
        DocumentVersion.document_id == Document.id,
        DocumentVersion.status == DocumentVersionStatus.PUBLISHED.value,
        Document.user_id == scope.user_id,
    )
    if require_current:
        statement = statement.where(Document.current_version_id == DocumentVersion.id)
    rows = {row.id: row for row in (await session.scalars(statement)).all()}
    documents = []
    for parent_id, score in ranked_parents:
        row = rows.get(parent_id)
        if row is not None:
            documents.append({
                "id": row.id, "text": row.content, "metadata": row.source or "未知来源",
                "document_id": row.document_id, "version_id": row.version_id,
                "rrf_score": score,
            })
    return documents


# 作用：仅让有效租约持有者原子保存成功检索结果、来源版本及 trace。
async def finish_retrieval(
    session: AsyncSession, *, run_id: str, owner: str, attempt: int,
    result: dict, trace: dict,
) -> bool:
    documents = result["documents"]
    updated = await session.execute(update(RetrievalRun).where(
        RetrievalRun.id == run_id, RetrievalRun.status == RetrievalStatus.RUNNING.value,
        RetrievalRun.lease_owner == owner, RetrievalRun.attempt == attempt,
        RetrievalRun.lease_expires_at > utc_now(),
    ).values(
        status=RetrievalStatus.SUCCEEDED.value, result_data=result, trace_data=trace,
        retrieved_parent_ids=result["retrieved_parent_ids"],
        source_version_ids=sorted({document["version_id"] for document in documents}),
        lease_owner=None, lease_expires_at=None, last_error=None,
    ))
    return updated.rowcount == 1


# 作用：将当前检索尝试记为失败，允许相同输入重试且不误记为成功检索。
async def fail_retrieval(
    session: AsyncSession, *, run_id: str, owner: str, attempt: int, error_code: str,
) -> bool:
    updated = await session.execute(update(RetrievalRun).where(
        RetrievalRun.id == run_id, RetrievalRun.status == RetrievalStatus.RUNNING.value,
        RetrievalRun.lease_owner == owner, RetrievalRun.attempt == attempt,
        RetrievalRun.lease_expires_at > utc_now(),
    ).values(
        status=RetrievalStatus.FAILED.value, last_error=error_code,
        lease_owner=None, lease_expires_at=None,
    ))
    return updated.rowcount == 1
