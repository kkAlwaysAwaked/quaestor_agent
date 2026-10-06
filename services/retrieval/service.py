"""检索应用用例：控制受理量、持久化幂等结果并处理超时和取消。"""

# 回答：总超时、受理上限、事务和算法调用分别在哪里控制？

from __future__ import annotations

import asyncio
import logging

from pydantic import ValidationError

from core.config import RetrievalSettings
from core.retrieval_contracts import RetrieveRequest, RetrieveResponse, RetrievalError, RetrievalScope, build_retrieve_response
from core.service_auth import ServicePrincipal
from infra.mysql.base import new_id
from infra.mysql.repositories.retrieval import claim_retrieval, fail_retrieval, fetch_parent_documents, finish_retrieval


LOG = logging.getLogger(__name__)


# 一次检索流程的负责人：安排谁可以查，是否需要重新查，什么时候提交数据库，出错后怎样恢复
class RetrievalService:
    # 作用：准备流程所需要的资源
    # 资源由外部准备好，再交给这个类。
    # 分别保存：数据库会话工厂，检索算法流水线，超时、并发配置，当前正在处理的请求数
    def __init__(self, *, sessions, pipeline, settings: RetrievalSettings) -> None:
        self.sessions = sessions
        self.pipeline = pipeline
        self.settings = settings
        self.active = 0

    # 作用：核对凭据请求绑定、限制受理数量，并给整次检索设置总时限。
    # 此前 app.py 已经验证 JWT 签名，得到 principal。这里继续确认：
    # 凭据允许处理 R1，请求体提交的也必须是 R1。这样调用方不能拿着 R1 的有效凭据，把请求正文改成 R2。
    async def retrieve(self, payload: RetrieveRequest, principal: ServicePrincipal) -> RetrieveResponse:
        if principal.request_id != str(payload.request_id):
            raise RetrievalError(403, "token_request_mismatch", "服务凭据不允许访问此 request_id")
        if self.active >= self.settings.max_inflight:
            raise RetrievalError(503, "retrieval_overloaded", "检索服务繁忙，请稍后重试")
        self.active += 1
        try:
            async with asyncio.timeout(self.settings.request_timeout_seconds):
                return await self._retrieve(payload, principal)
        except TimeoutError as exc:
            raise RetrievalError(504, "retrieval_timeout", "检索超过总时限，可用相同输入重试") from exc
        finally:
            self.active -= 1

    # 作用：对成功快照重新检查用户及保留版本权限，再复用原成功结果。
    async def _cached(self, data: dict, *, user_id: str, request_id: str) -> RetrieveResponse:
        try:
            response = RetrieveResponse.model_validate(data)
        except ValidationError as exc:
            raise RetrievalError(409, "retrieval_snapshot_unavailable", "检索快照格式不完整") from exc
        if response.request_id != request_id:
            raise RetrievalError(409, "retrieval_snapshot_unavailable", "检索快照的请求标识不一致")
        documents = response.documents
        scope = RetrievalScope(user_id, tuple({document.version_id for document in documents}))
        async with self.sessions() as session:
            authorized = await fetch_parent_documents(
                session, ranked_parents=[(document.parent_id, document.rrf_score) for document in documents],
                scope=scope, require_current=False,
            )
        rows = {row["id"]: row for row in authorized}
        for document in documents:
            row = rows.get(document.parent_id)
            if (
                row is None or row["version_id"] != document.version_id
                or row["document_id"] != document.document_id or row["text"] != document.content
                or row["metadata"] != document.source
            ):
                raise RetrievalError(409, "retrieval_snapshot_unavailable", "检索快照的来源已不可访问")
        return build_retrieve_response(request_id, [document.model_dump() for document in documents])

    # 作用：在有限清理时限内记录失败；数据库不可用时由过期租约允许后续重领。
    async def _mark_failed(self, claim, *, owner: str, code: str) -> None:
        if claim is None or claim.cached_result is not None:
            return
        try:
            async with asyncio.timeout(3):
                async with self.sessions() as session, session.begin():
                    await fail_retrieval(
                        session, run_id=claim.run_id, owner=owner,
                        attempt=claim.attempt, error_code=code,
                    )
        except Exception:
            LOG.exception("检索失败状态暂未提交: %s", claim.run_id)

    # 作用：短事务领取、事务外检索、短事务保存结果，避免长时间占用数据库行锁。
    #     短事务一：核对任务、领取检索
    #               ↓
    #     事务外：执行检索算法
    #               ↓
    #     短事务二：复核来源、保存成功结果
    async def _retrieve(self, payload: RetrieveRequest, principal: ServicePrincipal) -> RetrieveResponse:
        owner = new_id()
        claim = None
        try:
            async with self.sessions() as session, session.begin():
                claim = await claim_retrieval(
                    session, payload=payload, user_id=principal.user_id,
                    owner=owner, lease_seconds=self.settings.run_lease_seconds,
                )
            if claim.cached_result is not None:
                return await self._cached(claim.cached_result, user_id=principal.user_id, request_id=str(payload.request_id))
            documents, trace = await self.pipeline.search(
                [message.model_dump() for message in payload.messages], payload.search_query, claim.scope,
            )
            async with self.sessions() as session, session.begin():
                # 保存前再次复核当前版本，避免检索途中版本切换后把旧版本当作新召回返回。
                verified = await fetch_parent_documents(
                    session, ranked_parents=[(document["parent_id"], document["rrf_score"]) for document in documents],
                    scope=claim.scope,
                )
                verified_ids = {document["id"] for document in verified}
                documents = [document for document in documents if document["parent_id"] in verified_ids]
                response = build_retrieve_response(str(payload.request_id), documents)
                trace["retrieved_parent_ids"] = response.retrieved_parent_ids
                if not await finish_retrieval(
                    session, run_id=claim.run_id, owner=owner, attempt=claim.attempt,
                    result=response.model_dump(mode="json"), trace=trace,
                ):
                    raise RetrievalError(409, "retrieval_lease_lost", "本次检索执行权已过期，请稍后重试")
            return response
        except asyncio.CancelledError:
            await asyncio.shield(self._mark_failed(claim, owner=owner, code="cancelled_or_timed_out"))
            raise
        except RetrievalError as exc:
            await self._mark_failed(claim, owner=owner, code=exc.code)
            raise
        except Exception as exc:
            LOG.exception("检索执行失败: request_id=%s", payload.request_id)
            await self._mark_failed(claim, owner=owner, code="retrieval_backend_failed")
            raise RetrievalError(502, "retrieval_backend_failed", "检索依赖暂不可用，可用相同输入重试") from exc
