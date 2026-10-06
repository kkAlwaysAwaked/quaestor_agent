"""经真实 HTTP 调用核对固定文档召回、成功快照复用与数据库 trace。"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import select

from core.config import PROJECT_ROOT, RetrievalSettings, load_retrieval_settings
from core.retrieval_contracts import RetrieveRequest, RetrieveResponse
from core.service_auth import issue_retrieval_token
from infra.mysql.base import new_id
from infra.mysql.models import ChatRequest, Conversation, Document, DocumentVersion, IngestJob, Message, RetrievalRun
from infra.mysql.repositories.retrieval import fetch_parent_documents, load_published_scope
from infra.mysql.repositories.tasks import claim_task, transition_task
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.mysql.status import DocumentVersionStatus, RetrievalStatus, TaskStatus


# 作用：确认显式指定的入库任务已经发布为固定样本的当前版本。
async def load_baseline(sessions, job_id: str, cases: list[dict]) -> dict:
    async with sessions() as session:
        job = await session.get(IngestJob, job_id)
        if job is None or job.status != TaskStatus.SUCCEEDED.value:
            raise ValueError("入库任务不存在或尚未成功，请先完成第四阶段基线")
        document = await session.get(Document, job.document_id)
        version = await session.get(DocumentVersion, job.version_id)
        fixture = PROJECT_ROOT / "tests" / "fixtures" / "employee_handbook.md"
        if (
            document is None or version is None or document.user_id != job.user_id
            or version.document_id != document.id
            or document.current_version_id != version.id
            or version.status != DocumentVersionStatus.PUBLISHED.value
            or version.file_sha256 != hashlib.sha256(fixture.read_bytes()).hexdigest()
            or any(case["expected_source"] != document.original_filename for case in cases)
        ):
            raise ValueError("入库任务必须对应已发布的固定 employee_handbook.md 样本")
        return {"user_id": job.user_id, "document_id": document.id, "version_id": version.id}


# 作用：建立独立测试会话、真实用户消息和聊天任务，再领取有效的执行租约。
async def create_baseline_task(sessions, *, user_id: str, case: dict, owner: str, lease_seconds: int) -> RetrieveRequest:
    request_id, conversation_id, message_id = new_id(), new_id(), new_id()
    async with sessions() as session, session.begin():
        conversation = Conversation(id=conversation_id, user_id=user_id, next_message_sequence=2)
        session.add(conversation)
        await session.flush()
        session.add(Message(
            id=message_id, conversation_id=conversation_id, request_id=request_id,
            sequence=1, role="user", content=case["question"],
        ))
        await session.flush()
        session.add(ChatRequest(
            id=request_id, user_id=user_id, conversation_id=conversation_id,
            user_message_id=message_id, history_until_sequence=1,
            idempotency_key=f"retrieval-baseline:{request_id}",
            request_fingerprint=hashlib.sha256(case["question"].encode("utf-8")).hexdigest(),
        ))
        await session.flush()
        conversation.active_request_id = request_id
        if not await claim_task(
            session, ChatRequest, task_id=request_id, owner=owner, lease_seconds=lease_seconds,
        ):
            raise ValueError("无法领取基线聊天任务")
    # 此脚本仅测试 Retrieval，不创建 Agent 工作通知，避免测试请求被真实 Worker 抢走。
    return RetrieveRequest(
        request_id=request_id, messages=[{"role": "user", "content": case["question"]}],
        search_query=case["search_query"],
    )


# 作用：结束未执行答案生成的测试任务并释放会话，保留消息与检索记录供学习核查。
async def close_baseline_task(sessions, *, request_id: str, owner: str) -> None:
    async with sessions() as session, session.begin():
        task = await session.scalar(select(ChatRequest).where(ChatRequest.id == request_id).with_for_update())
        if task is None:
            return
        conversation = await session.get(Conversation, task.conversation_id, with_for_update=True)
        if not await transition_task(
            session, ChatRequest, task_id=request_id, owner=owner, attempt=1,
            target=TaskStatus.FAILED, error="retrieval baseline finished; Agent generation not executed",
        ):
            raise ValueError(f"基线任务租约已变化，请检查任务：{request_id}")
        if conversation is not None and conversation.active_request_id == request_id:
            conversation.active_request_id = None


# 作用：签发绑定任务的短期凭据并调用接口，不在控制台或报告中输出凭据。
async def call_retrieval(client: httpx.AsyncClient, settings: RetrievalSettings, user_id: str, payload: RetrieveRequest) -> httpx.Response:
    token = issue_retrieval_token(settings, user_id=user_id, request_id=str(payload.request_id))
    return await client.post(
        "/v1/retrieve", json=payload.model_dump(mode="json"),
        headers={"Authorization": f"Bearer {token}"},
    )


# 作用：核对返回父块的授权、来源和正文，同时检查成功结果与 trace 是否已真实持久化。
async def verify_result(sessions, *, payload: RetrieveRequest, baseline: dict, case: dict, response: dict) -> dict:
    result = RetrieveResponse.model_validate(response)
    if result.request_id != str(payload.request_id) or result.status != "success":
        raise ValueError("固定样本没有得到成功召回")
    async with sessions() as session:
        scope = await load_published_scope(session, user_id=baseline["user_id"])
        rows = await fetch_parent_documents(
            session, ranked_parents=[(item.parent_id, item.rrf_score) for item in result.documents], scope=scope,
        )
        by_id = {row["id"]: row for row in rows}
        for item in result.documents:
            row = by_id.get(item.parent_id)
            if (
                row is None or row["document_id"] != item.document_id
                or row["version_id"] != item.version_id or row["text"] != item.content
            ):
                raise ValueError("响应含不可访问父块，或正文/来源标识与数据库不符")
        if not any(
            item.document_id == baseline["document_id"] and item.version_id == baseline["version_id"]
            and item.source == case["expected_source"] and case["expected_content"] in item.content
            for item in result.documents
        ):
            raise ValueError(f"问题未召回预期来源和正文：{case['question']}")
        run = await session.scalar(select(RetrievalRun).where(RetrievalRun.request_id == str(payload.request_id)))
        if (
            run is None or run.status != RetrievalStatus.SUCCEEDED.value or run.attempt != 1
            or run.input_data != payload.model_dump(mode="json") or run.result_data != response
            or run.retrieved_parent_ids != result.retrieved_parent_ids
            or set(run.source_version_ids or []) != {item.version_id for item in result.documents}
            or not run.trace_data or run.trace_data.get("query_transformation", {}).get("query_mode") != "fixed"
        ):
            raise ValueError("成功结果/输入/来源/trace 不一致，或服务未使用 fixed 基线模式")
        return {"request_id": str(payload.request_id), "question": case["question"], "response": response, "trace": run.trace_data}


# 作用：并发发送同一任务的重复调用，检查执行中冲突或成功复用，随后核对持久化结果。
async def check_case(client, settings, sessions, baseline, case) -> dict:
    owner = new_id()
    payload = await create_baseline_task(
        sessions, user_id=baseline["user_id"], case=case, owner=owner,
        lease_seconds=max(600, settings.request_timeout_seconds * 4 + 60),
    )
    try:
        responses = await asyncio.gather(
            call_retrieval(client, settings, baseline["user_id"], payload),
            call_retrieval(client, settings, baseline["user_id"], payload),
        )
        successful = []
        for response in responses:
            if response.status_code == 200:
                successful.append(response.json())
            elif response.status_code != 409 or response.json().get("detail", {}).get("code") != "retrieval_in_progress":
                raise ValueError(f"基线 HTTP 请求失败：{response.status_code} {response.text[:500]}")
        if not successful:
            raise ValueError("两次调用均未完成成功检索")
        cached = await call_retrieval(client, settings, baseline["user_id"], payload)
        cached.raise_for_status()
        if any(item != cached.json() for item in successful):
            raise ValueError("相同输入未复用同一成功快照")
        report = await verify_result(
            sessions, payload=payload, baseline=baseline, case=case, response=cached.json(),
        )
        report["initial_http_statuses"] = [response.status_code for response in responses]
        return report
    finally:
        await close_baseline_task(sessions, request_id=str(payload.request_id), owner=owner)


# 作用：并发验收固定问题，输出可沿 request_id 回查的结果与来源报告。
async def check_baseline(job_id: str, base_url: str, output: Path) -> None:
    settings = load_retrieval_settings()
    if settings.query_mode != "fixed":
        raise ValueError("请将 .env 的 RETRIEVAL_QUERY_MODE 设置为 fixed，并用该配置重启 Retrieval")
    cases = json.loads((PROJECT_ROOT / "tests" / "fixtures" / "retrieval_baseline.json").read_text(encoding="utf-8"))["questions"]
    engine = create_mysql_engine()
    try:
        sessions = create_session_factory(engine)
        baseline = await load_baseline(sessions, job_id, cases)
        async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=settings.request_timeout_seconds + 10) as client:
            health = await client.get("/health")
            health.raise_for_status()
            # return_exceptions 使一个问题失败时仍等待其他问题完成并释放其测试会话。
            results = await asyncio.gather(*[
                check_case(client, settings, sessions, baseline, case) for case in cases
            ], return_exceptions=True)
            failures = [item for item in results if isinstance(item, BaseException)]
            if failures:
                raise ValueError("基线验收失败：" + "; ".join(str(item) for item in failures))
        report = {"ingest_job_id": job_id, **baseline, "cases": results}
        output = output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Retrieval 基线通过：{len(cases)} 个问题，并发重复调用及成功快照复用均已核对")
        print(f"结果与 trace 已保存：{output}")
    finally:
        await engine.dispose()


# 作用：解析真实入库任务、服务地址和报告路径，执行 HTTP 基线验收。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="验收真实 Retrieval 服务及固定样本召回")
    parser.add_argument("job_id", help="已经通过 check_ingest_baseline 的入库任务 UUID")
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "outputs" / "retrieval_baseline.json")
    args = parser.parse_args()
    asyncio.run(check_baseline(str(UUID(args.job_id)), args.base_url, args.output))


if __name__ == "__main__":
    main()
