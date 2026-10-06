"""建立真实聊天任务，经 Worker、模型和 Retrieval 验收增量事件及完整答案。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import select

from core.config import PROJECT_ROOT, load_agent_settings, load_redis_runtime_settings
from core.retrieval_contracts import RetrieveResponse
from infra.mysql.base import new_id
from infra.mysql.models import ChatRequest, Conversation, Message, RetrievalRun
from infra.mysql.repositories.chat import accept_chat
from infra.mysql.repositories.retrieval import fetch_parent_documents, load_published_scope
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.redis_streams import ChatStreams, create_redis_client
from scripts.check_retrieval_baseline import load_baseline


# 作用：创建独立基线会话，避免测试问题混入已有的业务对话。
async def create_conversation(sessions, user_id: str) -> str:
    conversation_id = new_id()
    async with sessions() as session, session.begin():
        session.add(Conversation(id=conversation_id, user_id=user_id))
    return conversation_id


# 作用：经受理事务创建真实 user 消息、pending 任务和 Outbox，由运行中的 Worker 负责发布。
async def submit_question(sessions, *, user_id: str, conversation_id: str, question: str) -> str:
    async with sessions() as session, session.begin():
        task = await accept_chat(
            session, user_id=user_id, conversation_id=conversation_id, content=question,
            idempotency_key=f"agent-baseline:{new_id()}",
        )
        request_id = task.id
    print(f"任务已受理：request_id={request_id}", flush=True)
    return request_id


# 作用：按 entry ID 续读事件并结合数据库状态等待结果，按 reset 丢弃失败 attempt 的临时文本。
async def wait_for_answer(sessions, streams: ChatStreams, *, request_id: str, timeout_seconds: int) -> dict:
    cursor, attempt, parts, token_ids, event_count = "0-0", 0, [], [], 0
    observed_running, terminal_event = False, None
    async with asyncio.timeout(timeout_seconds):
        while True:
            entries = await streams.read(request_id, last_id=cursor, block_ms=500)
            async with sessions() as session:
                task = await session.get(ChatRequest, request_id)
                if task is None:
                    raise ValueError("基线聊天任务消失")
                status, last_error = task.status, task.last_error
                answer = await session.get(Message, task.result_message_id) if task.result_message_id else None
            for entry_id, event in entries:
                cursor, event_count = entry_id, event_count + 1
                event_attempt = event.data["attempt"]
                if event.event == "status" and event.data.get("reset") is True and event_attempt >= attempt:
                    attempt, parts, token_ids = event_attempt, [], []
                    observed_running = False
                if event_attempt != attempt:
                    continue
                if event.event == "token":
                    parts.append(event.data["token"])
                    token_ids.append(entry_id)
                    observed_running |= status == "running"
                elif event.event in ("done", "error"):
                    terminal_event = event.event
            if status == "failed":
                raise ValueError(f"Agent 基线任务最终失败：request_id={request_id} code={last_error}")
            if status == "succeeded" and answer is not None and terminal_event == "done":
                if answer.content != "".join(parts) or attempt != task.attempt or len(token_ids) < 2:
                    raise ValueError("本次 attempt 的完整增量文本与 MySQL 回答不一致，或未观察到多个增量")
                return {
                    "request_id": request_id, "attempt": attempt, "result_message_id": answer.id,
                    "answer": answer.content, "token_event_count": len(token_ids),
                    "first_token_entry_id": token_ids[0], "last_entry_id": cursor,
                    "event_count": event_count, "observed_token_while_running": observed_running,
                }
            # 已成功却暂时缺少 done 时继续等待 Outbox；超时后查询数据库，不重新提交。


# 作用：核对该问题有独立的成功检索记录，并再次验证返回正文、权限与固定样本来源。
async def verify_retrieval(sessions, *, request_id: str, baseline: dict, case: dict) -> dict:
    async with sessions() as session:
        task = await session.get(ChatRequest, request_id)
        run = await session.scalar(select(RetrievalRun).where(RetrievalRun.request_id == request_id))
        if (
            task is None or task.status != "succeeded" or task.user_id != baseline["user_id"]
            or run is None or run.user_id != task.user_id or run.status != "succeeded"
            or not run.trace_data
        ):
            raise ValueError("Agent 问题没有对应的成功检索记录和 trace")
        result = RetrieveResponse.model_validate(run.result_data)
        if result.request_id != request_id or result.retrieved_parent_ids != run.retrieved_parent_ids:
            raise ValueError("检索结果与请求或父块记录不一致")
        scope = await load_published_scope(session, user_id=baseline["user_id"])
        rows = await fetch_parent_documents(
            session, ranked_parents=[(item.parent_id, item.rrf_score) for item in result.documents], scope=scope,
        )
        by_id = {row["id"]: row for row in rows}
        for item in result.documents:
            row = by_id.get(item.parent_id)
            if row is None or row["document_id"] != item.document_id or row["version_id"] != item.version_id or row["text"] != item.content:
                raise ValueError("Agent 参考资料含未授权父块或正文与数据库不符")
        if not any(
            item.document_id == baseline["document_id"] and item.version_id == baseline["version_id"]
            and item.source == case["expected_source"] and case["expected_content"] in item.content
            for item in result.documents
        ):
            raise ValueError(f"问题没有召回预期资料：{case['question']}")
        return {"retrieval_input": run.input_data, "retrieved_parent_ids": run.retrieved_parent_ids, "trace": run.trace_data}


# 作用：在同一会话依次提交两个固定问题，验证每个请求都能独立检索并保存流式结果报告。
async def check_baseline(job_id: str, output: Path, timeout_seconds: int) -> None:
    settings = load_agent_settings()
    cases = json.loads((PROJECT_ROOT / "tests" / "fixtures" / "retrieval_baseline.json").read_text(encoding="utf-8"))["questions"]
    engine = create_mysql_engine()
    redis = create_redis_client(load_redis_runtime_settings(), timeout_seconds=settings.io_timeout_seconds)
    try:
        sessions, streams = create_session_factory(engine), ChatStreams(redis, settings)
        baseline = await load_baseline(sessions, job_id, cases)
        await redis.ping()
        conversation_id = await create_conversation(sessions, baseline["user_id"])
        report = {"ingest_job_id": job_id, **baseline, "conversation_id": conversation_id, "cases": []}
        for case in cases:
            request_id = await submit_question(
                sessions, user_id=baseline["user_id"], conversation_id=conversation_id, question=case["question"],
            )
            try:
                result = await wait_for_answer(sessions, streams, request_id=request_id, timeout_seconds=timeout_seconds)
            except TimeoutError as exc:
                raise ValueError(f"等待超时，请沿 request_id={request_id} 检查任务、Worker 和 Outbox；不要重新提交同一问题") from exc
            result.update(await verify_retrieval(sessions, request_id=request_id, baseline=baseline, case=case))
            result["question"] = case["question"]
            report["cases"].append(result)
            print(f"完成：{request_id}，增量数={result['token_event_count']}，执行中观察到 token={result['observed_token_while_running']}", flush=True)
        output = output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Agent 基线通过，报告已保存：{output}")
    finally:
        try:
            await redis.aclose()
        finally:
            await engine.dispose()


# 作用：解析入库基线 UUID 和等待时限，运行真实聊天链路验收，保留任务供回查。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="验收正在运行的 Agent Worker、真实流式模型及 Retrieval")
    parser.add_argument("job_id", help="已通过 check_ingest_baseline 的固定样本入库任务 UUID")
    parser.add_argument("--timeout", type=int, default=900, help="每个问题等待秒数，默认 900")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "outputs" / "agent_baseline.json")
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout 必须大于零")
    asyncio.run(check_baseline(str(UUID(args.job_id)), args.output, args.timeout))


if __name__ == "__main__":
    main()
