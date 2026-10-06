"""检索权限、幂等租约、真实异步调度和 HTTP 契约的离线验证。"""

from __future__ import annotations

import asyncio
import json
import threading
import tempfile
import unittest
from datetime import timedelta
from types import SimpleNamespace
from pathlib import Path

import httpx
import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from core.config import ModelSettings, RetrievalSettings
from core.index_config import DENSE_MODEL, SPARSE_MODEL
from core.retrieval_contracts import RetrieveRequest, RetrievalError, RetrievalScope, build_retrieve_response
from core.service_auth import ServicePrincipal, issue_retrieval_token
from infra.mysql.base import Base, new_id, utc_now
from infra.mysql.models import ChatRequest, Conversation, Document, DocumentVersion, Message, ParentChunk, RetrievalRun, User
from infra.mysql.repositories.retrieval import claim_retrieval, fetch_parent_documents, finish_retrieval, load_published_scope
from infra.mysql.session import create_session_factory
from services.retrieval.app import create_app
from services.retrieval.compute import BoundedCompute
from services.retrieval.Query_and_HyDE import QueryTransformer
from services.retrieval.Search_Internal_Docs import RetrievalPipeline
from services.retrieval.service import RetrievalService


class FakePipeline:
    # 作用：创建可控的检索替身以验证持久化和故障边界。
    def __init__(self, documents: list[dict]) -> None:
        self.documents = documents
        self.calls = 0
        self.fail = False
        self.delay = 0
        self.entered = asyncio.Event()
        self.release = None

    # 作用：模拟成功、失败、阻塞或超时检索，不加载真实模型。
    async def search(self, messages, search_query, scope):
        self.calls += 1
        self.entered.set()
        if self.release is not None:
            await self.release.wait()
        await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("模拟 Qdrant 故障")
        return self.documents, {"fake": True}


class FakeReranker:
    # 作用：记录真正进入模型的父块正文，便于检查数据隔离。
    def __init__(self) -> None:
        self.contents = []

    # 作用：为已授权正文提供确定性分数。
    def predict(self, pairs):
        self.contents.extend(pair[1] for pair in pairs)
        return [float(len(pairs) - index) for index in range(len(pairs))]


class FakeModels:
    # 作用：提供轻量编码与重排替身，测试不会下载模型。
    def __init__(self) -> None:
        self.reranker = FakeReranker()

    # 作用：返回一个模拟的 Dense 查询向量。
    def encode_dense(self, text):
        return [0.1, 0.2]

    # 作用：返回一个模拟的 Sparse 查询向量。
    def encode_sparse(self, text):
        return [0.3]


class FakeQdrant:
    # 作用：建立必须等三路查询同时到达才响应的假向量库。
    def __init__(self, points: list) -> None:
        self.points = points
        self.calls = []
        self.all_started = asyncio.Event()

    # 作用：记录每一路权限过滤，并用屏障验证 Dense 与 Sparse 被同时调度。
    async def query_points(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) >= 3:
            self.all_started.set()
        await asyncio.wait_for(self.all_started.wait(), timeout=2)
        return SimpleNamespace(points=self.points)


class FakeRuntime:
    # 作用：向 FastAPI 注入已构造的业务入口和服务配置。
    def __init__(self, service, settings) -> None:
        self.service = service
        self.settings = settings
        self.closed = False

    # 作用：记录 lifespan 是否正确关闭运行时。
    async def close(self) -> None:
        self.closed = True


class RetrievalComponentTests(unittest.IsolatedAsyncioTestCase):
    # 作用：准备两个用户、真实任务历史以及当前、旧版和未发布文档。
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        database_path = (Path(self.temp.name) / "retrieval.sqlite").as_posix()
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = create_session_factory(self.engine)
        self.user_id = new_id()
        self.other_user = new_id()
        async with self.sessions() as session, session.begin():
            session.add_all([
                User(id=self.user_id, email="retrieve@example.com", password_hash="!"),
                User(id=self.other_user, email="private@example.com", password_hash="!"),
            ])
        self.task_id = await self.seed_task(self.user_id)
        self.current = await self.seed_document(self.user_id, "published", True, "年假十天")
        self.old = await self.seed_document(self.user_id, "published", False, "已过时资料")
        self.processing = await self.seed_document(self.user_id, "processing", False, "尚未发布")
        self.private = await self.seed_document(self.other_user, "published", True, "他人的秘密")
        self.settings = RetrievalSettings(service_secret="s" * 48)
        self.payload = RetrieveRequest(
            request_id=self.task_id, messages=[{"role": "user", "content": "我有多少天年假？"}],
            search_query="员工手册 年假 天数",
        )
        self.principal = ServicePrincipal(self.user_id, self.task_id)
        self.pipeline = FakePipeline([self.result_document(self.current)])
        self.service = RetrievalService(sessions=self.sessions, pipeline=self.pipeline, settings=self.settings)

    # 作用：释放各测试独占的临时数据库及文件。
    async def asyncTearDown(self) -> None:
        await self.engine.dispose()
        self.temp.cleanup()

    # 作用：创建包含历史截止点和有效租约的一条真实聊天任务。
    async def seed_task(self, user_id: str) -> str:
        task_id, conversation_id, message_id = new_id(), new_id(), new_id()
        async with self.sessions() as session, session.begin():
            session.add(Conversation(id=conversation_id, user_id=user_id, next_message_sequence=2))
            await session.flush()
            session.add(Message(
                id=message_id, conversation_id=conversation_id, request_id=task_id,
                sequence=1, role="user", content="我有多少天年假？",
            ))
            await session.flush()
            session.add(ChatRequest(
                id=task_id, user_id=user_id, conversation_id=conversation_id,
                user_message_id=message_id, history_until_sequence=1,
                idempotency_key=new_id(), request_fingerprint="a" * 64,
                status="running", attempt=1, lease_owner="test-agent",
                lease_expires_at=utc_now() + timedelta(minutes=10),
            ))
        return task_id

    # 作用：创建指定归属和状态的版本与父块，覆盖版本过滤测试数据。
    async def seed_document(self, user_id: str, status: str, current: bool, content: str) -> dict:
        document_id, version_id, parent_id = new_id(), new_id(), new_id()
        async with self.sessions() as session, session.begin():
            document = Document(id=document_id, user_id=user_id, original_filename="employee_handbook.md", storage_key="test.md")
            session.add(document)
            await session.flush()
            session.add(DocumentVersion(
                id=version_id, document_id=document_id, storage_key="test.md",
                file_sha256="a" * 64, status=status,
                processing_config={"dense_model": DENSE_MODEL, "sparse_model": SPARSE_MODEL},
            ))
            await session.flush()
            if current:
                document.current_version_id = version_id
            session.add(ParentChunk(
                id=parent_id, document_id=document_id, version_id=version_id,
                chunk_index=0, content=content, source="employee_handbook.md",
            ))
        return {"user_id": user_id, "document_id": document_id, "version_id": version_id, "parent_id": parent_id, "content": content}

    # 作用：将测试父块转换成业务结果契约。
    def result_document(self, document: dict) -> dict:
        return {key: value for key, value in document.items() if key != "user_id"} | {
            "source": "employee_handbook.md", "rrf_score": 0.03, "rerank_score": 1.0,
        }

    # 作用：验证成功结果持久化且重复调用复用，改变输入则冲突。
    async def test_success_is_cached_and_input_is_immutable(self):
        first = await self.service.retrieve(self.payload, self.principal)
        second = await self.service.retrieve(self.payload, self.principal)
        self.assertEqual(first, second)
        self.assertEqual(self.pipeline.calls, 1)
        async with self.sessions() as session:
            run = await session.scalar(select(RetrievalRun))
            self.assertEqual(run.status, "succeeded")
            self.assertEqual(run.retrieved_parent_ids, [self.current["parent_id"]])
            self.assertEqual(run.source_version_ids, [self.current["version_id"]])
        changed = self.payload.model_copy(update={"search_query": "不同输入"})
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(changed, self.principal)
        self.assertEqual(raised.exception.code, "retrieval_input_conflict")

    # 作用：验证成功快照可以复用保留的已发布旧版，但来源撤销后必须拒绝返回。
    async def test_cached_snapshot_checks_retained_version_permission(self):
        first = await self.service.retrieve(self.payload, self.principal)
        async with self.sessions() as session, session.begin():
            document = await session.get(Document, self.current["document_id"])
            document.current_version_id = None
        second = await self.service.retrieve(self.payload, self.principal)
        self.assertEqual(first, second)
        self.assertEqual(self.pipeline.calls, 1)
        async with self.sessions() as session, session.begin():
            version = await session.get(DocumentVersion, self.current["version_id"])
            version.status = "failed"
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(self.payload, self.principal)
        self.assertEqual(raised.exception.code, "retrieval_snapshot_unavailable")
        self.assertEqual(self.pipeline.calls, 1)

    # 作用：验证达到在途上限时拒绝新请求，且不会创建多余的数据库检索记录。
    async def test_overload_rejects_before_database_claim(self):
        self.service.settings = RetrievalSettings(service_secret="s" * 48, max_inflight=1)
        self.pipeline.release = asyncio.Event()
        first = asyncio.create_task(self.service.retrieve(self.payload, self.principal))
        await asyncio.wait_for(self.pipeline.entered.wait(), timeout=2)
        other_task = await self.seed_task(self.user_id)
        other_payload = self.payload.model_copy(update={"request_id": other_task})
        try:
            with self.assertRaises(RetrievalError) as raised:
                await self.service.retrieve(other_payload, ServicePrincipal(self.user_id, other_task))
            self.assertEqual(raised.exception.code, "retrieval_overloaded")
            async with self.sessions() as session:
                self.assertIsNone(await session.scalar(select(RetrievalRun).where(RetrievalRun.request_id == other_task)))
        finally:
            self.pipeline.release.set()
            await first
        self.assertEqual(self.service.active, 0)

    # 作用：验证用户归属和数据库历史不能被调用方提供的消息绕过。
    async def test_task_owner_and_history_are_checked(self):
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(self.payload, ServicePrincipal(self.other_user, self.task_id))
        self.assertEqual(raised.exception.status_code, 404)
        forged = RetrieveRequest(request_id=self.task_id, messages=[{"role": "user", "content": "伪造问题"}], search_query="年假")
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(forged, self.principal)
        self.assertEqual(raised.exception.code, "history_mismatch")
        self.assertEqual(self.pipeline.calls, 0)

    # 作用：确认范围和父块读取均排除他人、旧版及处理中版本。
    async def test_scope_and_parent_documents_are_isolated(self):
        async with self.sessions() as session:
            scope = await load_published_scope(session, user_id=self.user_id)
            self.assertEqual(scope.version_ids, (self.current["version_id"],))
            candidates = [(item["parent_id"], 1.0) for item in [self.private, self.old, self.processing, self.current]]
            documents = await fetch_parent_documents(session, ranked_parents=candidates, scope=scope)
            self.assertEqual([document["id"] for document in documents], [self.current["parent_id"]])

    # 作用：验证三路召回同时开始、共享权限过滤且非法父块不会进入重排或 trace。
    async def test_pipeline_concurrent_routes_share_scope(self):
        points = [SimpleNamespace(id=new_id(), payload={key: item[key] for key in ("user_id", "document_id", "version_id", "parent_id")}) for item in [self.current, self.private, self.old, self.processing]]
        forged = {key: self.current[key] for key in ("user_id", "document_id", "version_id", "parent_id")}
        forged["parent_id"] = self.private["parent_id"]
        points.append(SimpleNamespace(id=new_id(), payload=forged))
        client, models, compute = FakeQdrant(points), FakeModels(), BoundedCompute()
        http = httpx.AsyncClient()
        try:
            pipeline = RetrievalPipeline(
                sessions=self.sessions, client=client, models_runtime=models, compute=compute,
                transformer=QueryTransformer(http, None, mode="fixed"),
            )
            documents, trace = await pipeline.search(
                [message.model_dump() for message in self.payload.messages], self.payload.search_query,
                RetrievalScope(self.user_id, (self.current["version_id"],)),
            )
            self.assertEqual(len(client.calls), 3)
            for call in client.calls:
                filters = call["query_filter"].model_dump()
                self.assertEqual(filters["must"][0]["match"]["value"], self.user_id)
                self.assertEqual(filters["must"][1]["match"]["any"], [self.current["version_id"]])
            self.assertEqual([document["parent_id"] for document in documents], [self.current["parent_id"]])
            self.assertEqual(models.reranker.contents, ["年假十天"])
            self.assertNotIn(self.private["parent_id"], str(trace))
        finally:
            await compute.close()
            await http.aclose()

    # 作用：验证用户没有已发布版本时直接返回空结果，绝不去向量库做无过滤查询。
    async def test_empty_scope_never_queries_qdrant(self):
        client, models, compute = FakeQdrant([]), FakeModels(), BoundedCompute()
        async with httpx.AsyncClient() as http:
            try:
                pipeline = RetrievalPipeline(
                    sessions=self.sessions, client=client, models_runtime=models, compute=compute,
                    transformer=QueryTransformer(http, None, mode="fixed"),
                )
                documents, trace = await pipeline.search([], "年假", RetrievalScope(self.user_id, ()))
                self.assertEqual(documents, [])
                self.assertEqual(client.calls, [])
                self.assertTrue(trace["empty_scope"])
                self.assertEqual(models.reranker.contents, [])
            finally:
                await compute.close()

    # 作用：确认暂时失败不是成功检索，同输入重试可以重新执行并成功。
    async def test_failure_allows_retry_without_success_cache(self):
        self.pipeline.fail = True
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(self.payload, self.principal)
        self.assertEqual(raised.exception.status_code, 502)
        async with self.sessions() as session:
            run = await session.scalar(select(RetrievalRun))
            self.assertEqual(run.status, "failed")
            self.assertIsNone(run.result_data)
        self.pipeline.fail = False
        await self.service.retrieve(self.payload, self.principal)
        async with self.sessions() as session:
            run = await session.scalar(select(RetrievalRun))
            self.assertEqual((run.status, run.attempt), ("succeeded", 2))

    # 作用：验证同一请求在执行中重复调用时返回冲突，取消后不会留下成功结果。
    async def test_duplicate_inflight_and_cancellation(self):
        self.pipeline.release = asyncio.Event()
        first = asyncio.create_task(self.service.retrieve(self.payload, self.principal))
        await asyncio.wait_for(self.pipeline.entered.wait(), timeout=2)
        try:
            with self.assertRaises(RetrievalError) as raised:
                await self.service.retrieve(self.payload, self.principal)
            self.assertEqual(raised.exception.code, "retrieval_in_progress")
        finally:
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
        async with self.sessions() as session:
            run = await session.scalar(select(RetrievalRun))
            self.assertEqual(run.status, "failed")

    # 作用：验证超时释放受理名额，并把结果记为可重试失败。
    async def test_timeout_is_persisted_and_capacity_is_released(self):
        self.pipeline.delay = 5
        self.service.settings = RetrievalSettings(service_secret="s" * 48, request_timeout_seconds=2)
        with self.assertRaises(RetrievalError) as raised:
            await self.service.retrieve(self.payload, self.principal)
        self.assertEqual(raised.exception.status_code, 504)
        self.assertTrue(self.pipeline.entered.is_set())
        self.assertEqual(self.service.active, 0)
        async with self.sessions() as session:
            run = await session.scalar(select(RetrievalRun))
            self.assertEqual(run.status, "failed")

    # 作用：模拟租约到期后重领，确认旧 attempt 无法覆盖新检索结果。
    async def test_expired_retrieval_lease_fences_previous_writer(self):
        owner_a, owner_b = new_id(), new_id()
        async with self.sessions() as session, session.begin():
            first = await claim_retrieval(session, payload=self.payload, user_id=self.user_id, owner=owner_a, lease_seconds=90)
        async with self.sessions() as session, session.begin():
            run = await session.get(RetrievalRun, first.run_id)
            run.lease_expires_at = utc_now() - timedelta(seconds=1)
        async with self.sessions() as session, session.begin():
            second = await claim_retrieval(session, payload=self.payload, user_id=self.user_id, owner=owner_b, lease_seconds=90)
        result = build_retrieve_response(self.task_id, [self.result_document(self.current)]).model_dump(mode="json")
        async with self.sessions() as session, session.begin():
            self.assertFalse(await finish_retrieval(session, run_id=first.run_id, owner=owner_a, attempt=first.attempt, result=result, trace={}))
            self.assertTrue(await finish_retrieval(session, run_id=second.run_id, owner=owner_b, attempt=second.attempt, result=result, trace={}))

    # 作用：通过真实 HTTP 路由验证签名、过期、audience、请求绑定和字段范围。
    async def test_http_authentication_and_contract(self):
        runtime = FakeRuntime(self.service, self.settings)

        # 作用：向应用生命周期注入测试运行时。
        async def runtime_factory():
            return runtime

        application = create_app(runtime_factory)
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://test") as client:
                body = self.payload.model_dump(mode="json")
                token = issue_retrieval_token(self.settings, user_id=self.user_id, request_id=self.task_id)
                self.assertEqual((await client.post("/v1/retrieve", json=body)).status_code, 401)
                claims = jwt.decode(token, options={"verify_signature": False})
                bad_claims = [dict(claims, aud="gateway"), dict(claims, iat=claims["iat"] - 300, exp=claims["iat"] - 200)]
                bad_tokens = [jwt.encode(data, self.settings.service_secret, algorithm="HS256") for data in bad_claims]
                bad_tokens.append(jwt.encode(claims, "x" * 48, algorithm="HS256"))
                for invalid in bad_tokens:
                    response = await client.post("/v1/retrieve", json=body, headers={"Authorization": f"Bearer {invalid}"})
                    self.assertEqual(response.status_code, 401)
                different_request = issue_retrieval_token(self.settings, user_id=self.user_id, request_id=new_id())
                self.assertEqual((await client.post("/v1/retrieve", json=body, headers={"Authorization": f"Bearer {different_request}"})).status_code, 403)
                headers = {"Authorization": f"Bearer {token}"}
                self.assertEqual((await client.post("/v1/retrieve", json=body | {"user_id": self.other_user}, headers=headers)).status_code, 422)
                response = await client.post("/v1/retrieve", json=body, headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["retrieved_parent_ids"], [self.current["parent_id"]])
                self.assertEqual((await client.get("/health")).status_code, 200)
        self.assertTrue(runtime.closed)


class ComputeCancellationTests(unittest.IsolatedAsyncioTestCase):
    # 作用：确认同步计算不阻塞事件循环，取消协程也不会提前释放共享模型。
    async def test_cancelled_compute_holds_slot_until_thread_finishes(self):
        compute = BoundedCompute()
        started, release, second_started = threading.Event(), threading.Event(), threading.Event()

        # 作用：模拟尚未结束的同步模型计算。
        def first_compute():
            started.set()
            release.wait(timeout=2)

        # 作用：记录下一次计算是否在前一次真正结束后开始。
        def second_compute():
            second_started.set()

        first = asyncio.create_task(compute.run(first_compute))
        await asyncio.to_thread(started.wait, 1)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(compute.run(second_compute))
        try:
            await asyncio.sleep(0.03)
            self.assertFalse(second_started.is_set())
            release.set()
            await asyncio.wait_for(second, timeout=2)
            self.assertTrue(second_started.is_set())
        finally:
            release.set()
            await compute.close()


class QueryTransformerTests(unittest.IsolatedAsyncioTestCase):
    # 作用：验证短查询的 Rewrite 与 HyDE 并发调用，且超长生成内容会被截断。
    async def test_rewrite_and_hyde_are_concurrent_and_bounded(self):
        calls, both_started = [], asyncio.Event()

        # 作用：模拟只有两项生成调用同时到达才响应的模型 API。
        async def handle_request(request):
            body = json.loads(request.content)
            calls.append(body)
            if len(calls) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=2)
            content = "年假规定。" * 40 if "max_tokens" in body else "员工 手册 年假 天数 申请"
            return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle_request)) as client:
            transformer = QueryTransformer(client, ModelSettings(api_key="test-key", base_url="https://llm.test/v1"))
            result = await transformer.transform([{"role": "user", "content": "我每年有多少天年假？"}], "年假")
        self.assertEqual(len(calls), 2)
        self.assertEqual(result["rewritten_query"], "员工 手册 年假 天数 申请")
        self.assertEqual(result["sparse_fallback_query"], "我每年有多少天年假？")
        self.assertLessEqual(len(result["hyde_document"]), 120)
        self.assertFalse(result["hyde_fallback"])

    # 作用：验证生成 API 临时失败时仍保留原始检索词和问题，不把错误伪装成空查询。
    async def test_generation_failure_preserves_original_query(self):
        # 作用：模拟生成 API 返回可恢复的服务错误。
        async def handle_request(request):
            return httpx.Response(503, json={"error": "temporary failure"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle_request)) as client:
            transformer = QueryTransformer(client, ModelSettings(api_key="test-key", base_url="https://llm.test/v1"))
            result = await transformer.transform([{"role": "user", "content": "我每年有多少天年假？"}], "年假")
        self.assertEqual(result["rewritten_query"], "年假")
        self.assertEqual(result["hyde_document"], "我每年有多少天年假？")
        self.assertTrue(result["hyde_fallback"])


if __name__ == "__main__":
    unittest.main()
