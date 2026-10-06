"""Retrieval 的 FastAPI 入口，资源由 lifespan 启动和关闭。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from core.config import load_database_settings, load_model_settings, load_qdrant_runtime_settings, load_retrieval_settings
from core.retrieval_contracts import RetrieveRequest, RetrieveResponse, RetrievalError
from core.service_auth import verify_retrieval_token
from infra.mysql.session import create_mysql_engine, create_session_factory
from services.retrieval.compute import BoundedCompute
from services.retrieval.models_runtime import RetrievalModels
from services.retrieval.qdrant_shared import check_collection, create_qdrant_client
from services.retrieval.Query_and_HyDE import QueryTransformer
from services.retrieval.Search_Internal_Docs import RetrievalPipeline
from services.retrieval.service import RetrievalService

class RetrievalRuntime:
    # 作用：集中保存检索服务的资源与业务入口，供生命周期管理。
    def __init__(self, *, settings, service, engine, qdrant, http, compute) -> None:
        self.settings = settings
        self.service = service
        self.engine = engine
        self.qdrant = qdrant
        self.http = http
        self.compute = compute

    # 作用：等待模型计算退出后关闭 HTTP、Qdrant 和 MySQL 连接。
    async def close(self) -> None:
        try:
            await self.compute.close()
        finally:
            try:
                await self.http.aclose()
            finally:
                try:
                    await self.qdrant.close()
                finally:
                    await self.engine.dispose()


# 作用：服务启动时检查依赖、加载一次模型并构建检索应用编排。
async def create_runtime() -> RetrievalRuntime:
    settings = load_retrieval_settings()
    model_settings = load_model_settings() if settings.query_mode == "llm" else None
    qdrant_settings = load_qdrant_runtime_settings()
    engine = create_mysql_engine(load_database_settings())
    compute = BoundedCompute()
    qdrant = create_qdrant_client(qdrant_settings, timeout_seconds=settings.qdrant_timeout_seconds)
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=10, max_keepalive_connections=5))
    runtime = RetrievalRuntime(settings=settings, service=None, engine=engine, qdrant=qdrant, http=http, compute=compute)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
            await connection.execute(text("SELECT attempt, lease_owner, lease_expires_at FROM retrieval_runs LIMIT 1"))
        await check_collection(qdrant)
        models_runtime = await compute.run(RetrievalModels)
        sessions = create_session_factory(engine)
        pipeline = RetrievalPipeline(
            sessions=sessions, client=qdrant, models_runtime=models_runtime, compute=compute,
            transformer=QueryTransformer(http, model_settings, mode=settings.query_mode),
            concurrency=settings.pipeline_concurrency,
        )
        runtime.service = RetrievalService(sessions=sessions, pipeline=pipeline, settings=settings)
        return runtime
    except BaseException:
        await runtime.close()
        raise


# 作用：创建可注入测试运行时的 FastAPI 应用，导入模块时不读取服务密钥或加载模型。
def create_app(runtime_factory=create_runtime) -> FastAPI:
    # 作用：在应用就绪前创建运行时，在退出时释放全部资源。
    @asynccontextmanager
    async def lifespan(application: FastAPI):
        runtime = await runtime_factory()
        application.state.runtime = runtime
        try:
            yield
        finally:
            application.state.runtime = None
            await runtime.close()

    application = FastAPI(title="Agentic RAG Retrieval", lifespan=lifespan)
    application.state.runtime = None

    # 两个配套机制，但它们不是独立接口。
    # 作用：把业务错误转换为稳定 HTTP 响应，认证错误和可重试冲突附带对应提示头。
    @application.exception_handler(RetrievalError)
    async def retrieval_error_handler(request: Request, error: RetrievalError) -> JSONResponse:
        headers = {}
        if error.status_code == 401:
            headers["WWW-Authenticate"] = "Bearer"
        if error.code in ("retrieval_in_progress", "retrieval_overloaded"):
            headers["Retry-After"] = "1"
        return JSONResponse(
            status_code=error.status_code,
            content={"detail": {"code": error.code, "message": str(error)}}, headers=headers,
        )

    # 作用：仅在依赖检查和模型加载完成后报告检索服务就绪。
    @application.get("/health")
    async def health(request: Request) -> dict:
        if request.app.state.runtime is None:
            raise RetrievalError(503, "retrieval_not_ready", "检索服务尚未就绪")
        return {"status": "ready"}

    # 作用：验证专用服务凭据，再将受限检索请求交给业务用例。
    @application.post("/v1/retrieve", response_model=RetrieveResponse)
    async def retrieve(
        payload: RetrieveRequest, request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ) -> RetrieveResponse:
        runtime = request.app.state.runtime
        if runtime is None:
            raise RetrievalError(503, "retrieval_not_ready", "检索服务尚未就绪")
        if not authorization or not authorization.startswith("Bearer ") or len(authorization) > 4096:
            raise RetrievalError(401, "invalid_service_token", "需要检索服务 Bearer 凭据")
        principal = verify_retrieval_token(runtime.settings, authorization[7:])
        return await runtime.service.retrieve(payload, principal)

    return application

app = create_app()