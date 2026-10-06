"""Qdrant 异步客户端及所有召回分支复用的权限过滤。"""

from __future__ import annotations

from qdrant_client import AsyncQdrantClient, models

from core.config import QdrantRuntimeSettings
from core.retrieval_contracts import RetrievalScope
from infra import topology as t


# 作用：在服务启动时创建连接 Qdrant Server 的异步客户端。
def create_qdrant_client(settings: QdrantRuntimeSettings, *, timeout_seconds: int) -> AsyncQdrantClient:
    return AsyncQdrantClient(url=settings.url, api_key=settings.api_key, timeout=timeout_seconds)


# 作用：启动时核对集合维度、距离、命名向量和权限过滤索引。
async def check_collection(client: AsyncQdrantClient) -> None:
    collection = await client.get_collection(t.COLLECTION)
    vectors = collection.config.params.vectors
    dense = vectors.get(t.DENSE_VECTOR) if isinstance(vectors, dict) else None
    sparse = collection.config.params.sparse_vectors or {}
    if (
        dense is None or dense.size != t.DENSE_SIZE
        or dense.distance.value.lower() != t.DENSE_DISTANCE.lower()
        or t.SPARSE_VECTOR not in sparse
    ):
        raise RuntimeError("Qdrant 集合向量配置与入库配置不一致")
    for field in t.FILTER_FIELDS:
        index = collection.payload_schema.get(field)
        if index is None or index.data_type.value != "keyword":
            raise RuntimeError(f"Qdrant 缺少 keyword 权限索引: {field}")


# 作用：生成用户与已发布版本的交集过滤；空范围禁止执行全库查询。
def build_scope_filter(scope: RetrievalScope) -> models.Filter:
    if not scope.version_ids:
        raise ValueError("空授权范围不能查询 Qdrant")
    return models.Filter(must=[
        models.FieldCondition(key="user_id", match=models.MatchValue(value=scope.user_id)),
        models.FieldCondition(key="version_id", match=models.MatchAny(any=list(scope.version_ids))),
    ])


# 作用：仅保留授权载荷完整的召回点，父块正文仍需再到 MySQL 验证。
def format_authorized_hits(points: list, scope: RetrievalScope) -> list[dict]:
    hits = []
    versions = set(scope.version_ids)
    for point in points:
        payload = point.payload or {}
        if (
            payload.get("user_id") != scope.user_id or payload.get("version_id") not in versions
            or not isinstance(payload.get("parent_id"), str)
            or not isinstance(payload.get("document_id"), str)
        ):
            continue
        hits.append({
            "id": str(point.id), "parent_id": payload["parent_id"],
            "document_id": payload["document_id"], "version_id": payload["version_id"],
        })
    return hits
