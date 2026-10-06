"""受权限约束的异步 Sparse 召回，关键词与补充问题使用同一过滤。"""

from core.retrieval_contracts import RetrievalScope
from infra.topology import COLLECTION, SPARSE_VECTOR
from services.retrieval.qdrant_shared import build_scope_filter, format_authorized_hits


# 作用：在线程中编码关键词，并异步查询用户当前已发布版本的 Sparse 点。
async def qdrant_search_sparse(text: str, *, client, models_runtime, compute, scope: RetrievalScope, limit: int = 10) -> list[dict]:
    if not scope.version_ids:
        return []
    vector = await compute.run(models_runtime.encode_sparse, text)
    result = await client.query_points(
        collection_name=COLLECTION, query=vector, using=SPARSE_VECTOR,
        query_filter=build_scope_filter(scope), limit=limit,
        with_payload=["parent_id", "user_id", "document_id", "version_id"], with_vectors=False,
    )
    return format_authorized_hits(result.points, scope)
