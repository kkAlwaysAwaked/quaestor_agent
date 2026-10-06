"""Qdrant Server 中子块的幂等 upsert、完整性校验与失败版本清理。"""

from __future__ import annotations

import asyncio

from qdrant_client import QdrantClient, models

from infra.topology import COLLECTION


class IngestVectorStore:
    # 作用：为本机或容器中的 Qdrant Server 创建客户端，绝不打开旧本地目录。
    def __init__(self, *, url: str, api_key: str) -> None:
        self.client = QdrantClient(url=url, api_key=api_key, timeout=60)

    # 作用：在 Worker 停止时关闭向量库连接。
    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)

    # 作用：按稳定点 ID 分批 upsert，并等待服务端确认落库。
    async def upsert(self, points: list[models.PointStruct]) -> None:
        for offset in range(0, len(points), 64):
            batch = points[offset:offset + 64]
            await asyncio.to_thread(
                self.client.upsert, collection_name=COLLECTION, points=batch, wait=True
            )

    # 作用：核对目标版本的点数和每个预期点 ID，防止部分写入的版本被发布。
    async def verify(
        self, *, version_id: str, user_id: str, document_id: str,
        point_to_parent: dict[str, str],
    ) -> None:
        point_ids = list(point_to_parent)
        condition = models.Filter(must=[models.FieldCondition(
            key="version_id", match=models.MatchValue(value=version_id)
        )])
        count = await asyncio.to_thread(
            self.client.count, collection_name=COLLECTION, count_filter=condition, exact=True
        )
        if count.count != len(point_ids):
            raise RuntimeError(f"向量点数量不一致：预期 {len(point_ids)}，实际 {count.count}")
        for offset in range(0, len(point_ids), 128):
            expected = set(point_ids[offset:offset + 128])
            found = await asyncio.to_thread(
                self.client.retrieve, collection_name=COLLECTION, ids=list(expected),
                with_payload=True, with_vectors=False,
            )
            if {str(item.id) for item in found} != expected:
                raise RuntimeError("向量点 ID 与预期不符")
            for item in found:
                payload = item.payload or {}
                if (
                    payload.get("version_id") != version_id
                    or payload.get("user_id") != user_id
                    or payload.get("document_id") != document_id
                    or payload.get("parent_id") != point_to_parent[str(item.id)]
                ):
                    raise RuntimeError("向量点归属或父块标记与预期不符")

    # 作用：仅删除指定失败版本的残留点，不影响当前已发布版本。
    async def delete_failed_version(self, *, version_id: str) -> None:
        condition = models.Filter(must=[models.FieldCondition(
            key="version_id", match=models.MatchValue(value=version_id)
        )])
        await asyncio.to_thread(
            self.client.delete, collection_name=COLLECTION,
            points_selector=models.FilterSelector(filter=condition), wait=True,
        )
