"""按版本 ID 清理失败入库留下的 Qdrant 点；不删除 MySQL 审计记录。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from uuid import UUID

from sqlalchemy import select

from core.config import load_qdrant_runtime_settings
from infra.mysql.models import Document, DocumentVersion, IngestJob
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.mysql.status import DocumentVersionStatus, TaskStatus
from services.ingest.vector_store import IngestVectorStore


# 作用：先核实 MySQL 中版本已失败，再仅清理该版本的向量残留。
async def cleanup(version_id: str) -> None:
    qdrant = load_qdrant_runtime_settings()
    engine = create_mysql_engine()
    vectors = IngestVectorStore(url=qdrant.url, api_key=qdrant.api_key)
    try:
        sessions = create_session_factory(engine)
        async with sessions() as session:
            version = await session.get(DocumentVersion, version_id)
            if version is None or version.status != DocumentVersionStatus.FAILED.value:
                raise ValueError("只允许清理数据库中已标记 failed 的版本")
            document = await session.get(Document, version.document_id)
            job = await session.scalar(select(IngestJob).where(IngestJob.version_id == version_id))
            if (
                document is None or document.current_version_id == version_id
                or job is None or job.status != TaskStatus.FAILED.value
            ):
                raise ValueError("失败版本仍被当前文档引用或任务尚未最终失败")
        await vectors.delete_failed_version(version_id=version_id)
        print(f"已清理失败版本 {version_id} 的 Qdrant 残留点")
    finally:
        await vectors.close()
        await engine.dispose()


# 作用：读取并校验命令行传入的目标版本 ID。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="清理失败文档版本的向量残留")
    parser.add_argument("version_id", help="明确指定 failed 文档版本的 UUID")
    args = parser.parse_args()
    asyncio.run(cleanup(str(UUID(args.version_id))))


if __name__ == "__main__":
    main()
