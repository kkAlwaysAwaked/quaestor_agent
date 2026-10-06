"""核对固定文档是否完整发布，并导出可追溯的版本/父块/向量点映射。"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy import select

from core.config import PROJECT_ROOT, load_qdrant_runtime_settings
from infra.mysql.models import Document, DocumentVersion, IngestJob, ParentChunk
from infra.mysql.session import create_mysql_engine, create_session_factory
from infra.mysql.status import DocumentVersionStatus, TaskStatus
from services.ingest.create_database import split_document
from services.ingest.submission import resolve_storage_key
from services.ingest.vector_store import IngestVectorStore


# 作用：核对数据库发布状态、父块与向量点完整性，并生成新基线的 ID 映射。
async def check_baseline(job_id: str, output: Path) -> None:
    qdrant = load_qdrant_runtime_settings()
    engine = create_mysql_engine()
    vectors = IngestVectorStore(url=qdrant.url, api_key=qdrant.api_key)
    try:
        sessions = create_session_factory(engine)
        async with sessions() as session:
            job = await session.get(IngestJob, job_id)
            if job is None or job.status != TaskStatus.SUCCEEDED.value:
                raise ValueError("入库任务不存在或尚未成功")
            version = await session.get(DocumentVersion, job.version_id)
            document = await session.get(Document, job.document_id)
            if (
                version is None or document is None
                or version.status != DocumentVersionStatus.PUBLISHED.value
                or document.current_version_id != version.id
                or document.user_id != job.user_id
            ):
                raise ValueError("文档版本尚未成为当前可检索版本")
            stored = (await session.scalars(
                select(ParentChunk).where(ParentChunk.version_id == version.id)
                .order_by(ParentChunk.chunk_index)
            )).all()
            file_data = resolve_storage_key(version.storage_key).read_bytes()
            if hashlib.sha256(file_data).hexdigest() != version.file_sha256:
                raise ValueError("原文文件哈希与版本记录不符")
            plan = split_document(file_data.decode("utf-8"), version_id=version.id)
            if [(row.id, row.content) for row in stored] != [
                (item.id, item.content) for item in plan.parents
            ]:
                raise ValueError("MySQL 父块与固定切分结果不一致")
            manifest = {
                "user_id": job.user_id,
                "document_id": document.id,
                "version_id": version.id,
                "job_id": job.id,
                "source": document.original_filename,
                "file_sha256": version.file_sha256,
                "processing_config": version.processing_config,
                "parent_ids": [item.id for item in plan.parents],
                "point_to_parent": {item.id: item.parent_id for item in plan.children},
                "regression": json.loads((
                    PROJECT_ROOT / "tests" / "fixtures" / "retrieval_baseline.json"
                ).read_text(encoding="utf-8")),
            }
        await vectors.verify(
            version_id=manifest["version_id"], user_id=manifest["user_id"],
            document_id=manifest["document_id"],
            point_to_parent=manifest["point_to_parent"],
        )
        output = output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"入库基线完整：{len(manifest['parent_ids'])} 个父块、{len(manifest['point_to_parent'])} 个向量点")
        print(f"ID 映射已保存：{output}")
    finally:
        await vectors.close()
        await engine.dispose()


# 作用：解析任务 ID 与输出位置，启动入库基线校验。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="验证并导出固定文档入库基线")
    parser.add_argument("job_id", help="seed_ingest_baseline 输出的入库任务 UUID")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "outputs" / "ingest_baseline.json")
    args = parser.parse_args()
    asyncio.run(check_baseline(str(UUID(args.job_id)), args.output))


if __name__ == "__main__":
    main()
