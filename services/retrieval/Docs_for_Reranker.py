"""从 MySQL 批量获取授权父块，替代本地 docstore.json。"""

from core.retrieval_contracts import RetrievalScope
from infra.mysql.repositories.retrieval import fetch_parent_documents


# 作用：为一次父块批量查询创建独立会话，并保持 RRF 顺序和来源信息。
async def fetch_parent_docs_by_ids(sorted_parents: list[tuple[str, float]], *, sessions, scope: RetrievalScope) -> list[dict]:
    async with sessions() as session:
        return await fetch_parent_documents(session, ranked_parents=sorted_parents, scope=scope)
