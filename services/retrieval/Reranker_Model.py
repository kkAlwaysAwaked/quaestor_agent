"""保留 CrossEncoder 重排算法；模型由服务启动流程注入。"""

from operator import itemgetter


# 作用：为已通过 MySQL 权限验证的父块评分，并保留来源、版本和融合分数。
def rerank_documents(query: str, retrieved_docs: list[dict], *, model, top_k: int = 5) -> list[dict]:
    if not retrieved_docs:
        return []
    scores = model.predict([[query, document["text"]] for document in retrieved_docs])
    if len(scores) != len(retrieved_docs):
        raise RuntimeError("重排模型返回的分数数量不一致")
    scored = []
    for document, score in zip(retrieved_docs, scores, strict=True):
        scored.append({**document, "rerank_score": float(score)})
    ordered = sorted(scored, key=itemgetter("rerank_score"), reverse=True)
    return [{
        "parent_id": document["id"], "content": document["text"],
        "source": document["metadata"], "document_id": document["document_id"],
        "version_id": document["version_id"], "rrf_score": document["rrf_score"],
        "rerank_score": document["rerank_score"],
    } for document in ordered[:top_k]]
