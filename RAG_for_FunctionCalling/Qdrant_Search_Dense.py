# 用 HyDE 结果进行 密集/向量(dense) 检索
import model_hub_setup  # noqa: F401 — 配置 HF 镜像后再加载 fastembed

from fastembed import TextEmbedding

from .qdrant_shared import client, COLLECTION_NAME

# Config: 配置dense_model
dense_model = TextEmbedding("BAAI/bge-small-en-v1.5")
# Dense
async def qdrant_search_dense(hyde_text, limit = 50):
    """
    接收 HyDE 生成的“长段落”，去 qdrant 的 dense_vector 抽屉里搜语义。
    """
    print("[Dense] 使用 HyDE 文本进行向量检索...")

    dense_vec = list(dense_model.embed([hyde_text]))[0].tolist()
    # 集合名称：在Qdrant里查找哪个库？
    raw_results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=dense_vec,
        using="dense_vector",  # 明确告诉它去哪个抽屉找
        limit=limit
    ).points

    formatted_results = [
        {
            "id": point.id,  # 子文档的 ID（用于 RRF 打分）
            "parent_id": point.payload["parent_id"],
            # 注意：这里连 text 都不用提取了，把极简主义发挥到极致！
        }
        for point in raw_results
    ]

    return formatted_results
