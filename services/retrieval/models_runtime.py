"""在服务启动时加载模型；模块导入时不下载或初始化推理模型。"""

from __future__ import annotations

from qdrant_client import models

from core.config import model_cache_dir
from core.index_config import DENSE_MODEL, RERANKER_MODEL, SPARSE_MODEL


class RetrievalModels:
    # 作用：加载与入库一致的向量模型，并从固定缓存目录加载重排模型。
    def __init__(self) -> None:
        from core import model_hub_setup
        from fastembed import SparseTextEmbedding, TextEmbedding
        from sentence_transformers import CrossEncoder

        cache = model_cache_dir()
        cache.mkdir(parents=True, exist_ok=True)
        self.dense = TextEmbedding(model_name=DENSE_MODEL, cache_dir=str(cache))
        self.sparse = SparseTextEmbedding(model_name=SPARSE_MODEL, cache_dir=str(cache))
        self.reranker = CrossEncoder(model_hub_setup.ensure_model_from_modelscope(RERANKER_MODEL))

    # 作用：把 HyDE 文本编码为与入库相同的 Dense 向量。
    def encode_dense(self, text: str) -> list[float]:
        return list(self.dense.embed([text]))[0].tolist()

    # 作用：把关键词编码为 Qdrant 接收的 SPLADE 稀疏向量。
    def encode_sparse(self, text: str) -> models.SparseVector:
        vector = list(self.sparse.embed([text]))[0]
        return models.SparseVector(indices=vector.indices.tolist(), values=vector.values.tolist())
