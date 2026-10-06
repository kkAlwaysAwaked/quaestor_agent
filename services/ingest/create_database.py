"""单份 Markdown 的确定性切分和向量编码；导入模块时不加载模型或连接数据库。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from qdrant_client import models

from core.config import model_cache_dir
from core.index_config import DENSE_MODEL, SPARSE_MODEL
from infra.topology import DENSE_VECTOR, SPARSE_VECTOR


PARENT_SIZE = 1500
PARENT_OVERLAP = 200
CHILD_SIZE = 10
PROCESSING_CONFIG = {
    "pipeline_version": 1,
    "fastembed_version": "0.7.4",
    "splitter_version": "1.1.1",
    "dense_model": DENSE_MODEL,
    "sparse_model": SPARSE_MODEL,
    "parent_size": PARENT_SIZE,
    "parent_overlap": PARENT_OVERLAP,
    "child_size": CHILD_SIZE,
}


@dataclass(frozen=True)
class ParentText:
    id: str
    index: int
    content: str


@dataclass(frozen=True)
class ChildText:
    id: str
    parent_id: str
    content: str


@dataclass(frozen=True)
class ChunkPlan:
    parents: list[ParentText]
    children: list[ChildText]


# 作用：删除文档加载器生成的 YAML 头，只把正文送入切分器。
def strip_front_matter(text: str) -> str:
    if not text.startswith("---"):
        return text
    end = text.find("\n---", 3)
    return text[end + 4:].lstrip("\n") if end >= 0 else text


# 作用：按固定配置将单份文档切成父块和子块，并由版本及序号生成可重试的稳定 ID。
def split_document(text: str, *, version_id: str) -> ChunkPlan:
    body = strip_front_matter(text)
    if not body.strip():
        raise ValueError("文档没有可入库的正文")
    parent_docs = []
    if re.search(r"(?m)^#{1,3}\s+\S", body):
        parent_docs = MarkdownHeaderTextSplitter(
            headers_to_split_on=[("#", "H1"), ("##", "H2"), ("###", "H3")]
        ).split_text(body)
    if len(parent_docs) <= 1:
        parent_docs = RecursiveCharacterTextSplitter(
            chunk_size=PARENT_SIZE,
            chunk_overlap=PARENT_OVERLAP,
            separators=["\n\n\n", "\n\n", "\n", "。", "！", "？", ".", "!", "?", " ", ""],
        ).split_text(body)
    parent_contents = [doc.page_content if hasattr(doc, "page_content") else doc for doc in parent_docs]
    child_splitter = RecursiveCharacterTextSplitter(
        separators=["。", "！", "？", ".", "!", "?", ""],
        chunk_size=CHILD_SIZE,
        chunk_overlap=0,
    )
    parents: list[ParentText] = []
    children: list[ChildText] = []
    for parent_index, content in enumerate(parent_contents):
        if not content.strip():
            continue
        parent_id = str(uuid5(NAMESPACE_URL, f"agentic-rag:{version_id}:parent:{parent_index}"))
        parents.append(ParentText(parent_id, parent_index, content))
        for child_index, chunk in enumerate(child_splitter.split_text(content)):
            if chunk.strip():
                child_id = str(uuid5(NAMESPACE_URL, f"agentic-rag:{version_id}:child:{parent_index}:{child_index}"))
                children.append(ChildText(child_id, parent_id, chunk))
    if not parents or not children:
        raise ValueError("文档切分后没有有效父块或子块")
    return ChunkPlan(parents, children)


class EmbeddingEncoder:
    """在 Worker 启动时创建模型，同一个 Worker 内复用。"""

    # 作用：按入库配置加载 Dense 和 Sparse 模型，不在模块导入阶段触发下载。
    def __init__(self) -> None:
        from fastembed import SparseTextEmbedding, TextEmbedding

        cache = model_cache_dir()
        cache.mkdir(parents=True, exist_ok=True)
        self.dense = TextEmbedding(model_name=DENSE_MODEL, cache_dir=str(cache))
        self.sparse = SparseTextEmbedding(model_name=SPARSE_MODEL, cache_dir=str(cache))

    # 作用：批量编码子块并生成带用户、文档、版本和父块标识的 Qdrant 点。
    def encode(self, plan: ChunkPlan, *, user_id: str, document_id: str, version_id: str) -> list[models.PointStruct]:
        texts = [child.content for child in plan.children]
        dense_vectors = list(self.dense.embed(texts))
        sparse_vectors = list(self.sparse.embed(texts))
        if len(dense_vectors) != len(texts) or len(sparse_vectors) != len(texts):
            raise RuntimeError("Embedding 模型返回的向量数量与子块数量不一致")
        points = []
        for child, dense, sparse in zip(plan.children, dense_vectors, sparse_vectors, strict=True):
            points.append(models.PointStruct(
                id=child.id,
                vector={
                    DENSE_VECTOR: dense.tolist(),
                    SPARSE_VECTOR: models.SparseVector(
                        indices=sparse.indices.tolist(), values=sparse.values.tolist()
                    ),
                },
                payload={
                    "user_id": user_id,
                    "document_id": document_id,
                    "version_id": version_id,
                    "parent_id": child.parent_id,
                    "text": child.content,
                },
            ))
        return points
