# 对文件进行切分 + 入库
# 首次运行会拉取 embedding 模型；国内网络通过 model_hub_setup 配置镜像源
from core import model_hub_setup  # noqa: F401

import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# 双策略父文档切分：
# 去掉 front matter
# 若存在 # 标题且能切出 多于 1 块 → 仍用 Markdown 标题 切父文档
# 否则（典型 PDF / 部分 PPT）→ 用 RecursiveCharacterTextSplitter，按 \n\n、句号等切，父块约 1500 字、重叠 200
import re
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from qdrant_client import QdrantClient, models
from fastembed import TextEmbedding, SparseTextEmbedding
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter


@dataclass
class ParentChunk:
    page_content: str

# 无 # 标题时（常见于 PDF / PPT 转 Markdown）的父块切分参数
PARENT_CHUNK_SIZE = 1500
PARENT_CHUNK_OVERLAP = 200

# 一、全局初始化逻辑
print("初始化模型和数据库...")

DB_PATH = str(Path(__file__).resolve().parents[1] / "qdrant_db")
DOCSTORE_PATH = os.path.join(DB_PATH, "docstore.json")
COLLECTION_NAME = "hybrid_collection"
# 确保数据库主目录存在
os.makedirs(DB_PATH, exist_ok=True)

# 1. 创建Qdrant客户端
client = QdrantClient(path = DB_PATH)

# 2. 初始化 FastEmbed 模型（首次需联网下载，缓存一般在 ~/.cache/fastembed）
def _init_embedding_models():
    try:
        dense = TextEmbedding("BAAI/bge-small-en-v1.5")
        sparse = SparseTextEmbedding("prithivida/Splade_PP_en_v1")
        return dense, sparse
    except Exception as exc:
        err = str(exc).lower()
        if "timeout" in err or "connect" in err or "10060" in err:
            raise RuntimeError(
                "无法下载 embedding 模型（连接超时）。\n"
                "  1) 确认能访问网络；已默认使用国内镜像 HF_ENDPOINT=https://hf-mirror.com\n"
                "  2) 若用代理，设置 HTTPS_PROXY 后再运行\n"
                "  3) 或在能联网的环境先跑一次，待模型缓存到 %USERPROFILE%\\.cache\\fastembed 后再离线用"
            ) from exc
        raise


dense_model, sparse_model = _init_embedding_models()

# 3. 检查集合是否存在，不存在才创建 (极其重要)
if not client.collection_exists(collection_name=COLLECTION_NAME):
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={"dense_vector": models.VectorParams(size=384, distance=models.Distance.COSINE)},
        sparse_vectors_config={"sparse_vector": models.SparseVectorParams()}
    )
    print(f"✅ 创建了新的 Qdrant 集合: {COLLECTION_NAME}")
else:
    print(f"ℹ️ 集合 {COLLECTION_NAME} 已存在，准备追加数据。")

# 4. 父文档存储：python字典
# 格式：{"uuid-xxxx": "完整的 Markdown 文本"}
if os.path.exists(DOCSTORE_PATH):
    with open(DOCSTORE_PATH, "r", encoding="utf-8") as f:
        docstore = json.load(f)
    print(f"ℹ️ 已加载本地 Docstore，当前包含 {len(docstore)} 个父文档。")
else:
    docstore = {}

# 二、单文本切分和处理逻辑
def strip_front_matter(text: str) -> str:
    """去掉 document_loader 写入的 YAML front matter。"""
    if not text.startswith("---"):
        return text
    end = text.find("\n---", 3)
    if end == -1:
        return text
    return text[end + 4 :].lstrip("\n")


def has_markdown_headers(text: str) -> bool:
    return bool(re.search(r"(?m)^#{1,3}\s+\S", text))


def split_parent_documents(text: str) -> tuple[list[ParentChunk], str]:
    """
    优先按 Markdown 标题切父文档；若无 # 标题（PDF 等），按段落/长度切分。
    返回 (父文档列表, 切分模式说明)。
    """
    body = strip_front_matter(text)

    if has_markdown_headers(body):
        headers_to_split_on = [
            ("#", "H1"),
            ("##", "H2"),
            ("###", "H3"),
        ]
        splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
        parent_docs = splitter.split_text(body)
        if len(parent_docs) > 1:
            return parent_docs, "Markdown 标题"

    parent_splitter = RecursiveCharacterTextSplitter(
        chunk_size=PARENT_CHUNK_SIZE,
        chunk_overlap=PARENT_CHUNK_OVERLAP,
        separators=["\n\n\n", "\n\n", "\n", "。", "！", "？", ".", "!", "?", " ", ""],
    )
    chunks = parent_splitter.split_text(body)
    docs = [ParentChunk(page_content=c) for c in chunks if c.strip()]
    return docs, "段落/长度（无有效标题结构）"


def ingest_data(markdown_text, source_name="unknown"):
    print(f"\n📦 正在处理文件: {source_name}")

    # 1. 父文档切分（有 # 用标题；无 # 用段落/长度）
    parent_docs, split_mode = split_parent_documents(markdown_text)
    print(f"  -> 父文档切分模式: {split_mode}，共 {len(parent_docs)} 块")

    # 2. 子文档切分：按句号切
    child_splitter = RecursiveCharacterTextSplitter(
        separators=["。", "！", "？", ".", "!", "?"],
        chunk_size=10,
        chunk_overlap=0
    )
    # 暂存子文档 Qdrant 数据点
    points = []

    for p_doc in parent_docs:
        parent_id = str(uuid.uuid4())
        # 存入父文档字典：{"parent_id"："原文"}
        docstore[parent_id] = {
            "source": source_name,
            "content": p_doc.page_content
        }
        # 准备子文档
        child_chunks = child_splitter.split_text(p_doc.page_content)

        for chunk in child_chunks:
            if not chunk.strip(): continue

            # 生成稠密向量和稀疏向量
            dense_vec = list(dense_model.embed([chunk]))[0].tolist()
            sparse_vec = list(sparse_model.embed([chunk]))[0]

            # 组装 Qdrant Point
            points.append(models.PointStruct(
                id=str(uuid.uuid4()),  # 子文档自己的 ID
                vector={
                    "dense_vector": dense_vec,
                    "sparse_vector": models.SparseVector(
                        indices=sparse_vec.indices.tolist(),
                        values=sparse_vec.values.tolist()
                    )
                },
                payload={"parent_id": parent_id, "text": chunk}  # text存入可选，为了调试方便可以留着
            ))

    if points:
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        print(f"  -> ✅ 入库成功！提取了 {len(parent_docs)} 个父文档，{len(points)} 个子句子。")
    else:
        print(f"  -> ⚠️ 文件无有效文本内容。")


# 三、批量读取文件并落盘
def build_database(folder_path):
    path = Path(folder_path)
    if not path.exists() or not path.is_dir():
        print(f"❌ 找不到原始数据文件夹: {folder_path}")
        return

    file_count = 0
    # 遍历文件夹下的所有 md 和 txt 文件
    for file_path in path.rglob("*"):
        if file_path.suffix.lower() in [".md", ".txt"]:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            # 调用处理逻辑
            ingest_data(content, source_name=file_path.name)
            file_count += 1

    # 【核心！】所有文件处理完毕后，把字典保存到 D 盘的 json 文件里
    with open(DOCSTORE_PATH, "w", encoding="utf-8") as f:
        json.dump(docstore, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 数据库构建完毕！共处理了 {file_count} 个文件。")
    print(f"💾 Qdrant 向量数据位于: {DB_PATH}")
    print(f"💾 父文档原文数据位于: {DOCSTORE_PATH}")


# 四、执行入口
if __name__ == "__main__":
    # document_loader 输出的 Markdown 目录
    SOURCE_DATA_FOLDER = r"D:\advanced_RAG\data\raw_md"

    # 确保文件夹存在，如果没有则自动创建一个空的
    os.makedirs(SOURCE_DATA_FOLDER, exist_ok=True)

    print(f"请确保你的文本文件已经放在了 {SOURCE_DATA_FOLDER} 下面。")
    try:
        build_database(SOURCE_DATA_FOLDER)
    finally:
        client.close()
