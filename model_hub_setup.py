"""
模型下载源配置：在导入 huggingface_hub / fastembed / sentence_transformers 之前执行。

说明：清华大学 TUNA 镜像站已于 2021 年移除 hugging-face-models，不再提供 HF 模型镜像。
此处对仍走 HuggingFace Hub 的组件（如 fastembed）使用国内社区镜像；
对 CrossEncoder 重排模型则通过 ModelScope（魔搭，国内 CDN）下载到本地后加载。
"""
import os
from pathlib import Path

# fastembed / transformers 等仍通过 huggingface_hub 拉取时使用的镜像
if not os.environ.get("HF_ENDPOINT"):
    os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

MODELS_DIR = Path(__file__).resolve().parent / "models"


def ensure_model_from_modelscope(model_id: str, local_dir: Path | None = None) -> str:
    """从 ModelScope 下载模型到本地目录，返回可用于 from_pretrained / CrossEncoder 的路径。"""
    from modelscope import snapshot_download

    if local_dir is None:
        local_dir = MODELS_DIR / model_id.replace("/", "__")

    local_dir = Path(local_dir)
    marker = local_dir / "config.json"
    if marker.exists():
        return str(local_dir)

    local_dir.parent.mkdir(parents=True, exist_ok=True)
    return snapshot_download(model_id, local_dir=str(local_dir))
