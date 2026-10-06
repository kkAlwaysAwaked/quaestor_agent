"""只读盘点旧本地 Qdrant 与 docstore，决定走迁移还是固定样本分支。"""

from __future__ import annotations

import json
import sys

from core.config import PROJECT_ROOT


# 作用：读取旧本地目录元数据并报告集合和父块正文文件是否存在。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    root = PROJECT_ROOT / "qdrant_db"
    meta_path = root / "meta.json"
    docstore_path = root / "docstore.json"
    if not meta_path.is_file():
        print("旧 Qdrant 元数据不存在；请确认旧数据来源，不能假定已有可迁移数据")
        return
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    collections = meta.get("collections") or {}
    print(f"旧本地集合：{list(collections)}")
    print(f"旧 docstore.json：{'存在' if docstore_path.is_file() else '不存在'}")
    if collections or docstore_path.is_file():
        print("发现旧数据：先停写、备份并确认归属用户与来源映射，再执行迁移")
    else:
        print("未发现旧数据：按固定样本文档建立新基线")


if __name__ == "__main__":
    main()
