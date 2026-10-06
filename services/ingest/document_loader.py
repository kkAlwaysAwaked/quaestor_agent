# 将 Office / PDF 等原始文件转为 Markdown，供后续 RAG 建库使用

from __future__ import annotations

import shutil
from pathlib import Path

from markitdown import MarkItDown
from core.config import upload_root

# 需要 MarkItDown 转换的格式（.doc / .ppt 为旧版，需本机能识别；推荐 .docx / .pptx）
CONVERT_SUFFIXES = {
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xlsx",
    ".xls",
}

# 已是文本，直接复制到输出目录
PASS_THROUGH_SUFFIXES = {".md", ".txt"}

SUPPORTED_SUFFIXES = CONVERT_SUFFIXES | PASS_THROUGH_SUFFIXES

class DocumentLoader:
    # 作用：按需创建文档转换器，避免模块导入时加载外部转换工具。
    def __init__(self):
        self.converter = MarkItDown(enable_plugins=False)

    # 作用：读取单份支持的文档，转换失败时把错误交给调用方处理。
    def load_file(self, file_path: str | Path) -> dict:
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        suffix = path.suffix.lower()

        if suffix not in SUPPORTED_SUFFIXES:
            raise ValueError(f"暂不支持该文件类型: {suffix}")

        if suffix in PASS_THROUGH_SUFFIXES:
            text = path.read_text(encoding="utf-8")
        else:
            result = self.converter.convert_local(str(path))
            text = result.text_content or ""

        return {
            "source": path.name,
            "file_path": str(path.resolve()),
            "file_type": suffix,
            "text": text,
        }

    # 作用：读取目录中的所有支持文件，目录不存在时明确报错。
    def load_folder(self, folder_path: str | Path) -> list[dict]:
        folder = Path(folder_path)
        if not folder.is_dir():
            raise FileNotFoundError(f"输入目录不存在: {folder}")
        docs: list[dict] = []

        for path in sorted(folder.rglob("*")):
            if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES:
                docs.append(self.load_file(path))

        return docs

    # 作用：给转换后的 Markdown 正文附上来源文件信息。
    @staticmethod
    def _format_markdown(doc: dict) -> str:
        header = (
            "---\n"
            f"source: {doc['source']}\n"
            f"source_path: {doc['file_path']}\n"
            f"file_type: {doc['file_type']}\n"
            "---\n\n"
        )
        body = doc["text"].strip()
        return header + body + ("\n" if body else "")

    # 作用：转换单份文件并保存到指定输出目录。
    def export_file(self, file_path: str | Path, output_dir: str | Path) -> Path:
        """将单个文件转为 .md 并写入 output_dir（保持相对子目录结构）。"""
        src = Path(file_path).resolve()
        out_root = Path(output_dir)
        out_root.mkdir(parents=True, exist_ok=True)

        doc = self.load_file(src)
        out_path = out_root / f"{src.stem}.md"
        out_path.write_text(self._format_markdown(doc), encoding="utf-8")
        return out_path

    # 作用：批量转换支持的文件，失败时停止并显式报告来源文件。
    def export_folder(
        self,
        input_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        *,
        raw_root: str | Path | None = None,
    ) -> list[Path]:
        """
        批量转换 input_dir 下支持的文件，输出到 output_dir。

        raw_root: 计算相对路径的根目录，默认等于 input_dir；用于保留子文件夹结构。
        """
        in_root = Path(input_dir) if input_dir is not None else upload_root() / "raw"
        out_root = Path(output_dir) if output_dir is not None else upload_root() / "converted"
        base = Path(raw_root) if raw_root is not None else in_root

        if not in_root.exists():
            raise FileNotFoundError(f"输入目录不存在: {in_root}")

        out_root.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        for src in sorted(in_root.rglob("*")):
            if not src.is_file():
                continue

            suffix = src.suffix.lower()
            if suffix not in SUPPORTED_SUFFIXES:
                continue

            rel_parent = src.parent.relative_to(base) if src.parent != base else Path(".")
            target_dir = out_root / rel_parent
            target_dir.mkdir(parents=True, exist_ok=True)
            out_path = target_dir / f"{src.stem}.md"

            try:
                if suffix in PASS_THROUGH_SUFFIXES:
                    shutil.copy2(src, out_path)
                else:
                    doc = self.load_file(src)
                    out_path.write_text(
                        self._format_markdown(doc), encoding="utf-8"
                    )
                written.append(out_path)
                print(f"  -> {src.name} => {out_path.relative_to(out_root)}")
            except Exception as exc:
                raise RuntimeError(f"转换失败: {src}") from exc

        print(
            f"\n完成: 成功 {len(written)} 个"
            + f"\n输出目录: {out_root.resolve()}"
        )
        return written


# 作用：从环境配置的上传目录读取原始文件并导出转换后的 Markdown。
def main() -> None:
    loader = DocumentLoader()
    loader.export_folder()


if __name__ == "__main__":
    main()
