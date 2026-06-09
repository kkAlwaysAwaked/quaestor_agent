import asyncio
import sys
from pathlib import Path

import httpx

VENV_PYTHON = Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"


def _ensure_project_env():
    """用系统 Python 直接跑时，依赖往往未安装；提示改用项目虚拟环境。"""
    if VENV_PYTHON.exists() and Path(sys.executable).resolve() != VENV_PYTHON.resolve():
        print(
            "当前未使用项目虚拟环境，可能导致依赖缺失。\n"
            f"请改用: {VENV_PYTHON.name} run_agent_cli.py\n"
            f"或执行: {VENV_PYTHON} {Path(__file__).name}\n"
        )


try:
    from agent_engine import run_agent_async
except ModuleNotFoundError as exc:
    _ensure_project_env()
    print(f"导入失败: {exc}")
    print("若尚未安装依赖，请先执行: .venv\\Scripts\\pip install -r requirements.txt")
    raise SystemExit(1) from exc


def parse_sse_data(chunk: str) -> str:
    lines = []
    for line in chunk.splitlines():
        if line.startswith("data: "):
            lines.append(line.removeprefix("data: "))
    return "".join(lines)


async def main():
    question = input("User: ").strip()
    if not question:
        print("请输入问题后再启动 Agent。")
        return

    messages = [
        {
            "role": "system",
            "content": "你是一个基于内部知识库回答问题的 RAG Agent。需要事实依据时调用 RAG 工具；整段对话只允许调用一次 RAG，请在 query 中尽量覆盖问题要点，不要分多次检索。",
        },
        {"role": "user", "content": question},
    ]

    async with httpx.AsyncClient(timeout=None) as http_client:
        async for chunk in run_agent_async(messages, http_client):
            text = parse_sse_data(chunk)
            if text == "[DONE]":
                print()
                break
            if text:
                print(text.replace("<br>", "\n"), end="", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
