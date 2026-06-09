"""
Agentic RAG — Gradio Web UI

启动方式：
  .venv\\Scripts\\python.exe run_agent_ui.py
  .venv\\Scripts\\python.exe run_agent_ui.py --port 7860 --share
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import httpx

VENV_PYTHON = Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"

# Gradio 6 会在启动时用 httpx 请求本机 startup-events。
# 在开启系统代理的 Windows 环境中，localhost 请求可能被代理转发并返回 502。
LOCAL_PROXY_BYPASS = "localhost,127.0.0.1,0.0.0.0,::1"
for proxy_key in ("NO_PROXY", "no_proxy"):
    current_value = os.environ.get(proxy_key)
    if current_value:
        if "127.0.0.1" not in current_value and "localhost" not in current_value:
            os.environ[proxy_key] = f"{current_value},{LOCAL_PROXY_BYPASS}"
    else:
        os.environ[proxy_key] = LOCAL_PROXY_BYPASS

SYSTEM_PROMPT = (
    "你是一个基于内部知识库回答问题的 RAG Agent。"
    "需要事实依据时调用 RAG 工具；整段对话只允许调用一次 RAG，"
    "请在 query 中尽量覆盖问题要点，不要分多次检索。"
)

APP_CSS = """
.gradio-container {
    max-width: 1120px !important;
    margin: auto;
    background:
        radial-gradient(circle at 12% 0%, rgba(99, 102, 241, 0.14), transparent 28%),
        radial-gradient(circle at 88% 8%, rgba(14, 165, 233, 0.12), transparent 30%);
}

.app-hero {
    padding: 28px 30px;
    border: 1px solid rgba(148, 163, 184, 0.22);
    border-radius: 24px;
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(30, 41, 59, 0.92));
    color: #f8fafc;
    box-shadow: 0 22px 70px rgba(15, 23, 42, 0.18);
}

.app-eyebrow {
    margin-bottom: 10px;
    color: #93c5fd;
    font-size: 13px;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
}

.app-hero h1 {
    margin: 0 0 12px;
    font-size: 38px;
    line-height: 1.1;
    color: #ffffff;
}

.app-hero p {
    max-width: 760px;
    margin: 0;
    color: #cbd5e1;
    font-size: 16px;
    line-height: 1.7;
}

.feature-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 14px;
    margin: 18px 0 12px;
}

.feature-card {
    padding: 18px;
    border: 1px solid rgba(148, 163, 184, 0.24);
    border-radius: 18px;
    background: rgba(255, 255, 255, 0.74);
    box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
}

.feature-card h3 {
    margin: 0 0 8px;
    color: #0f172a;
    font-size: 16px;
}

.feature-card p {
    margin: 0;
    color: #475569;
    font-size: 14px;
    line-height: 1.65;
}

.flow-bar {
    margin: 10px 0 20px;
    padding: 14px 18px;
    border-radius: 16px;
    background: rgba(238, 242, 255, 0.82);
    color: #334155;
    font-size: 14px;
}

.chat-panel {
    border-radius: 22px;
    overflow: hidden;
    border: 1px solid rgba(148, 163, 184, 0.22);
    box-shadow: 0 14px 44px rgba(15, 23, 42, 0.10);
}

footer {
    display: none !important;
}

@media (max-width: 860px) {
    .feature-grid {
        grid-template-columns: 1fr;
    }

    .app-hero h1 {
        font-size: 30px;
    }
}
"""


def _ensure_project_env() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable).resolve() != VENV_PYTHON.resolve():
        print(
            "当前未使用项目虚拟环境，可能导致依赖缺失。\n"
            f"请改用: {VENV_PYTHON} {Path(__file__).name}\n"
        )


try:
    import gradio as gr
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


def build_messages(user_message: str, history: list[dict]) -> list[dict]:
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})
    return messages


async def chat_stream(message: str, history: list[dict]):
    """Gradio ChatInterface 流式回调：复用 agent_engine.run_agent_async。"""
    if not message.strip():
        return

    messages = build_messages(message, history)
    response = ""

    async with httpx.AsyncClient(timeout=None) as http_client:
        async for chunk in run_agent_async(messages, http_client):
            text = parse_sse_data(chunk)
            if text == "[DONE]":
                break
            if text:
                response += text.replace("<br>", "\n")
                yield response


def create_demo() -> gr.Blocks:
    description = (
        "输入业务问题后，Agent 会判断是否需要调用内部知识库检索工具，"
        "并将检索结果整合为可追溯、低幻觉的回答。"
    )

    with gr.Blocks(title="Agentic RAG Assistant") as demo:
        gr.HTML(
            """
            <section class="app-hero">
                <div class="app-eyebrow">Agentic Retrieval-Augmented Generation</div>
                <h1>Agentic RAG Assistant</h1>
                <p>
                    面向私有知识库的智能问答界面。系统会结合多轮上下文理解、查询改写、
                    混合检索、父子文档映射和重排序能力，让 Agent 在需要事实依据时主动检索，
                    并基于证据生成结构化回答。
                </p>
            </section>
            """
        )

        gr.HTML(
            """
            <section class="feature-grid">
                <div class="feature-card">
                    <h3>它能做什么</h3>
                    <p>
                        将 CLI 里的 RAG Agent 封装为可交互 Web 界面，
                        支持自然语言提问、流式回答、工具调用状态展示和多轮上下文传递。
                    </p>
                </div>
                <div class="feature-card">
                    <h3>检索链路</h3>
                    <p>
                        通过 Query Rewrite / HyDE 扩展查询意图，结合 Dense + Sparse
                        混合召回、RRF 融合和 Cross-Encoder 精排提升命中质量。
                    </p>
                </div>
                <div class="feature-card">
                    <h3>可靠性设计</h3>
                    <p>
                        每段对话限制一次真实 RAG 调用，减少重复检索和上下文污染；
                        证据不足时要求说明信息缺口，避免无依据编造。
                    </p>
                </div>
            </section>
            <div class="flow-bar">
                工作流：用户问题 → Agent 判断是否检索 → 查询改写 / HyDE → 混合检索 →
                父文档映射与重排 → 基于证据生成回答
            </div>
            """
        )

        with gr.Group(elem_classes=["chat-panel"]):
            gr.ChatInterface(
                fn=chat_stream,
                title=None,
                description=description,
                cache_examples=False,
                concurrency_limit=1,
                fill_height=True,
                autoscroll=True,
                stop_btn=True,
                flagging_mode="never",
                chatbot=gr.Chatbot(
                    height=560,
                    placeholder="请输入你想查询的问题，Agent 会在需要时自动调用内部知识库。",
                    buttons=["copy", "copy_all"],
                ),
                textbox=gr.Textbox(
                    placeholder="输入问题，例如：请根据知识库说明这个方案的关键约束和依据。",
                    lines=2,
                    max_lines=6,
                    submit_btn="发送",
                ),
            )

        gr.Markdown(
            "<center><sub>"
            "启动命令：<code>.venv\\Scripts\\python.exe run_agent_ui.py</code>"
            "</sub></center>"
        )

    return demo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agentic RAG Gradio UI")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=7860, help="监听端口，默认 7860")
    parser.add_argument("--share", action="store_true", help="生成 Gradio 公网临时链接")
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args()
    demo = create_demo()
    print(f"正在启动 UI：http://{args.host}:{args.port}")
    demo.launch(
        server_name=args.host,
        server_port=args.port,
        share=args.share,
        show_error=True,
        theme=gr.themes.Soft(primary_hue="indigo", secondary_hue="slate"),
        css=APP_CSS,
    )


if __name__ == "__main__":
    main()
