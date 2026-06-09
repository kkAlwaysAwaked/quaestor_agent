"""
Agent 自动化评测脚本。

测试集格式：
  - 问题
  - 需要召回的文档
  - 期望召回的父文档id（必要证据，<=5，对齐精排 Top-5）
  - 可选召回的父文档id（补充证据，不计入主召回率分母）
  - 预期agent回答

结果格式（在测试集基础上增加）：
  - 实际回答
  - RAG召回的父文档id

用法：
  .venv\\Scripts\\python.exe run_agent_eval.py
  .venv\\Scripts\\python.exe run_agent_eval.py --input data/eval/test_cases.example.jsonl --output data/eval/results.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from agent_engine import run_agent_async

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_ROOT / "data" / "eval" / "test_cases.example.jsonl"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "eval" / "results.jsonl"
SYSTEM_PROMPT = "你是一个基于内部知识库回答问题的 RAG Agent。需要事实依据时调用 RAG 工具；整段对话只允许调用一次 RAG，请在 query 中尽量覆盖问题要点，不要分多次检索。"

FIELD_QUESTION = "问题"
FIELD_EXPECTED_DOCS = "需要召回的文档"
FIELD_EXPECTED_PARENT_IDS = "期望召回的父文档id"
FIELD_OPTIONAL_PARENT_IDS = "可选召回的父文档id"
FIELD_EXPECTED_ANSWER = "预期agent回答"
FIELD_ACTUAL_ANSWER = "实际回答"
FIELD_RAG_PARENT_IDS = "RAG召回的父文档id"


def parse_sse_data(chunk: str) -> str:
    lines = []
    for line in chunk.splitlines():
        if line.startswith("data: "):
            lines.append(line.removeprefix("data: "))
    return "".join(lines)


def _normalize_field(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(v).strip() for v in value if str(v).strip())
    return str(value).strip()


def _normalize_parent_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def normalize_test_case(raw: Any, index: int) -> dict[str, Any]:
    """将 JSON / JSONL 中的一条样本统一为四元组字段（此时尚无实际回答与 RAG 召回）。"""
    if isinstance(raw, list):
        if len(raw) < 3:
            raise ValueError(f"第 {index + 1} 条样本应为长度 >= 3 的列表，当前: {raw}")
        question, expected_docs, expected_answer = raw[0], raw[1], raw[2]
        expected_parent_ids: Any = raw[3] if len(raw) >= 4 else []
        optional_parent_ids: Any = raw[4] if len(raw) >= 5 else []
    elif isinstance(raw, dict):
        question = raw.get(FIELD_QUESTION) or raw.get("question")
        expected_docs = raw.get(FIELD_EXPECTED_DOCS) or raw.get("expected_docs")
        expected_answer = raw.get(FIELD_EXPECTED_ANSWER) or raw.get("expected_answer")
        expected_parent_ids = (
            raw.get(FIELD_EXPECTED_PARENT_IDS)
            or raw.get("expected_parent_ids")
            or []
        )
        optional_parent_ids = (
            raw.get(FIELD_OPTIONAL_PARENT_IDS)
            or raw.get("optional_parent_ids")
            or []
        )
        if question is None or expected_docs is None or expected_answer is None:
            raise ValueError(
                f"第 {index + 1} 条样本缺少必要字段，"
                f"需要 {FIELD_QUESTION!r}、{FIELD_EXPECTED_DOCS!r}、{FIELD_EXPECTED_ANSWER!r}"
            )
    else:
        raise ValueError(f"第 {index + 1} 条样本格式不支持: {type(raw)}")

    return {
        FIELD_QUESTION: _normalize_field(question),
        FIELD_EXPECTED_DOCS: _normalize_field(expected_docs),
        FIELD_EXPECTED_PARENT_IDS: _normalize_parent_ids(expected_parent_ids),
        FIELD_OPTIONAL_PARENT_IDS: _normalize_parent_ids(optional_parent_ids),
        FIELD_EXPECTED_ANSWER: _normalize_field(expected_answer),
    }


def _load_raw_cases(path: Path) -> list[Any]:
    """支持 JSON 数组文件，也支持 JSONL（每行一个对象）。"""
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"测试集为空: {path}")

    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"{path} 顶层应为数组")
        return data

    cases: list[Any] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            cases.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_no} JSON 解析失败: {exc}") from exc
    return cases


def load_test_cases(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"找不到测试集: {path}")

    suffix = path.suffix.lower()
    if suffix not in {".json", ".jsonl"}:
        raise ValueError(f"仅支持 .json / .jsonl，当前文件: {path}")

    cases = _load_raw_cases(path)
    if not cases:
        raise ValueError(f"测试集为空: {path}")

    return [normalize_test_case(item, i) for i, item in enumerate(cases)]


def collect_rag_parent_ids(rag_trace: list[dict[str, Any]]) -> list[str]:
    """合并 Agent 各轮 RAG 调用返回的父文档 id（保序去重）。"""
    seen: set[str] = set()
    merged: list[str] = []
    for entry in rag_trace:
        for pid in entry.get("retrieved_parent_ids", []):
            if pid and pid not in seen:
                seen.add(pid)
                merged.append(pid)
    return merged


def compute_parent_recall(expected_ids: list[str], actual_ids: list[str]) -> float | None:
    if not expected_ids:
        return None
    hit = set(expected_ids) & set(actual_ids)
    return len(hit) / len(set(expected_ids))


async def run_single_question(
    question: str,
    http_client: httpx.AsyncClient,
    *,
    verbose: bool = True,
) -> tuple[str, list[str]]:
    """调用 Agent，返回 (最终回答, RAG 召回的父文档 id 列表)。"""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]

    answer_parts: list[str] = []
    in_final_answer = False
    rag_trace: list[dict[str, Any]] = []

    async for chunk in run_agent_async(messages, http_client, rag_trace=rag_trace):
        text = parse_sse_data(chunk)
        if text == "[DONE]":
            if verbose:
                print(flush=True)
            break
        if not text:
            continue

        display = text.replace("<br>", "\n")

        if "思考完毕，开始输出最终答案" in text:
            in_final_answer = True
            if verbose:
                print(display, end="", flush=True)
            continue

        if in_final_answer:
            answer_parts.append(display)
            if verbose:
                print(display, end="", flush=True)
        elif text.startswith("⚠️ [系统保护]"):
            if verbose:
                print(display, flush=True)
            return text.strip(), collect_rag_parent_ids(rag_trace)
        elif verbose:
            print(display, end="", flush=True)

    return "".join(answer_parts).strip(), collect_rag_parent_ids(rag_trace)


async def evaluate_all(cases: list[dict[str, Any]], quiet: bool = False) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    verbose = not quiet

    async with httpx.AsyncClient(timeout=None) as http_client:
        for idx, case in enumerate(cases, start=1):
            question = case[FIELD_QUESTION]
            print(f"\n[{idx}/{len(cases)}] 评测问题: {question}")
            if verbose:
                print("-" * 60)

            try:
                actual_answer, rag_parent_ids = await run_single_question(
                    question,
                    http_client,
                    verbose=verbose,
                )
            except Exception as exc:
                actual_answer = f"[评测异常] {exc}"
                rag_parent_ids = []
                print(f"  -> 失败: {exc}")

            required_recall = compute_parent_recall(
                case.get(FIELD_EXPECTED_PARENT_IDS, []),
                rag_parent_ids,
            )
            optional_recall = compute_parent_recall(
                case.get(FIELD_OPTIONAL_PARENT_IDS, []),
                rag_parent_ids,
            )

            result = {
                **case,
                FIELD_ACTUAL_ANSWER: actual_answer,
                FIELD_RAG_PARENT_IDS: rag_parent_ids,
            }
            results.append(result)

            preview = actual_answer.replace("\n", " ")
            print(f"\n  -> RAG召回父文档: {rag_parent_ids}")
            if required_recall is not None:
                req_hit = set(case[FIELD_EXPECTED_PARENT_IDS]) & set(rag_parent_ids)
                req_total = len(set(case[FIELD_EXPECTED_PARENT_IDS]))
                print(f"  -> 必要召回率: {required_recall:.0%} ({len(req_hit)}/{req_total})")
            opt_ids = case.get(FIELD_OPTIONAL_PARENT_IDS, [])
            if opt_ids and optional_recall is not None:
                opt_hit = set(opt_ids) & set(rag_parent_ids)
                print(f"  -> 可选召回率: {optional_recall:.0%} ({len(opt_hit)}/{len(set(opt_ids))})")
            print(f"  -> 实际回答: {preview[:120]}{'...' if len(preview) > 120 else ''}")

    return results


def save_results(results: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        with path.open("w", encoding="utf-8") as f:
            for row in results:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    elif suffix == ".json":
        with path.open("w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    else:
        raise ValueError(f"输出文件仅支持 .json / .jsonl: {path}")


def build_default_output_path(output: Path | None, input_path: Path) -> Path:
    if output is not None:
        return output
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "data" / "eval" / f"results_{stamp}.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agent 自动化评测：四元组测试集 -> 含 RAG 召回的结果")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"测试集路径（.json / .jsonl），默认 {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="结果输出路径（.json / .jsonl）；默认 data/eval/results_时间戳.jsonl",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="静默模式：不显示 Agent 思考过程，仅输出每条样本摘要",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()
    output_path = build_default_output_path(args.output, input_path).resolve()

    print(f"加载测试集: {input_path}")
    cases = load_test_cases(input_path)
    print(f"共 {len(cases)} 条样本，开始评测...\n")

    results = await evaluate_all(cases, quiet=args.quiet)

    save_results(results, output_path)
    print(f"\n评测完成，结果已保存: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
