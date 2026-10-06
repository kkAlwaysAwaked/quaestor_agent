"""从函数签名生成工具 schema；调用参数校验与内部上下文注入分开处理。"""

from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

from pydantic import ConfigDict, create_model


TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


# 作用：注册模型可见的工具参数与执行函数，隐藏由 Worker 注入的消息和检索身份。
def register_tool(func: Callable):
    hints = get_type_hints(func, include_extras=True)
    fields = {}
    for name, parameter in inspect.signature(func).parameters.items():
        if name in ("agent_messages", "retrieval_context"):
            continue
        annotation = hints.get(name, Any)
        default = parameter.default if parameter.default is not inspect.Parameter.empty else ...
        fields[name] = (annotation, default)
    input_model = create_model(f"{func.__name__}_Input", __config__=ConfigDict(extra="forbid"), **fields)
    TOOL_REGISTRY[func.__name__] = {
        "schema": {
            "type": "function",
            "function": {
                "name": func.__name__,
                "description": inspect.getdoc(func) or "未提供工具描述",
                "parameters": input_model.model_json_schema(),
            },
        },
        "input_model": input_model,
        "execute": func,
    }
    return func
