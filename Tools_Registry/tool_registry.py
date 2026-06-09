import inspect
from typing import Callable, Dict, Any
from pydantic import create_model, ValidationError

# 全局工具注册中心
TOOL_REGISTRY : Dict[str, Dict[str, Any]] = {}

# 写一个Tool注册器
def register_tool(func : Callable):
    """Tool 注册装饰器"""
    name = func.__name__
    # 1. 提取 DocsString，LLM 理解工具的唯一途径
    description = inspect.getdoc(func) or "未提供工具描述"

# --- 提取函数签名，参数名字，参数类型的标准方法 ---

    # 2. 提取函数签名，动态构建 Pydantic Model
    sig = inspect.signature(func)
    fields = {}

    # 3.16 修改：增加一个标志位，做幽灵参数
    needs_context = False
    # 3. 把扫描到的参数名字、类型（是整数还是布尔值）、有没有默认值，一个个整理出来，塞进 fields 字典里。
    # sig.parameters.items 类似一个有序字典
    # inspect.Parameter 对象存放4个函数核心属性：.name, .annotation(类型注解), .default, .kind
    for param_name, param in sig.parameters.items():
        # 如果函数需要传入完整的大模型上下文，修改needs_context标志位，下文再传
        if param_name == "agent_messages":
            needs_context = True
            continue
        # 如果没有写类型注解，默认当做 Any (生产环境建议强制校验必须有注解)
        annotation = param.annotation if param.annotation != inspect._empty else Any
        default = param.default if param.default != inspect._empty else ...
        fields[param_name] = (annotation, default)
    # create_model 是 Pydantic 提供的一个强大功能。
    # 它利用刚才整理好的 fields，在代码运行的瞬间，
    # 凭空捏造出一个全新的 Pydantic 校验类（类名叫做 get_user_info_Input）。
    InputModel = create_model(f"{name}_Input", **fields)

    # 利用前面捏造出的 InputModel，调用 .model_json_schema() 方法，
    # 一键把 Python 的类型转换成 JSON Schema 格式。
    # 然后按照大模型官方规定的格式，拼装成一个大字典。
    tool_schema = {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": InputModel.model_json_schema()
        }
    }

    # 3. 包装执行函数：拦截、校验、执行、容错
    def execute_wrapper(**kwargs):
        try:
            engine_messages = kwargs.pop("agent_messages", None)

            validated_inputs = InputModel(**kwargs)
            final_args = validated_inputs.model_dump()

            if needs_context:
                final_args["agent_messages"] = engine_messages
            return func(**final_args)

        except ValidationError as e:
            return f"工具参数校验失败 (ToolInputError): \n{e.json()}"
        except Exception as e:
            return f"工具内部执行错误: {str(e)}"

    TOOL_REGISTRY[name] = {
        "schema" : tool_schema,
        "execute" : execute_wrapper
    }

    return execute_wrapper # 返回包装后的原函数


# ---测试代码---
# @register_tool
# def get_user_info(uid : int, include_history : bool = False) -> str:
#     """
#         根据用户 UID 查询用户基本信息和历史记录。
#     """
#     return f"查询成功: 用户 {uid}, 是否包含历史: {include_history}"
#
#
# if __name__ == "__main__":
#     # 1. 打印生成的 Schema，检查是否符合 OpenAI 格式
#     print("=== 生成的 Schema ===")
#     print(TOOL_REGISTRY["get_user_info"]["schema"])
#
#     # 2. 模拟 LLM 传来了正确的 JSON
#     print("\n=== 正常调用 ===")
#     print(TOOL_REGISTRY["get_user_info"]["execute"](uid=1001, include_history=True))
#
#     # 3. 模拟 LLM 犯错：把 int 传成了 string，且漏了必填字段
#     print("\n=== 模拟 LLM 幻觉 (防御拦截) ===")
#     # 比如 LLM 传了: {"uid": "一千零一"}
#     # 或者漏了必填的 uid: {"include_history": True}
#     print(TOOL_REGISTRY["get_user_info"]["execute"](uid="一千零一"))
