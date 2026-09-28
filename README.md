# Agentic RAG

项目正在按 [实现步骤](docs/实现步骤.md) 重构。当前基础设施的启动与检查命令见 [部署说明](deploy/README.md)，检索算法介绍见 [RAG 说明](RAG_for_FunctionCalling/README.md)。

| 目录 | 职责 |
| --- | --- |
| `agent/` | Agent 执行逻辑 |
| `core/` | 环境配置与模型下载设置 |
| `ingest/`、`markitdown/` | 文档处理与当前的建库脚本 |
| `RAG_for_FunctionCalling/` | 检索、融合与重排算法 |
| `Tools_Registry/` | Agent 工具注册 |
| `evaluation/` | 评估入口 |
| `infra/`、`scripts/`、`deploy/` | 中间件拓扑、初始化检查与 Compose 编排 |
| `tests/`、`docs/` | 验证与实施文档 |

从项目根目录运行 Python 模块，例如 `python -m scripts.check_infra` 或 `python -m evaluation.run_agent_eval --help`。本地环境配置从 `.env.example` 复制到 `.env` 后填写。
