# quaestor_agent

项目正在按 [实现步骤](docs/实现步骤.md) 重构。基础设施的启动与检查命令见 [部署说明](deploy/README.md)，第四阶段入库数据流见 [入库说明](docs/04-入库.md)，第五阶段 HTTP 检索的阅读顺序、授权与验收见 [Retrieval 说明](docs/05-Retrieval.md)，第六阶段消费、真实流式生成与任务恢复见 [Agent Worker 说明](docs/06-Agent.md)。网关和浏览器 SSE 接入在第七、八阶段实现。

| 目录 | 职责 |
| --- | --- |
| `services/agent/` | 聊天任务消费、模型流式生成、检索 HTTP 调用、重试与 Outbox 发布编排 |
| `services/retrieval/` | 内部 HTTP 检索、查询改写、授权混合召回、融合和重排 |
| `services/ingest/` | 文档转换、确定性切分、任务消费和版本发布编排 |
| `core/` | 跨服务配置、数据契约、内部认证和模型下载设置 |
| `infra/` | MySQL 模型与数据访问、Redis Stream 读写、中间件拓扑及检查 |
| `migrations/` | Alembic 业务表迁移 |
| `scripts/` | 基础设施命令和评估命令入口 |
| `deploy/` | Docker Compose 与本机启动说明 |
| `tests/`、`docs/` | 测试和实施文档 |

从项目根目录运行 Python 模块，例如 `python -m scripts.check_infra` 或 `python -m scripts.check_retrieval_baseline --help`。本地环境配置从 `.env.example` 复制到 `.env` 后填写。`qdrant_db/` 是旧版本地 Qdrant 数据目录，保留供迁移检查；Retrieval 使用 Qdrant Server 和 MySQL，不读取该目录或本地 docstore。
