# 本机基础设施（部署配置）

从仓库根目录执行以下命令。需要 Docker Desktop（Linux 容器模式）和带 pip 的 Python 环境。

```powershell
Copy-Item .env.example .env
```

编辑 `.env` 中的 MySQL、Redis、RabbitMQ、Qdrant 密钥。基础设施初始化不读取 `DEEPSEEK_API_KEY`；运行 Agent 或 Retrieval 的 llm 模式时才需要填写它。已有 `.env` 时只补新增配置，不覆盖原文件。

```powershell
python -m pip install -r requirements.txt
docker compose --project-directory . --env-file .env -f deploy/docker-compose.yml config --quiet
docker compose --project-directory . --env-file .env -f deploy/docker-compose.yml up -d --wait
python -m scripts.bootstrap_infra
python -m scripts.check_infra
python -m alembic upgrade head
```

`bootstrap_infra` 幂等创建两条工作队列、对应的死信队列、交换机和 Qdrant 的 `hybrid_collection`。集合已存在但向量规格不符时会报错，不会删除旧数据。`check_infra` 逐项检查四个服务的连接，并核对 RabbitMQ 队列及 Qdrant 集合。`alembic upgrade head` 创建 MySQL 业务表；模型、迁移与数据流说明见 [MySQL 组件说明](../docs/03-MySQL.md)。

单份 Markdown 入库、固定样本和入库 Worker 的运行顺序见 [第四阶段入库说明](../docs/04-入库.md)。

第五阶段 Retrieval 使用单进程 FastAPI + Uvicorn，启动前执行最新迁移并填写专用 `RETRIEVAL_SERVICE_SECRET`。模型生命周期、并发限制、fixed 基线模式和真实 HTTP 验收见 [Retrieval 说明](../docs/05-Retrieval.md)。

第六阶段 Agent 是独立队列进程，先启动 Retrieval，再运行 `python -m services.agent.worker`。它复用生成模型与 HTTP 客户端，启动时检查 MySQL、Redis、Retrieval 和 RabbitMQ。生成需要有效的 `DEEPSEEK_API_KEY`，内部检索需要与 Retrieval 相同的 `RETRIEVAL_SERVICE_SECRET`；新增参数从 `.env.example` 补到已有 `.env`。没有新增数据库表，本阶段仍执行 `alembic upgrade head` 确保第五阶段检索租约迁移已应用。模型、事件、ACK 与停机边界见 [Agent Worker 说明](../docs/06-Agent.md)。

离线测试需要额外安装 `python -m pip install -r requirements-test.txt`，运行 `python -m unittest discover -s tests -v`。已通过第四、五阶段基线并启动 Worker 后，可执行 `python -m scripts.check_agent_baseline <入库任务UUID>`；脚本会实际创建聊天任务并调用模型，报告保存到 `outputs/agent_baseline.json`。

四个中间件只映射到宿主机 `127.0.0.1`；本机 Python 用 `.env` 中的主机和映射端口连接。以后 Python 服务也进入 Compose 时，应将其连接地址改为服务名和容器内部端口，例如 `mysql:3306`，不要沿用 `127.0.0.1:3307`。

数据保存在四个 Docker 命名卷中。普通 `docker compose down` 保留卷；`down -v` 会删除卷中的数据，执行前要确认已备份。上传文件目录和模型缓存目录在 `.env` 中单独设置，不存放在中间件容器内。Redis 使用 AOF；MySQL、RabbitMQ、Qdrant 使用持久化卷。数据卷不替代备份。

排错时按顺序看：`docker compose ... ps` 的健康状态、`docker compose ... logs <service>` 的服务日志、`python -m scripts.check_infra` 的逐项结果。Qdrant 镜像内没有 curl，因此 Compose 健康检查只探测容器内 TCP 端口；检查脚本进一步请求 `/readyz` 并核对集合结构。RabbitMQ 管理界面默认在 `http://127.0.0.1:15673`，端口可在 `.env` 中修改。

学习检查点：

1. 说清楚为什么 MySQL 宿主机端口默认是 3307，容器内却是 3306，以及为什么本机 Python 不能直接使用 `mysql:3306`。
2. 连续执行两次 `python -m scripts.bootstrap_infra`；两次都应成功，队列与集合的数量不增加。然后用 `python -m scripts.check_infra` 逐项确认。
3. 停止再启动一个容器，确认命名卷的数据仍在，并解释“容器重启”和“删除数据卷”的区别。
4. 临时把本机 `.env` 中的一个连接密码改错，观察 `check_infra` 指向哪个服务，再恢复它。已经初始化的 MySQL/RabbitMQ 数据卷不会因为修改 `.env` 就自动更改已有账号密码；练习时只改客户端连接密码，不改容器凭据。
