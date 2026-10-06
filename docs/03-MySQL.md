# 第 3 阶段：MySQL 组件

这一轮完成的是 MySQL 的持久化能力：异步连接、表结构、迁移，以及会话行锁、任务租约和 Outbox 领取等数据库操作。RabbitMQ 发布、Redis Stream、HTTP 受理和 Agent 执行属于后续组件与应用编排；当前的数据库操作不自行提交事务。

## 从一条提问看数据如何变化

用户在一个会话中提交“今年有多少天年假？”，并附带幂等键。后续应用层会按以下顺序使用本轮的数据库能力：

```text
受理事务（一次提交）
  锁定 conversations 中属于该用户的会话
  → 检查 active_request_id 与幂等键
  → 分配消息序号，写入 user 消息
  → 写入 pending 的 chat_requests，固定 history_until_sequence
  → 占用会话，写入待发送的 outbox_events
  → 提交事务

发布进程（下一次事务）
  领取到期 outbox_events 并设置发布租约
  → 提交领取结果  → 向 RabbitMQ 发布
  → 收到确认后，另起事务把事件标记为 sent

Worker（后续阶段）
  收到任务 ID → 条件更新 chat_requests 为 running
  → attempt 加一并设置租约
  → 处理完成时核对 attempt 和租约
  → 在同一事务写入最终结果、释放会话、写入终态 Outbox
```

第一步的关键是 `chat_requests` 和 `outbox_events` 同事务：数据库提交失败，两者都不存在；提交成功，即使 RabbitMQ 暂时不可用，任务也留在数据库等待发布。发送成功但发布进程来不及标记 `sent` 时，事件可能再发一次，所以领取任务时必须依靠数据库条件更新处理重复消息。

## 关键边界


| 位置                                          | 负责什么              | 不负责什么                |
| ------------------------------------------- | ----------------- | -------------------- |
| `infra/mysql/session.py`                    | 创建异步引擎、连接池与会话工厂   | 在导入时连接数据库、替调用方提交事务   |
| `infra/mysql/health.py`                     | 用只读查询检查服务连接       | 创建或迁移业务表             |
| `infra/mysql/models/`                       | 表字段、外键、唯一约束和索引    | 决定失败后是否重试            |
| `infra/mysql/repositories/conversations.py` | 按用户锁会话、预留序号       | 返回 HTTP 409          |
| `infra/mysql/repositories/tasks.py`         | 原子领取、续租、按租约转换状态   | 执行 Agent 或计算退避时间     |
| `infra/mysql/repositories/outbox.py`        | 领取事件、确认发送、安排下次领取  | 调用 RabbitMQ 或 Redis  |
| `migrations/`                               | 用 Alembic 管理业务表版本 | 运行时按需 `create_all()` |


任务状态采用 `pending → running → succeeded/failed`，暂时性故障走 `running → retry_wait → running`。`attempt` 与租约共同隔离旧 Worker：续租和完成写入都检查当前执行者、执行次数和租约有效期。租约到期后的任务需要由后续恢复编排重新安排；数据库层提供锁定过期记录的操作。

消息序号和幂等键在数据库有唯一约束。业务层先检查已存在任务，数据库约束再挡住并发竞态。由于消息要在创建聊天任务前写入，`messages.request_id` 目前不设外键；两条记录必须由应用层在同一事务写入，任务的 `user_message_id` 则有外键指向消息。

## 本地验证

启动 MySQL 并配置 `.env` 后，在项目根目录执行：

```powershell
python -m alembic upgrade head
python -m alembic current
python -m unittest discover -s tests -v
```

`alembic upgrade head` 会修改所连接数据库的表结构，先确认 `.env` 中是预期的开发数据库。当前自动化测试使用 SQLite 验证约束和状态操作，不能证明 MySQL 的 `FOR UPDATE SKIP LOCKED` 并发行为；接入本机 MySQL 后，还需用两个独立连接验证只有一个 Worker 能领取同一任务，以及两个 Outbox 发布者不会同时领取同一事件。

MySQL 的 DDL 通常不能作为一个整体事务回滚。如果迁移中途失败，先检查已经建成的表与 Alembic 版本记录，再修复迁移状态；不要直接清空数据库重试。