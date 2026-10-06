# Retrieval 服务

该目录提供内部 `POST /v1/retrieve` 接口，保留现有混合检索算法。第五阶段的文件阅读顺序、逐步数据流、数据库变化和验收命令见 [Retrieval 实现说明](../../docs/05-Retrieval.md)。

## 算法顺序

1. Query Rewrite / HyDE：从对话和检索意图得到关键词及假设性文档；固定基线模式跳过 LLM 生成。
2. Dense / Sparse：Dense 使用 `BAAI/bge-small-en-v1.5`；Sparse 使用 `prithivida/Splade_PP_en_v1` 的 SPLADE 稀疏向量。关键词和原问题可以各查询一路 Sparse。
3. 子块映射与 RRF：每一路同父块只保留最佳名次，通过 `1 / (60 + rank)` 融合。
4. MySQL 父块正文：批量读取授权、已发布且仍为当前版本的父块，保持融合顺序。
5. CrossEncoder：用 `BAAI/bge-reranker-v2-m3` 重排候选，返回最多 5 个父块及来源版本。

## 执行边界

HTTP 层验证专用服务 JWT，数据库层校验聊天任务归属、历史截止点和版本范围。每一路 Qdrant 查询都先带用户及版本过滤；正文进入重排模型前再次在 MySQL 授权。

网络调用使用异步 HTTP 和 `AsyncQdrantClient`。本地编码和重排由单线程执行器运行，避免阻塞事件循环，同时保护共享模型。模型与客户端在服务启动时创建，退出时关闭；导入工具模块不会下载模型。

成功结果与 trace 按 request_id 保存在 MySQL，同输入重复调用复用成功快照。任务领取、执行租约和最终提交由应用层编排。正文不再来自本地 `docstore.json`，向量客户端不再打开本地 `qdrant_db/`。

Agent 工具的 HTTP 调用在第六阶段接入。直接算法入口要求显式提供运行时和授权范围。
