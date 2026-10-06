"""Alembic 的异步 MySQL 迁移入口。"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context

from core.config import load_database_settings
from infra.mysql.base import Base
from infra.mysql.models import (  # noqa: F401 — 导入全部模型以注册元数据
    ChatRequest, Conversation, Document, DocumentVersion, IngestJob,
    Message, OutboxEvent, ParentChunk, RetrievalRun, User,
)
from infra.mysql.session import create_mysql_engine, database_url


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


# 作用：生成 SQL 脚本时按当前 MySQL 配置运行离线迁移。
def run_migrations_offline() -> None:
    url = database_url(load_database_settings()).render_as_string(hide_password=False)
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# 作用：在异步连接对应的同步适配器内执行迁移步骤。
def _run_sync_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


# 作用：创建异步 MySQL 引擎，运行迁移并释放连接池。
async def _run_async_migrations() -> None:
    engine = create_mysql_engine()
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_run_sync_migrations)
    finally:
        await engine.dispose()


# 作用：作为 Alembic 同步入口启动异步迁移协程。
def run_migrations_online() -> None:
    asyncio.run(_run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
