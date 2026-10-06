"""MySQL 异步连接生命周期；事务由调用方决定。"""

from __future__ import annotations

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from core.config import DatabaseSettings, load_database_settings


# 作用：根据配置构建安全转义密码的 MySQL 异步连接地址。
def database_url(settings: DatabaseSettings) -> URL:
    return URL.create(
        "mysql+aiomysql",
        username=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
        database=settings.database,
        query={"charset": "utf8mb4"},
    )


# 作用：在服务启动时创建 MySQL 异步引擎和连接池。
def create_mysql_engine(settings: DatabaseSettings | None = None) -> AsyncEngine:
    settings = settings or load_database_settings()
    return create_async_engine(
        database_url(settings),
        pool_pre_ping=True,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_recycle=settings.pool_recycle_seconds,
    )


# 作用：创建每次调用都会生成独立 AsyncSession 的工厂。
def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
