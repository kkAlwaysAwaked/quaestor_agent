"""把固定员工手册样本提交为一条真实的入库任务。"""

from __future__ import annotations

import argparse
import asyncio
import sys

from sqlalchemy import select

from core.config import PROJECT_ROOT
from infra.mysql.base import new_id
from infra.mysql.models import User
from infra.mysql.session import create_mysql_engine, create_session_factory
from services.ingest.submission import submit_markdown


# 作用：按显式指定的邮箱查找或创建不能用于登录的样本归属用户。
async def ensure_baseline_user(sessions, email: str) -> str:
    async with sessions() as session, session.begin():
        user = await session.scalar(select(User).where(User.email == email))
        if user is None:
            user = User(id=new_id(), email=email, password_hash="!baseline-user-no-login")
            session.add(user)
            await session.flush()
        return user.id


# 作用：为样本文档创建用户、文档版本、入库任务与 Outbox，并打印可追踪的任务 ID。
async def seed(email: str) -> str:
    engine = create_mysql_engine()
    try:
        sessions = create_session_factory(engine)
        user_id = await ensure_baseline_user(sessions, email)
        fixture = PROJECT_ROOT / "tests" / "fixtures" / "employee_handbook.md"
        job_id = await submit_markdown(
            sessions, user_id=user_id, source=fixture,
            idempotency_key="baseline:employee-handbook:v1",
        )
        print(f"user_id={user_id}\ningest_job_id={job_id}")
        return job_id
    finally:
        await engine.dispose()


# 作用：解析显式样本用户邮箱并执行固定文档入库提交。
def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="提交固定员工手册入库样本")
    parser.add_argument("--user-email", required=True, help="样本数据的归属邮箱")
    args = parser.parse_args()
    asyncio.run(seed(args.user_email))


if __name__ == "__main__":
    main()
