"""MySQL 的只读连通性检查。"""

from __future__ import annotations

from core.config import InfrastructureSettings


# 作用：连接 MySQL 并执行 SELECT 1，确认账号、数据库与服务均可用。
def check_mysql(settings: InfrastructureSettings) -> None:
    import pymysql

    connection = pymysql.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_database,
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("MySQL SELECT 1 returned an unexpected result")
    finally:
        connection.close()
