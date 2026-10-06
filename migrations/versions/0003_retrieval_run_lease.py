"""为跨实例检索幂等和进程退出后的恢复增加执行租约。"""

from alembic import op
import sqlalchemy as sa

revision = "0003_retrieval_run_lease"
down_revision = "0002_version_storage_key"
branch_labels = None
depends_on = None


# 作用：增加检索执行次数、租约持有者和到期时间，并建立恢复索引。
def upgrade() -> None:
    op.add_column("retrieval_runs", sa.Column("attempt", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("retrieval_runs", sa.Column("lease_owner", sa.String(36)))
    op.add_column("retrieval_runs", sa.Column("lease_expires_at", sa.DateTime()))
    op.create_index("ix_retrieval_runs_status_lease", "retrieval_runs", ["status", "lease_expires_at"])


# 作用：回滚检索租约索引和执行字段。
def downgrade() -> None:
    op.drop_index("ix_retrieval_runs_status_lease", table_name="retrieval_runs")
    op.drop_column("retrieval_runs", "lease_expires_at")
    op.drop_column("retrieval_runs", "lease_owner")
    op.drop_column("retrieval_runs", "attempt")
