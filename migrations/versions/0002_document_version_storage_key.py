"""将上传文件位置固定到每个文档版本，避免新旧版本互相覆盖。"""

from alembic import op
import sqlalchemy as sa


revision = "0002_version_storage_key"
down_revision = "0001_initial_business_schema"
branch_labels = None
depends_on = None


# 作用：为已有版本回填原文档文件标识，然后改为必填字段。
def upgrade() -> None:
    op.add_column("document_versions", sa.Column("storage_key", sa.String(512), nullable=True))
    op.execute(
        "UPDATE document_versions AS v JOIN documents AS d ON v.document_id = d.id "
        "SET v.storage_key = d.storage_key"
    )
    op.alter_column("document_versions", "storage_key", existing_type=sa.String(512), nullable=False)


# 作用：回滚版本级文件标识字段。
def downgrade() -> None:
    op.drop_column("document_versions", "storage_key")
