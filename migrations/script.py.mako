"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


# 作用：将本次版本定义的数据库结构升级到目标状态。
def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


# 作用：撤销本次版本定义的数据库结构变更。
def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
