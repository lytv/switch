"""add jira_issue_threads for thread-by-issue

Revision ID: b9d2e3f4a5c6
Revises: a8c1d2e3f4b5
Create Date: 2026-09-10 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b9d2e3f4a5c6"
down_revision: str | Sequence[str] | None = "a8c1d2e3f4b5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jira_issue_threads",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("room_id", sa.Text(), nullable=False),
        sa.Column("issue_key", sa.Text(), nullable=False),
        sa.Column("thread_root_event_id", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "room_id", "issue_key", name="uq_jira_issue_threads_room_issue"
        ),
    )


def downgrade() -> None:
    op.drop_table("jira_issue_threads")
