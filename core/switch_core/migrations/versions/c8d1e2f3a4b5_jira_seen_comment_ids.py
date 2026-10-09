"""Durable Jira comment baseline shared by reads and writes.

Revision ID: c8d1e2f3a4b5
Revises: b7e2c9d4a610
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "c8d1e2f3a4b5"
down_revision = "b7e2c9d4a610"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("jira_seen_comment_ids", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("jira_worker_ticket_map", "jira_seen_comment_ids")
