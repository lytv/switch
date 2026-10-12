"""Durable intake transcript and question count.

Revision ID: 6a904d38e217
Revises: e9f0a1b2c3d4
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "6a904d38e217"
down_revision = "e9f0a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_ticket_map", sa.Column("description", JSONB(), nullable=True)
    )
    op.add_column(
        "jira_worker_ticket_map", sa.Column("intake_state", JSONB(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("jira_worker_ticket_map", "intake_state")
    op.drop_column("jira_worker_ticket_map", "description")
