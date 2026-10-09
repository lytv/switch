"""Admin card state on the Jira ticket map.

Revision ID: e9f0a1b2c3d4
Revises: c8d1e2f3a4b5
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "e9f0a1b2c3d4"
down_revision = "c8d1e2f3a4b5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("admin_card_event_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("admin_reasons", JSONB(), nullable=True),
    )
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("wait_anchor", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("wait_fired", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("jira_worker_ticket_map", "wait_fired")
    op.drop_column("jira_worker_ticket_map", "wait_anchor")
    op.drop_column("jira_worker_ticket_map", "admin_reasons")
    op.drop_column("jira_worker_ticket_map", "admin_card_event_id")
