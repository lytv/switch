"""Durable ticket activity, read cursors, and capacity claims.

Revision ID: b7e2c9d4a610
Revises: a8b4c6d7e9f0
"""

import sqlalchemy as sa
from alembic import op

revision = "b7e2c9d4a610"
down_revision = "a8b4c6d7e9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for column in (
        sa.Column(
            "agent_state", sa.Text(), nullable=False, server_default="wake_requested"
        ),
        sa.Column("agent_generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("wake_at", sa.DateTime(timezone=True)),
        sa.Column("tokens_used", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "room_claimed", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("queue_reason", sa.Text()),
        sa.Column("thread_cursor", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("jira_read_updated", sa.Text()),
        sa.Column("jira_read_changelog_id", sa.Text()),
        sa.Column("jira_worker_account_id", sa.Text()),
    ):
        op.add_column("jira_worker_ticket_map", column)
    op.execute(
        "UPDATE jira_worker_ticket_map SET room_claimed = true WHERE room_id IS NOT NULL"
    )
    op.execute(
        "UPDATE jira_worker_ticket_map SET jira_read_updated = last_event_at::text"
    )
    op.execute(
        "UPDATE jira_worker_ticket_map SET agent_state = 'parked' WHERE worker_parked_reason IS NOT NULL OR lower(trim(status)) IN ('done', 'cancelled', 'canceled')"
    )
    op.add_column(
        "jira_worker_event_log",
        sa.Column("consumed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("jira_worker_event_log", "consumed")
    for name in (
        "jira_worker_account_id",
        "jira_read_changelog_id",
        "jira_read_updated",
        "thread_cursor",
        "queue_reason",
        "room_claimed",
        "tokens_used",
        "wake_at",
        "agent_generation",
        "agent_state",
    ):
        op.drop_column("jira_worker_ticket_map", name)
