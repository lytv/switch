"""Jira outbox delivery state and ticket parking.

Revision ID: a8b4c6d7e9f0
Revises: 514c2211a4bebe57
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8b4c6d7e9f0"
down_revision: str | Sequence[str] | None = "514c2211a4bebe57"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_ticket_map", sa.Column("worker_parked_reason", sa.Text())
    )
    op.add_column(
        "jira_worker_outbox",
        sa.Column("instance", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column("jira_worker_outbox", sa.Column("command_key", sa.Text()))
    op.add_column(
        "jira_worker_outbox",
        sa.Column(
            "due_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.add_column(
        "jira_worker_outbox",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("jira_worker_outbox", sa.Column("error", sa.Text()))
    op.create_unique_constraint(
        "uq_jira_worker_outbox_command",
        "jira_worker_outbox",
        ["instance", "command_key"],
    )
    op.create_index(
        "ix_jira_worker_outbox_due",
        "jira_worker_outbox",
        ["channel", "status", "due_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_jira_worker_outbox_due", table_name="jira_worker_outbox")
    op.drop_constraint(
        "uq_jira_worker_outbox_command", "jira_worker_outbox", type_="unique"
    )
    for column in ("error", "attempts", "due_at", "command_key", "instance"):
        op.drop_column("jira_worker_outbox", column)
    op.drop_column("jira_worker_ticket_map", "worker_parked_reason")
