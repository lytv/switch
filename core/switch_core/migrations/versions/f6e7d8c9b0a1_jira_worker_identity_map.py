"""Scope Jira worker identities and reporter resolution.

Revision ID: f6e7d8c9b0a1
Revises: ac187a6d14e08d9f
Create Date: 2026-10-09 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f6e7d8c9b0a1"
down_revision: str | Sequence[str] | None = "ac187a6d14e08d9f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_identity_map",
        sa.Column("instance", sa.Text(), nullable=False, server_default=""),
    )
    op.alter_column(
        "jira_worker_identity_map",
        "switch_agent_name",
        new_column_name="switch_user_id",
        existing_type=sa.Text(),
    )
    op.execute("UPDATE jira_worker_identity_map SET switch_user_id = NULL")
    op.drop_constraint(
        "jira_worker_identity_map_jira_account_id_key",
        "jira_worker_identity_map",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_jira_worker_identity_instance_account",
        "jira_worker_identity_map",
        ["instance", "jira_account_id"],
    )

    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("reporter_account_id", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("reporter_switch_user_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column(
            "wait_channel", sa.Text(), nullable=False, server_default="jira_comments"
        ),
    )
    op.create_index(
        "ix_jira_worker_ticket_map_instance_reporter",
        "jira_worker_ticket_map",
        ["instance", "reporter_account_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_jira_worker_ticket_map_instance_reporter",
        table_name="jira_worker_ticket_map",
    )
    op.drop_column("jira_worker_ticket_map", "wait_channel")
    op.drop_column("jira_worker_ticket_map", "reporter_switch_user_id")
    op.drop_column("jira_worker_ticket_map", "reporter_account_id")

    op.drop_constraint(
        "uq_jira_worker_identity_instance_account",
        "jira_worker_identity_map",
        type_="unique",
    )
    op.create_unique_constraint(
        "jira_worker_identity_map_jira_account_id_key",
        "jira_worker_identity_map",
        ["jira_account_id"],
    )
    op.alter_column(
        "jira_worker_identity_map",
        "switch_user_id",
        new_column_name="switch_agent_name",
        existing_type=sa.Text(),
        nullable=True,
    )
    op.drop_column("jira_worker_identity_map", "instance")
