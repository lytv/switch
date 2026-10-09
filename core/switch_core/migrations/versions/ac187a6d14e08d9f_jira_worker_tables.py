"""jira ticket worker tables (step 1: intake only)

Revision ID: ac187a6d14e08d9f
Revises: f5a6b7c8d9e0
Create Date: 2026-10-08 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "ac187a6d14e08d9f"
down_revision: str | Sequence[str] | None = "f5a6b7c8d9e0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jira_worker_identity_map",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("jira_account_id", sa.Text(), nullable=False),
        sa.Column("switch_agent_name", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("jira_account_id"),
    )
    op.create_table(
        "jira_worker_ticket_map",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("instance", sa.Text(), nullable=False),
        sa.Column("issue_key", sa.Text(), nullable=False),
        sa.Column("issue_id", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("room_id", sa.Text(), nullable=True),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_event_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "instance", "issue_key", name="uq_jira_worker_ticket_instance_issue"
        ),
    )
    op.create_index(
        "ix_jira_worker_ticket_map_instance_project",
        "jira_worker_ticket_map",
        ["instance", "project_key"],
    )
    op.create_table(
        "jira_worker_job_record",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("instance", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_jira_worker_job_record_due",
        "jira_worker_job_record",
        ["status", "due_at"],
    )
    op.create_table(
        "jira_worker_event_log",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("instance", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("issue_key", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("event_kind", sa.Text(), nullable=False),
        sa.Column("webhook_event", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "instance", "idempotency_key", name="uq_jira_worker_event_instance_key"
        ),
    )
    op.create_index(
        "ix_jira_worker_event_log_instance_issue",
        "jira_worker_event_log",
        ["instance", "issue_key"],
    )
    op.create_table(
        "jira_worker_outbox",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("command", sa.Text(), nullable=False),
        sa.Column("issue_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_jira_worker_outbox_status",
        "jira_worker_outbox",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_jira_worker_outbox_status", table_name="jira_worker_outbox")
    op.drop_table("jira_worker_outbox")
    op.drop_index(
        "ix_jira_worker_event_log_instance_issue", table_name="jira_worker_event_log"
    )
    op.drop_table("jira_worker_event_log")
    op.drop_index("ix_jira_worker_job_record_due", table_name="jira_worker_job_record")
    op.drop_table("jira_worker_job_record")
    op.drop_index(
        "ix_jira_worker_ticket_map_instance_project",
        table_name="jira_worker_ticket_map",
    )
    op.drop_table("jira_worker_ticket_map")
    op.drop_table("jira_worker_identity_map")
