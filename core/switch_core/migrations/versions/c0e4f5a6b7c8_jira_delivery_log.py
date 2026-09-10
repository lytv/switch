"""extend jira_trigger_firings into a delivery log

Revision ID: c0e4f5a6b7c8
Revises: b9d2e3f4a5c6
Create Date: 2026-09-10 11:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c0e4f5a6b7c8"
down_revision: str | Sequence[str] | None = "b9d2e3f4a5c6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_jira_trigger_firings_dedupe",
        "jira_trigger_firings",
        type_="unique",
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column("instance", sa.Text(), server_default="", nullable=False),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column("rule_name", sa.Text(), server_default="", nullable=False),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column(
            "status",
            sa.Text(),
            server_default="delivered",
            nullable=False,
        ),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column(
            "matched_rule_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column(
            "room_results",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.add_column(
        "jira_trigger_firings",
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    # Active dedupe claim flag: at most one held claim per key (atomic).
    # Suppression rows and expired claims set this false so history is kept.
    op.add_column(
        "jira_trigger_firings",
        sa.Column(
            "claim_held",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
    )
    # Existing Phase-1 rows were unique claims; keep them held until expiry logic
    # releases them. Suppression inserts always write claim_held=false.
    op.execute(
        sa.text(
            "UPDATE jira_trigger_firings SET claim_held = true "
            "WHERE status IN ('pending', 'delivered', 'error')"
        )
    )
    op.execute(
        sa.text(
            "UPDATE jira_trigger_firings SET claim_held = false "
            "WHERE status LIKE 'suppressed_%'"
        )
    )
    op.create_index(
        "uq_jira_trigger_firings_active_claim",
        "jira_trigger_firings",
        ["issue_key", "rule_id", "transition_key"],
        unique=True,
        postgresql_where=sa.text("claim_held IS TRUE"),
    )
    op.create_index(
        "ix_jira_trigger_firings_created",
        "jira_trigger_firings",
        ["created_at"],
    )
    op.create_index(
        "ix_jira_trigger_firings_instance_created",
        "jira_trigger_firings",
        ["instance", "created_at"],
    )
    op.create_index(
        "ix_jira_trigger_firings_status_created",
        "jira_trigger_firings",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_jira_trigger_firings_status_created",
        table_name="jira_trigger_firings",
    )
    op.drop_index(
        "ix_jira_trigger_firings_instance_created",
        table_name="jira_trigger_firings",
    )
    op.drop_index(
        "ix_jira_trigger_firings_created",
        table_name="jira_trigger_firings",
    )
    op.drop_index(
        "uq_jira_trigger_firings_active_claim",
        table_name="jira_trigger_firings",
    )
    # Collapse duplicate (issue_key, rule_id, transition_key) rows that the
    # delivery log intentionally allowed (claim + suppressed_dedupe, etc.)
    # before recreating the Phase-1 unique constraint.
    op.execute(
        sa.text(
            """
            DELETE FROM jira_trigger_firings a
            USING jira_trigger_firings b
            WHERE a.issue_key = b.issue_key
              AND a.rule_id = b.rule_id
              AND a.transition_key = b.transition_key
              AND a.created_at < b.created_at
            """
        )
    )
    op.execute(
        sa.text(
            """
            DELETE FROM jira_trigger_firings a
            USING jira_trigger_firings b
            WHERE a.issue_key = b.issue_key
              AND a.rule_id = b.rule_id
              AND a.transition_key = b.transition_key
              AND a.created_at = b.created_at
              AND a.id < b.id
            """
        )
    )
    op.drop_column("jira_trigger_firings", "claim_held")
    op.drop_column("jira_trigger_firings", "attempt_count")
    op.drop_column("jira_trigger_firings", "error")
    op.drop_column("jira_trigger_firings", "room_results")
    op.drop_column("jira_trigger_firings", "matched_rule_ids")
    op.drop_column("jira_trigger_firings", "status")
    op.drop_column("jira_trigger_firings", "rule_name")
    op.drop_column("jira_trigger_firings", "instance")
    op.create_unique_constraint(
        "uq_jira_trigger_firings_dedupe",
        "jira_trigger_firings",
        ["issue_key", "rule_id", "transition_key"],
    )
