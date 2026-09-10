"""add jira_triggers and jira_trigger_firings tables

Revision ID: a8c1d2e3f4b5
Revises: 04f27f37e474, f4c2a8e6d193
Create Date: 2026-09-10 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8c1d2e3f4b5"
down_revision: str | Sequence[str] | None = ("04f27f37e474", "f4c2a8e6d193")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jira_triggers",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column("instance", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False, server_default=""),
        sa.Column("issue_type", sa.Text(), nullable=False, server_default=""),
        sa.Column("fire_on", sa.Text(), nullable=False),
        sa.Column("target_status", sa.Text(), nullable=False, server_default=""),
        sa.Column("jql", sa.Text(), nullable=False, server_default=""),
        sa.Column("target_kind", sa.Text(), nullable=False, server_default="room"),
        sa.Column("target_room_id", sa.Text(), nullable=True),
        sa.Column("target_group_id", sa.Text(), nullable=True),
        sa.Column("agent_name", sa.Text(), nullable=False),
        sa.Column("message_template", sa.Text(), nullable=False),
        sa.Column("thread_by", sa.Text(), nullable=False, server_default="new"),
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
        sa.ForeignKeyConstraint(["target_room_id"], ["rooms.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["target_group_id"], ["room_groups.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_jira_triggers_instance_enabled",
        "jira_triggers",
        ["instance", "enabled"],
    )

    op.create_table(
        "jira_trigger_firings",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("issue_key", sa.Text(), nullable=False),
        sa.Column("rule_id", sa.Text(), nullable=False),
        sa.Column("transition_key", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["rule_id"], ["jira_triggers.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "issue_key",
            "rule_id",
            "transition_key",
            name="uq_jira_trigger_firings_dedupe",
        ),
    )
    op.create_index(
        "ix_jira_trigger_firings_rule_created",
        "jira_trigger_firings",
        ["rule_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_jira_trigger_firings_rule_created", table_name="jira_trigger_firings"
    )
    op.drop_table("jira_trigger_firings")
    op.drop_index("ix_jira_triggers_instance_enabled", table_name="jira_triggers")
    op.drop_table("jira_triggers")
