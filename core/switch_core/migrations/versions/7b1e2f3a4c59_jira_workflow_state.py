"""Frozen Jira workflow state and execution identity.

Revision ID: 7b1e2f3a4c59
Revises: 6a904d38e217
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "7b1e2f3a4c59"
down_revision = "6a904d38e217"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jira_worker_ticket_map", sa.Column("service_value", sa.Text()))
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column(
            "workflow_state",
            JSONB(),
            nullable=False,
            server_default=sa.text(
                '\'{"schema_version": 1,"policy":"legacy_intake",'
                '"pack_id":"delivery","pack_version":"1"}\'::jsonb'
            ),
        ),
    )
    op.add_column("jira_worker_job_record", sa.Column("ticket_id", sa.Text()))
    op.create_foreign_key(
        "fk_jira_worker_job_ticket",
        "jira_worker_job_record",
        "jira_worker_ticket_map",
        ["ticket_id"],
        ["id"],
    )
    op.add_column("jira_worker_job_record", sa.Column("execution_key", sa.Text()))
    op.add_column("jira_worker_job_record", sa.Column("generation", sa.Integer()))
    for name in (
        "lease_until",
        "last_heartbeat_at",
        "last_progress_at",
        "cancel_requested_at",
    ):
        op.add_column(
            "jira_worker_job_record", sa.Column(name, sa.DateTime(timezone=True))
        )
    op.add_column("jira_worker_job_record", sa.Column("result", JSONB()))
    op.execute(
        "UPDATE jira_worker_job_record AS job SET ticket_id = ticket.id "
        "FROM jira_worker_ticket_map AS ticket "
        "WHERE job.kind IN ('ticket_room_create', 'orchestrator_turn') "
        "AND jsonb_typeof(job.payload->'ticket_id') = 'string' "
        "AND job.payload->>'ticket_id' = ticket.id "
        "AND job.instance = ticket.instance AND job.project_key = ticket.project_key"
    )
    op.create_index(
        "uq_jira_worker_job_execution_key",
        "jira_worker_job_record",
        ["execution_key"],
        unique=True,
        postgresql_where=sa.text("execution_key IS NOT NULL"),
    )
    op.create_index(
        "ix_jira_worker_job_active_role",
        "jira_worker_job_record",
        ["ticket_id", "kind", "status", "due_at"],
        postgresql_where=sa.text("kind = 'role_job'"),
    )
    op.create_index(
        "uq_jira_worker_job_active_generation",
        "jira_worker_job_record",
        ["ticket_id", "generation"],
        unique=True,
        postgresql_where=sa.text(
            "kind = 'role_job' AND status IN "
            "('pending','claimed','running','waiting_input','paused',"
            "'cancel_requested','uncertain')"
        ),
    )
    op.create_check_constraint(
        "ck_jira_worker_role_identity",
        "jira_worker_job_record",
        "kind <> 'role_job' OR (ticket_id IS NOT NULL "
        "AND execution_key IS NOT NULL AND generation IS NOT NULL AND generation > 0 "
        "AND execution_key ~ '^[^/]+/[1-9][0-9]*/(reproduce|fix|review|qa|smoke)/[1-9][0-9]*$' "
        "AND split_part(execution_key, '/', 1) = ticket_id "
        "AND split_part(execution_key, '/', 2) = generation::text)",
    )
    op.create_check_constraint(
        "ck_jira_worker_role_status",
        "jira_worker_job_record",
        "kind <> 'role_job' OR status IN "
        "('pending','claimed','running','waiting_input','paused','cancel_requested',"
        "'cancelled','uncertain','done','error')",
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Lock out admission until the destructive rollback transaction finishes.
    bind.execute(
        sa.text(
            "LOCK TABLE jira_worker_ticket_map, jira_worker_job_record IN ACCESS EXCLUSIVE MODE"
        )
    )
    has_delivery = bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM jira_worker_job_record "
            "WHERE kind = 'role_job' OR execution_key IS NOT NULL) OR EXISTS "
            "(SELECT 1 FROM jira_worker_ticket_map "
            "WHERE workflow_state->>'policy' IS DISTINCT FROM 'legacy_intake' "
            "OR service_value IS NOT NULL)"
        )
    ).scalar_one()
    if has_delivery:
        raise RuntimeError(
            "Cannot downgrade while version 2 workflow or execution records exist"
        )
    op.drop_constraint("ck_jira_worker_role_status", "jira_worker_job_record")
    op.drop_constraint("ck_jira_worker_role_identity", "jira_worker_job_record")
    op.drop_index("uq_jira_worker_job_active_generation", "jira_worker_job_record")
    op.drop_index("ix_jira_worker_job_active_role", "jira_worker_job_record")
    op.drop_index("uq_jira_worker_job_execution_key", "jira_worker_job_record")
    op.drop_constraint(
        "fk_jira_worker_job_ticket", "jira_worker_job_record", type_="foreignkey"
    )
    for name in (
        "result",
        "cancel_requested_at",
        "last_progress_at",
        "last_heartbeat_at",
        "lease_until",
        "generation",
        "execution_key",
        "ticket_id",
    ):
        op.drop_column("jira_worker_job_record", name)
    op.drop_column("jira_worker_ticket_map", "workflow_state")
    op.drop_column("jira_worker_ticket_map", "service_value")
