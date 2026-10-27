"""Admit checkpoint input revisions without changing existing execution keys.

Revision ID: 8c2f4a6b8d70
Revises: 7b1e2f3a4c59
"""

import sqlalchemy as sa
from alembic import op

revision = "8c2f4a6b8d70"
down_revision = "7b1e2f3a4c59"
branch_labels = None
depends_on = None


def _identity(suffix: str) -> str:
    return (
        "kind <> 'role_job' OR (ticket_id IS NOT NULL "
        "AND execution_key IS NOT NULL AND generation IS NOT NULL AND generation > 0 "
        "AND execution_key ~ '^[^/]+/[1-9][0-9]*/(reproduce|fix|review|qa|smoke)/[1-9][0-9]*"
        + suffix
        + "$' AND split_part(execution_key, '/', 1) = ticket_id "
        "AND split_part(execution_key, '/', 2) = generation::text)"
    )


def upgrade() -> None:
    op.drop_constraint("ck_jira_worker_role_identity", "jira_worker_job_record")
    op.create_check_constraint(
        "ck_jira_worker_role_identity",
        "jira_worker_job_record",
        _identity("(/input/[1-9][0-9]*)?"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("LOCK TABLE jira_worker_job_record IN ACCESS EXCLUSIVE MODE"))
    if bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM jira_worker_job_record WHERE execution_key LIKE '%/input/%')"
        )
    ).scalar_one():
        raise RuntimeError("Cannot downgrade while checkpoint revision records exist")
    op.drop_constraint("ck_jira_worker_role_identity", "jira_worker_job_record")
    op.create_check_constraint(
        "ck_jira_worker_role_identity", "jira_worker_job_record", _identity("")
    )
