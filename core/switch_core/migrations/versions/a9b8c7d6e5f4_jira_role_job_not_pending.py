"""Keep role jobs out of the pending state older workers claim.

Revision ID: a9b8c7d6e5f4
Revises: 8c2f4a6b8d70
"""

import sqlalchemy as sa
from alembic import op

revision = "a9b8c7d6e5f4"
down_revision = "8c2f4a6b8d70"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    # An older worker claims every pending row and marks unknown kinds done.
    # Role jobs are created claimed, so this constraint keeps them unreachable
    # from that claim path during a mixed-version or rollback window.
    pending = bind.execute(
        sa.text(
            "SELECT count(*) FROM jira_worker_job_record "
            "WHERE kind = 'role_job' AND status = 'pending'"
        )
    ).scalar_one()
    if pending:
        raise RuntimeError(
            "Cannot add the role-job guard while pending role jobs exist; "
            "reconcile or cancel them first"
        )
    op.create_check_constraint(
        "ck_jira_worker_role_not_pending",
        "jira_worker_job_record",
        "kind <> 'role_job' OR status <> 'pending'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_jira_worker_role_not_pending",
        "jira_worker_job_record",
        type_="check",
    )
