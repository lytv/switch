"""Jira worker ticket card pointer (step 3: rooms).

Revision ID: 514c2211a4bebe57
Revises: f6e7d8c9b0a1
Create Date: 2026-10-09 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "514c2211a4bebe57"
down_revision: str | Sequence[str] | None = "f6e7d8c9b0a1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "jira_worker_ticket_map",
        sa.Column("card_event_id", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("jira_worker_ticket_map", "card_event_id")
