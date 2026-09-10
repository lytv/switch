"""Upgrade/downgrade of the Jira delivery-log migration stays reversible."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import create_async_engine

import switch_core.db.models  # noqa: F401
from switch_core.db.base import Base

_CORE = Path(__file__).resolve().parents[2]
_DB = "jira_delivery_log_mig"


def _script_directory(config: Config) -> ScriptDirectory:
    config.set_main_option("script_location", str(_CORE / "switch_core" / "migrations"))
    return ScriptDirectory.from_config(config)


def _run_migrations(connection: Connection, target: str) -> None:
    config = Config(str(_CORE / "alembic.ini"))
    script = _script_directory(config)

    def do_upgrade_or_down(revision: str, context: Any) -> Any:
        if target == "head":
            return script._upgrade_revs("head", revision)
        return script._downgrade_revs(target, revision)

    with EnvironmentContext(config, script, fn=do_upgrade_or_down) as environment:
        environment.configure(connection=connection, target_metadata=Base.metadata)
        with environment.begin_transaction():
            environment.run_migrations()


@pytest.fixture
async def mig_url(postgres_url: str) -> Any:
    admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
    async with admin.connect() as connection:
        await connection.execute(text(f'DROP DATABASE IF EXISTS "{_DB}"'))
        await connection.execute(text(f'CREATE DATABASE "{_DB}"'))
    await admin.dispose()
    base, _, _ = postgres_url.rpartition("/")
    try:
        yield f"{base}/{_DB}"
    finally:
        admin = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
        async with admin.connect() as connection:
            await connection.execute(text(f'DROP DATABASE IF EXISTS "{_DB}"'))
        await admin.dispose()


async def test_jira_delivery_log_downgrade_dedupes_before_unique(
    mig_url: str,
) -> None:
    """Duplicate key triples from claim+suppress must not block downgrade."""
    engine = create_async_engine(mig_url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "head"))

        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO jira_triggers (
                        id, name, enabled, instance, project_key, issue_type,
                        fire_on, target_status, jql, target_kind,
                        agent_name, message_template, thread_by
                    ) VALUES (
                        'rule-1', 't', true, 'acme', '', '',
                        'created', '', '', 'room',
                        'coder', 'x', 'new'
                    )
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO jira_trigger_firings (
                        id, issue_key, rule_id, transition_key, instance,
                        rule_name, status, attempt_count, claim_held, created_at
                    ) VALUES
                    ('f1', 'PROJ-1', 'rule-1', 'created', 'acme', 't',
                     'delivered', 1, true, now() - interval '1 second'),
                    ('f2', 'PROJ-1', 'rule-1', 'created', 'acme', 't',
                     'suppressed_dedupe', 0, false, now())
                    """
                )
            )

        async with engine.begin() as connection:
            await connection.run_sync(lambda c: _run_migrations(c, "b9d2e3f4a5c6"))

        async with engine.connect() as connection:
            count = (
                await connection.execute(
                    text("SELECT count(*) FROM jira_trigger_firings")
                )
            ).scalar_one()
            assert count == 1
            # Unique constraint is back.
            constraint = (
                await connection.execute(
                    text(
                        """
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'uq_jira_trigger_firings_dedupe'
                        """
                    )
                )
            ).scalar_one_or_none()
            assert constraint == 1
    finally:
        await engine.dispose()
