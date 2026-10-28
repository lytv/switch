"""Delivery tmux paths must fit Unix sockets, independently of worker-home length."""

from pathlib import Path

import pytest

from tests.switch_core.bridges.jira.test_firstmate_jobs import (
    lab,  # noqa: F401
)


@pytest.mark.parametrize("limit", [104, 108])
def test_dispatch_environment_fits_unix_socket_limit(lab, limit):
    home = Path("/Users/operator/switch-delivery/homes") / ("a" * 64)
    environment = lab.copy._environment(home)
    socket = Path(environment["TMUX_TMPDIR"]) / "tmux-501/default"
    assert len(bytes(socket)) < limit
    assert environment["FM_HOME"] == str(home)
    assert environment["TMPDIR"] == str(home / "tmp")
