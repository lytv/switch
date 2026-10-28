"""Delivery tmux paths must fit Unix sockets, independently of worker-home length."""

import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from switch_core.bridges.jira.contracts import Job, Repository, Run, digest
from switch_core.bridges.jira.firstmate_copy import FirstmateCopy, tmux_directory
from tests.switch_core.bridges.jira.test_firstmate_jobs import (
    RESERVATION,
    _script,
    lab,  # noqa: F401
)


@pytest.fixture
def native_lab(request):
    return request.getfixturevalue("lab")


@pytest.fixture
def native_job(native_lab):
    return Job(
        schema_version=1,
        run=Run(
            instance="fixture",
            ticket_id="ticket",
            generation=1,
            step="reproduce",
            attempt=1,
            execution_key="ticket/1/reproduce/1",
        ),
        pack_digest=digest(native_lab.pack),
        service="Bug",
        repository=Repository(
            supplied_url="https://example.invalid/repo",
            outcome="verified",
            canonical_url="https://example.invalid/repo",
            base_branch="main",
            base_sha=native_lab.base_sha,
            evidence="fixture triage",
        ),
        acceptance=("Reproduce the fixture failure",),
    )


@pytest.mark.parametrize("limit", [104, 108])
def test_dispatch_environment_fits_unix_socket_limit(native_lab, limit):
    home = Path("/Users/operator/switch-delivery/homes") / ("a" * 64)
    environment = native_lab.copy._environment(home)
    socket = Path(environment["TMUX_TMPDIR"]) / "tmux-501/default"
    assert len(os.fsencode(socket)) < limit
    assert environment["FM_HOME"] == str(home)
    assert environment["TMPDIR"] == str(home / "tmp")


@pytest.mark.parametrize("platform,limit", [("darwin", 104), ("linux", 108)])
@pytest.mark.parametrize("multibyte", [False, True])
def test_socket_limit_counts_encoded_bytes_and_terminator(
    native_lab, monkeypatch, platform, limit, multibyte
):
    monkeypatch.setattr(sys, "platform", platform)
    home = native_lab.homes / ("a" * 64)
    suffix = f"/{'0' * 32}/tmux-{os.getuid()}/default"
    budget = limit - 1 - len(os.fsencode(suffix)) - 1
    name = "é" * (budget // 2) + "s" * (budget % 2) if multibyte else "s" * budget
    root = Path("/") / name
    assert (
        len(os.fsencode(tmux_directory(home, root) / f"tmux-{os.getuid()}/default"))
        == limit - 1
    )
    with pytest.raises(
        ValueError, match=f"platform limit is {limit}.*shorter tmux_sockets"
    ):
        tmux_directory(home, Path(str(root) + "s"))


def test_default_root_is_short_and_execution_specific(native_lab, native_job):
    home = native_lab.copy.location(native_job)
    directory = tmux_directory(home, None)
    assert directory.parent == Path("/tmp").resolve() / f"switch-tmux-{os.getuid()}"
    assert len(os.fsencode(directory / f"tmux-{os.getuid()}/default")) < 104
    other = home.parent.parent / "other-homes" / home.name
    assert tmux_directory(other, None) != directory


def test_overlong_root_fails_before_home_or_dispatch(native_lab, native_job, tmp_path):
    with pytest.raises(ValueError, match="Configure a shorter tmux_sockets root"):
        FirstmateCopy(
            checkout=native_lab.checkout,
            homes=native_lab.homes,
            project=native_lab.copy._project,
            executable_path=native_lab.copy._path,
            secrets=native_lab.copy._secrets,
            tmux_sockets=tmp_path / ("x" * 100),
        )
    assert not native_lab.copy.location(native_job).exists()


def test_socket_root_rejects_relative_paths_and_symlinks(tmp_path):
    home = tmp_path / ("a" * 64)
    with pytest.raises(ValueError, match="absolute, without symlinks"):
        tmux_directory(home, Path("relative"))
    linked = tmp_path / "linked"
    linked.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="absolute, without symlinks"):
        tmux_directory(home, linked)


@pytest.mark.parametrize("damage", ["public", "symlink", "foreign"])
def test_prepare_rejects_unsafe_socket_directory(native_lab, native_job, damage):
    copy = native_lab.copy
    sockets = tmux_directory(copy.location(native_job), copy._tmux_sockets)
    if damage == "symlink":
        sockets.symlink_to(sockets.parent, target_is_directory=True)
    else:
        sockets.mkdir(mode=0o700)
        if damage == "public":
            sockets.chmod(0o755)
        if damage == "foreign":
            (sockets / "home").write_text("another execution")
    with pytest.raises(ValueError, match="private|symlinks|another execution"):
        copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    assert not copy.location(native_job).exists()


@pytest.mark.parametrize("step", ["reproduce", "fix", "review", "qa", "smoke"])
def test_role_socket_is_private_and_deterministic(native_lab, native_job, step):
    copy = native_lab.copy
    job = native_job.model_copy(
        update={
            "run": native_job.run.model_copy(
                update={"step": step, "execution_key": f"ticket/1/{step}/1"}
            )
        }
    )
    home = copy.location(job)
    assert (
        home.name
        == hashlib.sha256(
            f"{job.run.instance}/{job.run.execution_key}".encode()
        ).hexdigest()
    )
    assert copy._prepare(job, native_lab.pack, RESERVATION, 60, None)
    sockets = Path(copy._environment(home)["TMUX_TMPDIR"])
    assert sockets == tmux_directory(home, copy._tmux_sockets)
    assert sockets.stat().st_mode & 0o777 == 0o700
    assert sockets.stat().st_uid == os.getuid()
    assert sockets.parent.stat().st_mode & 0o777 == 0o700
    assert (sockets / "home").read_bytes() == os.fsencode(home)
    assert (
        home / "config/launch-env-allowlist"
    ).read_text() == "FM_HOME\nTMUX_TMPDIR\n"
    assert not copy._prepare(job, native_lab.pack, RESERVATION, 60, None)


async def test_real_tmux_cancel_cleans_stale_socket(native_lab, native_job):
    tmux = shutil.which("tmux")
    if tmux is None:
        pytest.skip("tmux is not installed")
    copy = native_lab.copy
    await copy.start(native_job, native_lab.pack, RESERVATION, 60)
    home = copy.location(native_job)
    sockets = tmux_directory(home, copy._tmux_sockets)
    endpoint = sockets / f"tmux-{os.getuid()}/default"
    endpoint.parent.mkdir(mode=0o700)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stale:
        stale.bind(str(endpoint))
    assert endpoint.is_socket()
    _script(
        native_lab.checkout.parent / "tools/tmux",
        f"import os, sys\nos.execv({tmux!r}, [{tmux!r}, *sys.argv[1:]])\n",
    )

    await copy.cancel(native_job)

    assert (home / "cancel-reached").read_text() == copy.task_id(native_job)
    assert not sockets.exists()


@pytest.mark.parametrize("failure", ["live", "unknown", "control"])
async def test_cancel_keeps_socket_directory_when_shutdown_is_unproven(
    native_lab, native_job, failure
):
    copy = native_lab.copy
    await copy.start(native_job, native_lab.pack, RESERVATION, 60)
    home = copy.location(native_job)
    sockets = tmux_directory(home, copy._tmux_sockets)
    endpoint = sockets / f"tmux-{os.getuid()}/default"
    endpoint.parent.mkdir(mode=0o700)
    _script(native_lab.checkout.parent / "tools/tmux", "raise SystemExit(1)\n")
    if failure == "control":
        _script(native_lab.checkout / "bin/fm-control.sh", "raise SystemExit(1)\n")
    kind = socket.SOCK_DGRAM if failure == "unknown" else socket.SOCK_STREAM
    with socket.socket(socket.AF_UNIX, kind) as server:
        server.bind(str(endpoint))
        if kind == socket.SOCK_STREAM:
            server.listen()
        command = "fm-control.sh" if failure == "control" else "tmux"
        with pytest.raises(RuntimeError, match=f"Delivery command {command} failed"):
            await copy.cancel(native_job)
        assert endpoint.is_socket()
        assert (sockets / "home").read_bytes() == os.fsencode(home)
        assert (home / "cancel-reached").exists() == (failure != "control")


async def test_real_tmux_dispatch_get_cancel_keep_execution_isolation(
    native_lab, native_job
):
    tmux = shutil.which("tmux")
    if tmux is None:
        pytest.skip("tmux is not installed")
    copy = native_lab.copy
    _script(
        native_lab.checkout.parent / "tools/tmux",
        f"import os, sys\nos.execv({tmux!r}, [{tmux!r}, *sys.argv[1:]])\n",
    )
    other = native_job.model_copy(
        update={
            "run": native_job.run.model_copy(
                update={"step": "fix", "execution_key": "ticket/1/fix/1"}
            )
        }
    )
    jobs = (native_job, other)
    environments = []
    try:
        for job in jobs:
            assert (
                await copy.start(job, native_lab.pack, RESERVATION, 60)
            ).status == "running"
            home = copy.location(job)
            environment = json.loads((home / "environment.json").read_text())
            environments.append(environment)
            socket = Path(environment["TMUX_TMPDIR"]) / f"tmux-{os.getuid()}/default"
            assert socket.is_socket()
            assert len(os.fsencode(socket)) < (104 if sys.platform == "darwin" else 108)
            actual = subprocess.check_output(
                [tmux, "display-message", "-p", "#{socket_path}"],
                env=environment,
                text=True,
            ).strip()
            assert actual == str(socket)
            assert (await copy.get(job)).status == "running"
        assert environments[0]["TMUX_TMPDIR"] != environments[1]["TMUX_TMPDIR"]
        await copy.cancel(native_job)
        assert not Path(environments[0]["TMUX_TMPDIR"]).exists()
        assert (
            Path(environments[1]["TMUX_TMPDIR"]) / f"tmux-{os.getuid()}/default"
        ).is_socket()
        await copy.cancel(other)
        for job, environment in zip(jobs, environments, strict=True):
            assert not Path(environment["TMUX_TMPDIR"]).exists()
            cancelled = json.loads(
                (copy.location(job) / "cancel-environment.json").read_text()
            )
            assert cancelled["TMUX_TMPDIR"] == environment["TMUX_TMPDIR"]
    finally:
        for environment in environments:
            socket = Path(environment["TMUX_TMPDIR"]) / f"tmux-{os.getuid()}/default"
            if socket.exists():
                subprocess.run([tmux, "kill-server"], env=environment, check=True)
