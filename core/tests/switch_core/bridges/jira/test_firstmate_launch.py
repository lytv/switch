"""Execute the native brief interface and keep command failures safe to report."""

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from switch_core.bridges.jira.firstmate_copy import MAX_COMMAND_STDERR_BYTES
from tests.switch_core.bridges.jira.test_firstmate_jobs import (
    RESERVATION,
    SECRET,
    _commit,
    _script,
    lab,  # noqa: F401
)
from tests.switch_core.bridges.jira.test_firstmate_tmux import (
    native_job as native_job,
)
from tests.switch_core.bridges.jira.test_firstmate_tmux import (
    native_lab as native_lab,
)


async def test_supported_brief_start_get_cancel_remain_idempotent(
    native_lab, native_job
):
    copy = native_lab.copy
    async with asyncio.timeout(5):
        assert (
            await copy.start(native_job, native_lab.pack, RESERVATION, 60)
        ).status == "running"
        assert (
            await copy.start(native_job, native_lab.pack, RESERVATION, 60)
        ).status == "running"
        assert (await copy.get(native_job)).status == "running"
    assert (copy.location(native_job) / "starts").read_text() == "1"
    await copy.cancel(native_job)
    await copy.cancel(native_job)


@pytest.mark.parametrize("launched", [False, True])
@pytest.mark.parametrize("tampered", [False, True])
async def test_dispatched_legacy_brief_is_not_rewritten_or_relaunched(
    native_lab, native_job, launched, tampered
):
    copy = native_lab.copy
    if launched:
        await copy.start(native_job, native_lab.pack, RESERVATION, 60)
    else:
        copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    brief = home / "data" / copy.task_id(native_job) / "brief.md"
    legacy = brief.read_bytes().replace(b"# Task\n", b"# Switch delivery task\n", 1)
    if tampered:
        legacy += b"Changed instruction.\n"
    brief.write_bytes(legacy)
    if tampered:
        with pytest.raises(ValueError, match="reused with different input"):
            await copy.start(native_job, native_lab.pack, RESERVATION, 60)
    else:
        for _ in range(2):
            assert (
                await copy.start(native_job, native_lab.pack, RESERVATION, 60)
            ).status == ("running" if launched else "uncertain")
    assert brief.read_bytes() == legacy
    assert (home / "starts").exists() == launched
    if launched:
        assert (home / "starts").read_text() == "1"
        await copy.cancel(native_job)


@pytest.mark.parametrize("marker", [None, b"another-input-digest"])
async def test_legacy_brief_requires_its_frozen_dispatch(
    native_lab, native_job, marker
):
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    task = home / "data" / copy.task_id(native_job)
    brief = task / "brief.md"
    brief.write_bytes(
        brief.read_bytes().replace(b"# Task\n", b"# Switch delivery task\n", 1)
    )
    dispatch = task / "dispatch"
    if marker is None:
        dispatch.unlink()
    else:
        dispatch.write_bytes(marker)
    with pytest.raises(ValueError, match="reused with different input"):
        await copy.start(native_job, native_lab.pack, RESERVATION, 60)
    assert not (home / "starts").exists()


@pytest.mark.parametrize(
    "brief",
    ["# Switch delivery task\nDo the task.\n", "# Task\n\n# Other\nNot the task.\n"],
)
async def test_rejected_brief_retains_uncertain_dispatch(native_lab, native_job, brief):
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    (home / "data" / copy.task_id(native_job) / "brief.md").write_text(brief)
    with pytest.raises(
        RuntimeError, match="Firstmate requires a nonempty # Task brief section"
    ):
        await copy._command(
            home, str(native_lab.checkout / "bin/fm-spawn.sh"), copy.task_id(native_job)
        )
    assert (await copy.get(native_job)).status == "uncertain"
    assert not (home / "starts").exists()
    with pytest.raises(RuntimeError, match="Cannot prove cancellation"):
        await copy.cancel(native_job)


@pytest.mark.parametrize(
    "native_message,category",
    [
        ("", "unrecognized stderr withheld"),
        (
            "error: /private/brief must contain nonempty ## Captain's intent and ## Firstmate spec subsections (or a nonempty legacy # Task body) before spawn\n",
            "Firstmate requires a nonempty # Task brief section",
        ),
        ("pi executable not found on PATH", "required executable is missing from PATH"),
        ("refusing to race", "Firstmate refused concurrent project allocation"),
    ],
)
@pytest.mark.parametrize("oversized", [False, True])
async def test_failure_diagnostic_never_emits_untrusted_stderr(
    native_lab, native_job, native_message, category, oversized
):
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    message = (
        f"\x1b[31m{SECRET}\nBearer PLACEHOLDER_BEARER\n"
        "password=PLACEHOLDER_PASSWORD\n"
        "https://example.invalid/model/capability/PLACEHOLDER_CAPABILITY/v1\n"
        "https://role:PLACEHOLDER_PASSWORD@example.invalid/repo?token=PLACEHOLDER_TOKEN\n"
    )
    message += native_message
    if oversized:
        message += "x" * (MAX_COMMAND_STDERR_BYTES * 256)
    script = native_lab.checkout / "bin/fm-control.sh"
    _script(
        script, f"import sys\nsys.stderr.write({message!r})\nraise SystemExit(17)\n"
    )
    with pytest.raises(RuntimeError) as failure:
        async with asyncio.timeout(10):
            await copy._command(home, str(script))
    assert str(
        failure.value
    ) == f"Delivery command fm-control.sh failed (17): {category}" + (
        "; stderr truncated" if oversized else ""
    )
    assert len(str(failure.value)) < 200


async def test_diagnostic_does_not_parse_beyond_capture_limit(native_lab, native_job):
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    script = native_lab.checkout / "bin/fm-control.sh"
    _script(
        script,
        f"import sys\nsys.stderr.write('x' * {MAX_COMMAND_STDERR_BYTES} + 'executable not found on PATH')\nraise SystemExit(1)\n",
    )
    with pytest.raises(
        RuntimeError, match="unrecognized stderr withheld; stderr truncated"
    ):
        await copy._command(copy.location(native_job), str(script))


@pytest.mark.parametrize("stop", ["cancel", "timeout"])
async def test_stopping_command_while_draining_stderr_stops_process_group(
    native_lab, native_job, monkeypatch, stop
):
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    script = native_lab.checkout / "bin/fm-control.sh"
    _script(
        script,
        "import os, pathlib, sys, time\npathlib.Path(os.environ['FM_HOME'], 'command.pid').write_text(str(os.getpid()))\nwhile True:\n    sys.stderr.write('x' * 4096)\n    sys.stderr.flush()\n    time.sleep(0.01)\n",
    )
    if stop == "timeout":
        timeout = asyncio.timeout
        monkeypatch.setattr(
            asyncio,
            "timeout",
            lambda seconds: timeout(0.5 if seconds == 120 else seconds),
        )
    task = asyncio.create_task(copy._command(home, str(script)))
    try:
        async with asyncio.timeout(5):
            while not (home / "command.pid").exists():
                await asyncio.sleep(0.01)
        pid = int((home / "command.pid").read_text())
        if stop == "cancel":
            task.cancel()
        with pytest.raises(
            asyncio.CancelledError if stop == "cancel" else TimeoutError
        ):
            await task
        with pytest.raises(ProcessLookupError):
            os.killpg(pid, 0)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("valid", [False, True])
async def test_pinned_firstmate_pre_window_brief_gate(
    native_lab, native_job, tmp_path, valid
):
    """Optional offline check against the operator's immutable runtime checkout.

    Only tmux window creation is faked. No provider, pool, or forge operation runs.
    """
    source = os.environ.get("SWITCH_TEST_FIRSTMATE_CHECKOUT")
    if source is None:
        pytest.skip(
            "set SWITCH_TEST_FIRSTMATE_CHECKOUT to test the pinned native brief gate"
        )
    source = Path(source)
    pack_file = (
        Path(__file__).parents[4]
        / "switch_core/bridges/jira/packs/delivery/2.0.0-deepseek.json"
    )
    pin = json.loads(pack_file.read_text())["firstmate_commit"]
    assert (
        subprocess.check_output(
            ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
        ).strip()
        == pin
    )
    runtime = tmp_path / "runtime"
    shutil.copytree(source / "bin", runtime / "bin")
    _commit(runtime)
    tools = native_lab.checkout.parent / "tools"
    jq = shutil.which("jq")
    if jq is None:
        pytest.skip("jq is required by the pinned runtime")
    (tools / "jq").symlink_to(jq)
    _script(
        tools / "pi",
        "import sys\nassert sys.argv[1:] == ['--help']\nprint('--tui-mode regular')\n",
    )
    _script(
        tools / "tmux",
        "import os, pathlib, sys\nif sys.argv[1] == 'new-window':\n    pathlib.Path(os.environ['FM_HOME'], 'window-boundary').touch()\n    raise SystemExit(98)\nassert sys.argv[1] in {'has-session', 'list-windows'}\n",
    )
    copy = native_lab.copy
    copy._prepare(native_job, native_lab.pack, RESERVATION, 60, None)
    home = copy.location(native_job)
    if not valid:
        brief = home / "data" / copy.task_id(native_job) / "brief.md"
        brief.write_text(
            brief.read_text().replace("# Task\n", "# Switch delivery task\n", 1)
        )
    with pytest.raises(RuntimeError) as failure:
        await copy._command(
            home,
            str(runtime / "bin/fm-spawn.sh"),
            copy.task_id(native_job),
            str(copy.project(native_job)),
            "--scout",
            "--harness",
            "pi",
            "--model",
            "switch-delivery-deepseek/deepseek-v4-pro",
            "--backend",
            "tmux",
        )
    assert (home / "window-boundary").exists() == valid
    assert (
        "Firstmate requires a nonempty # Task brief section" in str(failure.value)
    ) != valid
