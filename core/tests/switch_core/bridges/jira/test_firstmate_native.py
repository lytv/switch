"""Optional offline launches through the pinned runtime and real private allocation."""

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest

from switch_core.bridges.jira.contracts import (
    Job,
    Pack,
    Repository,
    Reservation,
    Run,
    digest,
)
from switch_core.bridges.jira.delivery import role_job
from switch_core.bridges.jira.delivery_contracts import (
    ApprovalRoute,
    ApprovedCheck,
    CheckEvidence,
    ExecutionEnvelope,
    Handoff,
    RoleProfile,
    ScopeContract,
    VerificationContract,
    VerificationRole,
)
from switch_core.bridges.jira.firstmate_copy import (
    FirstmateCopy,
    SecretGuard,
    tmux_directory,
)
from switch_core.bridges.jira.live import LocalRepositories
from switch_core.bridges.jira.publisher import GitCandidateObjects
from switch_core.config import JiraWorkerDeliveryRuntime
from tests.switch_core.bridges.jira.test_firstmate_jobs import _script
from tests.switch_core.bridges.jira.test_firstmate_tmux import _real_tmux_script
from tests.switch_core.bridges.jira.test_live import git


@pytest.mark.parametrize(
    "step", ["reproduce", "fix", "review", "qa", "smoke", "no-capacity"]
)
async def test_pinned_native_launch_uses_frozen_local_source(
    tmp_path, monkeypatch, step
):
    source = os.environ.get("SWITCH_TEST_FIRSTMATE_CHECKOUT")
    if source is None:
        pytest.skip("set SWITCH_TEST_FIRSTMATE_CHECKOUT for offline native allocation")
    binaries = {
        name: shutil.which(name) for name in ("git", "tmux", "treehouse", "jq", "pi")
    }
    if not all(binaries.values()):
        pytest.skip("native allocation requires git, tmux, treehouse, jq and pi")
    pack_file = (
        Path(__file__).parents[4]
        / "switch_core/bridges/jira/packs/delivery/2.0.0-deepseek.json"
    )
    pack = Pack.model_validate_json(pack_file.read_text())
    assert git(Path(source), "rev-parse", "HEAD") == pack.firstmate_commit
    # Treehouse can add an ignore entry beside its pool; contain that edit too.
    git(tmp_path, "init", "-q")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    git(runtime, "init", "-q")
    git(runtime, "fetch", "-q", "--depth=1", source, pack.firstmate_commit)
    git(runtime, "checkout", "-q", "--detach", "FETCH_HEAD")
    (runtime / ".switch-delivery-copy").write_text("switch-firstmate-delivery-v1\n")
    origin = tmp_path / "origin"
    origin.mkdir()
    git(origin, "init", "-q", "-b", "main")
    git(origin, "config", "user.name", "Fixture")
    git(origin, "config", "user.email", "fixture@example.invalid")
    (origin / "fixture.txt").write_text("approved source\n")
    git(origin, "add", ".")
    git(origin, "commit", "-qm", "fixture base")
    base = git(origin, "rev-parse", "HEAD")
    (origin / "fixture.txt").write_text("candidate source\n")
    git(origin, "commit", "-qam", "fixture candidate")
    candidate_sha = git(origin, "rev-parse", "HEAD")
    homes, repositories, tools = (
        tmp_path / name for name in ("homes", "repositories", "tools")
    )
    for directory in (homes, repositories, tools):
        directory.mkdir(mode=0o700)
    (homes / ".switch-delivery-workers").write_text("switch-firstmate-delivery-v1\n")
    path = os.pathsep.join(
        dict.fromkeys(
            [
                str(tools),
                *(str(Path(binary).parent) for binary in binaries.values()),
                *os.defpath.split(os.pathsep),
            ]
        )
    )
    # Fail offline if native allocation ever uses the forge transport.
    url = "https://forge.example.invalid/team/fixture.git"
    settings = JiraWorkerDeliveryRuntime(
        checkout=runtime,
        homes=homes,
        repositories=repositories,
        executable_path=path,
        model_url="https://model.example.invalid",
        forge_api_url="https://forge.example.invalid",
        forge_repository_host="forge.example.invalid",
        forge_authorization="PLACEHOLDER_FORGE_VALUE",
        policies={},
        prices={
            "input": "0",
            "output": "0",
            "cache_read": "0",
            "cache_write": "0",
            "source": "fixture",
        },
    )
    local = LocalRepositories(settings)
    original_git = GitCandidateObjects._git
    fetches = []

    async def trusted_transport(objects, *args, authorization=None):
        assert args[0] == "fetch" and args[3] == url
        assert authorization == "PLACEHOLDER_FORGE_VALUE"
        fetches.append(args[-1])
        return await original_git(objects, *args[:3], str(origin), *args[4:])

    monkeypatch.setattr(GitCandidateObjects, "_git", trusted_transport)
    reservation = Reservation(
        tokens=1000, estimated_usd=Decimal("1"), price_source="fixture"
    )
    job = Job(
        schema_version=1,
        run=Run(
            instance="fixture-"
            + hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12],
            ticket_id="t",
            generation=1,
            step="reproduce",
            attempt=1,
            execution_key="t/1/reproduce/1",
        ),
        pack_digest=digest(pack),
        service="Bug",
        repository=Repository(
            supplied_url=url,
            outcome="verified",
            canonical_url=url,
            base_branch="main",
            base_sha=base,
            evidence="disposable approved fixture",
        ),
        acceptance=("Run only an inert final agent",),
    )
    content = "Inspect only the approved fixture. Never call a provider or forge."
    profile = RoleProfile(
        commit=pack.firstmate_commit,
        content=content,
        sha256=hashlib.sha256(content.encode()).hexdigest(),
    )
    role = VerificationRole(
        profile=profile, reservation=reservation, estimate_seconds=60
    )
    scope = ScopeContract(
        job=job,
        branch="switch/t/1",
        allowed_paths=("fixture.txt",),
        forbidden_paths=(".env",),
        checks=(
            ApprovedCheck(
                id="fixture", argv=("true",), expected="inert", kind="behavior"
            ),
        ),
        approval_route=ApprovalRoute(
            source_status_id="approval",
            target_status_id="progress",
            transition_id="approve",
            unique_edge_verified=True,
            evidence="fixture approval",
        ),
        profile=profile,
        reproduction_reservation=reservation,
        fix_reservation=reservation,
        reproduction_estimate_seconds=60,
        fix_estimate_seconds=60,
        verification=VerificationContract(
            review=role,
            qa=role,
            smoke=role,
            behavior_check_ids=("fixture",),
            smoke_check_ids=("fixture",),
            complete_transition_id="done",
        ),
    )
    reproduction = Handoff(
        input_digest=digest(job),
        verdict="PASS",
        head=base,
        findings=("fixture failure",),
        changed_paths=(),
        checks=(
            CheckEvidence(
                check_id="fixture",
                commit=base,
                exit_code=1,
                expected="inert",
                actual="fixture failure",
                environment="disposable",
                artifact_digest="a" * 64,
            ),
        ),
    )
    target = candidate_sha if step in {"review", "qa", "smoke"} else base
    envelope = ExecutionEnvelope(
        scope=scope,
        reproduction=None if step in {"reproduce", "no-capacity"} else reproduction,
        candidate=reproduction.model_copy(update={"head": candidate_sha})
        if target == candidate_sha
        else None,
        target_sha=target if target == candidate_sha else None,
    )
    if step != "no-capacity":
        job = role_job(scope, step, 1)
    await local.prepare(job, envelope)
    fetch_count = len(fetches)
    await local.prepare(job, envelope)
    assert len(fetches) > fetch_count
    project = local.project(job)
    native_source = project.parent / "native.git"
    assert git(project, "config", "--get", "remote.origin.url") == url
    assert git(project, "remote", "get-url", "origin") == str(native_source)
    assert git(native_source, "rev-parse", "HEAD") == target
    assert not git(native_source, "remote")
    assert not (project / ".git/objects/info/alternates").exists()
    assert not (native_source / "objects/info/alternates").exists()
    # A moving external default cannot change the frozen native source.
    (origin / "fixture.txt").write_text("unapproved later source\n")
    git(origin, "commit", "-qam", "later source")
    _script(
        tools / "pi",
        f"""import json, os, pathlib, sys
if sys.argv[1:] == ['--help']:
    os.execv({binaries["pi"]!r}, [{binaries["pi"]!r}, '--help'])
home = pathlib.Path(os.environ['FM_HOME'])
with (home / 'inert-agent.jsonl').open('a') as stream:
    stream.write(json.dumps({{'cwd': os.getcwd(), 'home': os.environ['HOME']}}) + '\\n')
""",
    )
    # A short /tmp socket root is unavoidable on macOS; never adopt one.
    socket_root = Path("/tmp").resolve() / (
        "sw-native-" + hashlib.sha256(str(tmp_path).encode()).hexdigest()[:8]
    )
    socket_root.mkdir(mode=0o700)
    copy = FirstmateCopy(
        checkout=runtime,
        homes=homes,
        project=local.root(job),
        projects=local.project,
        base_project=local.base(job),
        executable_path=path,
        tmux_sockets=socket_root,
        secrets=SecretGuard(forbidden_values=("PLACEHOLDER_FORGE_VALUE",)),
    )
    home = copy.location(job)
    task_tmp = Path("/tmp").resolve()
    assert not list(task_tmp.glob(f"fm-{copy.task_id(job)}*"))
    sockets = tmux_directory(home, socket_root)
    sockets.mkdir(mode=0o700)
    endpoint = sockets / f"tmux-{os.getuid()}/default"
    endpoint.parent.mkdir(mode=0o700)
    created = sockets / "test-server-pid"
    _real_tmux_script(tools / "tmux", binaries["tmux"])
    if step == "no-capacity":
        prepare = copy._prepare

        def refuse_allocation(*args):
            fresh = prepare(*args)
            config = home / "os-home/.config/treehouse"
            config.mkdir(parents=True)
            (config / "config.toml").write_text("max_trees = 0\n")
            return fresh

        monkeypatch.setattr(copy, "_prepare", refuse_allocation)
    try:
        if step == "no-capacity":
            with pytest.raises(
                RuntimeError, match="could not allocate an isolated task checkout"
            ):
                await copy.start(job, pack, reservation, 60, envelope=envelope)
            assert (await copy.get(job)).status == "uncertain"
            assert not (home / "inert-agent.jsonl").exists()
            assert not (home / "state" / f"{copy.task_id(job)}.meta").exists()
            assert (home / "data" / copy.task_id(job) / "launch-brief.md").is_file()
            with pytest.raises(ValueError, match="Live or ambiguous"):
                await copy.verify_unstarted(job, envelope)
        else:
            assert (
                await copy.start(job, pack, reservation, 60, envelope=envelope)
            ).status == "running"
            async with asyncio.timeout(10):
                while not (home / "inert-agent.jsonl").exists():
                    await asyncio.sleep(0.05)
            receipt = json.loads((home / "inert-agent.jsonl").read_text())
            worktree = Path(receipt["cwd"])
            assert worktree != project and worktree.is_relative_to(
                home / "os-home/.treehouse"
            )
            assert git(worktree, "rev-parse", "--show-toplevel") == str(worktree)
            assert git(worktree, "rev-parse", "HEAD") == target
            assert git(worktree, "rev-parse", "--absolute-git-dir") != git(
                project, "rev-parse", "--path-format=absolute", "--git-common-dir"
            )
            assert receipt["home"] == str(home / "os-home")
            assert (
                await copy.start(job, pack, reservation, 60, envelope=envelope)
            ).status == "running"
            assert len((home / "inert-agent.jsonl").read_text().splitlines()) == 1
            assert git(project, "rev-parse", "HEAD") == target
            if step in {"review", "qa", "smoke"}:
                assert git(project, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
            assert git(runtime, "rev-parse", "HEAD") == pack.firstmate_commit
    finally:
        if created.exists():
            subprocess.run(
                [str(tools / "tmux"), "-S", str(endpoint), "kill-server"],
                env=copy._environment(home),
                check=True,
            )
        else:
            assert not endpoint.exists(), "cleanup refused: server creation is unproven"
        shutil.rmtree(socket_root)
        for directory in task_tmp.glob(f"fm-{copy.task_id(job)}*"):
            assert directory.is_dir() and not directory.is_symlink()
            shutil.rmtree(directory)
