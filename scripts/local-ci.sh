#!/usr/bin/env bash
# Keep the filters and commands aligned with .github/workflows/pr-ci.yml.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

select_jobs() {
    backend=0 console=0 gateway=0 artifacts=0
    local path
    for path in "$@"; do
        case "$path" in
            .github/workflows/pr-ci.yml|ruff.toml|justfile)
                backend=1 console=1 gateway=1 artifacts=1 ;;
        esac
        case "$path" in core/*|connectors/*) backend=1 ;; esac
        case "$path" in console/*|deploy/local/standalone-docker-compose.yml) console=1 ;; esac
        case "$path" in gateway/*) gateway=1 ;; esac
        case "$path" in
            artifacts.yaml|scripts/gen_artifacts.py|core/switch_core/artifacts.py|core/pyproject.toml|\
            console/packages/shared/src/artifacts.ts|console/packages/switch-agent-runtime/src/artifacts.ts|\
            console/apps/switch-console-desktop/package.json|\
            console/apps/switch-console-desktop/src/shared/app-identity*.ts|\
            console/packages/switch-agent-runtime/package.json|connectors/*/plugin.json)
                artifacts=1 ;;
        esac
    done
}

local_ci_reuse_context() {
    local cli context
    cli=${LOCAL_CI_CONTEXT_CLI:-no-mistakes}
    context=$("$cli" axi context --json 2>/dev/null) || return 1
    LOCAL_CI_SCOPE=$(printf '%s\n' "$context" | jq -ce '
        select(.schema_version == 1 and .available == true)
        | select(.phase | startswith("gate."))
        | select([.run_id, .repo_id, .worktree, .phase] | all(.[]; type == "string" and length > 0))
        | [.run_id, .repo_id, .worktree, .phase]
    ') || return 1
}

local_ci_tool_versions() {
    {
        git --version || echo 'git=missing'
        if command -v uv >/dev/null; then uv --version; else echo 'uv=missing'; fi
        if command -v just >/dev/null; then just --version; else echo 'just=missing'; fi
        if command -v docker >/dev/null; then
            docker version --format '{{.Client.Version}} {{.Server.Version}}' || echo 'docker=unknown'
        else
            echo 'docker=missing'
        fi
        if command -v node >/dev/null; then node --version; else echo 'node=missing'; fi
        if command -v pnpm >/dev/null; then pnpm --version; else echo 'pnpm=missing'; fi
        if command -v npm >/dev/null; then npm --version; else echo 'npm=missing'; fi
    } 2>/dev/null
}

local_ci_fingerprint() {
    local job=$1
    {
        printf 'job %s\nbase %s\nhead %s\ntree %s\n' "$job" "$base" "$head" "$tree"
        git rev-list "$base..HEAD" || echo 'range=unknown'
        local_ci_tool_versions
        if [[ $job == gitleaks && -f .gitleaks.toml ]]; then
            git hash-object .gitleaks.toml
        elif [[ $job == gitleaks ]]; then
            echo 'gitleaks-config=missing'
        fi
        printf 'scope %s\n' "$LOCAL_CI_SCOPE"
    } | git hash-object --stdin
}

local_ci_reuse_home() {
    local root scope_id
    root=${XDG_STATE_HOME:-$HOME/.local/state}
    scope_id=$(printf '%s' "$LOCAL_CI_SCOPE" | git hash-object --stdin)
    printf '%s\n' "$root/switch-local-ci/$scope_id"
}

local_ci_try_reuse() {
    local job=$1 home fp receipt tested when stored
    [[ $fresh == 0 && -n $LOCAL_CI_SCOPE ]] || return 1
    home=$(local_ci_reuse_home) || return 1
    fp=$(local_ci_fingerprint "$job") || return 1
    receipt=$home/$job.json
    [[ -f $receipt ]] || return 1
    tested=$(jq -er 'select(.schema == 1 and .exit == 0) | .tested_sha' "$receipt" 2>/dev/null) || return 1
    when=$(jq -er '.finished_at' "$receipt" 2>/dev/null) || return 1
    stored=$(jq -er '.fingerprint' "$receipt" 2>/dev/null) || return 1
    [[ $stored == "$fp" && $tested =~ ^[0-9a-f]{40}$ ]] || return 1
    printf 'REUSED %s from %s at %s (same verified gate, same inputs)\n' "$job" "$tested" "$when"
}

local_ci_save_receipt() {
    local job=$1 duration=$2 log=$3 home fp tmp
    [[ -n $LOCAL_CI_SCOPE ]] || return 0
    home=$(local_ci_reuse_home) || return 0
    fp=$(local_ci_fingerprint "$job") || return 0
    mkdir -p "$home" || return 0
    chmod 700 "$home" "$(dirname "$home")" 2>/dev/null || true
    tmp=$(mktemp "$home/.receipt.XXXXXX") || return 0
    if ! jq -n --arg sha "$head" --arg fp "$fp" --arg when "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson duration "$duration" --arg log "$log" \
        '{schema:1,exit:0,tested_sha:$sha,fingerprint:$fp,finished_at:$when,duration_s:$duration,log:$log}' >"$tmp"; then
        rm -f "$tmp"
        return 0
    fi
    chmod 600 "$tmp"
    mv -f "$tmp" "$home/$job.json"
}

local_ci_reuse_self_test() {
    local root repo bin
    root=$(mktemp -d)
    repo=$root/repo
    bin=$root/bin
    mkdir -p "$repo/scripts" "$repo/core" "$bin" "$root/state"
    cp "$PWD/scripts/local-ci.sh" "$repo/scripts/local-ci.sh"
    git -C "$repo" init -b main >/dev/null
    git -C "$repo" config user.email ci@example.invalid
    git -C "$repo" config user.name 'CI fixture'
    printf 'x = 1\n' >"$repo/core/example.py"
    git -C "$repo" add .
    git -C "$repo" commit -m base >/dev/null
    git -C "$repo" remote add origin "$repo"
    git -C "$repo" checkout -b feature >/dev/null
    printf 'y = 1\n' >"$repo/core/example.py"
    git -C "$repo" add core/example.py
    git -C "$repo" commit -m change >/dev/null
    printf '#!/bin/sh\nexit 0\n' >"$bin/uv"
    # The quoted command is the fixture executable, not this test script.
    # shellcheck disable=SC2016
    printf '%s\n' '#!/bin/sh' 'printf "%s\n" "$*" >>"$REUSE_COUNTS"' 'exit 0' >"$bin/just"
    cat >"$bin/docker" <<'EOF'
#!/bin/sh
if [ "$1" = run ]; then
    printf '%s\n' docker >>"$REUSE_COUNTS"
    if [ -n "${REUSE_SCAN_FAIL:-}" ] && [ -f "$REUSE_SCAN_FAIL" ]; then
        echo 'synthetic scanner failure'
        exit 1
    fi
    if [ -n "${REUSE_SECRET_FILE:-}" ] && [ -f "$REUSE_SECRET_FILE" ]; then
        echo 'synthetic secret'
        exit 1
    fi
fi
exit 0
EOF
    cat >"$bin/no-mistakes" <<'EOF'
#!/bin/sh
if [ "$1" = axi ] && [ "$2" = context ] && [ "$3" = --json ]; then
    if [ -n "$REUSE_CONTEXT" ]; then
        printf '%s\n' "$REUSE_CONTEXT"
        exit 0
    fi
    printf '%s\n' '{"schema_version":1,"available":false,"reason":"no_enclosing_custom_gate"}'
    exit 1
fi
exit 1
EOF
    chmod 755 "$bin"/*
    export PATH="$bin:$PATH" XDG_STATE_HOME="$root/state" LOCAL_CI_REPO="$repo"
    export REUSE_COUNTS="$root/counts" REUSE_SCAN_FAIL="$root/scan-fail" REUSE_SECRET_FILE="$root/secret-present" REUSE_CONTEXT
    context() {
        printf '{"schema_version":1,"available":true,"run_id":"%s","repo_id":"repo-a","worktree":"%s","phase":"gate.local-ci"}\n' "$1" "$repo"
    }
    run_ci() {
        : >"$REUSE_COUNTS"
        (cd "$repo" && bash scripts/local-ci.sh "$@") >"$root/out" 2>&1
    }
    expect() {
        grep -q "$1" "$root/out" || { echo "missing: $1" >&2; cat "$root/out" >&2; exit 1; }
    }
    absent() {
        if grep -q "$1" "$root/out"; then echo "unexpected: $1" >&2; cat "$root/out" >&2; exit 1; fi
    }
    touch "$REUSE_SCAN_FAIL"
    REUSE_CONTEXT=$(context run-a)
    run_ci && { echo 'expected scan failure' >&2; exit 1; }
    expect 'PASS backend'
    expect 'FAIL gitleaks'
    rm -f "$REUSE_SCAN_FAIL"
    run_ci || { echo 'expected reuse pass' >&2; cat "$root/out" >&2; exit 1; }
    expect 'REUSED backend'
    expect 'RUN gitleaks'
    expect 'PASS gitleaks'
    if grep -qx check "$REUSE_COUNTS"; then echo 'backend reran' >&2; exit 1; fi
    run_ci --fresh || exit 1
    expect 'RUN backend'
    grep -qx check "$REUSE_COUNTS"
    REUSE_CONTEXT=$(context run-b)
    run_ci || exit 1
    expect 'RUN backend'
    absent 'REUSED backend'
    unset REUSE_CONTEXT
    run_ci || exit 1
    expect 'RUN backend'
    absent 'REUSED'
    REUSE_CONTEXT=$(context run-a)
    find "$root/state" -name backend.json -exec sh -c 'printf "not-json\n" >"$1"' _ {} \;
    run_ci || exit 1
    expect 'RUN backend'
    absent 'REUSED backend'
    REUSE_CONTEXT=$(context run-c)
    run_ci || { echo 'expected clean scan' >&2; cat "$root/out" >&2; exit 1; }
    expect 'PASS gitleaks'
    printf 'aws_secret_access_key = AKIASYNTHETICFIXTUREONLY\n' >"$repo/core/secret.py"
    git -C "$repo" add core/secret.py
    git -C "$repo" commit -m 'add synthetic secret' >/dev/null
    : >"$REUSE_SECRET_FILE"
    run_ci && { echo 'expected secret failure' >&2; cat "$root/out" >&2; exit 1; }
    expect 'RUN gitleaks'
    expect 'FAIL gitleaks'
    absent 'REUSED gitleaks'
    echo 'PASS reuse self-test'
    rm -rf "$root"
}

fresh=0
reuse_test=0
while (( $# )); do
    case "$1" in
        --fresh) fresh=1 ;;
        --self-test) ;;
        --reuse) reuse_test=1 ;;
        *)
            echo 'Usage: scripts/local-ci.sh [--fresh] [--self-test [--reuse]]' >&2
            exit 2 ;;
    esac
    if [[ $1 == --self-test ]]; then self_test=1; fi
    shift
done

if [[ ${self_test:-0} == 1 ]]; then
    set -e
    if (( reuse_test )); then
        local_ci_reuse_self_test
        exit 0
    fi
    select_jobs core/switch_core/main.py
    [[ "$backend$console$gateway$artifacts" == 1000 ]]
    for path in .github/workflows/pr-ci.yml ruff.toml justfile; do
        select_jobs "$path"
        [[ "$backend$console$gateway$artifacts" == 1111 ]]
    done
    select_jobs deploy/local/standalone-docker-compose.yml
    [[ "$backend$console$gateway$artifacts" == 0100 ]]
    select_jobs connectors/codex-plugin/.codex-plugin/plugin.json
    [[ "$backend$console$gateway$artifacts" == 1001 ]]
    select_jobs core/pyproject.toml
    [[ "$backend$console$gateway$artifacts" == 1001 ]]
    select_jobs console/apps/switch-console-desktop/src/shared/app-identity.test.ts
    [[ "$backend$console$gateway$artifacts" == 0101 ]]
    select_jobs gateway/src/main.ts README.md
    [[ "$backend$console$gateway$artifacts" == 0010 ]]
    select_jobs
    [[ "$backend$console$gateway$artifacts" == 0000 ]]
    echo 'PASS path-filter self-test'
    exit 0
fi

need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "ERROR: missing $1. $2" >&2
        exit 1
    }
}
need git 'Install Git: https://git-scm.com/downloads'
need jq 'Install jq: https://jqlang.org/download/'
# Use the fetched remote main, not a potentially stale local main branch.
git fetch origin main || exit 1
base=$(git merge-base HEAD refs/remotes/origin/main) || exit 1
head=$(git rev-parse HEAD) || exit 1
git diff --name-only --no-renames "$base" HEAD >/dev/null || exit 1
# NUL delimiters preserve whitespace and newlines in names.
paths=()
while IFS= read -r -d '' path; do paths+=("$path"); done < <(git diff --name-only --no-renames -z "$base" HEAD)
if (( ${#paths[@]} )); then
    select_jobs "${paths[@]}"
else
    select_jobs
fi
tree=$(git rev-parse 'HEAD^{tree}') || exit 1
if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
    tree=dirty
fi
printf 'Merge base: %s\nJobs: backend=%s console=%s gateway=%s artifacts=%s gitleaks=1\n' \
    "$base" "$backend" "$console" "$gateway" "$artifacts"

LOCAL_CI_SCOPE=
if (( fresh )); then
    echo 'Fresh mode: reuse disabled'
else
    local_ci_reuse_context || LOCAL_CI_SCOPE=
    if [[ -z $LOCAL_CI_SCOPE ]]; then
        echo 'No verified gate context: reuse disabled'
    fi
fi

check_docker() {
    need docker 'Install Docker Desktop: https://docs.docker.com/desktop/setup/install/'
    docker info >/dev/null || {
        echo 'ERROR: start Docker Desktop and check docker info.' >&2
        exit 1
    }
}

check_node() {
    need node 'Install Node 24: https://nodejs.org/en/download'
    [[ "$(node -p 'process.versions.node.split(".")[0]')" == 24 ]] || {
        echo 'ERROR: use Node 24 on PATH. Install: https://nodejs.org/en/download' >&2
        exit 1
    }
}

backend_checks() {
    need uv 'Install uv: https://docs.astral.sh/uv/getting-started/installation/'
    need just 'Install just: brew install just (macOS), cargo install just (Linux).'
    check_docker
    uv sync --project core --python 3.12
    just check
    just typecheck
    just test
}

console_checks() {
    check_node
    need pnpm 'Install pnpm: npm install --global pnpm@11.5.3'
    [[ "$(pnpm --version)" == 11.5.3 ]] || {
        echo 'ERROR: use pnpm 11.5.3. Install: npm install --global pnpm@11.5.3' >&2
        exit 1
    }
    cd console
    pnpm install --frozen-lockfile
    pnpm exec playwright install --with-deps chromium
    pnpm --filter @switch-console/desktop run sync:compose:check
    pnpm -r --filter './packages/**' run build
    pnpm -r run lint
    pnpm -r run format:check
    pnpm -r run typecheck
    pnpm -r run test
}

artifact_checks() {
    need uv 'Install uv: https://docs.astral.sh/uv/getting-started/installation/'
    need just 'Install just: brew install just (macOS), cargo install just (Linux).'
    uv sync --project core --python 3.12
    just artifacts-check
}

gateway_checks() {
    check_node
    need npm 'Install Node 24 (includes npm): https://nodejs.org/en/download'
    cd gateway
    npm ci
    npm run build
}

secret_checks() {
    check_docker
    local common
    common=$(git rev-parse --git-common-dir)
    common=$(cd "$common" && pwd -P)
    # A linked worktree's .git file points outside /repo; mount its metadata too.
    docker run --rm -v "$PWD:/repo:ro" -v "$common:$common:ro" -w /repo \
        zricethezav/gitleaks:v8.30.1 detect --source=/repo --redact \
        --log-opts="$base..HEAD" --verbose
}

failures=0
run_job() {
    local name=$1 start=$SECONDS result
    shift
    if local_ci_try_reuse "$name"; then
        return 0
    fi
    printf '\nRUN %s\n' "$name"
    local log
    log=$(mktemp "${TMPDIR:-/tmp}/local-ci.$name.XXXXXX") || return 1
    (set -e; "$@") >"$log" 2>&1
    result=$?
    cat "$log"
    if (( result == 0 )); then
        printf 'PASS %s (%ss)\n' "$name" "$((SECONDS - start))"
        local_ci_save_receipt "$name" "$((SECONDS - start))" "$log"
    else
        printf 'FAIL %s (%ss, exit %s)\n' "$name" "$((SECONDS - start))" "$result" >&2
        failures=$((failures + 1))
    fi
}

if (( backend )); then run_job backend backend_checks; fi
if (( console )); then run_job console console_checks; fi
if (( artifacts )); then run_job artifacts artifact_checks; fi
if (( gateway )); then run_job gateway gateway_checks; fi
run_job gitleaks secret_checks
printf '\nLocal CI: %s failed job(s), %ss total\n' "$failures" "$SECONDS"
(( failures == 0 ))
