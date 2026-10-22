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

if [[ "${1:-}" == --self-test && $# == 1 ]]; then
    set -e
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
if (( $# != 0 )); then
    echo 'Usage: scripts/local-ci.sh [--self-test]' >&2
    exit 2
fi

need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "ERROR: missing $1. $2" >&2
        exit 1
    }
}
need git 'Install Git: https://git-scm.com/downloads'
# Use the fetched remote main, not a potentially stale local main branch.
git fetch origin main || exit 1
base=$(git merge-base HEAD refs/remotes/origin/main) || exit 1
git diff --name-only --no-renames "$base" HEAD >/dev/null || exit 1
# NUL delimiters preserve whitespace and newlines in names.
paths=()
while IFS= read -r -d '' path; do paths+=("$path"); done < <(git diff --name-only --no-renames -z "$base" HEAD)
if (( ${#paths[@]} )); then
    select_jobs "${paths[@]}"
else
    select_jobs
fi
printf 'Merge base: %s\nJobs: backend=%s console=%s gateway=%s artifacts=%s gitleaks=1\n' \
    "$base" "$backend" "$console" "$gateway" "$artifacts"

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
    printf '\nRUN %s\n' "$name"
    (set -e; "$@")
    result=$?
    if (( result == 0 )); then
        printf 'PASS %s (%ss)\n' "$name" "$((SECONDS - start))"
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
