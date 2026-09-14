#!/usr/bin/env bash
# Bootstrap a data engineering workstation.
#
#   bash scripts/bootstrap.sh
#
# Safe to rerun. Installs only what is missing. Works on macOS and Debian/Ubuntu
# (including WSL2). Windows users: run this inside WSL2, not PowerShell.

set -euo pipefail

BLUE=$'\033[0;34m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[0;33m'; RED=$'\033[0;31m'; NC=$'\033[0m'
info()  { echo "${BLUE}==>${NC} $*"; }
ok()    { echo "${GREEN} ok ${NC} $*"; }
warn()  { echo "${YELLOW}warn${NC} $*"; }
fail()  { echo "${RED}fail${NC} $*" >&2; }

have() { command -v "$1" >/dev/null 2>&1; }

case "$(uname -s)" in
    Darwin) OS=macos ;;
    Linux)  OS=linux ;;
    *)      fail "Unsupported OS. On Windows, run this inside WSL2."; exit 1 ;;
esac
info "Detected ${OS}"

# ---------------------------------------------------------------- package manager
if [[ "$OS" == "macos" ]]; then
    if ! have brew; then
        info "Installing Homebrew"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    ok "Homebrew present"
    PKG_INSTALL="brew install"
else
    sudo apt-get update -qq
    PKG_INSTALL="sudo apt-get install -y -qq"
fi

# ---------------------------------------------------------------- core tools
for tool in git curl jq make; do
    if have "$tool"; then ok "$tool"; else info "Installing $tool"; $PKG_INSTALL "$tool"; fi
done

# ---------------------------------------------------------------- uv (Python manager)
if have uv; then
    ok "uv $(uv --version | awk '{print $2}')"
else
    info "Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

info "Ensuring Python 3.12"
uv python install 3.12
ok "Python 3.12 available via uv"

# ---------------------------------------------------------------- Docker
if have docker; then
    ok "Docker $(docker --version | awk '{print $3}' | tr -d ,)"
    if docker info >/dev/null 2>&1; then
        ok "Docker daemon running"
    else
        warn "Docker installed but the daemon is not running. Start Docker Desktop."
    fi
else
    if [[ "$OS" == "macos" ]]; then
        warn "Install Docker Desktop: https://www.docker.com/products/docker-desktop"
    else
        info "Installing Docker Engine"
        curl -fsSL https://get.docker.com | sudo sh
        sudo usermod -aG docker "$USER"
        warn "Log out and back in for the docker group to take effect."
    fi
fi

# ---------------------------------------------------------------- project env
info "Creating project virtualenv"
uv venv --python 3.12 .venv
# shellcheck disable=SC1091
source .venv/bin/activate
uv pip install -e ".[dev,dbt]"
ok "Dependencies installed"

if have pre-commit || [[ -x .venv/bin/pre-commit ]]; then
    pre-commit install
    ok "Pre-commit hooks installed"
fi

[[ -f .env ]] || { cp .env.example .env; ok "Created .env from template"; }

# ---------------------------------------------------------------- git identity
if ! git config user.email >/dev/null 2>&1; then
    warn "Git identity unset. Run:"
    echo "    git config --global user.name  'Your Name'"
    echo "    git config --global user.email 'you@example.com'"
fi

# ---------------------------------------------------------------- verify
echo
info "Verifying the toolchain"
FAILED=0
run_check() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then ok "$label"; else fail "$label"; FAILED=1; fi
}
run_check "ruff"     ruff --version
run_check "mypy"     mypy --version
run_check "pytest"   pytest --version
run_check "dbt"      dbt --version
run_check "duckdb"   python -c "import duckdb; duckdb.connect(':memory:')"
run_check "imports"  python -c "import pipeline, pipeline.extract, pipeline.load"

echo
if [[ $FAILED -eq 0 ]]; then
    ok "Environment ready."
    echo
    echo "  source .venv/bin/activate"
    echo "  make check      # lint + types + tests"
    echo "  make up         # start Airflow at localhost:8080"
else
    fail "Some checks failed. See output above."
    exit 1
fi
