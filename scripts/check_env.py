#!/usr/bin/env python3
"""Data engineering environment check.

Read-only. Installs nothing, changes nothing. Reports what you have, what you
need, and the exact command to fix each gap.

    python3 check_env.py          # macOS / Linux / WSL
    python check_env.py           # Windows

Requires only the standard library on Python 3.8+.
"""

from __future__ import annotations

import os
import platform
import re
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------- colour

_WIN = platform.system() == "Windows"
if _WIN:
    os.system("")

_TTY = sys.stdout.isatty()


def _c(code: str) -> str:
    return code if _TTY else ""


GREEN, YELLOW, RED, BLUE, GREY, BOLD, RESET = (
    _c("\033[32m"),
    _c("\033[33m"),
    _c("\033[31m"),
    _c("\033[34m"),
    _c("\033[90m"),
    _c("\033[1m"),
    _c("\033[0m"),
)

OK, WARN, MISS = f"{GREEN}ok{RESET}", f"{YELLOW}old{RESET}", f"{RED}--{RESET}"

# ---------------------------------------------------------------- os detection


def detect_os() -> str:
    system = platform.system()
    if system == "Linux":
        try:
            with Path("/proc/version").open() as handle:
                if "microsoft" in handle.read().lower():
                    return "wsl"
        except OSError:
            pass
        return "linux"
    return {"Darwin": "macos", "Windows": "windows"}.get(system, "unknown")


HOST_OS = detect_os()

# ------------------------------------------------------------------- checking


@dataclass
class Tool:
    name: str
    command: list[str]
    min_version: tuple[int, ...] | None = None
    why: str = ""
    install: dict[str, str] = field(default_factory=dict)
    version_line: int = 0

    # populated by run()
    found: bool = False
    version: str = ""
    status: str = "missing"


VERSION_RE = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?")


def parse_version(text: str) -> tuple[int, ...] | None:
    match = VERSION_RE.search(text)
    if not match:
        return None
    return tuple(int(g) for g in match.groups() if g is not None)


def run_tool(tool: Tool) -> Tool:
    exe = tool.command[0]
    if shutil.which(exe) is None:
        tool.status = "missing"
        return tool
    try:
        proc = subprocess.run(tool.command, capture_output=True, text=True, timeout=25)
        output = (proc.stdout or "") + (proc.stderr or "")
        lines = [ln for ln in output.splitlines() if ln.strip()]
        raw = (
            lines[tool.version_line]
            if len(lines) > tool.version_line
            else (lines[0] if lines else "")
        )
    except (subprocess.TimeoutExpired, OSError):
        tool.found = True
        tool.status = "ok"
        tool.version = "present"
        return tool

    tool.found = True
    tool.version = raw.strip()[:58]
    parsed = parse_version(raw)

    if tool.min_version and parsed:
        padded = parsed + (0,) * (len(tool.min_version) - len(parsed))
        tool.status = "ok" if padded[: len(tool.min_version)] >= tool.min_version else "old"
    else:
        tool.status = "ok"
    return tool


def install_hint(tool: Tool) -> str:
    return tool.install.get(HOST_OS) or tool.install.get("all", "see project docs")


# ------------------------------------------------------------------ inventory

REQUIRED = [
    Tool(
        "Python",
        [sys.executable, "--version"],
        (3, 12),
        "Runtime for everything",
        {
            "macos": "brew install python@3.12   (or: uv python install 3.12)",
            "linux": "uv python install 3.12",
            "wsl": "uv python install 3.12",
            "windows": "winget install Python.Python.3.12",
        },
    ),
    Tool(
        "git",
        ["git", "--version"],
        (2, 30),
        "Version control",
        {
            "macos": "brew install git",
            "linux": "sudo apt install git",
            "wsl": "sudo apt install git",
            "windows": "winget install Git.Git",
        },
    ),
    Tool(
        "uv",
        ["uv", "--version"],
        (0, 4),
        "Python package + version manager",
        {
            "macos": "curl -LsSf https://astral.sh/uv/install.sh | sh",
            "linux": "curl -LsSf https://astral.sh/uv/install.sh | sh",
            "wsl": "curl -LsSf https://astral.sh/uv/install.sh | sh",
            "windows": 'powershell -c "irm https://astral.sh/uv/install.ps1 | iex"',
        },
    ),
    Tool(
        "Docker",
        ["docker", "--version"],
        (24, 0),
        "Runs Airflow, Postgres, Kafka",
        {
            "macos": "brew install --cask docker",
            "linux": "curl -fsSL https://get.docker.com | sudo sh",
            "wsl": "Install Docker Desktop on Windows, enable WSL2 integration",
            "windows": "winget install Docker.DockerDesktop",
        },
    ),
    Tool(
        "Docker Compose",
        ["docker", "compose", "version"],
        (2, 0),
        "Multi-container stacks",
        {"all": "Ships with Docker Desktop. On Linux: sudo apt install docker-compose-plugin"},
    ),
]

RECOMMENDED = [
    Tool(
        "make",
        ["make", "--version"],
        (3, 81),
        "Runs the project shortcuts",
        {
            "macos": "xcode-select --install",
            "linux": "sudo apt install make",
            "wsl": "sudo apt install make",
            "windows": "Use WSL2, or: winget install GnuWin32.Make",
        },
    ),
    Tool(
        "Java (JDK)",
        ["java", "-version"],
        (17, 0),
        "PySpark will not start without it",
        {
            "macos": "brew install openjdk@17",
            "linux": "sudo apt install openjdk-17-jdk",
            "wsl": "sudo apt install openjdk-17-jdk",
            "windows": "winget install Microsoft.OpenJDK.17",
        },
    ),
    Tool(
        "curl",
        ["curl", "--version"],
        None,
        "Used by installers and API pokes",
        {
            "linux": "sudo apt install curl",
            "wsl": "sudo apt install curl",
            "all": "usually preinstalled",
        },
    ),
]

OPTIONAL = [
    Tool(
        "Terraform",
        ["terraform", "--version"],
        (1, 5),
        "Week 7: infrastructure as code",
        {
            "macos": "brew install terraform",
            "linux": "see developer.hashicorp.com/terraform/install",
            "wsl": "see developer.hashicorp.com/terraform/install",
            "windows": "winget install HashiCorp.Terraform",
        },
    ),
    Tool(
        "AWS CLI",
        ["aws", "--version"],
        (2, 0),
        "Only if you pick AWS",
        {
            "macos": "brew install awscli",
            "linux": "sudo apt install awscli",
            "wsl": "sudo apt install awscli",
            "windows": "winget install Amazon.AWSCLI",
        },
    ),
    Tool(
        "gcloud",
        ["gcloud", "--version"],
        None,
        "Only if you pick GCP",
        {"all": "https://cloud.google.com/sdk/docs/install"},
    ),
    Tool(
        "Node.js",
        ["node", "--version"],
        (20, 0),
        "Week 9: portfolio site build",
        {
            "macos": "brew install node",
            "linux": "sudo apt install nodejs npm",
            "wsl": "sudo apt install nodejs npm",
            "windows": "winget install OpenJS.NodeJS.LTS",
        },
    ),
    Tool(
        "jq",
        ["jq", "--version"],
        None,
        "Handy for poking JSON APIs",
        {
            "macos": "brew install jq",
            "linux": "sudo apt install jq",
            "wsl": "sudo apt install jq",
            "windows": "winget install jqlang.jq",
        },
    ),
]

# Tools that normally live inside the project venv, not globally.
PROJECT = [
    Tool("ruff", ["ruff", "--version"], None, "linter + formatter", {"all": "make install"}),
    Tool("mypy", ["mypy", "--version"], None, "type checker", {"all": "make install"}),
    Tool("pytest", ["pytest", "--version"], None, "test runner", {"all": "make install"}),
    Tool(
        "dbt",
        ["dbt", "--version"],
        None,
        "transformations",
        {"all": "make install"},
        version_line=1,
    ),
    Tool("pre-commit", ["pre-commit", "--version"], None, "git hooks", {"all": "make install"}),
]

# ------------------------------------------------------------------- reporting


def print_section(title: str, tools: list[Tool]) -> list[Tool]:
    print(f"\n{BOLD}{title}{RESET}")
    problems = []
    for tool in tools:
        result = run_tool(tool)
        badge = {"ok": OK, "old": WARN, "missing": MISS}[result.status]
        if result.status == "missing":
            print(f"  [{badge}] {result.name:<16} {GREY}{result.why}{RESET}")
            problems.append(result)
        else:
            detail = result.version
            if result.status == "old":
                detail += (
                    f"  {YELLOW}(want >= {'.'.join(map(str, result.min_version or ()))}){RESET}"
                )
                problems.append(result)
            print(f"  [{badge}] {result.name:<16} {detail}")
    return problems


def check_resources() -> list[str]:
    print(f"\n{BOLD}System resources{RESET}")
    issues = []

    total_gb = 0.0
    try:
        if HOST_OS in {"linux", "wsl"}:
            with Path("/proc/meminfo").open() as handle:
                for line in handle:
                    if line.startswith("MemTotal"):
                        total_gb = int(line.split()[1]) / 1024 / 1024
                        break
        elif HOST_OS == "macos":
            out = subprocess.run(
                ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=10
            )
            total_gb = int(out.stdout.strip()) / 1024**3
        elif HOST_OS == "windows":
            import ctypes

            class MemStat(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MemStat()
            stat.dwLength = ctypes.sizeof(MemStat)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))  # type: ignore[attr-defined]
            total_gb = stat.ullTotalPhys / 1024**3
    except Exception:
        pass

    if total_gb:
        badge = OK if total_gb >= 8 else (WARN if total_gb >= 4 else MISS)
        print(f"  [{badge}] {'RAM':<16} {total_gb:.1f} GB")
        if total_gb < 8:
            issues.append(
                "RAM under 8 GB. Airflow + Postgres + Kafka together will be tight. "
                "Run one stack at a time and give Docker at least 4 GB."
            )
    else:
        print(f"  [{GREY}??{RESET}] {'RAM':<16} {GREY}could not detect{RESET}")

    try:
        free_gb = shutil.disk_usage(Path.cwd()).free / 1024**3
        badge = OK if free_gb >= 30 else (WARN if free_gb >= 15 else MISS)
        print(f"  [{badge}] {'Disk free':<16} {free_gb:.1f} GB")
        if free_gb < 30:
            issues.append(
                f"Only {free_gb:.0f} GB free. Docker images for Airflow, Spark and "
                "Kafka run 15-20 GB together. Aim for 30 GB headroom."
            )
    except OSError:
        pass

    for port, service in ((8080, "Airflow UI"), (5432, "Postgres")):
        busy = False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.4)
            busy = sock.connect_ex(("127.0.0.1", port)) == 0
        badge = WARN if busy else OK
        state = f"{YELLOW}in use{RESET}" if busy else "free"
        print(f"  [{badge}] {'Port ' + str(port):<16} {state} {GREY}({service}){RESET}")
        if busy:
            issues.append(
                f"Port {port} is occupied. Stop whatever is using it, or remap "
                f"the port in docker-compose.yml before running make up."
            )
    return issues


def check_docker_daemon() -> list[str]:
    if shutil.which("docker") is None:
        return []
    try:
        proc = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=25)
        if proc.returncode == 0:
            print(f"  [{OK}] {'Docker daemon':<16} running")
            return []
    except (subprocess.TimeoutExpired, OSError):
        pass
    print(f"  [{MISS}] {'Docker daemon':<16} {RED}not running{RESET}")
    return [
        "Docker is installed but the daemon is not running. Start Docker Desktop "
        "(or: sudo systemctl start docker) and rerun this check."
    ]


def check_git_identity() -> list[str]:
    if shutil.which("git") is None:
        return []
    issues = []
    for key in ("user.name", "user.email"):
        try:
            proc = subprocess.run(
                ["git", "config", "--global", key], capture_output=True, text=True, timeout=10
            )
            value = proc.stdout.strip()
        except (subprocess.TimeoutExpired, OSError):
            value = ""
        badge = OK if value else MISS
        shown = value if value else f"{RED}unset{RESET}"
        print(f"  [{badge}] {'git ' + key:<16} {shown}")
        if not value:
            issues.append(
                f"git {key} is unset. Your commits will look anonymous on the "
                f"GitHub profile recruiters look at. Fix: "
                f'git config --global {key} "..."'
            )
    return issues


# ------------------------------------------------------------------------ main


def main() -> int:
    print(f"{BOLD}{BLUE}Data engineering environment check{RESET}")
    print(
        f"{GREY}{platform.system()} {platform.release()} · {platform.machine()} "
        f"· detected as: {HOST_OS}{RESET}"
    )
    if HOST_OS == "windows":
        print(
            f"{YELLOW}Note:{RESET} Airflow and most DE tooling are far less painful under "
            f"WSL2.\n      Consider: wsl --install -d Ubuntu, then work inside Ubuntu."
        )

    blockers = print_section("Required", REQUIRED)
    print(f"\n{BOLD}Docker and git health{RESET}")
    notes = check_docker_daemon() + check_git_identity()

    soon = print_section("Recommended (needed by week 4)", RECOMMENDED)
    later = print_section("Optional (needed by week 7+)", OPTIONAL)

    print(f"\n{BOLD}Project tools{RESET} {GREY}(expected inside .venv, not global){RESET}")
    in_venv = sys.prefix != sys.base_prefix
    print(f"  {GREY}virtualenv active: {'yes' if in_venv else 'no'}{RESET}")
    if in_venv:
        print_section("Installed in this venv", PROJECT)
    if not in_venv:
        for tool in PROJECT:
            result = run_tool(tool)
            badge = OK if result.status == "ok" else GREY + "--" + RESET
            note = result.version if result.found else f"{GREY}not global, fine{RESET}"
            print(f"  [{badge}] {result.name:<16} {note}")

    notes += check_resources()

    # ---------------------------------------------------------------- summary
    print(f"\n{BOLD}{'=' * 62}{RESET}")
    if blockers:
        print(f"\n{RED}{BOLD}Blocking — install these first:{RESET}")
        for tool in blockers:
            print(f"\n  {BOLD}{tool.name}{RESET}  {GREY}{tool.why}{RESET}")
            print(f"    {install_hint(tool)}")

    if soon:
        print(f"\n{YELLOW}{BOLD}Needed soon:{RESET}")
        for tool in soon:
            print(f"  {tool.name:<16} {install_hint(tool)}")

    if later:
        print(f"\n{BLUE}{BOLD}Can wait:{RESET}")
        for tool in later:
            print(f"  {tool.name:<16} {GREY}{tool.why}{RESET}")

    if notes:
        print(f"\n{YELLOW}{BOLD}Notes:{RESET}")
        for note in notes:
            print(f"  - {note}")

    if not blockers and not soon:
        print(f"\n{GREEN}{BOLD}Ready to go.{RESET} Next:")
        print("    bash scripts/bootstrap.sh")
        print("    source .venv/bin/activate && make check")
    print()
    return 1 if blockers else 0


if __name__ == "__main__":
    sys.exit(main())
