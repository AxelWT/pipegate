#!/usr/bin/env bash
#
# start-tunnel.sh — Start a pipegate tunnel client via a config profile.
#
# Thin wrapper around `pipegate connect` that adds two conveniences:
#   1. Auto-loads ./.env so PIPEGATE_JWT_SECRET is set without manual export.
#   2. Pre-flight checks the local service port is listening (uses the
#      real pipegate config loader to resolve the profile's target port),
#      failing fast with a clear message instead of a 504 later.
#
# Usage:
#   scripts/start-tunnel.sh                          # default profile
#   scripts/start-tunnel.sh app1                     # named profile
#   scripts/start-tunnel.sh app1 --target http://localhost:4000   # pass-through
#   scripts/start-tunnel.sh -h|--help
#
# Prerequisites:
#   - Run `uv sync` in the repo root first (creates .venv with pipegate CLI)
#   - A profile defined in ~/.config/pipegate/config.toml or ./.pipegate.toml
#     (copy from .pipegate.toml.example)
#   - ./.env with PIPEGATE_JWT_SECRET (copy from .env.example), OR the
#     secret exported in your shell
#   - Local service running on the profile's target port
#
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/start-tunnel.sh [PROFILE] [CONNECT_FLAGS...]

Start a pipegate tunnel client via a config profile.

Arguments:
  PROFILE         Profile name (default: "default"). Must be defined in
                  ~/.config/pipegate/config.toml or ./.pipegate.toml.
  CONNECT_FLAGS   Extra flags passed through to `pipegate connect`
                  (e.g. --target, --server, --cid, --secret).

Options:
  -h, --help      Show this help

Examples:
  scripts/start-tunnel.sh
  scripts/start-tunnel.sh app1
  scripts/start-tunnel.sh app1 --target http://localhost:4000

Prerequisites:
  - `uv sync` run in repo root (creates .venv with the pipegate CLI)
  - A profile defined in ~/.config/pipegate/config.toml or ./.pipegate.toml
  - ./.env with PIPEGATE_JWT_SECRET (or exported in your shell)
  - Local service running on the profile's target port
EOF
}

# ---------------------------------------------------------------------------
# Parse first argument: profile name, -h/--help, or default
# ---------------------------------------------------------------------------

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

PROFILE="${1:-default}"
# If a profile name was given, shift it off so "$@" holds connect flags.
# A leading "--" means no profile was given, only flags — keep $1 intact.
if [[ "$#" -gt 0 && "$1" != --* ]]; then
    shift
fi

# ---------------------------------------------------------------------------
# Locate repo root and pipegate CLI / python
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PIPEGATE="$REPO_ROOT/.venv/bin/pipegate"
PYTHON="$REPO_ROOT/.venv/bin/python"

if [[ ! -x "$PIPEGATE" ]]; then
    echo "Error: pipegate CLI not found at $PIPEGATE" >&2
    echo "Run 'uv sync' in the repo root first." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Load ./.env if present (auto-inject PIPEGATE_JWT_SECRET)
# ---------------------------------------------------------------------------

if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.env"
    set +a
fi

# ---------------------------------------------------------------------------
# Resolve the profile's target port via the real config loader.
# This also validates the profile exists, target is set, and secret_env
# points to a populated variable — failing fast with a clear error.
# ---------------------------------------------------------------------------

set +e
PORT="$("$PYTHON" - "$PROFILE" <<'PYEOF'
import sys
from urllib.parse import urlparse
from pipegate.config import load_profile

try:
    p = load_profile(sys.argv[1])
except Exception as e:
    print(f"config: {e}", file=sys.stderr)
    sys.exit(1)

parsed = urlparse(p.target)
port = parsed.port or (443 if parsed.scheme == "https" else 80)
print(port)
PYEOF
)"
RC=$?
set -e

if [[ $RC -ne 0 ]]; then
    echo "Error: failed to resolve profile '$PROFILE' (see message above)." >&2
    exit 1
fi

if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
    echo "Error: resolved port is not numeric: '$PORT'" >&2
    echo "Profile '$PROFILE' target may be malformed." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Pre-flight: local service must be listening on the port
# ---------------------------------------------------------------------------

if ! (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    echo "Error: no service listening on 127.0.0.1:$PORT" >&2
    echo "Profile '$PROFILE' targets that port. Start your local service first." >&2
    exit 1
fi
exec 3>&- 3<&- 2>/dev/null || true

# ---------------------------------------------------------------------------
# Start the tunnel — exec replaces this process so Ctrl-C goes straight
# to `pipegate connect`, which handles its own reconnect loop.
# ---------------------------------------------------------------------------

echo "Profile:      $PROFILE"
echo "Target port:  $PORT (local service OK)"
echo "Starting pipegate connect ..."
echo "---"
exec "$PIPEGATE" connect "$PROFILE" "$@"
