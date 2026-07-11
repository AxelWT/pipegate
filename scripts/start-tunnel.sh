#!/usr/bin/env bash
#
# start-tunnel.sh — Start a pipegate tunnel client connecting to a remote server.
#
# Prerequisites:
#   - Run `uv sync` in repo root first (creates .venv with pipegate CLI)
#   - Local service must be running on the target port
#
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/start-tunnel.sh --secret S --cid C --port P --domain D [--no-wss]

Required:
  --secret S    JWT secret (must match server's PIPEGATE_JWT_SECRET)
  --cid C       connection_id, e.g. deerflow
  --port P      local service port, e.g. 2026
  --domain D    server domain, e.g. deerflow.axello.cn

Optional:
  --no-wss      Use ws:// instead of wss:// (for HTTP-only setups)
  -h, --help    Show this help

Example:
  scripts/start-tunnel.sh \
    --secret my-secret \
    --cid deerflow \
    --port 2026 \
    --domain deerflow.axello.cn

Security note:
  --secret appears in the process list and shell history. In shared
  environments prefer running in a private shell or wiping history after.
EOF
}

# ---------------------------------------------------------------------------
# Parse args
# ---------------------------------------------------------------------------

SECRET=""
CID=""
PORT=""
DOMAIN=""
NO_WSS=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --secret)  SECRET="$2";  shift 2 ;;
        --cid)     CID="$2";     shift 2 ;;
        --port)    PORT="$2";    shift 2 ;;
        --domain)  DOMAIN="$2";  shift 2 ;;
        --no-wss)  NO_WSS=1;     shift ;;
        -h|--help) usage;        exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate required args
# ---------------------------------------------------------------------------

missing=()
[[ -z "$SECRET"  ]] && missing+=(--secret)
[[ -z "$CID"     ]] && missing+=(--cid)
[[ -z "$PORT"    ]] && missing+=(--port)
[[ -z "$DOMAIN"  ]] && missing+=(--domain)
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: missing required argument(s): ${missing[*]}" >&2
    echo >&2
    usage >&2
    exit 2
fi

# Validate port is numeric
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [[ "$PORT" -lt 1 || "$PORT" -gt 65535 ]]; then
    echo "Error: --port must be a number in 1..65535 (got: $PORT)" >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# Locate repo root and pipegate CLI
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PIPEGATE="$REPO_ROOT/.venv/bin/pipegate"

if [[ ! -x "$PIPEGATE" ]]; then
    echo "Error: pipegate CLI not found at $PIPEGATE" >&2
    echo "Run 'uv sync' in the repo root first." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Pre-flight: local service must be listening on the port
# ---------------------------------------------------------------------------

if ! (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    echo "Error: no service listening on 127.0.0.1:$PORT" >&2
    echo "Start your local service first." >&2
    exit 1
fi
exec 3>&- 3<&- 2>/dev/null || true

# ---------------------------------------------------------------------------
# Generate token
# ---------------------------------------------------------------------------

export PIPEGATE_JWT_SECRET="$SECRET"
export PIPEGATE_JWT_ALGORITHMS='["HS256"]'

echo "Generating token for connection_id=$CID ..."
TOKEN_OUTPUT="$("$PIPEGATE" token -c "$CID")"
JWT="$(echo "$TOKEN_OUTPUT" | grep '^JWT Bearer:' | sed 's/^JWT Bearer:[[:space:]]*//')"

if [[ -z "$JWT" ]]; then
    echo "Error: failed to parse JWT from token output." >&2
    echo "--- token output ---" >&2
    echo "$TOKEN_OUTPUT" >&2
    exit 1
fi

echo "Token generated (connection_id=$CID)"

# ---------------------------------------------------------------------------
# Build WS URL and start client
# ---------------------------------------------------------------------------

SCHEME="wss"
[[ "$NO_WSS" == "1" ]] && SCHEME="ws"
WS_URL="${SCHEME}://${DOMAIN}/?token=${JWT}"

echo "Connecting to ${SCHEME}://${DOMAIN}/ ..."
echo "---"
# exec replaces this process with pipegate client so Ctrl-C is delivered
# directly to the client (pipegate client handles its own reconnect loop).
exec "$PIPEGATE" client "http://localhost:${PORT}" "$WS_URL"
