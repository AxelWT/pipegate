#!/usr/bin/env bash
#
# start-tunnel-docker.sh — Manage two pipegate tunnel clients in Docker.
#
# Wraps `docker compose -f docker-compose.client.yml` so you don't have to
# remember the file name or the build/up/logs/down incantations. Reads the
# IMAGE override from .env (or the environment) so a remote registry image
# skips the local build step.
#
# Usage:
#   scripts/start-tunnel-docker.sh build     # build pipegate:local (once)
#   scripts/start-tunnel-docker.sh up        # start both tunnels (detached)
#   scripts/start-tunnel-docker.sh logs      # follow logs (Ctrl-C to detach)
#   scripts/start-tunnel-docker.sh status    # show container state
#   scripts/start-tunnel-docker.sh restart   # restart both clients
#   scripts/start-tunnel-docker.sh down      # stop and remove containers
#   scripts/start-tunnel-docker.sh -h|--help
#
# Prerequisites:
#   - ./.env contains PIPEGATE_JWT_SECRET (and optional IMAGE override)
#   - ~/.config/pipegate/config.toml has [profiles.app1] and [profiles.app2]
#   - docker-compose.client.yml has <PORT1>/<PORT2> filled in
#   - server side runs in subdomain mode (PIPEGATE_BASE_DOMAIN set)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$REPO_ROOT/docker-compose.client.yml"

usage() {
    cat <<'EOF'
Usage: scripts/start-tunnel-docker.sh <command>

Commands:
  build     Build the pipegate:local image (run once, or after code changes)
  up        Start both tunnel clients in detached mode
  logs      Follow logs from both clients (Ctrl-C to detach)
  status    Show container state (ps)
  restart   Restart both clients
  down      Stop and remove containers (keeps the image)
  -h|--help Show this help

Environment:
  IMAGE   Override the image tag (default: pipegate:local).
          Set in .env to use a remote registry image and skip `build`.

Examples:
  scripts/start-tunnel-docker.sh build
  scripts/start-tunnel-docker.sh up
  scripts/start-tunnel-docker.sh logs
EOF
}

run_compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

# Abort if the user forgot to replace <PORT1>/<PORT2> placeholders in the
# compose file. Connecting to http://host.docker.internal:<PORT1> would
# fail with a confusing error instead of a clear "edit the file" message.
# Note: comment lines are stripped first — the header documents the
# placeholders as <PORT1>/<PORT2>, which must NOT trip the guard.
guard_placeholders() {
    if grep -vE '^[[:space:]]*#' "$COMPOSE_FILE" | grep -qE '<PORT[0-9]+>'; then
        echo "Error: placeholder(s) <PORT1>/<PORT2> still present in" >&2
        echo "  $COMPOSE_FILE" >&2
        echo "Edit it and replace them with your services' host ports." >&2
        exit 2
    fi
}

cmd="${1:-}"
case "$cmd" in
    build)
        echo "Building pipegate:local ..."
        docker build -t pipegate:local "$REPO_ROOT"
        ;;
    up)
        guard_placeholders
        run_compose up -d
        echo
        echo "Tunnels started. Follow logs with:"
        echo "  scripts/start-tunnel-docker.sh logs"
        ;;
    logs)
        run_compose logs -f
        ;;
    status)
        run_compose ps
        ;;
    restart)
        guard_placeholders
        run_compose restart
        ;;
    down)
        run_compose down
        ;;
    -h|--help|"")
        usage
        [[ -z "$cmd" ]] && exit 2 || exit 0
        ;;
    *)
        echo "Unknown command: $cmd" >&2
        echo >&2
        usage >&2
        exit 2
        ;;
esac
