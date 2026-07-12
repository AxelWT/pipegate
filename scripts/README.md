# Tunnel Scripts

## start-tunnel-docker.sh

Manages two pipegate tunnel clients running in Docker (via
`docker-compose.client.yml`), so tunnels survive terminal close and
auto-restart on crash / Docker Desktop launch. Use this for long-running
tunnels; use `start-tunnel.sh` below for a foreground one-shot.

### Prerequisites

- `./.env` with `PIPEGATE_JWT_SECRET` (copy from `.env.example`)
- `./.pipegate.toml` with `[profiles.app1]` / `[profiles.app2]`
  (copy from `.pipegate.toml.example`)
- `docker-compose.client.yml` has `<PORT1>` / `<PORT2>` filled in
- Server runs in subdomain mode (`PIPEGATE_BASE_DOMAIN` set)

### Usage

```bash
./scripts/start-tunnel-docker.sh build     # build pipegate:local (once)
./scripts/start-tunnel-docker.sh up        # start both tunnels (detached)
./scripts/start-tunnel-docker.sh logs      # follow logs (Ctrl-C to detach)
./scripts/start-tunnel-docker.sh status    # show container state
./scripts/start-tunnel-docker.sh restart   # restart both clients
./scripts/start-tunnel-docker.sh down      # stop and remove containers
```

### Environment

- `IMAGE` — override the image tag (default: `pipegate:local`). Set in
  `.env` to use a remote registry image and skip `build`.

### How it works

Thin wrapper around `docker compose -f docker-compose.client.yml`. Each
subcommand maps to the corresponding compose verb. `up` aborts with a
clear error if `<PORT1>` / `<PORT2>` placeholders in the compose file
have not been replaced.

## start-tunnel.sh

Thin wrapper around `pipegate connect` that adds two conveniences: it
auto-loads `./.env` (so `PIPEGATE_JWT_SECRET` is set without manual
export) and pre-flight checks the local service port is listening —
failing fast with a clear message instead of a 504 later. The JWT is
signed by `pipegate connect` internally; the script never handles tokens
or WebSocket URLs directly.

### Prerequisites

- Run `uv sync` in the repo root first (creates `.venv` with the `pipegate` CLI)
- A profile defined in `~/.config/pipegate/config.toml` or `./.pipegate.toml`
  (copy from `.pipegate.toml.example`)
- `./.env` with `PIPEGATE_JWT_SECRET` (copy from `.env.example`), or the
  secret exported in your shell
- Local service running on the profile's target port

### Usage

```bash
./scripts/start-tunnel.sh                          # default profile
./scripts/start-tunnel.sh app1                     # named profile
./scripts/start-tunnel.sh app1 --target http://localhost:4000   # pass-through
./scripts/start-tunnel.sh -h|--help
```

| Argument | Required | Description |
|---|---|---|
| `PROFILE` | no | Profile name (default: `default`) |
| `CONNECT_FLAGS` | no | Extra flags passed through to `pipegate connect` (`--target`, `--server`, `--cid`, `--secret`) |
| `-h`, `--help` | no | Show help |

### How it works

1. Resolves the profile's target port via the real `pipegate` config
   loader (`load_profile`). This validates the profile exists, `target`
   is set, and `secret_env` points to a populated variable — all before
   attempting a connection.
2. Checks the local service is listening on that port (`/dev/tcp`).
3. Auto-loads `./.env` if present (injects `PIPEGATE_JWT_SECRET`).
4. `exec`s `pipegate connect <profile> [flags]` — replaces the shell
   process so Ctrl-C is delivered directly to the client.

### Security

- The secret is read from `./.env` or the environment — it never appears
  in the process arguments or shell history (unlike the old `--secret`
  flag).
- Never commit a config file containing an inline `secret`. `.pipegate.toml`
  is in `.gitignore` as a defensive measure.
- `pipegate connect` handles its own reconnection (exponential backoff,
  1s → 60s), so this script does not wrap it in a restart loop.
