# Tunnel Scripts

## start-tunnel.sh

Starts a pipegate tunnel client connecting to a remote server. Generates a
JWT token locally (using the same secret as the server) and connects via
WebSocket — no manual `pipegate token` + `pipegate client` two-step needed.

### Prerequisites

- Run `uv sync` in the repo root first (creates `.venv` with the `pipegate` CLI)
- Local service must be running on the target port
- The remote server's `PIPEGATE_JWT_SECRET` must match the `--secret` you pass

### Usage

```bash
./scripts/start-tunnel.sh --secret S --cid C --port P --domain D [--no-wss]
```

| Flag | Required | Description |
|---|---|---|
| `--secret S` | yes | JWT secret (must match server's `PIPEGATE_JWT_SECRET`) |
| `--cid C` | yes | connection_id, e.g. `deerflow` |
| `--port P` | yes | local service port, e.g. `2026` |
| `--domain D` | yes | server domain, e.g. `deerflow.axello.cn` |
| `--no-wss` | no | Use `ws://` instead of `wss://` (HTTP-only setups) |
| `-h, --help` | no | Show help |

### Example

```bash
./scripts/start-tunnel.sh \
  --secret my-secret \
  --cid deerflow \
  --port 2026 \
  --domain deerflow.axello.cn
```

### How it works

1. Validates args and checks the local port is listening.
2. Exports `PIPEGATE_JWT_SECRET` / `PIPEGATE_JWT_ALGORITHMS`.
3. Calls `pipegate token -c <cid>` and parses the JWT from the output.
4. Builds the WebSocket URL: `wss://<domain>/?token=<jwt>`.
5. `exec`s `pipegate client http://localhost:<port> <ws_url>` — replaces the
   shell process so Ctrl-C is delivered directly to the client.

### Security

- `--secret` appears in the process list and shell history. In shared
  environments prefer a private shell or wipe history afterwards.
- Never commit a config file containing your secret. `.tunnel.conf` is in
  `.gitignore` as a defensive measure.
- `pipegate client` handles its own reconnection (exponential backoff,
  1s → 60s), so this script does not wrap it in a restart loop.
