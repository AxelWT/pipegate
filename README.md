# PipeGate

**Self-hosted HTTP tunnel -- poor man's ngrok.**

Expose a local server to the internet through a single WebSocket. No accounts, no cloud dependencies, no daemon -- just a server on your VPS and a client next to your app. ~400 lines of Python.

```
[Any HTTP client] ---> https://yourserver/a1b2c3/api/data
                              |
                        PipeGate Server
                              |  (WebSocket)
                        PipeGate Client
                              |
                        http://localhost:3000/api/data
```

## Quick Start

```bash
git clone https://github.com/janbjorge/pipegate.git && cd pipegate
uv sync

export PIPEGATE_JWT_SECRET="change-me-to-something-secret"
export PIPEGATE_JWT_ALGORITHMS='["HS256"]'

# Run the server (on your public VPS)
pipegate server

# Run the client (on your local machine)
pipegate client http://localhost:3000 "ws://yourserver:8000/?token=<jwt>"
```

Requests to `http://yourserver:8000/a1b2c3d4/anything` now reach `http://localhost:3000/anything`.

For one-command tunnels without copy-pasting JWTs, use `pipegate connect`
with a profile — see [Profiles](#profiles) below.

## CLI

```
pipegate token [-c ID]              Generate a JWT bearer token
pipegate client TARGET_URL WS_URL   Start the tunnel client
pipegate connect [PROFILE]          Start the tunnel via a config profile (recommended)
pipegate server [--host H] [-p N]   Start the server (default: 0.0.0.0:8000)
```

## How It Works

A caller hits the server at `/{connection_id}/{path}`. The server wraps the request into a JSON message (method, path, headers, base64-encoded body) tagged with a `correlation_id` (UUID4), and pushes it into an in-memory `asyncio.Queue` for that connection. A background task drains the queue over the WebSocket to the tunnel client.

The client receives the message, makes a real HTTP request to your local service, and sends back a response message with the same `correlation_id`. The server matches it to the waiting `asyncio.Future` and returns the response to the original caller.

Multiple requests fly concurrently over one WebSocket -- the correlation ID is what ties each request to its response. Bodies are base64-encoded so binary payloads survive the JSON text frames.

Tunnel connections are JWT-authenticated: the token carries the connection ID as its `sub` claim, signed with the shared `PIPEGATE_JWT_SECRET`. External HTTP callers don't need the JWT — only the WebSocket upgrade does; the server rejects invalid tokens with close code 1008.

### What happens when things go wrong

| Situation | What PipeGate does |
|---|---|
| Client is slow / not connected | Queue fills up, caller gets **503** |
| Request body too large | Rejected immediately with **413** |
| Client disconnects mid-request | Pending future fails with **502** |
| No response within 5 minutes | Caller gets **504** |
| Server shuts down | All pending futures resolve with **504** (no hanging requests) |
| WebSocket drops | Client reconnects automatically (exponential backoff, 1s to 60s) |
| Client can't reach local service | Returns **504** to server, which forwards it to caller |

## Authentication

The server and the token generator share `PIPEGATE_JWT_SECRET`. Generate a token with `pipegate token` (optionally pin a connection ID via `--connection-id` or `PIPEGATE_CONNECTION_ID`), then pass it to the client as `?token=<jwt>`. Invalid, expired, or missing tokens are rejected at the WebSocket upgrade with close code 1008.

## Configuration

Environment variables via pydantic-settings:

| Variable | Required | Default | Description |
|---|---|---|---|
| `PIPEGATE_JWT_SECRET` | Yes | -- | Shared secret for JWT signing/verification |
| `PIPEGATE_JWT_ALGORITHMS` | No | `["HS256"]` | Algorithm list, e.g. `'["HS256"]'` |
| `PIPEGATE_JWT_ISSUER` | No | `pipegate` | JWT `iss` claim — must match on both sides |
| `PIPEGATE_JWT_AUDIENCE` | No | `pipegate` | JWT `aud` claim — must match on both sides |
| `PIPEGATE_JWT_TTL_DAYS` | No | `None` (never expires) | Token lifetime in days; unset = never expires |
| `PIPEGATE_CONNECTION_ID` | No | random UUID | Pin a connection ID when generating tokens (flag `--connection-id` takes precedence) |
| `PIPEGATE_MAX_BODY_BYTES` | No | 10 MB | Reject requests larger than this (413) |
| `PIPEGATE_MAX_QUEUE_DEPTH` | No | 100 | Per-tunnel queue size before returning 503 |
| `PIPEGATE_BASE_DOMAIN` | No | -- | Enable subdomain routing (see below) |

## Profiles

`pipegate connect` reads profiles from TOML config files. Two locations are
searched and merged (project-level keys override user-level for the same
profile name):

1. `~/.config/pipegate/config.toml` (or `$XDG_CONFIG_HOME/pipegate/config.toml`)
2. `./.pipegate.toml` in the current working directory

### Schema

```toml
[profiles.<name>]
target     = "http://localhost:3000"   # required: local server to forward to
server     = "https://tunnel.example.com"  # required: server base URL
cid        = "my-app"                  # optional: pin connection_id (random if omitted)
secret     = "inline-secret"           # one of secret / secret_env is required
secret_env = "PIPEGATE_JWT_SECRET"     #   takes precedence over 'secret' when both set
ttl_days   = 30                        # optional: token lifetime, must be > 0 (omit = never expires)
```

`server`'s scheme decides the WebSocket scheme: `https://` → `wss://`,
`http://` → `ws://`. The JWT is appended as `?token=…` automatically.

### Example

```toml
# ~/.config/pipegate/config.toml
[profiles.default]
target     = "http://localhost:3000"
server     = "https://tunnel.example.com"
cid        = "my-app"
secret_env = "PIPEGATE_JWT_SECRET"

[profiles.app2]
target   = "http://localhost:3001"
server   = "https://app2.example.com"
cid      = "app2"
secret_env = "PIPEGATE_JWT_SECRET"
```

```bash
pipegate connect              # → "default" profile
pipegate connect app2         # → "app2" profile
pipegate connect --cid temp   # override cid for this run only
```

**Security:** prefer `secret_env` over inline `secret` — it keeps the secret
out of the config file. Never commit a config containing an inline `secret`.

## Subdomain Routing

By default, PipeGate routes by path prefix: `http://server/{connection_id}/{path}`. This breaks frontends whose HTML references absolute asset paths like `/static/main.js` — the prefix is lost and the asset 404s. (Seeing static-asset 404s on a proxied frontend? This is the fix.)

Set `PIPEGATE_BASE_DOMAIN` to route by **subdomain** instead. The connection_id is taken from the leftmost label of the `Host` header, and the full path is forwarded as-is — so absolute paths work without any frontend changes:

```
export PIPEGATE_BASE_DOMAIN="tunnel.example.com"
pipegate server

# Pin a connection id, then access via subdomain:
#   http://myapp.tunnel.example.com/            -> forwards /         to myapp
#   http://myapp.tunnel.example.com/static/main.js -> forwards /static/main.js (works!)
```

This requires wildcard DNS (`*.tunnel.example.com -> your server IP`) and, for HTTPS, a wildcard TLS certificate. Path-based routing still works when `PIPEGATE_BASE_DOMAIN` is unset — the two modes are mutually exclusive per deployment.

## Multi-tunnel Client in Docker

For long-lived tunnels that survive terminal close and auto-restart on
reboot, run the client(s) in Docker. One tunnel is a strict 1:1:1 binding
(one `cid` → one WebSocket → one `target`), so tunneling two services
needs two client processes — `docker-compose.client.yml` runs them as two
independent services sharing one image and one config file.

### Prerequisites

```bash
cp .env.example .env                              # fill in PIPEGATE_JWT_SECRET
cp .pipegate.toml.example .pipegate.toml          # fill in server URL + ports
docker build -t pipegate:local .                  # once (or set IMAGE in .env)
# edit docker-compose.client.yml: replace <PORT1>/<PORT2> with your host ports
```

Server must run in **subdomain mode** (`PIPEGATE_BASE_DOMAIN` set) with
wildcard DNS + TLS, so `app1.<base>` and `app2.<base>` resolve.

### Usage

```bash
scripts/start-tunnel-docker.sh build && up && logs   # wrapper
docker compose -f docker-compose.client.yml up -d    # or raw compose
```

Public URLs: `https://app1.<base_domain>/` and `https://app2.<base_domain>/`.
The container reaches host services via `host.docker.internal`; see
`scripts/README.md` and the compose file header for details. To add a
third tunnel, copy a service block and add a matching `[profiles.app3]`.

## Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/healthz` | None | Returns `{"status": "ok"}` |
| `*` | `/{connection_id}/{path}` | None | Tunnel passthrough (path mode, default) |
| `*` | `/{path}` | None | Tunnel passthrough (subdomain mode, Host: `{cid}.{base_domain}`) |
| `WS` | `/?token=<jwt>` | JWT | Tunnel client connection |

## Design Notes

**No external state.** The entire coordination layer is `dict[str, asyncio.Queue]` for pending requests and `dict[UUID, asyncio.Future]` for pending responses. This makes PipeGate trivially deployable (single process, no Redis/database), but means it doesn't survive server restarts and doesn't scale horizontally. That's fine for the intended use case.

**Closure-based app factory.** `create_app()` captures all mutable state in a closure rather than using global variables. Each call gets completely fresh state, which makes tests fully isolated without any cleanup fixtures.

**The server injects `x-pipegate-correlation-id`** into forwarded request headers. Your local service can log this to correlate requests end-to-end through the tunnel.

**Query parameters are preserved faithfully** -- including duplicate keys and ordering -- by serializing `multi_items()` as `[[key, value], ...]` rather than collapsing into a dict.

## Development

```bash
uv run pytest tests/ -v             # tests
uv run ruff check . && uv run ruff format --check .  # lint
uv run mypy pipegate/ tests/        # typecheck (strict mode)
```

CI runs lint, typecheck, and tests on Python 3.12 and 3.13.

## License

[MIT](LICENSE)
