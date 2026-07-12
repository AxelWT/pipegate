from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

_DEFAULT_NAME = "default"


@dataclass(frozen=True)
class Profile:
    """A resolved tunnel profile ready to connect."""

    name: str
    target: str  # local server URL, e.g. http://localhost:3000
    server: str  # server base URL, e.g. https://tunnel.example.com
    cid: str | None  # connection_id, None = random per call
    secret: str  # resolved JWT signing secret (plaintext)
    ttl_days: int | None  # None = never expires


class ConfigError(Exception):
    """Raised when a profile cannot be loaded or is incomplete."""


def _user_config_path() -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "pipegate" / "config.toml"


def _project_config_path() -> Path:
    return Path.cwd() / ".pipegate.toml"


def _load_profiles(path: Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Failed to parse {path}: {e}") from e
    raw = data.get("profiles", {})
    if not isinstance(raw, dict):
        raise ConfigError(f"[profiles] in {path} must be a table")
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def _merge_profiles(
    user: dict[str, dict[str, object]],
    project: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Project-level keys override user-level keys for the same profile."""
    merged: dict[str, dict[str, object]] = {
        name: dict(props) for name, props in user.items()
    }
    for name, props in project.items():
        merged.setdefault(name, {}).update(props)
    return merged


def _resolve_secret(props: dict[str, object], profile_name: str, src: str) -> str:
    env_name = props.get("secret_env")
    if isinstance(env_name, str):
        val = os.environ.get(env_name)
        if not val:
            raise ConfigError(
                f"profile '{profile_name}' ({src}): "
                f"environment variable '{env_name}' is not set"
            )
        return val
    inline = props.get("secret")
    if isinstance(inline, str):
        return inline
    raise ConfigError(
        f"profile '{profile_name}' ({src}): "
        "must define 'secret' or 'secret_env' "
        "('secret_env' takes precedence when both are set)"
    )


def load_profile(name: str | None = None) -> Profile:
    """Load and resolve a profile from user + project config files.

    Search order (project-level keys override user-level for the same
    profile name):

    1. ``$XDG_CONFIG_HOME/pipegate/config.toml`` (or ``~/.config/pipegate/config.toml``)
    2. ``./.pipegate.toml`` in the current working directory

    Raises ``ConfigError`` if the profile is missing or incomplete.
    """
    pname = name or _DEFAULT_NAME
    user = _load_profiles(_user_config_path())
    project = _load_profiles(_project_config_path())
    merged = _merge_profiles(user, project)

    if pname not in merged:
        raise ConfigError(
            f"profile '{pname}' not found. "
            f"Define it in {_user_config_path()} or .pipegate.toml"
        )
    props = merged[pname]

    target = props.get("target")
    if not isinstance(target, str) or not target:
        raise ConfigError(f"profile '{pname}': 'target' is required")

    server = props.get("server")
    if not isinstance(server, str) or not server:
        raise ConfigError(f"profile '{pname}': 'server' is required")

    cid = props.get("cid")
    if not (cid is None or isinstance(cid, str)):
        raise ConfigError(f"profile '{pname}': 'cid' must be a string")

    secret = _resolve_secret(props, pname, "config")

    ttl_raw = props.get("ttl_days")
    if ttl_raw is None:
        ttl_days: int | None = None
    elif isinstance(ttl_raw, int) and not isinstance(ttl_raw, bool):
        if ttl_raw <= 0:
            raise ConfigError(
                f"profile '{pname}': 'ttl_days' must be a positive integer"
            )
        ttl_days = ttl_raw
    else:
        raise ConfigError(f"profile '{pname}': 'ttl_days' must be an int")

    return Profile(
        name=pname,
        target=target,
        server=server,
        cid=cid or None,
        secret=secret,
        ttl_days=ttl_days,
    )


def to_ws_url(server: str, jwt_token: str) -> str:
    """Convert a server HTTP base URL into a WebSocket URL with the token.

    ``https://`` -> ``wss://``, ``http://`` -> ``ws://``. The path is
    replaced with ``/`` and the JWT appended as ``?token=``.
    """
    parsed = urlparse(server)
    if parsed.scheme not in ("http", "https"):
        raise ConfigError(f"server URL must use http or https (got: {parsed.scheme!r})")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    ws = parsed._replace(
        scheme=scheme, path="/", query=f"token={quote(jwt_token, safe='.')}"
    )
    return urlunparse(ws)
