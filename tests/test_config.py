from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from pipegate.config import (
    ConfigError,
    Profile,
    load_profile,
    to_ws_url,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    user: str | None = None,
    project: str | None = None,
) -> None:
    xdg = tmp_path / "xdg"
    xdg.mkdir()
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.chdir(tmp_path)
    if user is not None:
        _write(xdg / "pipegate" / "config.toml", user)
    if project is not None:
        _write(tmp_path / ".pipegate.toml", project)


class TestLoadProfile:
    def test_user_level_inline_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "my-app"
secret = "inline-secret"
""",
        )
        p = load_profile(None)
        assert p.name == "default"
        assert p.target == "http://localhost:3000"
        assert p.server == "https://tunnel.example.com"
        assert p.cid == "my-app"
        assert p.secret == "inline-secret"
        assert p.ttl_days is None

    def test_secret_env_resolution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MY_TUNNEL_SECRET", "from-env")
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret_env = "MY_TUNNEL_SECRET"
""",
        )
        assert load_profile().secret == "from-env"

    def test_secret_env_missing_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("UNSET_SECRET", raising=False)
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret_env = "UNSET_SECRET"
""",
        )
        with pytest.raises(ConfigError, match="UNSET_SECRET"):
            load_profile()

    def test_project_overrides_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "from-user"
secret = "s"
""",
            project="""
[profiles.default]
cid = "from-project"
""",
        )
        p = load_profile()
        assert p.cid == "from-project"
        assert p.target == "http://localhost:3000"
        assert p.secret == "s"

    def test_named_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.deerflow]
target = "http://localhost:2026"
server = "https://deerflow.example.com"
cid = "deerflow"
secret = "s"
""",
        )
        p = load_profile("deerflow")
        assert p.name == "deerflow"
        assert p.cid == "deerflow"
        assert p.target == "http://localhost:2026"

    def test_profile_only_in_project_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            project="""
[profiles.work]
target = "http://localhost:8080"
server = "http://localhost:8000"
secret = "s"
""",
        )
        p = load_profile("work")
        assert p.target == "http://localhost:8080"

    def test_missing_profile_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
""",
        )
        with pytest.raises(ConfigError, match="nonexistent"):
            load_profile("nonexistent")

    def test_no_config_files_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(tmp_path, monkeypatch)
        with pytest.raises(ConfigError, match="not found"):
            load_profile()

    def test_missing_target_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
server = "https://tunnel.example.com"
secret = "s"
""",
        )
        with pytest.raises(ConfigError, match="target"):
            load_profile()

    def test_missing_server_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
secret = "s"
""",
        )
        with pytest.raises(ConfigError, match="server"):
            load_profile()

    def test_missing_secret_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
""",
        )
        with pytest.raises(ConfigError, match="secret"):
            load_profile()

    def test_ttl_days_int(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
ttl_days = 30
""",
        )
        assert load_profile().ttl_days == 30

    def test_cid_optional(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
""",
        )
        assert load_profile().cid is None

    def test_profile_is_frozen(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
""",
        )
        p = load_profile()
        assert isinstance(p, Profile)
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.target = "x"  # type: ignore[misc]

    def test_ttl_days_zero_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
ttl_days = 0
""",
        )
        with pytest.raises(ConfigError, match="positive"):
            load_profile()

    def test_ttl_days_negative_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "s"
ttl_days = -1
""",
        )
        with pytest.raises(ConfigError, match="positive"):
            load_profile()

    def test_toml_parse_error_wrapped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="this is not valid toml === [",
        )
        with pytest.raises(ConfigError, match="Failed to parse"):
            load_profile()

    def test_secret_env_takes_precedence_over_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV_SECRET", "from-env")
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
secret = "inline"
secret_env = "ENV_SECRET"
""",
        )
        assert load_profile().secret == "from-env"

    def test_missing_secret_error_mentions_precedence(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
""",
        )
        with pytest.raises(ConfigError, match="takes precedence"):
            load_profile()


class TestToWsUrl:
    def test_https_to_wss(self) -> None:
        url = to_ws_url("https://tunnel.example.com", "JWT")
        assert url == "wss://tunnel.example.com/?token=JWT"

    def test_http_to_ws(self) -> None:
        url = to_ws_url("http://localhost:8000", "JWT")
        assert url == "ws://localhost:8000/?token=JWT"

    def test_preserves_port(self) -> None:
        url = to_ws_url("https://tunnel.example.com:8443", "JWT")
        assert url == "wss://tunnel.example.com:8443/?token=JWT"

    def test_strips_path(self) -> None:
        url = to_ws_url("https://tunnel.example.com/some/path", "JWT")
        assert url == "wss://tunnel.example.com/?token=JWT"

    def test_invalid_scheme_raises(self) -> None:
        with pytest.raises(ConfigError, match="http or https"):
            to_ws_url("ftp://example.com", "JWT")

    def test_jwt_with_special_chars_is_encoded(self) -> None:
        # JWTs are base64url (A-Za-z0-9-_.) which are all URL-safe,
        # but quote() is applied defensively for robustness.
        jwt = "abc.def.ghi"
        url = to_ws_url("https://tunnel.example.com", jwt)
        assert url == "wss://tunnel.example.com/?token=abc.def.ghi"

    def test_jwt_with_unsafe_chars_is_encoded(self) -> None:
        jwt = "ab c+d/e"
        url = to_ws_url("https://tunnel.example.com", jwt)
        # space, +, / must be percent-encoded
        assert "%20" in url
        assert "%2B" in url or "%2b" in url
        assert "%2F" in url or "%2f" in url
        assert "+" not in url.split("token=")[1]
