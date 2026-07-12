from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import jwt as pyjwt
import pytest
from typer.testing import CliRunner

from pipegate.cli import app

from .conftest import JWT_ALGORITHM, JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET

runner = CliRunner()

ENV = {
    "PIPEGATE_JWT_SECRET": JWT_SECRET,
    "PIPEGATE_JWT_ALGORITHMS": '["HS256"]',
}


def _find_line(output: str, prefix: str) -> str:
    return next(line for line in output.splitlines() if prefix in line)


class TestTokenCommand:
    def test_exits_zero(self) -> None:
        result = runner.invoke(app, ["token"], env=ENV)
        assert result.exit_code == 0, result.output

    def test_output_has_connection_id_and_bearer(self) -> None:
        result = runner.invoke(app, ["token"], env=ENV)
        assert "Connection-id:" in result.output
        assert "JWT Bearer:" in result.output

    def test_random_ids_differ(self) -> None:
        r1 = runner.invoke(app, ["token"], env=ENV)
        r2 = runner.invoke(app, ["token"], env=ENV)
        id1 = _find_line(r1.output, "Connection-id:")
        id2 = _find_line(r2.output, "Connection-id:")
        assert id1 != id2

    def test_pinned_via_flag(self) -> None:
        result = runner.invoke(app, ["token", "--connection-id", "flagged"], env=ENV)
        assert "Connection-id: flagged" in result.output

    def test_short_flag(self) -> None:
        result = runner.invoke(app, ["token", "-c", "short"], env=ENV)
        assert "Connection-id: short" in result.output

    def test_pinned_via_env_var(self) -> None:
        env = {**ENV, "PIPEGATE_CONNECTION_ID": "pinned"}
        result = runner.invoke(app, ["token"], env=env)
        assert "Connection-id: pinned" in result.output

    def test_flag_overrides_env_var(self) -> None:
        env = {**ENV, "PIPEGATE_CONNECTION_ID": "from-env"}
        result = runner.invoke(app, ["token", "-c", "from-flag"], env=env)
        assert "Connection-id: from-flag" in result.output

    def test_generated_token_is_verifiable(self) -> None:
        env = {**ENV, "PIPEGATE_CONNECTION_ID": "verifyme"}
        result = runner.invoke(app, ["token"], env=env)
        assert result.exit_code == 0
        raw = _find_line(result.output, "JWT Bearer:").split("JWT Bearer:")[1].strip()
        decoded = pyjwt.decode(
            raw,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
        assert decoded["sub"] == "verifyme"

    def test_missing_jwt_secret_exits_nonzero(self) -> None:
        clean = {k: v for k, v in os.environ.items() if k != "PIPEGATE_JWT_SECRET"}
        with patch.dict(os.environ, clean, clear=True):
            result = runner.invoke(app, ["token"])
        assert result.exit_code != 0


def _write_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    user: str | None = None,
    project: str | None = None,
) -> None:
    xdg = tmp_path / "xdg"
    xdg.mkdir(exist_ok=True)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.chdir(tmp_path)
    if user is not None:
        cfg = xdg / "pipegate" / "config.toml"
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_text(user)
    if project is not None:
        (tmp_path / ".pipegate.toml").write_text(project)


class TestConnectCommand:
    def test_connect_uses_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
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
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0, result.output
        mock_client.assert_awaited_once()
        args = mock_client.call_args.args
        assert args[0] == "http://localhost:3000"
        assert args[1].startswith("wss://tunnel.example.com/?token=")

    def test_connect_named_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
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
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect", "deerflow"])
        assert result.exit_code == 0, result.output
        args = mock_client.call_args.args
        assert args[0] == "http://localhost:2026"
        assert args[1].startswith("wss://deerflow.example.com/?token=")

    def test_connect_token_has_no_exp_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
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
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0, result.output
        ws_url = mock_client.call_args.args[1]
        token = ws_url.split("token=")[1]
        decoded = pyjwt.decode(
            token,
            "inline-secret",
            algorithms=["HS256"],
            audience="pipegate",
            issuer="pipegate",
            options={"verify_exp": False},
        )
        assert "exp" not in decoded
        assert decoded["sub"] == "my-app"

    def test_connect_flag_overrides_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "from-profile"
secret = "s"
""",
        )
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(
                app,
                ["connect", "--cid", "from-flag", "--target", "http://localhost:9999"],
            )
        assert result.exit_code == 0, result.output
        args = mock_client.call_args.args
        assert args[0] == "http://localhost:9999"
        token = args[1].split("token=")[1]
        decoded = pyjwt.decode(
            token,
            "s",
            algorithms=["HS256"],
            audience="pipegate",
            issuer="pipegate",
            options={"verify_exp": False},
        )
        assert decoded["sub"] == "from-flag"

    def test_connect_missing_config_exits_nonzero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(tmp_path, monkeypatch)
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code != 0
        mock_client.assert_not_called()

    def test_connect_secret_env_resolution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TUNNEL_SECRET", "env-secret")
        _write_config(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "my-app"
secret_env = "TUNNEL_SECRET"
""",
        )
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0, result.output
        token = mock_client.call_args.args[1].split("token=")[1]
        decoded = pyjwt.decode(
            token,
            "env-secret",
            algorithms=["HS256"],
            audience="pipegate",
            issuer="pipegate",
            options={"verify_exp": False},
        )
        assert decoded["sub"] == "my-app"

    def test_connect_ttl_days_from_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "my-app"
secret = "s"
ttl_days = 7
""",
        )
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0, result.output
        token = mock_client.call_args.args[1].split("token=")[1]
        decoded = pyjwt.decode(
            token,
            "s",
            algorithms=["HS256"],
            audience="pipegate",
            issuer="pipegate",
            options={"verify_exp": False},
        )
        assert "exp" in decoded

    def test_connect_reads_algorithm_from_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PIPEGATE_JWT_ALGORITHMS", '["HS512"]')
        _write_config(
            tmp_path,
            monkeypatch,
            user="""
[profiles.default]
target = "http://localhost:3000"
server = "https://tunnel.example.com"
cid = "my-app"
secret = "a-very-long-secret-that-is-long-enough-for-hs512-yes!"
""",
        )
        mock_client = AsyncMock()
        with patch("pipegate.cli.run_client", mock_client):
            result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0, result.output
        token = mock_client.call_args.args[1].split("token=")[1]
        # Header should announce HS512, not the hardcoded HS256
        header = pyjwt.get_unverified_header(token)
        assert header["alg"] == "HS512"
