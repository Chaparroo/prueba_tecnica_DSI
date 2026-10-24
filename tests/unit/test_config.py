from __future__ import annotations

from pathlib import Path

import pytest

from rpa_worker.config import ConfigError, load_settings

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config" / "config.example.yaml"

ENV = {
    "REDIS_URL": "redis://localhost:6379/0",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_DB": "rpa",
    "POSTGRES_USER": "rpa",
    "POSTGRES_PASSWORD": "s3cr'et",
}


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_example_config_is_valid() -> None:
    settings = load_settings(EXAMPLE, ENV)
    assert settings.config.worker.max_consecutive_system_exceptions == 3
    assert settings.config.worker.max_retry_number == 2
    assert settings.config.queue.stream_for("high") == "rpa:queue:high"


def test_password_never_appears_in_repr() -> None:
    settings = load_settings(EXAMPLE, ENV)
    assert "s3cr" not in repr(settings)
    assert "password='s3cr\\'et'" in settings.secrets.postgres.conninfo()


def test_redis_url_and_postgres_password_never_leak() -> None:
    env = {**ENV, "REDIS_URL": "redis://default:r3dis-pw@redis:6379/0"}
    settings = load_settings(EXAMPLE, env)
    rendered = [
        repr(settings),
        str(settings),
        repr(settings.secrets),
        settings.model_dump_json(),
        str(settings.model_dump()),
    ]
    for text in rendered:
        assert "r3dis-pw" not in text
        assert "s3cr'et" not in text
    assert settings.secrets.redis_url_for_logs() == "redis://***@redis:6379/0"
    assert "r3dis-pw" in settings.secrets.redis_url.get_secret_value()


def test_redis_url_without_credentials_is_logged_as_is() -> None:
    settings = load_settings(EXAMPLE, ENV)
    assert settings.secrets.redis_url_for_logs() == "redis://localhost:6379/0"


def test_liveness_window_shorter_than_blocking_op_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "http: {base_url: 'https://x.test'}\nhealth: {liveness_max_age_seconds: 5}\n",
    )
    with pytest.raises(ConfigError, match="liveness_max_age_seconds"):
        load_settings(path, ENV)


def test_missing_file_fails_fast(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="No existe"):
        load_settings(tmp_path / "nope.yaml", ENV)


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "http: {base_url: 'https://x.test'}\nworker: {max_retry_numbr: 2}\n")
    with pytest.raises(ConfigError, match="max_retry_numbr"):
        load_settings(path, ENV)


def test_missing_env_vars_are_listed(tmp_path: Path) -> None:
    path = _write(tmp_path, "http: {base_url: 'https://x.test'}\n")
    with pytest.raises(ConfigError, match="POSTGRES_PASSWORD"):
        load_settings(path, {"REDIS_URL": "redis://x"})


def test_grace_shorter_than_worst_case_item_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "http: {base_url: 'https://x.test'}\n"
        "worker: {stop_grace_seconds: 60}\n",  # peor caso por defecto: 69 s
    )
    with pytest.raises(ConfigError, match="stop_grace_seconds"):
        load_settings(path, ENV)


def test_lease_shorter_than_worst_case_item_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "http: {base_url: 'https://x.test'}\nqueue: {item_lease_ms: 10000}\n",
    )
    with pytest.raises(ConfigError, match="item_lease_ms"):
        load_settings(path, ENV)


def test_backoff_base_greater_than_max_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "http: {base_url: 'https://x.test', backoff: {base_seconds: 10, max_seconds: 1}}\n",
    )
    with pytest.raises(ConfigError, match="base_seconds"):
        load_settings(path, ENV)
