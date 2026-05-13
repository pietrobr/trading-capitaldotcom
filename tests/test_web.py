from fastapi.testclient import TestClient

from src.web.server import ACCESS_COOKIE, ACCESS_TOKEN, create_app


def _client(app):
    c = TestClient(app)
    c.cookies.set(ACCESS_COOKIE, ACCESS_TOKEN)
    return c


def test_server_starts_without_credentials(monkeypatch, tmp_path):
    # Force credentials to empty (load_dotenv won't override existing env vars)
    for k in ("CAPITAL_API_KEY", "CAPITAL_IDENTIFIER", "CAPITAL_PASSWORD"):
        monkeypatch.setenv(k, "")
    # Use the real config.yaml from repo root
    app = create_app("config.yaml")
    client = _client(app)

    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["ready"] is False
    assert body["credentials_ok"] is False
    assert body["logged_in"] is False

    r = client.get("/")
    assert r.status_code == 200
    assert "Capital.com Trading Bot" in r.text


def test_server_starts_without_config(monkeypatch, tmp_path):
    for k in ("CAPITAL_API_KEY", "CAPITAL_IDENTIFIER", "CAPITAL_PASSWORD"):
        monkeypatch.setenv(k, "")
    missing = tmp_path / "does_not_exist.yaml"
    app = create_app(str(missing))
    client = _client(app)

    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["ready"] is False
    assert body["config_error"] is not None
    assert body["instruments"] == []
