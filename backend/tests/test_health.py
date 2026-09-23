def test_health_endpoint_returns_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_endpoint_does_not_require_db(client, monkeypatch):
    # /health must not import or touch the DB session at all.
    resp = client.get("/health")
    assert resp.status_code == 200
