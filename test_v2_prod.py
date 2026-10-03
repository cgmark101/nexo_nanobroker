"""V2 prod tests: auth, idempotency, limits, DLQ, metrics."""

import importlib
import os


def _reload_with_env(monkeypatch, **env):
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    # Limpia restos de corridas previas con broker_local.db
    try:
        os.unlink("broker_local.db")
    except OSError:
        pass
    monkeypatch.setenv("NANOBROKER_DB_FILE", db_path)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import main
    importlib.reload(main)
    main.init_storage()
    from fastapi.testclient import TestClient
    return TestClient(main.app)


SAMPLE = {
    "event_id": "v2-001",
    "event_type": "test.event",
    "timestamp": "2026-10-03T00:00:00Z",
    "payload": {"a": 1},
}


def test_push_requires_auth_when_key_set(monkeypatch):
    c = _reload_with_env(monkeypatch, NANOBROKER_API_KEY="secret123")
    r = c.post("/api/v1/push/q1", json=SAMPLE)
    assert r.status_code == 401
    r2 = c.post("/api/v1/push/q1", json=SAMPLE, headers={"Authorization": "Bearer secret123"})
    assert r2.status_code == 201


def test_admin_required_for_purge(monkeypatch):
    c = _reload_with_env(monkeypatch, NANOBROKER_API_KEY="k", NANOBROKER_ADMIN_KEY="adm")
    h = {"Authorization": "Bearer k"}
    c.post("/api/v1/push/q1", json=SAMPLE, headers=h)
    r = c.delete("/api/v1/system/queue/q1/purge", headers=h)
    assert r.status_code == 403
    r2 = c.delete("/api/v1/system/queue/q1/purge", headers={"Authorization": "Bearer adm"})
    assert r2.status_code == 200


def test_idempotent_push_409(monkeypatch):
    c = _reload_with_env(monkeypatch)
    assert c.post("/api/v1/push/q1", json=SAMPLE).status_code == 201
    r2 = c.post("/api/v1/push/q1", json=SAMPLE)
    assert r2.status_code == 409


def test_peek_limit_capped(monkeypatch):
    c = _reload_with_env(monkeypatch)
    for i in range(5):
        c.post("/api/v1/push/q1", json={**SAMPLE, "event_id": f"m-{i}"})
    r = c.get("/api/v1/queue/q1/peek?limit=5000")
    assert r.status_code == 200
    assert r.json()["count"] <= 100


def test_invalid_queue_name_400(monkeypatch):
    c = _reload_with_env(monkeypatch)
    r = c.post("/api/v1/push/mala cola!", json=SAMPLE)
    assert r.status_code == 400


def test_dlq_list_and_requeue(monkeypatch):
    c = _reload_with_env(monkeypatch)
    c.post("/api/v1/push/q1?max_retries=1", json=SAMPLE)
    mid = c.post("/api/v1/queue/q1/pop").json()["id"]
    c.post(f"/api/v1/message/{mid}/nack")
    c.post(f"/api/v1/message/{mid}/nack")
    dlq = c.get("/api/v1/system/dlq")
    assert dlq.status_code == 200
    assert dlq.json()["count"] >= 1
    rid = dlq.json()["messages"][0]["id"]
    rq = c.post(f"/api/v1/system/dlq/{rid}/requeue")
    assert rq.status_code == 200
    assert rq.json()["queue"] == "q1"


def test_metrics_and_dashboard(monkeypatch):
    c = _reload_with_env(monkeypatch)
    assert c.get("/metrics").status_code == 200
    assert "nanobroker_backlog" in c.get("/metrics").text
    r = c.get("/")
    assert r.status_code == 200
    assert "NanoBroker" in r.text
