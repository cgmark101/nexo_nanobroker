"""Tests for NanoBroker (pytest + TestClient)."""

import os
import gc
import time
import tempfile
import importlib

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session", autouse=True)
def _env_setup():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    os.environ["NANOBROKER_DB_FILE"] = db_path
    os.environ["NANOBROKER_LOG_LEVEL"] = "CRITICAL"

    import main
    importlib.reload(main)
    main.init_storage()

    yield

    gc.collect()
    for path in [db_path, db_path + "-wal", db_path + "-shm"]:
        try:
            os.unlink(path)
        except (FileNotFoundError, PermissionError):
            pass


@pytest.fixture
def client():
    from main import app
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clean_db():
    yield
    from main import _db_conn
    with _db_conn() as conn:
        conn.execute("DELETE FROM message_store")


SAMPLE_MESSAGE = {
    "event_id": "msg-001",
    "event_type": "order.created",
    "timestamp": "2026-07-04T12:00:00Z",
    "payload": {"order_id": 42, "amount": 99.90},
}


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["processed_total"] == 0


def test_push_and_stats(client):
    resp = client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)
    assert resp.status_code == 201
    assert resp.json() == {"status": "ACK", "queue": "mi_cola", "event_id": "msg-001"}

    stats = client.get("/api/v1/system/stats").json()
    assert stats["metrics"]["queues"]["mi_cola"] == 1
    assert stats["metrics"]["total_backlog"] == 1


def test_pop(client):
    client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)

    resp = client.post("/api/v1/queue/mi_cola/pop")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["id"], int)
    assert body["id"] > 0
    assert body["event_id"] == "msg-001"
    assert body["event_type"] == "order.created"
    assert body["payload"]["order_id"] == 42
    assert body["retry_count"] == 1
    assert body["max_retries"] == 5

    resp2 = client.post("/api/v1/queue/mi_cola/pop")
    assert resp2.status_code == 404


def test_pop_empty(client):
    resp = client.post("/api/v1/queue/otra_cola/pop")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Queue Empty"


def test_dynamic_queue(client):
    name = "coladinamica"
    resp = client.post(f"/api/v1/push/{name}", json=SAMPLE_MESSAGE)
    assert resp.status_code == 201
    assert resp.json()["queue"] == name

    stats = client.get("/api/v1/system/stats").json()
    assert name in stats["metrics"]["queues"]
    assert stats["metrics"]["queues"][name] == 1

    popped = client.post(f"/api/v1/queue/{name}/pop")
    assert popped.status_code == 200
    assert popped.json()["event_id"] == "msg-001"


def test_fifo_ordering(client):
    for i in range(3):
        msg = {**SAMPLE_MESSAGE, "event_id": f"msg-{i:03d}"}
        client.post("/api/v1/push/mi_cola", json=msg)

    for expected in ("msg-000", "msg-001", "msg-002"):
        resp = client.post("/api/v1/queue/mi_cola/pop")
        assert resp.status_code == 200
        assert resp.json()["event_id"] == expected


def test_push_any_payload(client):
    msg = {
        "event_id": "any-id",
        "event_type": "custom.event",
        "timestamp": "2026-01-01T00:00:00Z",
        "payload": {"any": {"nested": ["data"]}, "number": 1},
    }
    resp = client.post("/api/v1/push/cola", json=msg)
    assert resp.status_code == 201

    popped = client.post("/api/v1/queue/cola/pop").json()
    assert popped["payload"]["any"]["nested"] == ["data"]


def test_purge(client):
    for i in range(5):
        msg = {**SAMPLE_MESSAGE, "event_id": f"msg-{i:03d}"}
        client.post("/api/v1/push/mi_cola", json=msg)

    resp = client.delete("/api/v1/system/queue/mi_cola/purge")
    assert resp.status_code == 200
    assert resp.json()["deleted"] == 5

    stats = client.get("/api/v1/system/stats").json()
    assert stats["metrics"]["queues"].get("mi_cola", 0) == 0


def test_purge_dynamic_queue(client):
    name = "purgeable"
    for i in range(3):
        msg = {**SAMPLE_MESSAGE, "event_id": f"d-{i:03d}"}
        client.post(f"/api/v1/push/{name}", json=msg)

    resp = client.delete(f"/api/v1/system/queue/{name}/purge")
    assert resp.status_code == 200
    assert resp.json()["deleted"] == 3


def test_stats_multiple_queues(client):
    expected = {"alpha": 2, "beta": 3, "gamma": 1}

    for q, n in expected.items():
        for i in range(n):
            msg = {**SAMPLE_MESSAGE, "event_id": f"{q}-{i:03d}"}
            client.post(f"/api/v1/push/{q}", json=msg)

    stats = client.get("/api/v1/system/stats").json()
    for q, n in expected.items():
        assert stats["metrics"]["queues"][q] == n
    assert stats["metrics"]["total_backlog"] == 6


def test_pop_by_pattern(client):
    for i in range(3):
        client.post(f"/api/v1/push/pagos.{i}", json={**SAMPLE_MESSAGE, "event_id": f"p-{i:03d}"})
    client.post("/api/v1/push/otros", json={**SAMPLE_MESSAGE, "event_id": "other"})

    for expected in ("p-000", "p-001", "p-002"):
        resp = client.post("/api/v1/queue/pop/like?pattern=pagos.%")
        assert resp.status_code == 200
        assert resp.json()["queue_name"].startswith("pagos.")
        assert resp.json()["event_id"] == expected

    resp = client.post("/api/v1/queue/pop/like?pattern=pagos.%")
    assert resp.status_code == 404

    other = client.post("/api/v1/queue/pop/like?pattern=otros")
    assert other.status_code == 200
    assert other.json()["event_id"] == "other"


def test_peek(client):
    client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)

    resp = client.get("/api/v1/queue/mi_cola/peek")
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body["id"], int)
    assert body["id"] > 0
    assert body["event_id"] == "msg-001"
    assert body["retry_count"] == 0
    assert body["max_retries"] == 5

    # Peek is non-destructive — message is still there
    resp2 = client.get("/api/v1/queue/mi_cola/peek")
    assert resp2.status_code == 200
    assert resp2.json()["event_id"] == "msg-001"


def test_peek_limit(client):
    for i in range(5):
        msg = {**SAMPLE_MESSAGE, "event_id": f"m-{i:03d}"}
        client.post("/api/v1/push/mi_cola", json=msg)

    resp = client.get("/api/v1/queue/mi_cola/peek?limit=3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 3
    assert body["messages"][0]["event_id"] == "m-000"
    assert body["messages"][2]["event_id"] == "m-002"


def test_peek_empty(client):
    resp = client.get("/api/v1/queue/noexiste/peek")
    assert resp.status_code == 404


def test_count(client):
    for i in range(4):
        msg = {**SAMPLE_MESSAGE, "event_id": f"msg-{i:03d}"}
        client.post("/api/v1/push/mi_cola", json=msg)

    resp = client.get("/api/v1/queue/mi_cola/count")
    assert resp.status_code == 200
    assert resp.json()["count"] == 4


def test_count_zero(client):
    resp = client.get("/api/v1/queue/vacia/count")
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_ack(client):
    client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)
    pop_resp = client.post("/api/v1/queue/mi_cola/pop")
    msg_id = pop_resp.json()["id"]

    ack_resp = client.post(f"/api/v1/message/{msg_id}/ack")
    assert ack_resp.status_code == 200
    assert ack_resp.json()["status"] == "ACK"

    ack_resp2 = client.post(f"/api/v1/message/{msg_id}/ack")
    assert ack_resp2.status_code == 404
    assert ack_resp2.json()["detail"] == "Message Not Found"


def test_nack_retry(client):
    client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)

    pop1 = client.post("/api/v1/queue/mi_cola/pop")
    msg_id = pop1.json()["id"]
    assert pop1.json()["retry_count"] == 1

    nack = client.post(f"/api/v1/message/{msg_id}/nack")
    assert nack.status_code == 200
    assert nack.json()["status"] == "NACK"

    pop2 = client.post("/api/v1/queue/mi_cola/pop")
    assert pop2.status_code == 200
    assert pop2.json()["id"] == msg_id
    assert pop2.json()["retry_count"] == 2


def test_nack_poison(client):
    client.post("/api/v1/push/mi_cola?max_retries=2", json=SAMPLE_MESSAGE)

    pop1 = client.post("/api/v1/queue/mi_cola/pop")
    msg_id = pop1.json()["id"]
    client.post(f"/api/v1/message/{msg_id}/nack")

    pop2 = client.post("/api/v1/queue/mi_cola/pop")
    msg_id2 = pop2.json()["id"]
    assert msg_id2 == msg_id
    assert pop2.json()["retry_count"] == 2

    poison = client.post(f"/api/v1/message/{msg_id}/nack")
    assert poison.status_code == 200
    assert poison.json()["status"] == "POISON"
    assert poison.json()["poison_queue"] == "failed_mi_cola"
    assert poison.json()["retry_count"] == 2

    pop3 = client.post("/api/v1/queue/mi_cola/pop")
    assert pop3.status_code == 404

    failed = client.post("/api/v1/queue/pop/like?pattern=failed_%")
    assert failed.status_code == 200
    assert failed.json()["queue_name"] == "failed_mi_cola"
    assert failed.json()["event_id"] == "msg-001"


def test_nack_not_found(client):
    resp = client.post("/api/v1/message/9999/nack")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Message Not Found"


def test_expires_at_skip_expired(client):
    expired_msg = {**SAMPLE_MESSAGE, "event_id": "expired"}
    valid_msg = {**SAMPLE_MESSAGE, "event_id": "valid"}

    client.post("/api/v1/push/mi_cola?ttl=-1", json=expired_msg)
    client.post("/api/v1/push/mi_cola", json=valid_msg)

    popped = client.post("/api/v1/queue/mi_cola/pop")
    assert popped.status_code == 200
    assert popped.json()["event_id"] == "valid"


def test_expires_at_all_expired(client):
    for i in range(3):
        msg = {**SAMPLE_MESSAGE, "event_id": f"e-{i:03d}"}
        client.post("/api/v1/push/mi_cola?ttl=-1", json=msg)

    resp = client.post("/api/v1/queue/mi_cola/pop")
    assert resp.status_code == 404


def test_ack_counter(client):
    health_before = client.get("/health").json()
    total_before = health_before["processed_total"]

    ids = []
    for i in range(3):
        msg = {**SAMPLE_MESSAGE, "event_id": f"c-{i:03d}"}
        client.post("/api/v1/push/mi_cola", json=msg)

    for _ in range(3):
        resp = client.post("/api/v1/queue/mi_cola/pop")
        ids.append(resp.json()["id"])

    health_mid = client.get("/health").json()
    assert health_mid["processed_total"] == total_before

    for mid in ids:
        client.post(f"/api/v1/message/{mid}/ack")

    health_after = client.get("/health").json()
    assert health_after["processed_total"] == total_before + 3


def test_pop_by_pattern_excludes_failed_by_default(client):
    client.post("/api/v1/push/mi_cola", json=SAMPLE_MESSAGE)

    pop1 = client.post("/api/v1/queue/mi_cola/pop")
    msg_id = pop1.json()["id"]
    client.post(f"/api/v1/message/{msg_id}/nack?max_retries=0")

    # Move to failed by setting max_retries=0 so it goes directly to poison
    actual_msg = {**SAMPLE_MESSAGE, "event_id": "fresh"}
    client.post("/api/v1/push/mi_cola", json=actual_msg)
    pop2 = client.post("/api/v1/queue/mi_cola/pop")
    client.post(f"/api/v1/message/{pop2.json()['id']}/ack")

    result = client.post("/api/v1/queue/pop/like?pattern=%")
    assert result.status_code == 200
    assert result.json()["event_id"] == "fresh"


def test_pop_by_pattern_can_target_failed(client):
    client.post("/api/v1/push/mi_cola?max_retries=1", json=SAMPLE_MESSAGE)

    pop = client.post("/api/v1/queue/mi_cola/pop")
    client.post(f"/api/v1/message/{pop.json()['id']}/nack")
    client.post(f"/api/v1/message/{pop.json()['id']}/nack")

    failed = client.post("/api/v1/queue/pop/like?pattern=failed_%")
    assert failed.status_code == 200
    assert failed.json()["queue_name"] == "failed_mi_cola"
    assert failed.json()["event_id"] == "msg-001"


def test_list_queues(client):
    client.post("/api/v1/push/alpha", json=SAMPLE_MESSAGE)
    client.post("/api/v1/push/beta", json=SAMPLE_MESSAGE)
    client.post("/api/v1/push/beta", json=SAMPLE_MESSAGE)

    resp = client.get("/api/v1/system/queues")
    assert resp.status_code == 200
    queues = {q["name"]: q["pending"] for q in resp.json()["queues"]}
    assert queues["alpha"] == 1
    assert queues["beta"] == 2


def test_missing_fields(client):
    resp = client.post("/api/v1/push/cola", json={"incompleto": True})
    assert resp.status_code == 422


def test_wrong_types(client):
    resp = client.post(
        "/api/v1/push/cola",
        json={**SAMPLE_MESSAGE, "event_id": 12345},
    )
    assert resp.status_code == 422
