"""NanoBroker — FIFO message broker on SQLite WAL with ack/nack semantics."""

import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_FILE = os.getenv("NANOBROKER_DB_FILE", "broker_local.db")
HOST = os.getenv("NANOBROKER_HOST", "0.0.0.0")
PORT = int(os.getenv("NANOBROKER_PORT", "8000"))
LOG_LEVEL = os.getenv("NANOBROKER_LOG_LEVEL", "INFO").upper()
DB_TIMEOUT = int(os.getenv("NANOBROKER_DB_TIMEOUT", "1"))
JANITOR_INTERVAL_SEC = int(os.getenv("NANOBROKER_JANITOR_INTERVAL_SEC", "30"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("NanoBroker")

_processed_total = 0

# ---------------------------------------------------------------------------
# Storage engine
# ---------------------------------------------------------------------------
def _wal_checkpoint():
    try:
        with sqlite3.connect(DB_FILE, timeout=DB_TIMEOUT) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            logger.info("WAL checkpoint complete")
    except Exception as exc:
        logger.warning("WAL checkpoint failed: %s", exc)

def init_storage():
    with sqlite3.connect(DB_FILE, timeout=DB_TIMEOUT) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=%d;" % (DB_TIMEOUT * 1000))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS message_store (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 5,
                expires_at REAL DEFAULT NULL,
                visible_after REAL NOT NULL DEFAULT 0.0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_fifo
            ON message_store(queue_name, id ASC)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_fifo_v2
            ON message_store(queue_name, visible_after, expires_at, id ASC)
        """)

        # Migration: add columns for databases created before v2
        for col, col_type in [
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("max_retries", "INTEGER NOT NULL DEFAULT 5"),
            ("expires_at", "REAL DEFAULT NULL"),
            ("visible_after", "REAL NOT NULL DEFAULT 0.0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE message_store ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass

    logger.info("Storage engine ready (WAL mode)")

async def janitor_loop():
    """Background maintenance: purge expired messages, passive WAL checkpoint."""
    logger.info("Janitor service started (interval: %ds)", JANITOR_INTERVAL_SEC)
    while True:
        try:
            await asyncio.sleep(JANITOR_INTERVAL_SEC)
            now = time.time()
            with _db_conn() as conn:
                cursor = conn.execute(
                    "DELETE FROM message_store WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,),
                )
                if cursor.rowcount > 0:
                    logger.info("Janitor: purged %d expired messages", cursor.rowcount)
                conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
        except asyncio.CancelledError:
            logger.info("Janitor service stopping...")
            break
        except Exception as exc:
            logger.error("Janitor execution error: %s", exc)

@asynccontextmanager
async def lifespan(app):
    init_storage()
    janitor_task = asyncio.create_task(janitor_loop())
    yield
    janitor_task.cancel()
    await asyncio.gather(janitor_task, return_exceptions=True)
    _wal_checkpoint()

app = FastAPI(
    title="NanoBroker",
    description="Universal HTTP message broker with FIFO queues on SQLite WAL.",
    version="2.0.0",
    lifespan=lifespan,
    contact={
        "name": "Github Repo",
        "url": "https://github.com/cgmark101/nexo_nanobroker"
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/license/mit/"
    }
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
class EventEnvelope(BaseModel):
    event_id: str
    event_type: str
    timestamp: str
    payload: dict[str, Any]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _db_conn():
    conn = sqlite3.connect(DB_FILE, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=%d;" % (DB_TIMEOUT * 1000))
    return conn

def _db_integrity() -> Optional[str]:
    try:
        with _db_conn() as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
            result = row[0] if row else "unknown"
            return None if result == "ok" else result
    except Exception as exc:
        return str(exc)

_POP_SELECT = "id, queue_name, event_id, event_type, timestamp, payload, retry_count, max_retries"

# ---------------------------------------------------------------------------
# Push endpoint
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/push/{queue_name}",
    status_code=status.HTTP_201_CREATED,
    summary="Push a message into a queue (creates the queue if it does not exist)",
)
def push_router(
    queue_name: str,
    envelope: EventEnvelope,
    max_retries: int = 5,
    ttl: Optional[int] = None,
):
    expires_at = (time.time() + ttl) if ttl is not None else None
    try:
        with _db_conn() as conn:
            conn.execute(
                "INSERT INTO message_store (queue_name, event_id, event_type, timestamp, payload, max_retries, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    queue_name,
                    envelope.event_id,
                    envelope.event_type,
                    envelope.timestamp,
                    json.dumps(envelope.payload),
                    max_retries,
                    expires_at,
                ),
            )
        return {"status": "ACK", "queue": queue_name, "event_id": envelope.event_id}
    except Exception as exc:
        logger.error("Push failed: %s", exc)
        raise HTTPException(status_code=500, detail="Storage Write Error") from exc

# ---------------------------------------------------------------------------
# Pop endpoint (atomic FIFO, non-destructive)
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/queue/{queue_name}/pop",
    status_code=status.HTTP_200_OK,
    summary="Consume the oldest message atomically (non-destructive, must ack/nack)",
)
def pop_dispatcher(queue_name: str, visibility_timeout: int = 30):
    now = time.time()
    conn = _db_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            f"SELECT {_POP_SELECT} FROM message_store WHERE queue_name = ? AND visible_after < ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY id ASC LIMIT 1",
            (queue_name, now, now),
        ).fetchone()
        if not row:
            conn.commit()
            raise HTTPException(status_code=404, detail="Queue Empty")

        conn.execute(
            "UPDATE message_store SET visible_after = ?, retry_count = retry_count + 1 WHERE id = ?",
            (now + visibility_timeout, row["id"]),
        )
        conn.commit()

        return {
            "id": row["id"],
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "timestamp": row["timestamp"],
            "payload": json.loads(row["payload"]),
            "retry_count": row["retry_count"] + 1,
            "max_retries": row["max_retries"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        logger.error("Pop transaction failed: %s", exc)
        raise HTTPException(status_code=500, detail="Transaction Rollback Error") from exc
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Pop by pattern (wildcard)
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/queue/pop/like",
    status_code=status.HTTP_200_OK,
    summary="Consume the oldest message from any queue matching a LIKE pattern",
)
def pop_by_pattern(pattern: str, visibility_timeout: int = 30):
    now = time.time()
    conn = _db_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        params: list = [pattern, now, now]
        conditions = "queue_name LIKE ? AND visible_after < ? AND (expires_at IS NULL OR expires_at > ?)"
        if not pattern.startswith("failed_"):
            conditions += " AND queue_name NOT LIKE 'failed_%'"

        row = conn.execute(
            f"SELECT {_POP_SELECT} FROM message_store WHERE {conditions} ORDER BY id ASC LIMIT 1",
            params,
        ).fetchone()
        if not row:
            conn.commit()
            raise HTTPException(status_code=404, detail="Queue Empty")

        conn.execute(
            "UPDATE message_store SET visible_after = ?, retry_count = retry_count + 1 WHERE id = ?",
            (now + visibility_timeout, row["id"]),
        )
        conn.commit()

        return {
            "id": row["id"],
            "queue_name": row["queue_name"],
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "timestamp": row["timestamp"],
            "payload": json.loads(row["payload"]),
            "retry_count": row["retry_count"] + 1,
            "max_retries": row["max_retries"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        logger.error("Pop transaction failed: %s", exc)
        raise HTTPException(status_code=500, detail="Transaction Rollback Error") from exc
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Ack endpoint
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/message/{message_id}/ack",
    status_code=status.HTTP_200_OK,
    summary="Acknowledge a message by ID (deletes it from the queue)",
)
def ack_message(message_id: int):
    global _processed_total
    try:
        with _db_conn() as conn:
            cursor = conn.execute("DELETE FROM message_store WHERE id = ?", (message_id,))
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Message Not Found")
        _processed_total += 1
        return {"status": "ACK", "id": message_id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Ack failed: %s", exc)
        raise HTTPException(status_code=500, detail="Ack Error") from exc

# ---------------------------------------------------------------------------
# Nack endpoint
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/message/{message_id}/nack",
    status_code=status.HTTP_200_OK,
    summary="Negative acknowledgement — retry or move to poison queue",
)
def nack_message(message_id: int):
    conn = _db_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT retry_count, max_retries, queue_name FROM message_store WHERE id = ?",
            (message_id,),
        ).fetchone()
        if not row:
            conn.commit()
            raise HTTPException(status_code=404, detail="Message Not Found")

        if row["retry_count"] < row["max_retries"]:
            conn.execute(
                "UPDATE message_store SET visible_after = 0 WHERE id = ?",
                (message_id,),
            )
            conn.commit()
            return {
                "status": "NACK",
                "id": message_id,
                "retry_count": row["retry_count"],
                "max_retries": row["max_retries"],
            }

        poison_queue = (
            row["queue_name"]
            if row["queue_name"].startswith("failed_")
            else "failed_" + row["queue_name"]
        )
        conn.execute(
            "UPDATE message_store SET queue_name = ?, visible_after = 0 WHERE id = ?",
            (poison_queue, message_id),
        )
        conn.commit()
        logger.warning("Message %d moved to poison queue '%s'", message_id, poison_queue)
        return {
            "status": "POISON",
            "id": message_id,
            "poison_queue": poison_queue,
            "retry_count": row["retry_count"],
            "max_retries": row["max_retries"],
        }
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        logger.error("Nack failed: %s", exc)
        raise HTTPException(status_code=500, detail="Nack Error") from exc
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Peek endpoint (non-destructive read)
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/queue/{queue_name}/peek",
    status_code=status.HTTP_200_OK,
    summary="View messages without consuming them. Default limit=1 returns a single object; limit>1 returns a list.",
)
def peek_queue(queue_name: str, limit: int = 1):
    try:
        with _db_conn() as conn:
            rows = conn.execute(
                f"SELECT {_POP_SELECT} FROM message_store WHERE queue_name = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY id ASC LIMIT ?",
                (queue_name, time.time(), limit),
            ).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Queue Empty")

        def build(r):
            return {
                "id": r["id"],
                "event_id": r["event_id"],
                "event_type": r["event_type"],
                "timestamp": r["timestamp"],
                "payload": json.loads(r["payload"]),
                "retry_count": r["retry_count"],
                "max_retries": r["max_retries"],
            }

        if limit == 1:
            return build(rows[0])
        return {"messages": [build(r) for r in rows], "count": len(rows)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Peek failed: %s", exc)
        raise HTTPException(status_code=500, detail="Peek Error") from exc

# ---------------------------------------------------------------------------
# Count endpoint (specific queue)
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/queue/{queue_name}/count",
    status_code=status.HTTP_200_OK,
    summary="Count messages in a specific queue",
)
def count_queue(queue_name: str):
    try:
        with _db_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM message_store WHERE queue_name = ?",
                (queue_name,),
            ).fetchone()
        return {"queue": queue_name, "count": row["total"]}
    except Exception as exc:
        logger.error("Count failed: %s", exc)
        raise HTTPException(status_code=500, detail="Count Error") from exc

# ---------------------------------------------------------------------------
# List queues endpoint
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/system/queues",
    status_code=status.HTTP_200_OK,
    summary="List all queue names with non-zero backlog",
)
def list_queues():
    try:
        with _db_conn() as conn:
            rows = conn.execute(
                "SELECT queue_name, COUNT(*) AS pending FROM message_store GROUP BY queue_name ORDER BY queue_name"
            ).fetchall()
        return {
            "queues": [{"name": r["queue_name"], "pending": r["pending"]} for r in rows],
        }
    except Exception as exc:
        logger.error("List queues failed: %s", exc)
        raise HTTPException(status_code=500, detail="List Error") from exc

# ---------------------------------------------------------------------------
# Purge endpoint (admin)
# ---------------------------------------------------------------------------
@app.delete(
    "/api/v1/system/queue/{queue_name}/purge",
    status_code=status.HTTP_200_OK,
    summary="Delete all messages in a queue (emergency maintenance)",
)
def purge_queue(queue_name: str):
    try:
        with _db_conn() as conn:
            cursor = conn.execute("DELETE FROM message_store WHERE queue_name = ?", (queue_name,))
            deleted = cursor.rowcount
        logger.warning("Queue '%s' purged (%d messages deleted)", queue_name, deleted)
        return {"status": "PURGED", "queue": queue_name, "deleted": deleted}
    except Exception as exc:
        logger.error("Purge failed: %s", exc)
        raise HTTPException(status_code=500, detail="Purge Error") from exc

# ---------------------------------------------------------------------------
# Stats endpoint (observability)
# ---------------------------------------------------------------------------
@app.get(
    "/api/v1/system/stats",
    summary="Backlog per queue",
)
def system_stats():
    try:
        with _db_conn() as conn:
            rows = conn.execute(
                "SELECT queue_name, COUNT(*) AS pending FROM message_store GROUP BY queue_name"
            ).fetchall()
        metrics = {row["queue_name"]: row["pending"] for row in rows}
        return {
            "engine_status": "ONLINE",
            "storage_mode": "WAL",
            "metrics": {
                "total_backlog": sum(metrics.values()),
                "queues": metrics,
            },
        }
    except Exception as exc:
        logger.error("Stats error: %s", exc)
        raise HTTPException(status_code=500, detail="Stats Error") from exc

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", summary="Liveness probe with DB integrity check")
def health():
    fault = _db_integrity()
    base = {"processed_total": _processed_total}
    if fault is None:
        return {**base, "status": "ok", "db_integrity": "ok"}
    return {**base, "status": "degraded", "db_integrity": fault}

# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="NanoBroker — FIFO message broker on SQLite WAL")
    parser.add_argument("--host", default=None, help="Listen address (default: %s)" % HOST)
    parser.add_argument("--port", type=int, default=None, help="Listen port (default: %s)" % PORT)
    parser.add_argument("--database", default=None, help="SQLite database path (default: %s)" % DB_FILE)
    parser.add_argument("--janitor-interval", type=int, default=None, help="Janitor interval in seconds (default: %s)" % JANITOR_INTERVAL_SEC)
    args = parser.parse_args()

    if args.host is not None:
        HOST = args.host
    if args.port is not None:
        PORT = args.port
    if args.database is not None:
        DB_FILE = args.database
    if args.janitor_interval is not None:
        JANITOR_INTERVAL_SEC = args.janitor_interval

    logger.info("Starting NanoBroker on %s:%s", HOST, PORT)
    uvicorn.run(app, host=HOST, port=PORT, log_level=LOG_LEVEL.lower())
