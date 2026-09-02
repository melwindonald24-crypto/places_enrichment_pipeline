import base64
import gzip
import json
import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "data" / "hogona_worker_canonical_state.sql.gz.b64"
REQUEST = ROOT / "worker" / "request.json"
RESPONSE = ROOT / "worker" / "response.json"
DB = ROOT / "hogona_worker.sqlite"

# GitHub is the durable state; request/response are the narrow research handoff.


def connect_state():
    sql = gzip.decompress(base64.b64decode(STATE.read_bytes(), validate=True)).decode("utf-8")
    con = sqlite3.connect(DB)
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(sql)
    return con


def canonical_bytes(con):
    con.commit()
    dump = "\n".join(con.iterdump()) + "\n"
    return base64.b64encode(gzip.compress(dump.encode("utf-8"), compresslevel=9, mtime=0))


def select_batch(con):
    row = con.execute(
        "SELECT batch_id FROM batches WHERE EXISTS "
        "(SELECT 1 FROM jobs WHERE jobs.batch_id=batches.batch_id "
        "AND jobs.status IN ('exported','processing')) "
        "ORDER BY batch_id LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def prepare():
    con = connect_state()
    try:
        batch_id = select_batch(con)
        if not batch_id:
            return False
        rows = con.execute(
            "SELECT job_id,status,input_data FROM jobs "
            "WHERE batch_id=? AND status IN ('exported','processing') ORDER BY job_id",
            (batch_id,),
        ).fetchall()
        artifact_blob_sha = os.popen("git rev-parse HEAD:data/hogona_worker_canonical_state.sql.gz.b64").read().strip()
        payload = {
            "protocol": 1,
            "batch_id": batch_id,
            "artifact_blob_sha": artifact_blob_sha,
            "jobs": [
                {"job_id": jid, "status": status, "input_data": json.loads(inp)}
                for jid, status, inp in rows
            ],
        }
        REQUEST.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        return True
    finally:
        con.close()
        if DB.exists(): DB.unlink()


def apply():
    response = json.loads(RESPONSE.read_text(encoding="utf-8"))
    if response.get("protocol") != 1:
        raise SystemExit("invalid response protocol")

    current_blob_sha = os.popen("git rev-parse HEAD:data/hogona_worker_canonical_state.sql.gz.b64").read().strip()
    if current_blob_sha != response.get("artifact_blob_sha"):
        raise SystemExit("canonical artifact changed since request; refusing overwrite")

    con = connect_state()
    try:
        batch_id = response["batch_id"]
        jobs = response["results"]
        expected = {
            r[0] for r in con.execute(
                "SELECT job_id FROM jobs WHERE batch_id=? AND status IN ('exported','processing')",
                (batch_id,),
            ).fetchall()
        }
        received = {r["job_id"] for r in jobs}
        if expected != received:
            raise SystemExit("response does not cover exactly the selected batch")

        import importlib.util
        spec = importlib.util.spec_from_file_location("worker", ROOT / "worker" / "worker.py")
        worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(worker)

        con.execute("BEGIN IMMEDIATE")
        for item in jobs:
            jid = item["job_id"]
            row = con.execute("SELECT status,input_data FROM jobs WHERE job_id=?", (jid,)).fetchone()
            if not row or row[0] not in ("exported", "processing"):
                raise SystemExit(f"unexpected state for {jid}")
            inp = json.loads(row[1])
            result = item.get("result")
            if result is None:
                con.execute("UPDATE jobs SET status='failed',output_data=NULL,updated_at=datetime('now') WHERE job_id=?", (jid,))
                continue
            validated, logical = worker.validate(result, inp, jid)
            con.execute("UPDATE jobs SET status='processing',output_data=NULL,updated_at=datetime('now') WHERE job_id=?", (jid,))
            con.execute("UPDATE jobs SET output_data=?,updated_at=datetime('now') WHERE job_id=? AND status='processing'", (logical, jid))
            stored = con.execute("SELECT output_data FROM jobs WHERE job_id=?", (jid,)).fetchone()[0]
            parsed = json.loads(stored)
            if parsed != validated:
                raise SystemExit(f"post-write verification failed for {jid}")
            con.execute("UPDATE jobs SET status='completed',updated_at=datetime('now') WHERE job_id=? AND status='processing'", (jid,))

        remaining = con.execute(
            "SELECT COUNT(*) FROM jobs WHERE batch_id=? AND status NOT IN ('completed','failed')", (batch_id,)
        ).fetchone()[0]
        if remaining:
            raise SystemExit("selected batch is not terminal")
        con.commit()
        STATE.write_bytes(canonical_bytes(con))
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
        if DB.exists(): DB.unlink()


if __name__ == "__main__":
    mode = os.environ.get("HOGONA_BRIDGE_MODE")
    if mode == "prepare":
        raise SystemExit(0 if prepare() else 0)
    if mode == "apply":
        apply()
    else:
        raise SystemExit("set HOGONA_BRIDGE_MODE=prepare|apply")
