import base64, gzip, hashlib, json, os, sqlite3, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / 'data' / 'hogona_worker_canonical_state.sql.gz.b64'
REQUEST = ROOT / 'worker' / 'request.json'
RESPONSE = ROOT / 'worker' / 'response.json'
DB = ROOT / 'hogona_worker.sqlite'


def connect_state():
    sql = gzip.decompress(base64.b64decode(STATE.read_bytes(), validate=True)).decode('utf-8')
    con = sqlite3.connect(DB)
    con.execute('PRAGMA foreign_keys=ON')
    con.executescript(sql)
    return con


def canonical_bytes(con):
    con.commit()
    dump = '\n'.join(con.iterdump()) + '\n'
    return base64.b64encode(gzip.compress(dump.encode('utf-8'), compresslevel=9, mtime=0))


def canonical_blob_sha():
    return os.popen('git rev-parse HEAD:data/hogona_worker_canonical_state.sql.gz.b64').read().strip()


def prepare():
    # A handoff is a transaction boundary. Never create a second handoff.
    if REQUEST.exists() or RESPONSE.exists():
        return False

    con = connect_state()
    try:
        row = con.execute("SELECT batch_id FROM batches WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.batch_id=batches.batch_id AND status='exported') ORDER BY batch_id LIMIT 1").fetchone()
        if not row:
            return False

        batch_id = row[0]
        rows = con.execute("SELECT job_id,status,input_data FROM jobs WHERE batch_id=? AND status='exported' ORDER BY job_id", (batch_id,)).fetchall()
        if not rows:
            return False

        payload = {
            'protocol': 1,
            'batch_id': batch_id,
            'artifact_blob_sha': canonical_blob_sha(),
            'jobs': [
                {'job_id': jid, 'status': status, 'input_data': json.loads(inp)}
                for jid, status, inp in rows
            ],
        }
        REQUEST.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        return True
    finally:
        con.close()
        if DB.exists(): DB.unlink()


def apply():
    if not RESPONSE.exists():
        return False
    if not REQUEST.exists():
        raise SystemExit('request.json missing for response handoff')

    response = json.loads(RESPONSE.read_text(encoding='utf-8'))
    request = json.loads(REQUEST.read_text(encoding='utf-8'))
    request_sha = request.get('artifact_blob_sha')
    current_sha = canonical_blob_sha()

    if response.get('protocol') != 1:
        raise SystemExit('invalid response protocol')
    if current_sha != request_sha:
        raise SystemExit('canonical artifact changed since request; refusing overwrite')
    if response.get('artifact_blob_sha') != request_sha:
        raise SystemExit('response artifact SHA does not match request')
    if response.get('batch_id') != request.get('batch_id'):
        raise SystemExit('response batch_id does not match request')

    import importlib.util
    spec = importlib.util.spec_from_file_location('worker', ROOT / 'worker' / 'worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    worker.validate_batch_response(response, request, current_sha)

    requested_jobs = request.get('jobs')
    if not isinstance(requested_jobs, list) or not requested_jobs:
        raise SystemExit('request jobs are not a non-empty list')
    requested_ids = [j.get('job_id') for j in requested_jobs]
    if any(not isinstance(jid, str) or not jid for jid in requested_ids):
        raise SystemExit('request contains invalid job ID')
    if len(set(requested_ids)) != len(requested_ids):
        raise SystemExit('request contains duplicate job IDs')

    response_ids = [r.get('job_id') for r in response.get('results', [])]
    if response_ids != requested_ids:
        raise SystemExit('response does not cover exactly the requested jobs')

    con = connect_state()
    try:
        batch_id = request.get('batch_id')
        batch_rows = con.execute("SELECT job_id,status,input_data FROM jobs WHERE batch_id=? AND status='exported' ORDER BY job_id", (batch_id,)).fetchall()
        actual = {r[0]: (r[1], r[2]) for r in batch_rows}
        if set(actual) != set(requested_ids):
            raise SystemExit('request job set no longer matches canonical exported jobs')

        con.execute('BEGIN IMMEDIATE')
        for item, requested in zip(response['results'], requested_jobs):
            jid = item['job_id']
            row = actual.get(jid)
            if not row or row[0] != 'exported':
                raise SystemExit(f'unexpected state for {jid}')
            if requested.get('job_id') != jid:
                raise SystemExit(f'request ordering mismatch for {jid}')
            if requested.get('status') != 'exported':
                raise SystemExit(f'request state is not exported for {jid}')
            if requested.get('input_data') != json.loads(row[1]):
                raise SystemExit(f'canonical input changed for {jid}')

            result = item.get('result')
            if result is None:
                con.execute("UPDATE jobs SET status='failed',output_data=NULL,updated_at=datetime('now') WHERE job_id=? AND status='exported'", (jid,))
                continue

            validated, logical = worker.validate(result, json.loads(row[1]), jid)
            con.execute("UPDATE jobs SET status='completed',output_data=?,updated_at=datetime('now') WHERE job_id=? AND status='exported'", (logical, jid))
            stored = con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
            if stored[0] != 'completed' or not stored[1]:
                raise SystemExit(f'post-write verification failed for {jid}')
            parsed = json.loads(stored[1])
            if parsed != validated:
                raise SystemExit(f'post-write verification failed for {jid}')
            worker.validate(parsed, json.loads(row[1]), jid)

        for jid in requested_ids:
            status, out = con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
            if status not in ('completed', 'failed'):
                raise SystemExit(f'selected job {jid} is not terminal')
            if status == 'completed' and not out:
                raise SystemExit(f'completed job {jid} has no output')
            if status == 'failed' and out not in (None, ''):
                raise SystemExit(f'failed job {jid} has output')

        con.commit()
        state_bytes = canonical_bytes(con)

        # Verify the exact serialized canonical artifact before replacing the
        # repository state file. This closes the DB->artifact boundary: a batch
        # cannot be acknowledged unless the artifact we are about to publish
        # can be loaded and contains the terminal outputs we just validated.
        with tempfile.NamedTemporaryFile(prefix='hogona-state-', suffix='.sql.gz.b64', delete=False) as tmp:
            tmp.write(state_bytes)
            tmp_path = Path(tmp.name)
        try:
            check_sql = gzip.decompress(base64.b64decode(tmp_path.read_bytes(), validate=True)).decode('utf-8')
            check_db = tempfile.NamedTemporaryFile(prefix='hogona-check-', suffix='.sqlite', delete=False)
            check_db.close()
            check_path = Path(check_db.name)
            try:
                check_con = sqlite3.connect(check_path)
                check_con.execute('PRAGMA foreign_keys=ON')
                check_con.executescript(check_sql)
                for jid in requested_ids:
                    status, out = check_con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
                    if status not in ('completed', 'failed'):
                        raise SystemExit(f'canonical artifact verification failed for {jid}')
                    if status == 'completed':
                        if not out:
                            raise SystemExit(f'canonical artifact missing output for {jid}')
                        parsed = json.loads(out)
                        worker.validate(parsed, json.loads(actual[jid][1]), jid)
                    elif out not in (None, ''):
                        raise SystemExit(f'canonical artifact has output for failed job {jid}')
                check_con.close()
            finally:
                if check_path.exists():
                    check_path.unlink()
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        # Replace atomically only after the serialized artifact has passed the
        # independent reload/validation check above.
        STATE_TMP = STATE.with_name(STATE.name + '.tmp')
        STATE_TMP.write_bytes(state_bytes)
        os.replace(STATE_TMP, STATE)
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
        if DB.exists(): DB.unlink()
    return True


if __name__ == '__main__':
    mode = os.environ.get('HOGONA_BRIDGE_MODE')
    if mode == 'prepare':
        raise SystemExit(0 if prepare() else 0)
    if mode == 'apply':
        raise SystemExit(0 if apply() else 0)
    raise SystemExit('set HOGONA_BRIDGE_MODE=prepare|apply')