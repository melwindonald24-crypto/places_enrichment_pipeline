import base64, gzip, json, os, sqlite3, tempfile
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
        raise SystemExit('canonical artifact changed since request')
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
        batch_id = request['batch_id']
        placeholders = ','.join('?' for _ in requested_ids)
        rows = con.execute(
            f"SELECT job_id,status,input_data,output_data FROM jobs WHERE batch_id=? AND job_id IN ({placeholders})",
            [batch_id, *requested_ids],
        ).fetchall()
        actual = {r[0]: r for r in rows}
        if set(actual) != set(requested_ids):
            raise SystemExit('requested jobs are not present in canonical batch')

        validated_results = []
        for item, requested in zip(response['results'], requested_jobs):
            jid = item['job_id']
            _, status, input_data, _ = actual[jid]
            if requested.get('job_id') != jid or requested.get('status') != 'exported':
                raise SystemExit(f'request mismatch for {jid}')
            parsed_input = json.loads(input_data)
            if requested.get('input_data') != parsed_input:
                raise SystemExit(f'canonical input changed for {jid}')
            if status != 'exported':
                raise SystemExit(f'job {jid} is not exported')
            result = item.get('result')
            if result is None:
                raise SystemExit(f'job {jid} has no enrichment result')
            validated, logical = worker.validate(result, parsed_input, jid)
            validated_results.append((jid, logical, validated, parsed_input))

        con.execute('BEGIN IMMEDIATE')
        for jid, logical, validated, parsed_input in validated_results:
            cur = con.execute("UPDATE jobs SET status='completed',output_data=?,updated_at=datetime('now') WHERE job_id=? AND status='exported'", (logical, jid))
            if cur.rowcount != 1:
                raise SystemExit(f'atomic update failed for {jid}')
            status, stored = con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
            if status != 'completed' or not stored:
                raise SystemExit(f'post-write verification failed for {jid}')
            parsed = json.loads(stored)
            if parsed != validated:
                raise SystemExit(f'post-write verification failed for {jid}')
            worker.validate(parsed, parsed_input, jid)

        for jid, _, _, _ in validated_results:
            status, out = con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
            if status != 'completed' or not out:
                raise SystemExit(f'job {jid} is not fully enriched')

        con.commit()
        state_bytes = canonical_bytes(con)

        with tempfile.NamedTemporaryFile(prefix='hogona-state-', suffix='.sql.gz.b64', delete=False) as tmp:
            tmp.write(state_bytes)
            tmp_path = Path(tmp.name)
        try:
            check_sql = gzip.decompress(base64.b64decode(tmp_path.read_bytes(), validate=True)).decode('utf-8')
            check_db_file = tempfile.NamedTemporaryFile(prefix='hogona-check-', suffix='.sqlite', delete=False)
            check_db_file.close()
            check_path = Path(check_db_file.name)
            try:
                check_con = sqlite3.connect(check_path)
                check_con.execute('PRAGMA foreign_keys=ON')
                check_con.executescript(check_sql)
                for jid, _, _, parsed_input in validated_results:
                    status, out = check_con.execute('SELECT status,output_data FROM jobs WHERE job_id=?', (jid,)).fetchone()
                    if status != 'completed' or not out:
                        raise SystemExit(f'canonical artifact verification failed for {jid}')
                    parsed = json.loads(out)
                    worker.validate(parsed, parsed_input, jid)
                check_con.close()
            finally:
                if check_path.exists():
                    check_path.unlink()
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        state_tmp = STATE.with_name(STATE.name + '.tmp')
        state_tmp.write_bytes(state_bytes)
        os.replace(state_tmp, STATE)
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