import base64, gzip, json, os, sqlite3, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / 'data' / 'hogona_worker_canonical_state.sql.gz.b64'
REQUEST = ROOT / 'worker' / 'request.json'
RESPONSE = ROOT / 'worker' / 'response.json'
DB = ROOT / 'hogona_worker.sqlite'


def sha():
    return os.popen('git rev-parse HEAD:data/hogona_worker_canonical_state.sql.gz.b64').read().strip()


def db():
    con = sqlite3.connect(DB)
    con.execute('PRAGMA foreign_keys=ON')
    sql = gzip.decompress(base64.b64decode(STATE.read_bytes(), validate=True)).decode()
    con.executescript(sql)
    return con


def prepare():
    if REQUEST.exists() or RESPONSE.exists():
        return
    con = db()
    try:
        row = con.execute("SELECT batch_id FROM batches WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.batch_id=batches.batch_id AND status='exported') ORDER BY batch_id LIMIT 1").fetchone()
        if not row:
            return
        batch = row[0]
        jobs = con.execute("SELECT job_id,status,input_data FROM jobs WHERE batch_id=? AND status='exported' ORDER BY job_id", (batch,)).fetchall()
        REQUEST.write_text(json.dumps({
            'protocol': 1,
            'batch_id': batch,
            'artifact_blob_sha': sha(),
            'jobs': [{'job_id': j, 'status': s, 'input_data': json.loads(i)} for j, s, i in jobs]
        }, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    finally:
        con.close()
        if DB.exists(): DB.unlink()


def apply():
    if not RESPONSE.exists():
        return
    if not REQUEST.exists():
        raise SystemExit('request.json missing')

    request = json.loads(REQUEST.read_text(encoding='utf-8'))
    response = json.loads(RESPONSE.read_text(encoding='utf-8'))
    current = sha()
    if request.get('artifact_blob_sha') != current:
        raise SystemExit('canonical SHA mismatch')
    if response.get('protocol') != 1 or response.get('batch_id') != request.get('batch_id') or response.get('artifact_blob_sha') != current:
        raise SystemExit('response/request mismatch')

    import importlib.util
    spec = importlib.util.spec_from_file_location('worker', ROOT / 'worker' / 'worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    worker.validate_batch_response(response, request, current)

    jobs = request['jobs']
    results = response['results']
    con = db()
    try:
        con.execute('BEGIN IMMEDIATE')
        for job, item in zip(jobs, results):
            row = con.execute('SELECT status,input_data FROM jobs WHERE batch_id=? AND job_id=?', (request['batch_id'], job['job_id'])).fetchone()
            if not row or row[0] != 'exported' or json.loads(row[1]) != job['input_data']:
                raise SystemExit('canonical job mismatch')
            _, output = worker.validate(item['result'], job['input_data'], job['job_id'])
            con.execute("UPDATE jobs SET status='completed',output_data=?,updated_at=datetime('now') WHERE batch_id=? AND job_id=? AND status='exported'", (output, request['batch_id'], job['job_id']))
        con.commit()
        state = base64.b64encode(gzip.compress(('\n'.join(con.iterdump()) + '\n').encode(), 9, mtime=0))
        tmp = STATE.with_name(STATE.name + '.tmp')
        tmp.write_bytes(state)
        os.replace(tmp, STATE)
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
        if DB.exists(): DB.unlink()


if __name__ == '__main__':
    if os.environ.get('HOGONA_BRIDGE_MODE') == 'prepare':
        prepare()
    elif os.environ.get('HOGONA_BRIDGE_MODE') == 'apply':
        apply()
    else:
        raise SystemExit('invalid mode')
