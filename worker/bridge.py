import base64, gzip, json, os, sqlite3, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / 'data' / 'hogona_worker_canonical_state.sql.gz.b64'
REQUEST = ROOT / 'worker' / 'request.json'
RESPONSE = ROOT / 'worker' / 'response.json'
DB = ROOT / 'hogona_worker.sqlite'


def canonical_sha():
    return os.popen('git rev-parse HEAD:data/hogona_worker_canonical_state.sql.gz.b64').read().strip()


def open_state():
    con = sqlite3.connect(DB)
    con.execute('PRAGMA foreign_keys=ON')
    sql = gzip.decompress(base64.b64decode(STATE.read_bytes(), validate=True)).decode()
    con.executescript(sql)
    return con


def prepare():
    if REQUEST.exists() or RESPONSE.exists():
        return
    con = open_state()
    try:
        batch = con.execute("SELECT batch_id FROM batches WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.batch_id=batches.batch_id AND status='exported') ORDER BY batch_id LIMIT 1").fetchone()
        if not batch:
            return
        jobs = con.execute("SELECT job_id,status,input_data FROM jobs WHERE batch_id=? AND status='exported' ORDER BY job_id", (batch[0],)).fetchall()
        REQUEST.write_text(json.dumps({
            'protocol': 1,
            'batch_id': batch[0],
            'artifact_blob_sha': canonical_sha(),
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
    current = canonical_sha()
    if request.get('artifact_blob_sha') != current:
        raise SystemExit('canonical SHA mismatch')

    import importlib.util
    spec = importlib.util.spec_from_file_location('worker', ROOT / 'worker' / 'worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    worker.validate_batch_response(response, request, current)

    con = open_state()
    try:
        con.execute('BEGIN IMMEDIATE')
        for job, item in zip(request['jobs'], response['results']):
            row = con.execute('SELECT status,input_data FROM jobs WHERE batch_id=? AND job_id=?', (request['batch_id'], job['job_id'])).fetchone()
            if not row or row[0] != 'exported' or json.loads(row[1]) != job['input_data']:
                raise SystemExit('canonical job mismatch')
            _, output = worker.validate(item['result'], job['input_data'], job['job_id'])
            if con.execute("UPDATE jobs SET status='completed',output_data=?,updated_at=datetime('now') WHERE batch_id=? AND job_id=? AND status='exported'", (output, request['batch_id'], job['job_id'])).rowcount != 1:
                raise SystemExit('job update failed')

        for job in request['jobs']:
            row = con.execute('SELECT status,output_data,input_data FROM jobs WHERE batch_id=? AND job_id=?', (request['batch_id'], job['job_id'])).fetchone()
            if not row or row[0] != 'completed' or not row[1] or json.loads(row[2]) != job['input_data']:
                raise SystemExit('post-write verification failed')
            worker.validate(json.loads(row[1]), job['input_data'], job['job_id'])

        con.commit()
        state = base64.b64encode(gzip.compress(('\n'.join(con.iterdump()) + '\n').encode(), 9, mtime=0))
        with tempfile.NamedTemporaryFile(dir=STATE.parent, prefix=STATE.name + '.', delete=False) as f:
            f.write(state)
            temp = Path(f.name)
        os.replace(temp, STATE)
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
        if DB.exists(): DB.unlink()


if __name__ == '__main__':
    mode = os.environ.get('HOGONA_BRIDGE_MODE')
    if mode == 'prepare':
        prepare()
    elif mode == 'apply':
        apply()
    else:
        raise SystemExit('invalid mode')
