import base64, gzip, json, os, sqlite3

DB='hogona_worker.sqlite'
STATE='data/hogona_worker_canonical_state.sql.gz.b64'

CORE='''HOGONA WORKER. Read only the supplied request and process only its jobs. Research each place and verify identity, geography, and relevant facts using authoritative sources where available. Preserve supplied coordinates exactly; use null when evidence is unavailable; preserve credible disagreements. Return exactly the schema defined by this module and validate every result before persistence. Do not modify canonical state directly.'''

OUTPUT_KEYS={'job_id','place_fields','planning_attributes','sources'}
PF_KEYS={'traversability','micro_region','vibe','summary','best_time','visit_duration_minutes','co_ords'}
PA_KEYS={'description','elevation_or_height','legend_or_history','how_to_reach','access_restrictions','safety_notes','local_name_variants','notes'}


def decode_state():
    raw=base64.b64decode(open(STATE,'rb').read(),validate=True)
    sql=gzip.decompress(raw).decode('utf-8')
    con=sqlite3.connect(DB)
    con.executescript(sql)
    con.execute('PRAGMA foreign_keys=ON')
    con.commit(); return con


def encode_state(con):
    con.commit()
    dump='\n'.join(con.iterdump())+'\n'
    data=base64.b64encode(gzip.compress(dump.encode(),9,mtime=0))
    tmp=STATE+'.tmp'; open(tmp,'wb').write(data); os.replace(tmp,STATE)


def research(inp):
    raise RuntimeError('Research must be supplied by the ChatGPT-side worker; this module never calls the OpenAI API.')


def hostname_token(url):
    from urllib.parse import urlparse
    u=url.strip().lower()
    p=urlparse(u if '://' in u else 'https://'+u)
    host=p.hostname or ''
    if host.startswith('www.'): host=host[4:]
    if not host: raise ValueError('source has no hostname')
    return host.replace('.','(dot)')


def validate(result, inp, jid):
    if not isinstance(result,dict) or set(result)!=OUTPUT_KEYS: raise ValueError('output shape mismatch')
    if result['job_id']!=jid: raise ValueError('job_id mismatch')
    pf=result['place_fields']; pa=result['planning_attributes']; sources=result['sources']
    if set(pf)!=PF_KEYS or set(pa)!=PA_KEYS or not isinstance(sources,list): raise ValueError('schema mismatch')
    if 'confidence' in result: raise ValueError('confidence present')
    if pf['traversability'] not in ('easy','moderate','difficult'): raise ValueError('invalid traversability')
    if not isinstance(pf['visit_duration_minutes'],int): raise ValueError('duration must be integer')
    if pf['visit_duration_minutes']<0 or (pf['traversability']!='difficult' and pf['visit_duration_minutes']==0): raise ValueError('invalid duration')
    supplied=inp.get('serper_metadata',{})
    if 'latitude' in supplied and 'longitude' in supplied:
        expected={'lat':supplied['latitude'],'lng':supplied['longitude']}
        if pf['co_ords']!=expected: raise ValueError('coordinates changed')
    elif pf['co_ords'] is not None: raise ValueError('coordinates invented')
    normalized=[]
    for s in sources:
        if not isinstance(s,str): raise ValueError('source not string')
        normalized.append(hostname_token(s))
    normalized=list(dict.fromkeys(normalized))
    if any('/' in s or '://' in s or '.' in s or s!=s.lower() for s in normalized): raise ValueError('source normalization failed')
    narrative=list(pf[k] for k in ('micro_region','vibe','summary','best_time'))+list(pa.values())
    filled=any(v not in (None,'',[]) for v in narrative)
    if filled and not normalized: raise ValueError('narrative without sources')
    result['sources']=normalized
    logical=json.dumps(result,ensure_ascii=False,separators=(',',':'))
    return result,logical


def apply_researched_result(con,jid,inp,result):
    result,logical=validate(result,inp,jid)
    row=con.execute("SELECT status FROM jobs WHERE job_id=?",(jid,)).fetchone()
    if not row or row[0] != 'exported': raise RuntimeError(f'job {jid} is not in exported state')
    cur=con.execute("UPDATE jobs SET output_data=?,status='completed',updated_at=datetime('now') WHERE job_id=? AND status='exported'",(logical,jid))
    if cur.rowcount!=1: raise RuntimeError(f'job {jid} update failed')
    stored=con.execute("SELECT status,output_data FROM jobs WHERE job_id=?",(jid,)).fetchone()
    if stored[0]!='completed' or not stored[1]: raise RuntimeError('post-write verification failed')
    parsed=json.loads(stored[1])
    if parsed!=result: raise RuntimeError('post-write verification failed')
    validate(parsed,inp,jid)
    return True


def select_one_batch(con):
    return con.execute("SELECT batch_id FROM batches WHERE EXISTS (SELECT 1 FROM jobs WHERE jobs.batch_id=batches.batch_id AND status='exported') ORDER BY batch_id LIMIT 1").fetchone()


def claim_job(con,jid):
    cur=con.execute("UPDATE jobs SET status='processing',updated_at=datetime('now') WHERE job_id=? AND status='exported'",(jid,))
    if cur.rowcount!=1: raise RuntimeError('claim failure')
    con.commit()


def verify_terminal_batch(con,batch_id):
    final=con.execute("SELECT status,output_data FROM jobs WHERE batch_id=? ORDER BY job_id",(batch_id,)).fetchall()
    for status,out in final:
        if status not in ('completed','failed'): raise RuntimeError('batch not terminal')
        if status=='completed' and not out: raise RuntimeError('completed without output')
        if status=='failed' and out not in (None,''): raise RuntimeError('failed with output')


def validate_batch_response(response,request,canonical_blob_sha=None):
    if not isinstance(response,dict) or response.get('protocol') != 1: raise ValueError('invalid response protocol')
    if response.get('batch_id') != request.get('batch_id'): raise ValueError('batch_id mismatch')
    if response.get('artifact_blob_sha') != request.get('artifact_blob_sha'): raise ValueError('artifact_blob_sha mismatch')
    if canonical_blob_sha is not None and response.get('artifact_blob_sha') != canonical_blob_sha: raise ValueError('response artifact SHA is stale')
    jobs=request.get('jobs'); results=response.get('results')
    if not isinstance(jobs,list) or not isinstance(results,list) or not jobs: raise ValueError('request/response jobs must be non-empty lists')
    expected=[j['job_id'] for j in jobs]
    received=[r.get('job_id') for r in results]
    if received != expected or len(set(received)) != len(received): raise ValueError('response does not cover exactly request jobs in order')
    for j,r in zip(jobs,results):
        if j.get('status') != 'exported': raise ValueError('request job is not exported')
        result=r.get('result')
        if result is None: raise ValueError('missing enrichment result')
        validate(result,j['input_data'],j['job_id'])
    return True


def main():
    raise SystemExit('This module is the deterministic state/validation library. The ChatGPT-side worker supplies research and invokes its functions; it intentionally has no OpenAI API dependency.')

if __name__=='__main__':
    main()
