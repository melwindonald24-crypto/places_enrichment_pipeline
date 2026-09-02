import base64, gzip, json, os, sqlite3, sys

DB='hogona_worker.sqlite'
STATE='data/hogona_worker_canonical_state.sql.gz.b64'

CORE='''You are the Hogona enrichment worker. Process only the supplied job. Research factual/narrative fields with web search and never guess. If reliable evidence is unavailable, use null. Check place identity and geographic consistency; never silently accept a wrong classification. Preserve supplied coordinates exactly. Normal tourist output requires traversability exactly easy|moderate|difficult, a positive integer visit_duration_minutes, exact job_id, and no confidence field. Clearly non-tourist/restricted/private/administrative places use difficult, duration 0, narrative fields null, sources [] unless another populated field requires a source. Every populated narrative field must be supported by sources. Sources in the research result must be full URLs; before persistence they are normalized to lowercase hostname tokens with protocol, www, path, query, fragment and trailing slash removed, then every literal dot replaced with (dot), deduplicated. Return exactly one JSON object with job_id, place_fields, planning_attributes, sources. Do not add fields. The durable state is the GitHub artifact data/hogona_worker_canonical_state.sql.gz.b64; local hogona_worker.sqlite files are disposable. This worker never uses an OpenAI API key.'''

OUTPUT_KEYS={'job_id','place_fields','planning_attributes','sources'}
PF_KEYS={'traversability','micro_region','vibe','summary','best_time','visit_duration_minutes','co_ords'}
PA_KEYS={'description','elevation_or_height','legend_or_history','how_to_reach','access_restrictions','safety_notes','local_name_variants','notes'}


def decode_state():
    raw=base64.b64decode(open(STATE,'rb').read())
    sql=gzip.decompress(raw).decode('utf-8')
    con=sqlite3.connect(DB)
    con.executescript(sql)
    con.execute('PRAGMA foreign_keys=ON')
    con.commit(); return con


def encode_state(con):
    con.commit()
    dump='\n'.join(con.iterdump())+'\n'
    data=base64.b64encode(gzip.compress(dump.encode(),9))
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
    if len(logical)>=1800: raise ValueError('logical JSON >= 1800 chars')
    return result,logical


def apply_researched_result(con, jid, inp, result):
    result,logical=validate(result,inp,jid)
    con.execute("UPDATE jobs SET output_data=?,updated_at=datetime('now') WHERE job_id=? AND status='processing'",(logical,jid)); con.commit()
    stored=con.execute("SELECT output_data FROM jobs WHERE job_id=?",(jid,)).fetchone()[0]
    parsed=json.loads(stored)
    if parsed!=result: raise RuntimeError('post-write verification failed')
    validate(parsed,inp,jid)
    con.execute("UPDATE jobs SET status='completed',updated_at=datetime('now') WHERE job_id=? AND status='processing'",(jid,)); con.commit()


def select_one_batch(con):
    # EXACTLY ONE batch-selection query for this run.
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


def main():
    raise SystemExit('This module is the deterministic state/validation library. The ChatGPT-side worker supplies research and invokes its functions; it intentionally has no OpenAI API dependency.')


if __name__=='__main__':
    main()
