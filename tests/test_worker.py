import sqlite3, json, pathlib
p=pathlib.Path('hogona_worker.sqlite')
assert p.exists(), 'database not reconstructed'
c=sqlite3.connect(p)
state=dict(c.execute('select status,count(*) from jobs group by status').fetchall())
assert sum(state.values())==611
assert state.get('completed',0)>=0
assert state.get('exported',0)>=0
assert c.execute("select count(*) from jobs where status in ('exported','failed') and coalesce(output_data,'')!=''").fetchone()[0]==0
assert c.execute("select count(*) from jobs where status='completed' and (output_data is null or output_data='')").fetchone()[0]==0
for jid,out in c.execute("select job_id,output_data from jobs where status='completed'"):
    o=json.loads(out); assert o['job_id']==jid; assert 'confidence' not in o
    assert o['place_fields']['traversability'] in ('easy','moderate','difficult')
    assert isinstance(o['place_fields']['visit_duration_minutes'],int)
print('CI PASS',state)
