import json
from urllib.parse import urlparse

OUTPUT_KEYS = {'job_id', 'place_fields', 'planning_attributes', 'sources'}
PF_KEYS = {'traversability', 'micro_region', 'vibe', 'summary', 'best_time', 'visit_duration_minutes', 'co_ords'}
PA_KEYS = {'description', 'elevation_or_height', 'legend_or_history', 'how_to_reach', 'access_restrictions', 'safety_notes', 'local_name_variants', 'notes'}


def validate(result, inp, jid):
    if not isinstance(result, dict) or set(result) != OUTPUT_KEYS:
        raise ValueError('output shape mismatch')
    if result['job_id'] != jid:
        raise ValueError('job_id mismatch')

    pf = result['place_fields']
    pa = result['planning_attributes']
    sources = result['sources']
    if not isinstance(pf, dict) or set(pf) != PF_KEYS:
        raise ValueError('place_fields schema mismatch')
    if not isinstance(pa, dict) or set(pa) != PA_KEYS:
        raise ValueError('planning_attributes schema mismatch')
    if not isinstance(sources, list):
        raise ValueError('sources must be a list')

    if pf['traversability'] not in ('easy', 'moderate', 'difficult'):
        raise ValueError('invalid traversability')
    if not isinstance(pf['visit_duration_minutes'], int):
        raise ValueError('duration must be integer')
    if pf['visit_duration_minutes'] < 0:
        raise ValueError('invalid duration')
    if pf['traversability'] != 'difficult' and pf['visit_duration_minutes'] == 0:
        raise ValueError('invalid duration')

    supplied = inp.get('serper_metadata', {})
    if 'latitude' in supplied and 'longitude' in supplied:
        if pf['co_ords'] != {'lat': supplied['latitude'], 'lng': supplied['longitude']}:
            raise ValueError('coordinates changed')
    elif pf['co_ords'] is not None:
        raise ValueError('coordinates invented')

    normalized = []
    for source in sources:
        if not isinstance(source, str):
            raise ValueError('source not string')
        value = source.strip().lower()
        parsed = urlparse(value if '://' in value else 'https://' + value)
        host = parsed.hostname or ''
        if host.startswith('www.'):
            host = host[4:]
        if not host:
            raise ValueError('source has no hostname')
        token = host.replace('.', '(dot)')
        if '/' in token or '://' in token or '.' in token or token != token.lower():
            raise ValueError('source normalization failed')
        normalized.append(token)

    normalized = list(dict.fromkeys(normalized))
    if normalized != sources:
        raise ValueError('sources not normalized')

    narrative = [pf[k] for k in ('micro_region', 'vibe', 'summary', 'best_time')] + list(pa.values())
    if any(value not in (None, '', []) for value in narrative) and not sources:
        raise ValueError('narrative without sources')

    logical = json.dumps(result, ensure_ascii=False, separators=(',', ':'))
    return result, logical


def validate_batch_response(response, request, canonical_blob_sha):
    if not isinstance(response, dict) or set(response) != {'protocol', 'batch_id', 'artifact_blob_sha', 'results'}:
        raise ValueError('invalid response shape')
    if response['protocol'] != 1:
        raise ValueError('invalid response protocol')
    if response['batch_id'] != request.get('batch_id'):
        raise ValueError('batch_id mismatch')
    if response['artifact_blob_sha'] != request.get('artifact_blob_sha'):
        raise ValueError('artifact_blob_sha mismatch')
    if response['artifact_blob_sha'] != canonical_blob_sha:
        raise ValueError('canonical SHA mismatch')

    jobs = request.get('jobs')
    results = response['results']
    if not isinstance(jobs, list) or not jobs:
        raise ValueError('request jobs must be a non-empty list')
    if not isinstance(results, list):
        raise ValueError('results must be a list')

    expected = [job['job_id'] for job in jobs]
    received = [item.get('job_id') for item in results]
    if received != expected:
        raise ValueError('response jobs do not match request order')

    for job, item in zip(jobs, results):
        if job.get('status') != 'exported':
            raise ValueError('request job is not exported')
        result = item.get('result')
        if result is None:
            raise ValueError('missing enrichment result')
        validate(result, job['input_data'], job['job_id'])

    return True
