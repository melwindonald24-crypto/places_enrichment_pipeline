# HOGONA WORKER

Repository: `melwindonald24-crypto/places_enrichment_pipeline`, branch `main`.

1. Read `worker/request.json` and `worker/response.json` from `main`.
   - No request: stop.
   - Response exists: report `existing handoff` and stop. Never overwrite it.

2. Read the current `worker/worker.py`. Use its `OUTPUT_KEYS`, `PF_KEYS`, `PA_KEYS`, `validate()`, and `validate_batch_response()` as the only output/schema rules.

3. Process only the jobs in the current request, in request order. Never use another batch.

4. Verify `request.json.artifact_blob_sha` equals the Git blob SHA of `data/hogona_worker_canonical_state.sql.gz.b64` on `main`.
   - Cannot verify: `canonical SHA unverifiable`.
   - Different: `canonical SHA stale`.

5. Research every job. Verify identity, geography, and required facts. Prefer authoritative sources. Preserve input coordinates exactly. Never guess. Every populated narrative field requires a source.

6. Return exactly one response accepted by `validate_batch_response()`: protocol `1`, same `batch_id`, same `artifact_blob_sha`, one result per request job, exact order, no extra fields.

7. Run `validate()` on every non-null result and `validate_batch_response()` on the complete response.

8. Immediately before writing, re-read `request.json`, `response.json`, `worker/worker.py`, and the canonical artifact SHA. If request identity or artifact SHA changed, stop with `validation/research failure`. If response exists, report `existing handoff`.

9. Create `worker/response.json` once with commit message `hogona: submit researched batch`. Never modify `request.json` or canonical state.

10. Re-read the committed response and validate it again against the current request, `worker.py`, and canonical SHA. Success requires complete validation; otherwise report `validation/research failure`.
