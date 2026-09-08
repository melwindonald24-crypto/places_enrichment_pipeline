# HOGONA WORKER

Repository: `melwindonald24-crypto/places_enrichment_pipeline`, branch `main`.

1. Read `worker/request.json` and `worker/response.json` from `main`.
   - No request: stop this run only.
   - Response exists: report `existing handoff` and stop this run only. Never overwrite it.

2. Read the current `worker/worker.py`. Use `OUTPUT_KEYS`, `PF_KEYS`, `PA_KEYS`, `validate()`, and `validate_batch_response()` as the only output/schema rules.

3. Process only the jobs in the current request, in request order. Never use another batch.

4. Verify `request.json.artifact_blob_sha` equals the Git blob SHA of `data/hogona_worker_canonical_state.sql.gz.b64` on `main`.
   - Cannot verify: `canonical SHA unverifiable`.
   - Different: `canonical SHA stale`.

5. Research every job. Verify identity, geography, and required facts. Prefer authoritative sources. Preserve input coordinates exactly. Never guess. Every populated narrative field requires a source. Every job must have a non-null result.

6. Build each result exactly as `validate()` requires: `job_id`, `place_fields`, `planning_attributes`, and `sources` as four sibling keys. `sources` must never be nested inside `place_fields` or `planning_attributes`. Build exactly one top-level JSON object: protocol `1`, same `batch_id`, same `artifact_blob_sha`, one result per request job in exact order, no extra fields. `worker/response.json` must contain raw valid JSON only: no Markdown, code fences, comments, or quoted/wrapped JSON text.

7. Validate every result with `validate()` and the complete parsed response with `validate_batch_response()` before writing. Do not write a response that is not syntactically valid JSON and accepted by both validators.

8. Immediately before writing, re-read `request.json`, `response.json`, `worker/worker.py`, and the canonical artifact SHA. If request identity or artifact SHA changed, stop with `validation/research failure`. If response exists, report `existing handoff`.

9. Create `worker/response.json` once with commit message `hogona: submit researched batch`. Never modify `request.json` or canonical state.

10. After the response commit, stop this run. GitHub Actions independently validates and applies the response to canonical state. Do not require `worker/response.json` to remain present for post-commit verification; the apply workflow may consume it immediately.
