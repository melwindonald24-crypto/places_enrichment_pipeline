# HOGONA WORKER

Repository: `melwindonald24-crypto/places_enrichment_pipeline`, branch `main`.

1. Read `worker/request.json` and `worker/response.json`. If no request, stop this run only. If response exists, stop this run only without changing it.

2. Read current `worker/worker.py` and use its `OUTPUT_KEYS`, `PF_KEYS`, `PA_KEYS`, `validate()`, and `validate_batch_response()` as the schema authority. Verify the request artifact SHA matches the Git blob SHA of `data/hogona_worker_canonical_state.sql.gz.b64`.

3. Process only the current request's jobs, in exact order. Research every job; verify identity/geography, preserve supplied coordinates, never guess, and source populated narrative fields.

4. Build exactly one raw JSON response with protocol `1`, the same `batch_id` and `artifact_blob_sha`, one result per job in order, and no extra fields. Each result must have exactly `job_id`, `place_fields`, `planning_attributes`, and `sources` as sibling keys. Sources must be lowercase hostnames with no scheme, path, or `www.`, and every `.` written as `(dot)`.

5. Validate the parsed response with `validate()` and `validate_batch_response()` before writing. Immediately before writing, re-read the request, response, worker.py, and canonical SHA. If the request changed or a response exists, stop this run without writing.

6. Create `worker/response.json` exactly once with commit message `hogona: submit researched batch`. Never modify `request.json` or canonical state. After committing, stop this run; GitHub Actions independently applies the response.

A run ending is never automation completion. NEVER pause, disable, suspend, terminate, cancel, or mark the recurring automation complete because of success, failure, no request, an existing response, or any other run outcome. Keep the recurring automation enabled. Only disable or pause it when the user explicitly asks.