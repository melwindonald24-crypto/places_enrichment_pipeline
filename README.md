# Hogona Places Enrichment Pipeline

Durable deterministic enrichment state and validation contract.

## Durable state

The authoritative SQLite state is stored as a compressed, base64-encoded SQLite SQL dump in `data/hogona_worker.sql.gz.b64`. The repository is the durable source of truth; local runner files are disposable.

## Execution model

The enrichment/research step is performed by the ChatGPT-side worker using web research and the deterministic contract in `worker/worker.py`. **No OpenAI API key is required by this repository.**

The Python module intentionally does not call the OpenAI API. It contains the state decoding, validation, normalization, serialization and post-write verification rules; research payloads must be supplied by the ChatGPT-side execution layer.

## Guarantees

- Exactly one queue-selection query per run.
- Exactly one selected batch per run.
- `exported -> processing -> output write -> readback verification -> completed`.
- Invalid completed state is rejected by SQLite triggers/schema validation.
- Coordinates are preserved exactly.
- Narrative claims require sources.
- Source tokens are normalized before persistence.
- Logical JSON is validated before and after write.
- No automation-management operation is performed by the worker.

## Current state

The checked-in state is the validated Hogona gold state currently containing 225 completed jobs and 386 exported jobs.

## CI

`tests/test_worker.py` validates the checked-in SQLite state.
