# Human Annotation Bootstrap

This folder provides draft annotation schemas plus starter tooling to build scorecards.

## Schemas

Draft schemas are available under `evaluation/schemas/`:

- `clustering_label.schema.json`
- `importance_label.schema.json`
- `contradiction_label.schema.json`
- `summary_label.schema.json`

These are intentionally strict for finalized labels.

## Generate Batch

Create deterministic JSONL annotation batches from canonical events:

```bash
python backend/evaluation/generate_annotation_batch.py \
  --kind clustering \
  --topic geopolitics \
  --limit 40 \
  --seed 13 \
  --output backend/evaluation/batches/clustering_geopolitics.jsonl
```

Supported `--kind` values:

- `clustering`
- `importance`
- `contradiction`
- `summary`

## Validate Labels

Validate finalized labels:

```bash
python backend/evaluation/validate_annotation_labels.py backend/evaluation/batches/clustering_labeled.jsonl
```

The validator returns non-zero if malformed records are found and reports line-level errors.

## Scorecard Snapshot API

You can fetch deterministic label aggregates from the API:

```bash
curl "http://127.0.0.1:8001/evaluation/scorecard?kind=clustering&topic=geopolitics"
```

Optional query params:

- `kind`: `clustering|importance|contradiction|summary`
- `topic`: filter labels by topic
- `limit_files`: max JSONL files scanned (default `80`)
- `include_error_samples`: include validation error examples (`true|false`)

## Event Debug QA Helper

Run a repeatable QA pass over sampled live canonical events:

```bash
python backend/evaluation/run_event_debug_qa.py --topic geopolitics --event-limit 5
```

Strict mode (fail when no validated labels are present):

```bash
python backend/evaluation/run_event_debug_qa.py --topic geopolitics --event-limit 5 --require-label-records
```

Checks performed:

- sampled canonical event IDs are still present on a second fetch (stability sanity check)
- each sampled event returns a debug payload with expected importance/cohesion surfaces
- scorecard payload includes operational cohesion metrics for the selected topic
- optional strict gate for non-zero `records_considered` using `--require-label-records`

Optional calibration:

- set `OTHELLO_EVALUATION_COHESION_HIGH_OUTLIER_THRESHOLD` to tune the `high_outlier_event_rate` threshold (default `0.34`)
