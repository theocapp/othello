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
