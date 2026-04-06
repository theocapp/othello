# Canonical Event Clustering Spec

This spec defines how Othello should decide whether multiple articles belong to the same event.

## Goal

The output should be a stable canonical event object instead of a loose article bucket.

## Inputs

Each candidate article or pre-cluster should be normalized into:

- timestamp window
- canonical title
- entities
- countries and locations
- event type
- source domain
- article URL

## Merge signals

Each candidate pair should get a weighted merge score.

### 1. URL overlap

Strongest signal.

- exact shared URL: merge
- exact shared canonical URL: merge
- multiple shared article URLs across clusters: strong merge boost

### 2. Title similarity

Use normalized title similarity after removing source-specific noise.

- entity-preserving normalization
- cosine or token-set similarity
- extra weight when rare entities match

### 3. Entity overlap

Compare named entities with type awareness.

- person to person matches matter more than generic organization tokens
- require stronger overlap for very generic entities
- reward overlap in top-ranked entities, not only long-tail mentions

### 4. Location overlap

Geography should heavily constrain merging.

- same city or facility: strong signal
- same country with matching entities: medium signal
- same region only: weak signal
- conflicting geographies should penalize merge score

### 5. Time proximity

Events should be close in time.

- same 6-hour window: strong signal
- same 24-hour window: medium signal
- beyond 48 hours: weak unless the event is clearly ongoing

### 6. Event type compatibility

Do not merge incompatible event shapes.

Examples:

- ceasefire talks should not merge with a sanctions package unless they share the same core event object
- a bombing and a diplomatic statement should usually remain separate even if they are in the same country on the same day

## Hard blockers

Two candidates should not merge when:

- primary geographies clearly conflict
- event types are incompatible
- entity overlap is weak and titles are only generically similar
- the overlap is driven by a broad topic rather than a concrete incident

## Canonical event output

A merged event should expose:

- `event_id`
- `canonical_title`
- `summary`
- `start_time`
- `latest_update`
- `entities`
- `locations`
- `event_type`
- `source_count`
- `article_count`
- `evidence_urls`
- `contradiction_count`
- `importance_score`

## Auditability

For every merge, store why it happened.

Suggested debug fields:

- `merge_score_total`
- `merge_score_breakdown`
- `merged_from_cluster_ids`
- `merge_blockers_avoided`

That gives you a way to debug bad joins instead of guessing.
