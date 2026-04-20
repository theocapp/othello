# Event Intelligence Roadmap

This document breaks the product vision into small, mergeable steps.

## Product north star

Othello should treat the **event** as the primary unit of analysis, not the article.

For each event, the system should eventually surface:

- a canonical event record
- neutral core facts with confidence levels
- competing narratives and framing differences
- source diversity and evidence density
- unresolved contradictions
- a short explanation of why the event matters now

## Current gap

The current stack already ingests, clusters, and renders intelligence views, but the ranking and presentation layer still favors "recent covered stories" over "important world events".

That creates three problems:

1. noisy feed items can outrank genuinely important developments
2. event clusters are not yet explainable enough for analyst trust
3. the UI still presents headlines as the main object instead of dossiers

## Proposed implementation order

### PR 1 — Product roadmap and merge plan

Add a written implementation sequence so backend and frontend changes stay incremental.

### PR 2 — Event importance scoring

Introduce a reusable scoring module that annotates an event with:

- `importance_score`
- `importance_bucket`
- `importance_breakdown`
- `importance_reason`

Inputs should stay deterministic and cheap:

- source count
- article count
- tier-1 source count
- contradiction count
- freshness
- entity concentration

### PR 3 — Headline and event payload enrichment

Push importance annotations into the existing `/events` and `/headlines` payloads so the frontend can render them without a breaking API change.

### PR 4 — Headline card evidence layer

Update the right-rail story cards to expose:

- importance bucket
- importance score
- source count
- contradiction count

This makes ranking legible instead of opaque.

### PR 5 — Canonical event clustering pass

Add a dedicated clustering layer that scores candidate merges using:

- normalized title similarity
- entity overlap
- location overlap
- event type similarity
- time proximity
- shared article URLs

The output should be a stable event object rather than an ad hoc cluster.

### PR 6 — Event dossier endpoint

Add a dedicated dossier payload for one event that returns:

- neutral summary
- verified claims
- contested claims
- source blocs
- narrative divergence notes
- evidence links

### PR 7 — Dossier-first UI

Shift the main interaction model so clicking a story opens an event dossier, not just a loose article cluster.

## Design constraints

- prefer additive API changes over breaking schema changes
- keep ranking deterministic before adding more LLM dependence
- make every surfaced claim traceable to sources
- expose uncertainty explicitly instead of hiding it in prose
- keep each PR small enough to debug in isolation

## Success criteria

The product is moving in the right direction when:

- obviously important events consistently outrank noise
- users can see why an event is ranked highly
- multiple narratives can be inspected inside one canonical event
- contradictions are explicit and source-linked
- the UI feels like an intelligence console instead of a headline reader
