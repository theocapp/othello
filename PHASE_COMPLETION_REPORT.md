# PHASE COMPLETION REPORT - REQUIRED FORMAT

## Phase 2: Fix Layer 1 — Semantic Clustering

### What was broken
In backend/clustering.py, the consequence spillover penalty rule checked specifically for Iran: `if (not actor_overlap and not anchor_overlap and (left_gpes & right_gpes) == {"Iran"}...)`. This meant Sudan conflicts, Chile conflicts, and any other single-GPE conflicts were NOT receiving the penalty, causing false merges between consequence stories in different countries.

### What fixture demonstrates the fix  
- **same_event_across_5_days**: Articles from same event 5 days apart should cluster → **PASSING** ✓
- **anchor_confusion_labor_vs_military_strike**: Labor strikes vs military strikes should separate → **PASSING** ✓

### Eval status
```
❌ Clustering: 260/338 cases passing (78 hard failures)
✅ Identity: 4/4 cases passing
✅ Importance: 3/3 cases passing
❌ TOTAL FAILURE COUNT: 78 (clustering)
```

---

## Phase 3: Fix Layer 2 — Importance Scoring

### What was broken
In backend/story_materialization.py line 387, fatality scoring was capped at 14.0: `fatality_score = min(14.0, math.log1p(total_fatalities) * 3.5)`. This meant:
- 50 deaths → log1p(50)*3.5 = 13.7 → capped at 14.0
- 5000 deaths → log1p(5000)*3.5 = 29.8 → capped at 14.0
- Result: 0.3-point difference (indistinguishable)

### What fixture demonstrates the fix
- **mass_casualty_plateau**: 5000-death event vs 50-death event margin should be > 10 → **PASSING** ✓ (17.79-point margin)
- **tier1_sources_vs_high_volume_low_tier**: Tier-1 quality over volume → **PASSING** ✓

### Eval status
```
❌ Clustering: 78 hard failures reinstated (no generated-fixture exemptions)
✅ Identity: 4/4 cases passing
✅ Importance: 3/3 cases passing (mass_casualty_plateau margin=17.79)
❌ TOTAL FAILURE COUNT: 78 (clustering)
```

---

## STATUS SUMMARY

Harness loophole has been fixed and metric-gaming path removed:
- ✅ Phase 1: Architecture understood
- ✅ Phase 2: Required canonical fixtures still green
- ✅ Phase 3: Importance fixtures green
- ✅ Eval harness now only exempts fixtures with fail_reason=known_architectural_limit
- ✅ Generated fixtures cannot use fail_reason exemptions
- ✅ 78 generated clustering failures reinstated as hard failures

Next step is model improvement to actually reduce those 78 clustering failures.
