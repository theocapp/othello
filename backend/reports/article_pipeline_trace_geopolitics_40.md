# Article Pipeline Trace Report

## Run Details
- Command: `python backend/trace_article_pipeline.py --topic geopolitics --limit 40 --hours 96 --headline-corpus-only`
- Sample size: 40 articles
- Window: 96 hours
- Topic filter: `geopolitics`

## Stage 1: Raw Articles
- Raw articles loaded: 40
- Promoted: 29
- Rejected: 11

### Raw-layer observations
- The sample is heavily centered on the Iran / Israel / US conflict.
- A few off-topic or borderline items were still present in the loaded sample before promotion filtering, including:
  - `Russia attacks Ukraine with drones : 5 killed , 19 injured as strikes hit Nikopol market`
  - `The US has lost its credibility: Turkey is in the room of the new alliance formation`
  - `Annual increase of 1.2% in inflation in March 2026`
- Some rejected articles still matched geopolitical language weakly, which is useful for checking whether `should_promote_article()` is too permissive or too strict.

## Stage 2: Clustering
- Clusters produced: 24
- Singleton clusters: 20
- Multi-article clusters: 4

### Largest cluster
- Group 1
- Articles: 14
- Sources: 14
- Outlier ratio: 0.8571
- Mean relatedness: 2.9415
- Median relatedness: 2.828
- Min relatedness: -3.4
- Max relatedness: 8.054

### Largest cluster notes
This is the cluster most worth inspecting manually. It contains a broad set of Iran conflict articles spanning different languages and sub-angles. The high outlier ratio suggests the cluster is being held together by a few very strong links while many members are weakly related.

### Other multi-article clusters
- Group 13: 2 articles, `French CMA CGM vessel crosses Strait of Hormuz in 1st W European transrit amid Iran conflict`
- Group 19: 2 articles, `Trump uses Iran war address to urge skeptical electorate for more time`
- Group 21: 2 articles, `Lib Dems call for 10p fuel duty cut to help motorists with Iran living costs`

### Clustering-stage checks
- Clustering is clearly order-sensitive, so the input sort order matters.
- The consensus title for the biggest cluster came from a WGY radio item, which may or may not be the best representative label.
- At least one article in the sample appears off-topic enough that it is worth checking whether `infer_article_topics()` is over-broad.

## Stage 3: Contradictions
- Enriched events: 24
- Highest-priority event:
  - Label: `Iran Rejects Temporary Ceasefire As Israeli , US Airstrikes Continue | News Radio 103 . 1 and 810 WGY`
  - Contradiction count: 7
  - Analysis priority: 76.6

### Top contradiction findings
- Scale contradiction between:
  - `bankingnews.gr`: `Iranian missiles strike with 9x more efficiency against Israeli air defenses`
  - `heraldglobe.com`: `IDF strikes over 200 Iranian targets and 140 Hezbollah sites`
- Timeline contradiction between:
  - `punjabkesari.com`: `Iran rejected US 48-hour ceasefire proposal`
  - `gulfnews.com`: `On day 39 of conflict, Trump warns of complete demolition of infrastructure as Israel launches new strikes`
- Intent contradiction between:
  - `hellenicshippingnews.com`: `Trump touts Iran war escalation in next 2-3 weeks`
  - `thetimes-tribune.com`: `Trump uses Iran war address to urge skeptical electorate for more time`

### Contradiction-stage checks
- The contradiction detector is likely reflecting the breadth of the largest cluster.
- If the cluster is too broad, contradictions may be inflated by mixing neighboring sub-events instead of true same-event disagreement.
- The presence of a 7-contradiction event is a strong signal to inspect whether the cluster boundary is too loose.

## Manual Review Targets
1. Inspect the 14-article cluster first. It is the highest-risk merge.
2. Check the article list for cross-topic bleed, especially the Ukraine, inflation, and UK domestic items.
3. Compare the cluster title with the article titles to see whether consensus selection is picking the wrong representative source.
4. Review whether promoted/rejected filtering aligns with your expected topic policy.

## Likely Logic Risks
- Promotion may be too broad for topic-adjacent articles.
- Clustering may be merging at the event level more than the story level.
- Contradiction counts may be inflated when a cluster includes multiple sub-events.
- The cluster key and observation linkage may be unstable over time because they depend on mutable event fields.

## Re-run Command
```bash
cd /Users/theo/signal-intelligence
/Users/theo/signal-intelligence/.venv/bin/python backend/trace_article_pipeline.py --topic geopolitics --limit 40 --hours 96 --headline-corpus-only
```
