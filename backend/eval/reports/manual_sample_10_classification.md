# Manual 10-Case Audit of Hard Clustering Failures (2026-04-07)

Sample source: first 10 IDs from current hard-failure set (78 total).

## Classification rubric
- SAME incident: should cluster.
- SAME conflict, different incident/frame: should not cluster.

## Results

1. generated_0013
- A: Why Iran believes it can outlast the US
- B: Analyst says that Iran’s interest is in an extended war
- Verdict: SAME conflict, different analysis framing (not same incident)

2. generated_0014
- A: Trump warns Iran deadline nearing, says strikes killed leaders
- B: Strikes between Israel and Iran continue
- Verdict: SAME conflict timeline updates, not clearly one incident

3. generated_0036
- A: Iran warns of 'big surprise' for US, Israel
- B: Middle East war intensifies with first strikes by Houthis
- Verdict: Different actions/actors; not same incident

4. generated_0044
- A: Italy’s Meloni meets Qatar emir to discuss energy issues amid Iran war
- B: Iran war live: Trump to address nation; Tehran denies seeking ceasefire
- Verdict: Diplomatic meeting vs live war update; not same incident

5. generated_0046
- A: US and Iran trade threats to unleash 'hell' as search for missing US airman continues
- B: As war rages, Iranian politicians push for exit from nuclear weapons treaty
- Verdict: Military/search update vs domestic policy signal; not same incident

6. generated_0048
- A: Will China join Pakistan-led efforts to mediate US-Iran peace?
- B: Asian shares fall and oil climbs on Iran war uncertainties
- Verdict: Mediation diplomacy vs market reaction; not same incident

7. generated_0051
- A: How will Pakistan deal with the fallout from Iran war?
- B: War on Iran: US’s history of making other nations pay for conflicts
- Verdict: Country-specific fallout vs historical/policy explainer; not same incident

8. generated_0052
- A: Analysis: 1 month into the war, Iran is fighting Israel and the US with insurgent tactics
- B: Qatari PM and US officials discuss strategic ties amid Iran war
- Verdict: Analysis piece vs diplomatic meeting; not same incident

9. generated_0055
- A: Iran War: What a Marine Expeditionary Unit is – and other US military terms
- B: Qatar says Iran’s attacks on neighbours crossed ‘many red lines’
- Verdict: Military explainer vs diplomatic response; not same incident

10. generated_0057
- A: War on Iran: Three key takeaways from Araghchi’s interview with Al Jazeera
- B: Guns in the streets as US, Israel intensify month-long attacks across Iran
- Verdict: Interview takeaways vs field incident update; not same incident

## Aggregate outcome (10-case sample)
- SAME incident (should cluster): 0/10
- SAME conflict only (should not cluster at incident granularity): 10/10

## Interpretation
This sample suggests most remaining "hard failures" are likely labeling/objective mismatch (conflict-level grouping) rather than representation misses at incident-level clustering.
