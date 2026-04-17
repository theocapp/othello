import { useEffect, useMemo, useState } from 'react'
import { fetchCanonicalEventDebug, fetchEvaluationScorecard } from '../api'
import EventCorrectionPanel from '../components/EventCorrectionPanel'
import { C } from '../constants/theme'
import { formatDateTime, friendlyErrorMessage } from '../lib/formatters'

function Section({ title, children, note = null }) {
  return (
    <section style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.14em', textTransform: 'uppercase' }}>{title}</div>
        {note && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, letterSpacing: '0.08em' }}>{note}</div>}
      </div>
      {children}
    </section>
  )
}

function CountChip({ label, value, tone = C.textSecondary }) {
  return (
    <div style={{ border: `1px solid ${C.borderMid}`, background: C.bg, padding: '0.55rem 0.7rem', minWidth: '7.4rem' }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.2rem' }}>{label}</div>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: tone }}>{value}</div>
    </div>
  )
}

export default function EventDebugOverlay({ eventId, onClose }) {
  const [payload, setPayload] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [scorecard, setScorecard] = useState(null)
  const [scorecardLoading, setScorecardLoading] = useState(false)
  const [scorecardError, setScorecardError] = useState(null)

  useEffect(() => {
    let cancelled = false
    async function load() {
      if (!eventId) {
        setPayload(null)
        setLoading(false)
        setScorecard(null)
        setScorecardLoading(false)
        setScorecardError(null)
        return
      }
      setLoading(true)
      setScorecard(null)
      setScorecardError(null)
      try {
        const data = await fetchCanonicalEventDebug(eventId)
        if (!cancelled) {
          setPayload(data)
          setError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setError(friendlyErrorMessage(err, 'event debug payload'))
          setPayload(null)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
    return () => {
      cancelled = true
    }
  }, [eventId])

  const scorecardTopic = (payload?.event?.topic || '').trim().toLowerCase() || null

  useEffect(() => {
    let cancelled = false

    async function loadScorecard() {
      if (!eventId || !payload) {
        setScorecard(null)
        setScorecardLoading(false)
        setScorecardError(null)
        return
      }

      setScorecardLoading(true)
      try {
        const data = await fetchEvaluationScorecard({
          topic: scorecardTopic,
          limitFiles: 120,
        })
        if (!cancelled) {
          setScorecard(data)
          setScorecardError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setScorecard(null)
          setScorecardError(friendlyErrorMessage(err, 'evaluation scorecard'))
        }
      } finally {
        if (!cancelled) setScorecardLoading(false)
      }
    }

    loadScorecard()
    return () => {
      cancelled = true
    }
  }, [eventId, payload, scorecardTopic])

  const frameDistribution = useMemo(() => {
    const out = {}
    for (const perspective of payload?.perspectives || []) {
      const frame = (perspective?.dominant_frame || 'unlabeled').toLowerCase()
      out[frame] = (out[frame] || 0) + 1
    }
    return Object.entries(out).sort((a, b) => b[1] - a[1])
  }, [payload])

  const evidenceByObservation = useMemo(() => {
    const grouped = new Map()
    for (const row of payload?.cluster_assignment_evidence || []) {
      const key = row?.observation_key || 'unknown'
      const existing = grouped.get(key) || []
      existing.push(row)
      grouped.set(key, existing)
    }
    return Array.from(grouped.entries()).map(([observationKey, rows]) => ({
      observationKey,
      rows: rows.sort((a, b) => (b.final_score || 0) - (a.final_score || 0)),
    }))
  }, [payload])

  const event = payload?.event || {}
  const importance = event?.importance || {}
  const counts = payload?.counts || {}
  const clusteringSummary = scorecard?.kind_summaries?.clustering || null
  const cohesionMetrics = scorecard?.operational_metrics?.cluster_cohesion || {}

  function formatRate(value) {
    return typeof value === 'number' ? `${(value * 100).toFixed(1)}%` : 'n/a'
  }

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 220, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'static', background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — EVENT DEBUG</div>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, letterSpacing: '0.08em' }}>{eventId}</div>
      </div>

      <div style={{ maxWidth: 1180, margin: '0 auto', padding: '2.2rem 1.4rem 5rem' }}>
        {loading && (
          <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1.2rem' }}>
            <div className="skeleton" style={{ height: '1.2rem', width: '58%', marginBottom: '0.8rem' }} />
            <div className="skeleton" style={{ height: '0.78rem', width: '40%', marginBottom: '0.55rem' }} />
            <div className="skeleton" style={{ height: '0.78rem', width: '84%', marginBottom: '0.55rem' }} />
            <div className="skeleton" style={{ height: '0.78rem', width: '72%' }} />
          </div>
        )}

        {!loading && error && (
          <div style={{ border: `1px solid ${C.redDeep}`, background: `${C.redDeep}16`, padding: '1rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.4rem' }}>Debug payload unavailable</div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.6 }}>{error}</div>
          </div>
        )}

        {!loading && !error && payload && (
          <>
            <div style={{ marginBottom: '1rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.16em', textTransform: 'uppercase', marginBottom: '0.6rem' }}>Analyst Debug Surface</div>
              <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.4rem, 3.4vw, 2.2rem)', color: C.textPrimary, lineHeight: 1.2, marginBottom: '0.65rem' }}>{event?.label || 'Unknown event'}</h1>
              <div style={{ display: 'flex', gap: '0.8rem', flexWrap: 'wrap', marginBottom: '0.85rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{(event?.topic || 'unknown').toUpperCase()}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.08em' }}>{(event?.status || 'developing').toUpperCase()}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.08em' }}>UPDATED {formatDateTime(event?.last_updated_at)}</div>
              </div>
              <div style={{ display: 'flex', gap: '0.55rem', flexWrap: 'wrap' }}>
                <CountChip label="Importance" value={Number(importance?.score || 0).toFixed(1)} tone={C.red} />
                <CountChip label="Articles" value={counts?.articles || 0} />
                <CountChip label="Perspectives" value={counts?.perspectives || 0} />
                <CountChip label="Claims" value={counts?.claims || 0} />
                <CountChip label="Contradictions" value={counts?.contradictions || 0} />
                <CountChip label="Identity events" value={counts?.identity_events || 0} />
              </div>
            </div>

            <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 320px', gap: '1rem', alignItems: 'start' }}>
              <div style={{ display: 'grid', gap: '1rem' }}>
                <Section title="Importance rationale" note="Top contributors and scoring context">
                  {(importance?.reasons || []).length > 0 ? (
                    <div style={{ display: 'grid', gap: '0.5rem', marginBottom: '0.75rem' }}>
                      {(importance.reasons || []).map((reason, idx) => (
                        <div key={`${reason}-${idx}`} style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.6 }}>
                          {reason}
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No explicit reasons were emitted for this event.</div>
                  )}
                  <pre style={{ marginTop: '0.2rem', maxHeight: 240, overflow: 'auto', background: C.bg, border: `1px solid ${C.borderMid}`, padding: '0.65rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, lineHeight: 1.55 }}>
{JSON.stringify(importance?.breakdown || {}, null, 2)}
                  </pre>
                </Section>

                <Section title="Cluster assignment evidence" note="Per-article assignment rules and score traces">
                  {evidenceByObservation.length === 0 && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No cluster assignment evidence rows were found for this event.</div>
                  )}
                  {evidenceByObservation.map(group => (
                    <div key={group.observationKey} style={{ marginBottom: '0.9rem', border: `1px solid ${C.borderMid}`, background: C.bg }}>
                      <div style={{ padding: '0.5rem 0.65rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, letterSpacing: '0.09em' }}>
                        {group.observationKey} · {group.rows.length} rows
                      </div>
                      {group.rows.slice(0, 12).map((row, idx) => (
                        <div key={`${row.article_url}-${idx}`} style={{ padding: '0.55rem 0.65rem', borderBottom: idx < Math.min(group.rows.length, 12) - 1 ? `1px solid ${C.border}` : 'none' }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.75rem', marginBottom: '0.25rem', flexWrap: 'wrap' }}>
                            <a href={row.article_url} target="_blank" rel="noreferrer" style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.8rem', color: C.textSecondary, textDecoration: 'underline', textUnderlineOffset: 2 }}>
                              {row.payload?.source || row.article_url}
                            </a>
                            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted }}>
                              score {Number(row.final_score || 0).toFixed(3)} · {row.rule}
                            </div>
                          </div>
                          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, lineHeight: 1.5 }}>
                            entity_overlap={row.entity_overlap || 0} · anchor_overlap={row.anchor_overlap || 0} · keyword_overlap={row.keyword_overlap || 0} · time_gap_hours={row.time_gap_hours ?? 'n/a'}
                          </div>
                        </div>
                      ))}
                    </div>
                  ))}
                </Section>

                <Section title="Perspectives and framing" note="Source-level perspectives and dominant frames">
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.45rem', marginBottom: '0.75rem' }}>
                    {frameDistribution.length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No frame distribution available.</div>}
                    {frameDistribution.map(([frame, count]) => (
                      <div key={frame} style={{ border: `1px solid ${C.borderMid}`, background: C.bg, padding: '0.4rem 0.55rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.08em' }}>
                        {frame}: {count}
                      </div>
                    ))}
                  </div>
                  {(payload.perspectives || []).slice(0, 12).map((row, idx) => (
                    <div key={`${row.perspective_id || idx}`} style={{ padding: '0.5rem 0', borderTop: idx === 0 ? `1px solid ${C.border}` : 'none', borderBottom: `1px solid ${C.border}` }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.75rem', flexWrap: 'wrap' }}>
                        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary }}>{row.source_name || 'Unknown source'}</div>
                        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted }}>
                          frame={row.dominant_frame || 'unlabeled'} · trust={row.source_trust_tier || 'n/a'}
                        </div>
                      </div>
                      {row.claim_text && (
                        <div style={{ marginTop: '0.28rem', fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', color: C.textMuted, lineHeight: 1.6 }}>
                          {row.claim_text}
                        </div>
                      )}
                    </div>
                  ))}
                </Section>

                <Section title="Claims and contradictions" note="Cross-source conflict trace">
                  <div style={{ display: 'grid', gap: '0.9rem' }}>
                    <div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, letterSpacing: '0.11em', marginBottom: '0.4rem' }}>CLAIMS</div>
                      {(payload.claims || []).length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No claim records for the selected event.</div>}
                      {(payload.claims || []).slice(0, 10).map((row, idx) => (
                        <div key={`${row.claim_record_key || idx}`} style={{ padding: '0.45rem 0', borderBottom: `1px solid ${C.border}` }}>
                          <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.6 }}>{row.claim_text || 'Unnamed claim'}</div>
                          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted }}>
                            {row.source_name || 'unknown source'} · {row.resolution_status || 'unresolved'} · obs={row.observation_key || 'n/a'}
                          </div>
                        </div>
                      ))}
                    </div>

                    <div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, letterSpacing: '0.11em', marginBottom: '0.4rem' }}>CONTRADICTIONS</div>
                      {(payload.contradictions || []).length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No contradiction snapshots for this event.</div>}
                      {(payload.contradictions || []).slice(0, 8).map((row, idx) => (
                        <div key={`${row.event_key || idx}`} style={{ padding: '0.45rem 0', borderBottom: `1px solid ${C.border}` }}>
                          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, marginBottom: '0.2rem' }}>
                            obs={row.observation_key || row.event_key || 'n/a'} · contradictions={row.contradiction_count || 0}
                          </div>
                          {(row.contradictions || []).slice(0, 2).map((item, itemIdx) => (
                            <div key={`${itemIdx}-${item.conflict_type || 'unknown'}`} style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', color: C.textMuted, lineHeight: 1.55 }}>
                              {item.conflict_type || 'conflict'}: {item.reasoning || item.claim_a || 'No detail'}
                            </div>
                          ))}
                        </div>
                      ))}
                    </div>
                  </div>
                </Section>

                <Section title="Raw payload" note="Copy-ready debug JSON">
                  <pre style={{ margin: 0, maxHeight: 420, overflow: 'auto', background: C.bg, border: `1px solid ${C.borderMid}`, padding: '0.65rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, lineHeight: 1.55 }}>
{JSON.stringify(payload, null, 2)}
                  </pre>
                </Section>
              </div>

              <aside className="briefing-sidebar" style={{ display: 'grid', gap: '1rem', position: 'sticky', top: '88px' }}>
                <Section title="System health" note="Latest evaluation scorecard snapshot">
                  {scorecardLoading && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>
                      Loading scorecard snapshot...
                    </div>
                  )}
                  {!scorecardLoading && scorecardError && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red }}>
                      {scorecardError}
                    </div>
                  )}
                  {!scorecardLoading && !scorecardError && scorecard && (
                    <div style={{ display: 'grid', gap: '0.45rem' }}>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted }}>
                        generated {formatDateTime(scorecard.generated_at)}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        labels_considered={scorecard.records_considered || 0} · invalid={scorecard.invalid_records || 0}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        clustering_agreement={formatRate(clusteringSummary?.agreement_rate)}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        cohesion_mean_relatedness={
                          typeof cohesionMetrics.avg_mean_relatedness === 'number'
                            ? cohesionMetrics.avg_mean_relatedness.toFixed(3)
                            : 'n/a'
                        }
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        cohesion_outlier_ratio={formatRate(cohesionMetrics.avg_outlier_ratio)}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        cohesion_high_outlier_threshold={formatRate(cohesionMetrics.high_outlier_threshold)}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary }}>
                        cohesion_outlier_p75={formatRate(cohesionMetrics.outlier_ratio_p75)} · p90={formatRate(cohesionMetrics.outlier_ratio_p90)}
                      </div>
                    </div>
                  )}
                  {!scorecardLoading && !scorecardError && !scorecard && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>
                      No scorecard snapshot available.
                    </div>
                  )}
                </Section>

                <Section title="Identity history" note="Recent resolver events">
                  {(payload.identity_history || []).length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No identity history entries.</div>}
                  {(payload.identity_history || []).slice(0, 16).map((row, idx) => (
                    <div key={`${row.created_at || idx}`} style={{ padding: '0.4rem 0', borderBottom: `1px solid ${C.border}` }}>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{row.action || 'unknown_action'}</div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', color: C.textMuted }}>
                        {row.observation_key || 'n/a'} · conf={row.confidence ?? 'n/a'}
                      </div>
                    </div>
                  ))}
                </Section>

                <Section title="Observation keys" note="Identity map projections">
                  {(payload.observation_keys || []).length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No observation keys.</div>}
                  {(payload.observation_keys || []).slice(0, 40).map((obs, idx) => (
                    <div key={`${obs}-${idx}`} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textSecondary, padding: '0.2rem 0', borderBottom: `1px solid ${C.border}` }}>
                      {obs}
                    </div>
                  ))}
                </Section>

                <Section title="Article pack" note="Citations in current payload">
                  {(payload.articles || []).length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>No article metadata included.</div>}
                  {(payload.articles || []).slice(0, 14).map((article, idx) => (
                    <a key={`${article.url || idx}`} href={article.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', borderBottom: `1px solid ${C.border}`, padding: '0.45rem 0' }}>
                      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.78rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.2rem' }}>{article.title || article.url}</div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted }}>
                        {(article.source || 'unknown source')} · {formatDateTime(article.published_at)}
                      </div>
                    </a>
                  ))}
                </Section>

                <EventCorrectionPanel eventId={eventId} payload={payload} />
              </aside>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
