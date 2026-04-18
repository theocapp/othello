import { C } from '../constants/theme'
import { formatDateTime } from '../lib/formatters'
import useCanonicalEvents from '../hooks/useCanonicalEvents'
import { friendlyErrorMessage } from '../lib/formatters'

function num(value, fallback = 0) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function growthSignal(event) {
  return num(event?.payload?.importance?.breakdown?.growth_novelty, 0)
}

function shortReason(event) {
  const first = (event?.importance_reasons || [])[0]
  return first || 'No primary reason emitted.'
}

function sectionRows(events, picker) {
  return picker(events).slice(0, 5)
}

function topImportance(events) {
  return [...events].sort((a, b) => num(b.importance_score) - num(a.importance_score))
}

function disputed(events) {
  return [...events]
    .filter(event => (event?.status || '').toLowerCase() === 'disputed' || num(event?.contradiction_count) > 0)
    .sort((a, b) => {
      const contradictionDelta = num(b?.contradiction_count) - num(a?.contradiction_count)
      if (contradictionDelta !== 0) return contradictionDelta
      return num(b?.importance_score) - num(a?.importance_score)
    })
}

function movers(events) {
  return [...events].sort((a, b) => {
    const growthDelta = growthSignal(b) - growthSignal(a)
    if (growthDelta !== 0) return growthDelta
    return num(b?.importance_score) - num(a?.importance_score)
  })
}

function EventRow({ event, onOpenEventDebug }) {
  return (
    <div style={{ padding: '0.58rem 0.2rem', borderBottom: `1px solid ${C.border}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.6rem', marginBottom: '0.24rem', flexWrap: 'wrap' }}>
        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.45 }}>
          {event?.label || event?.event_id}
        </div>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted }}>
          score {num(event?.importance_score).toFixed(1)}
        </div>
      </div>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', color: C.textMuted, lineHeight: 1.5, marginBottom: '0.35rem' }}>
        {formatDateTime(event?.last_updated_at)} · contradictions={num(event?.contradiction_count)} · growth={growthSignal(event).toFixed(2)}
      </div>
      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.76rem', color: C.textMuted, lineHeight: 1.55, marginBottom: '0.45rem' }}>
        {shortReason(event)}
      </div>
      <button
        onClick={() => onOpenEventDebug?.({ event_id: event?.event_id, headline: event?.label })}
        style={{
          background: 'none',
          border: `1px solid ${C.borderMid}`,
          color: C.silver,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: '0.44rem',
          letterSpacing: '0.1em',
          textTransform: 'uppercase',
          cursor: 'pointer',
          padding: '0.3rem 0.45rem',
        }}
      >
        Debug
      </button>
    </div>
  )
}

export default function DecisionSurfacePanel({
  onOpenEventDebug,
}) {
  const {
    data: canonicalEventsData,
    error: canonicalEventsError,
    isLoading: canonicalEventsLoading,
  } = useCanonicalEvents({ topic: null, limit: 160 })

  const events = canonicalEventsData?.events || []
  const canonicalEventsErrorText = canonicalEventsError
    ? friendlyErrorMessage(canonicalEventsError, 'canonical event feed')
    : null
  const topImportanceRows = sectionRows(events, topImportance)
  const disputedRows = sectionRows(events, disputed)
  const moverRows = sectionRows(events, movers)

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, marginBottom: '1rem' }}>
      <div style={{ padding: '0.95rem 1rem 0.85rem', borderBottom: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.7rem', marginBottom: '0.45rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red, boxShadow: `0 0 10px ${C.red}` }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase' }}>
            Decision Surface
          </div>
        </div>
        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', color: C.textMuted, lineHeight: 1.55 }}>
          Canonical event slices prioritized by importance, dispute intensity, and momentum.
        </div>
      </div>

      {canonicalEventsLoading && (
        <div style={{ padding: '0.9rem 1rem' }}>
          {[0, 1, 2].map(i => (
            <div key={i} style={{ marginBottom: '0.8rem' }}>
              <div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '42%' : '55%', marginBottom: '0.35rem' }} />
              <div className="skeleton" style={{ height: '0.7rem', width: '86%', marginBottom: '0.25rem' }} />
              <div className="skeleton" style={{ height: '0.7rem', width: '70%' }} />
            </div>
          ))}
        </div>
      )}

      {!canonicalEventsLoading && canonicalEventsErrorText && (
        <div style={{ padding: '0.9rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textSecondary }}>
          {canonicalEventsErrorText}
        </div>
      )}

      {!canonicalEventsLoading && !canonicalEventsErrorText && (
        <div className="coverage-summary" style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.85rem', padding: '0.9rem 1rem' }}>
          <div style={{ border: `1px solid ${C.borderMid}`, background: C.bg, padding: '0.6rem 0.7rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.silver, letterSpacing: '0.12em', marginBottom: '0.35rem' }}>WHAT MATTERS NOW</div>
            {topImportanceRows.length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted }}>No canonical events available.</div>}
            {topImportanceRows.map(event => <EventRow key={event.event_id} event={event} onOpenEventDebug={onOpenEventDebug} />)}
          </div>

          <div style={{ border: `1px solid ${C.borderMid}`, background: C.bg, padding: '0.6rem 0.7rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.silver, letterSpacing: '0.12em', marginBottom: '0.35rem' }}>NARRATIVES DISAGREE</div>
            {disputedRows.length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted }}>No active disputed events in this slice.</div>}
            {disputedRows.map(event => <EventRow key={event.event_id} event={event} onOpenEventDebug={onOpenEventDebug} />)}
          </div>

          <div style={{ border: `1px solid ${C.borderMid}`, background: C.bg, padding: '0.6rem 0.7rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.silver, letterSpacing: '0.12em', marginBottom: '0.35rem' }}>WHAT CHANGED 24H</div>
            {moverRows.length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted }}>No momentum signals available.</div>}
            {moverRows.map(event => <EventRow key={event.event_id} event={event} onOpenEventDebug={onOpenEventDebug} />)}
          </div>
        </div>
      )}
    </div>
  )
}
