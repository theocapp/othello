import { C } from '../constants/theme'
import useContradictions from '../hooks/useContradictions'
import { formatDateTime, friendlyErrorMessage, truncateText } from '../lib/formatters'
import { totalNarrativeFlags } from '../lib/hotspots'

export default function ContradictionsPanel({ onOpenContradiction }) {
  const {
    data: contradictionEvents = [],
    error,
    isLoading,
  } = useContradictions(6)

  const errorText = error ? friendlyErrorMessage(error, 'narrative fractures') : null

  return (
    <div id="narrative-fractures" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
        <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} />
        <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Narrative Fractures</h2>
      </div>
      <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
      {isLoading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '88%' : '74%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.55rem', width: '45%' }} /></div>)}</div>}
      {!isLoading && contradictionEvents.length === 0 && <div style={{ padding: '0.95rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>{errorText || 'No stored framing-divergence events are currently surfaced in the active corpus; this view highlights sources using different framing for the same event.'}</div>}
      {!isLoading && contradictionEvents.map((event, i) => <div key={event.event_id || i} className="theater-row" onClick={() => onOpenContradiction?.(event)} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.25rem' }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.94rem', color: C.textSecondary, lineHeight: 1.5 }}>{event.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>{totalNarrativeFlags(event)} FLAGS</div></div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.6 }}><div>{formatDateTime(event.latest_update)}</div><div>{event.source_count} sources split across the cluster{(event.narrative_fracture_count || 0) > 0 ? ` · ${event.narrative_fracture_count} framing fractures` : ''}</div><div>{truncateText(event.summary, 140)}</div></div></div>)}
    </div>
  )
}
