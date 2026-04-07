import { C } from '../constants/theme'

const TREND_ARROWS = { rising: '▲', falling: '▼', stable: '—', new: '●' }
const TREND_COLORS = { rising: '#ef4444', falling: '#4ade80', stable: '#7d8794', new: '#60a5fa' }
const CONVERGENCE_COLORS = {
  crisis_escalation: '#ef4444',
  military_escalation: '#f97316',
  information_crisis: '#a855f7',
  conflict_spotlight: '#ef4444',
  emerging_situation: '#fbbf24',
  narrative_instability: '#8b5cf6',
  multi_signal: '#60a5fa',
}

export default function CorrelationPanel({ data, loading, error, onAnalyze }) {
  const cards = Array.isArray(data?.cards) ? data.cards : []

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#a855f7' }} />
          <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Signal Convergence</h2>
        </div>
        {data && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>{data.card_count} CONVERGENCES</div>}
      </div>
      <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />
      {loading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.75rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.8rem', width: '70%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.55rem', width: '90%' }} /></div>)}</div>}
      {!loading && error && <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>{error}</div>}
      {!loading && !error && cards.length === 0 && <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>No cross-domain convergences detected in the current window.</div>}
      {!loading && !error && cards.slice(0, 8).map(card => (
        <div key={card.country} className="theater-row" onClick={() => onAnalyze?.(card)} style={{ padding: '0.7rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.3rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <span style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textPrimary }}>{card.label}</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: CONVERGENCE_COLORS[card.convergence_type] || C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{String(card.convergence_type || 'unknown').replace(/_/g, ' ')}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', flexShrink: 0 }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textPrimary }}>{card.score}</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: TREND_COLORS[card.trend] || C.textMuted }}>{TREND_ARROWS[card.trend] || '—'}</span>
            </div>
          </div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, lineHeight: 1.6 }}>{card.domain_count} domains active: {(Array.isArray(card.active_domains) ? card.active_domains : []).map(d => String(d || '').replace(/_/g, ' ')).join(' · ')}</div>
          {card.convergence_description && <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.78rem', color: C.textSecondary, marginTop: '0.25rem', lineHeight: 1.5 }}>{card.convergence_description}</div>}
        </div>
      ))}
    </div>
  )
}
