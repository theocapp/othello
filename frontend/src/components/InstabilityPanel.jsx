import { C } from '../constants/theme'
import useInstability from '../hooks/useInstability'
import { friendlyErrorMessage } from '../lib/formatters'

const LEVEL_COLORS = { critical: '#ef4444', high: '#f97316', elevated: '#fbbf24', low: '#4ade80' }
const TREND_ARROWS = { rising: '▲', falling: '▼', stable: '—', new: '●' }
const TREND_COLORS = { rising: '#ef4444', falling: '#4ade80', stable: '#7d8794', new: '#60a5fa' }

export default function InstabilityPanel({ onAnalyze }) {
  const {
    data,
    error,
    isLoading: loading,
  } = useInstability(3)

  const errorText = error ? friendlyErrorMessage(error, 'instability index') : null
  const countries = data?.countries || []
  const topCountries = countries.slice(0, 12)

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#f97316' }} />
          <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Country Instability Index</h2>
        </div>
        {data && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>{data.country_count} COUNTRIES · {data.window_days}D WINDOW</div>}
      </div>
      <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />

      {loading && <div>{[0, 1, 2, 3, 4].map(i => <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.55rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.75rem', width: '50%' }} /><div className="skeleton" style={{ height: '0.75rem', width: '12%' }} /></div>)}</div>}
      {!loading && errorText && <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>{errorText}</div>}
      {!loading && !errorText && topCountries.length === 0 && <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>No instability data available yet — structured events may still be ingesting.</div>}
      {!loading && !errorText && topCountries.map((country, i) => (
        <div key={country.country} className="theater-row" onClick={() => onAnalyze?.(country)} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.55rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', minWidth: 0 }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, width: '1rem', textAlign: 'right', flexShrink: 0 }}>{i + 1}</div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{country.label}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: LEVEL_COLORS[country.level] || C.textMuted, letterSpacing: '0.1em', textTransform: 'uppercase', flexShrink: 0 }}>{country.level}</div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', flexShrink: 0 }}>
            <div style={{ width: 60, height: 4, background: C.bg, borderRadius: 2, overflow: 'hidden' }}><div style={{ width: `${Math.min(100, country.score)}%`, height: '100%', background: LEVEL_COLORS[country.level] || C.textMuted, borderRadius: 2 }} /></div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textPrimary, width: '2rem', textAlign: 'right' }}>{country.score}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: TREND_COLORS[country.trend] || C.textMuted, width: '0.8rem', textAlign: 'center' }}>{TREND_ARROWS[country.trend] || '—'}</div>
          </div>
        </div>
      ))}
      {!loading && !errorText && countries.length > 12 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, padding: '0.6rem 0.2rem', letterSpacing: '0.06em' }}>+ {countries.length - 12} more countries tracked</div>}
    </div>
  )
}
