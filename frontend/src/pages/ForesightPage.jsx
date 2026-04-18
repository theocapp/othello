import useBeforeNewsArchive from '../hooks/useBeforeNewsArchive'
import usePredictionLedger from '../hooks/usePredictionLedger'
import { friendlyErrorMessage } from '../lib/formatters'
import { C } from '../constants/theme'
import { formatDateTime } from '../lib/formatters'

export default function ForesightPage({ mode, onClose }) {
  const {
    data: predictionLedgerResp,
    error: predictionLedgerError,
    isLoading: predictionLedgerLoading,
  } = usePredictionLedger()
  const {
    data: beforeNewsData,
    error: beforeNewsError,
    isLoading: beforeNewsLoading,
  } = useBeforeNewsArchive()

  const isPredictions = mode === 'predictions'
  const records = isPredictions
    ? (predictionLedgerResp?.predictions || [])
    : (beforeNewsData?.records || [])
  const loading = isPredictions ? predictionLedgerLoading : beforeNewsLoading
  const error = isPredictions
    ? (predictionLedgerError ? friendlyErrorMessage(predictionLedgerError, 'prediction ledger') : null)
    : (beforeNewsError ? friendlyErrorMessage(beforeNewsError, 'before-news archive') : null)
  const title = isPredictions ? 'Prediction Ledger' : 'Before It Was News'
  const subtitle = isPredictions ? 'Timestamped Othello forecasts and their tracked outcome status.' : 'Events Othello surfaced before major-source pickup, including open leads still awaiting broader confirmation.'

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'static', background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — FORESIGHT</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '1rem' }}>{isPredictions ? 'Forecast Audit Trail' : 'Early Signal Archive'}</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{title}</h1>
        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.98rem', color: C.textSecondary, lineHeight: 1.7, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{subtitle}</div>
        {loading && <div>{[0, 1, 2, 3].map(i => <div key={i} className="skeleton" style={{ height: i === 0 ? '1rem' : '0.75rem', width: i === 0 ? '75%' : `${90 - i * 8}%`, marginBottom: '0.5rem' }} />)}</div>}
        {!loading && error && <div style={{ border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '1rem 1.1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, marginBottom: '1rem' }}>{error}</div>}
        {!loading && !error && records.length === 0 && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary }}>{isPredictions ? 'No briefing-derived predictions have been logged yet.' : 'No early-signal records are available yet.'}</div>}
        {!loading && !error && records.map((item, i) => <div key={item.prediction_key || item.event_key || i} style={{ padding: '1rem 0.4rem', borderBottom: `1px solid ${C.border}` }}>
          {isPredictions ? <>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.35rem' }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.5 }}>{item.prediction_text}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: item.status === 'resolved_hit' ? C.silver : item.status === 'resolved_miss' ? C.red : C.textMuted, flexShrink: 0 }}>{(item.status || 'pending').replaceAll('_', ' ').toUpperCase()}</div></div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}><div>{item.topic} · {item.prediction_horizon_days} day horizon · logged {formatDateTime(new Date(item.created_at * 1000))}</div><div>{item.outcome_summary || 'Awaiting resolution from later reporting.'}</div></div>
          </> : <>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.35rem' }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.5 }}>{item.event_label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>+{Math.round(item.lead_time_hours || 0)}H</div></div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}><div>{item.status === 'awaiting_major_pickup' ? 'OPEN LEAD' : 'CONFIRMED LEAD'} · {item.topic}</div><div>{item.earliest_source || 'Unknown source'} before {item.earliest_major_source || 'major pickup not yet observed'}</div><div>Othello first saw it {formatDateTime(new Date(item.first_othello_seen_at * 1000))}</div></div>
          </>}
        </div>)}
      </div>
    </div>
  )
}
