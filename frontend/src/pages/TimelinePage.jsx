import { useEffect, useState } from 'react'
import { fetchTimeline } from '../api'
import { C } from '../constants/theme'
import { friendlyErrorMessage } from '../lib/formatters'

export default function TimelinePage({ query, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const result = await fetchTimeline(query)
        setData(result)
        setError(null)
      } catch (err) {
        setData({ error: true })
        setError(friendlyErrorMessage(err, 'timeline'))
      } finally {
        setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [query])

  const significanceColor = { HIGH: C.red, MEDIUM: C.silver, LOW: C.textMuted }
  const significanceDot = { HIGH: 10, MEDIUM: 7, LOW: 5 }

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — TIMELINE</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 800, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && <div><div className="skeleton" style={{ height: '2rem', width: '70%', marginBottom: '1rem' }} /><div className="skeleton" style={{ height: '0.85rem', width: '90%', marginBottom: '2rem' }} />{[0, 1, 2, 3, 4].map(i => <div key={i} style={{ display: 'flex', gap: 0, marginBottom: '2rem' }}><div style={{ width: 120, flexShrink: 0, paddingRight: '1.25rem', display: 'flex', justifyContent: 'flex-end' }}><div className="skeleton" style={{ height: '0.7rem', width: '80%' }} /></div><div style={{ flex: 1, paddingLeft: '1.5rem' }}><div className="skeleton" style={{ height: '0.9rem', width: '75%', marginBottom: '0.4rem' }} /><div className="skeleton" style={{ height: '0.75rem', width: '95%', marginBottom: '0.3rem' }} /><div className="skeleton" style={{ height: '0.75rem', width: '60%' }} /></div></div>)}</div>}
        {data && !loading && !data.error && <div style={{ animation: 'fadeIn 0.4s ease' }}>
          <div style={{ marginBottom: '3rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>CHRONOLOGICAL INTELLIGENCE TIMELINE</div>
            <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{data.title}</h1>
            <p style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', color: C.textSecondary, lineHeight: 1.6, fontStyle: 'italic' }}>{data.summary}</p>
            <div style={{ display: 'flex', gap: '1.5rem', marginTop: '1.5rem', paddingTop: '1rem', borderTop: `1px solid ${C.border}`, flexWrap: 'wrap' }}>{[['HIGH', 'Major event'], ['MEDIUM', 'Significant development'], ['LOW', 'Background event']].map(([sig, label]) => <div key={sig} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><div style={{ width: significanceDot[sig], height: significanceDot[sig], borderRadius: '50%', background: significanceColor[sig], flexShrink: 0 }} /><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{label}</span></div>)}</div>
          </div>
          <div style={{ position: 'relative' }}>
            <div style={{ position: 'absolute', left: 120, top: 0, bottom: 0, width: '1px', background: C.border }} />
            {data.events?.map((event, i) => <div key={i} style={{ display: 'flex', gap: 0, marginBottom: '2.5rem', animation: `fadeUp 0.4s ease ${i * 0.06}s both` }}><div style={{ width: 120, flexShrink: 0, paddingRight: '1.25rem', textAlign: 'right', paddingTop: '0.15rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, lineHeight: 1.4, letterSpacing: '0.05em' }}>{(() => { try { const d = new Date(event.date); return <><div style={{ color: C.textPrimary, fontWeight: 500 }}>{d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div><div>{d.getFullYear()}</div><div>{d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })}</div></> } catch { return <div>{event.date}</div> } })()}</div></div><div style={{ position: 'relative', flexShrink: 0, display: 'flex', alignItems: 'flex-start', paddingTop: '0.2rem' }}><div style={{ width: significanceDot[event.significance] || 7, height: significanceDot[event.significance] || 7, borderRadius: '50%', background: significanceColor[event.significance] || C.textMuted, position: 'relative', zIndex: 1, transform: 'translateX(-50%)', boxShadow: event.significance === 'HIGH' ? `0 0 12px ${C.red}60` : 'none', flexShrink: 0 }} /></div><div style={{ flex: 1, paddingLeft: '1.25rem' }}><div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: event.significance === 'HIGH' ? '1.05rem' : '0.9rem', fontWeight: 700, color: event.significance === 'HIGH' ? C.textPrimary : C.textSecondary, lineHeight: 1.3, marginBottom: '0.4rem' }}>{event.headline}</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.7, marginBottom: '0.35rem' }}>{event.description}</div>{event.source && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{event.source}</div>}</div></div>)}
            <div style={{ display: 'flex', alignItems: 'center', paddingLeft: 120, gap: '0.75rem' }}><div style={{ width: 8, height: 8, borderRadius: '50%', border: `1px solid ${C.borderMid}`, transform: 'translateX(-50%)', flexShrink: 0 }} /><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, letterSpacing: '0.1em', paddingLeft: '1.25rem' }}>ONGOING</div></div>
          </div>
        </div>}
        {data?.error && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem', color: C.textSecondary }}>{error || 'Not enough archived articles on this topic yet.'}</div>}
      </div>
    </div>
  )
}
