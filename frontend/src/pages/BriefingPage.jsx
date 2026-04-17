import { useEffect, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { fetchBriefing } from '../api'
import { C } from '../constants/theme'
import { formatDateTime, friendlyErrorMessage, parseBriefingSections } from '../lib/formatters'
import { MD } from '../lib/markdown'

export default function BriefingPage({ topic, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetchBriefing(topic.id)
      .then(result => {
        setData(result)
        setError(null)
      })
      .catch(err => {
        setData(null)
        setError(friendlyErrorMessage(err, topic.label.toLowerCase()))
      })
      .finally(() => setLoading(false))
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [topic.id, topic.label])

  const parsed = data ? parseBriefingSections(data.briefing) : {}

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'static', background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — {topic.tag} BRIEFING</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 900, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '3rem' }}><div>{[50, 100, 75, 88, 80, 92, 68, 85, 78, 90].map((w, i) => <div key={i} className="skeleton" style={{ height: i === 0 ? '0.6rem' : i === 1 ? '2rem' : '0.85rem', width: `${w}%`, marginBottom: i === 1 ? '1.5rem' : '0.6rem' }} />)}</div><div>{[100, 80, 90, 70, 85, 75].map((w, i) => <div key={i} className="skeleton" style={{ height: '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />)}</div></div>}
        {!loading && data && <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '3rem', alignItems: 'start' }}>
          <div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>INTELLIGENCE BRIEFING — {topic.tag}</div>
            <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.5rem' }}>{topic.label}</h1>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })} — {data.article_count} sources</div>
            {['SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS', 'SIGNAL vs NOISE', 'DEEPER CONTEXT', 'SOURCE CONTRADICTIONS'].map(section => parsed[section] ? <div key={section} style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: section === 'SOURCE CONTRADICTIONS' ? C.red : C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{section}</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.8, color: C.textSecondary }}><ReactMarkdown components={MD}>{parsed[section]}</ReactMarkdown></div></div> : null)}
          </div>
          <div className="briefing-sidebar" style={{ position: 'sticky', top: '80px', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
            {parsed.PREDICTIONS && <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.25rem', background: C.bgRaised }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>PREDICTIONS</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}><ReactMarkdown components={MD}>{parsed.PREDICTIONS}</ReactMarkdown></div></div>}
            {parsed['WHAT TO WATCH'] && <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>WHAT TO WATCH</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}><ReactMarkdown components={MD}>{parsed['WHAT TO WATCH']}</ReactMarkdown></div></div>}
            <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SOURCES</div>
              {(Array.isArray(data.sources) ? data.sources : []).map((s, i, list) => <a key={i} href={s.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.6rem', paddingBottom: '0.6rem', borderBottom: i < list.length - 1 ? `1px solid ${C.border}` : 'none' }}><div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.4, marginBottom: '0.15rem' }}>{s.title}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, lineHeight: 1.6 }}><div>{s.source}</div><div>{formatDateTime(s.published_at)}</div></div></a>)}
            </div>
          </div>
        </div>}
        {!loading && !data && error && <div style={{ border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '1rem 1.1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>{error}</div>}
      </div>
    </div>
  )
}
