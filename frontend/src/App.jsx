import { useState, useEffect, useRef } from 'react'
import { fetchBriefing, fetchHeadlines, fetchEntitySignals, sendQuery, fetchTimeline } from './api'
import ReactMarkdown from 'react-markdown'

// ─── Design tokens ────────────────────────────────────────────────────────────
const C = {
  bg: '#13161a',
  bgRaised: '#1a1e24',
  bgHover: '#1e2329',
  border: '#1e2228',
  borderMid: '#2a2f38',
  textPrimary: '#f0f0f0',
  textSecondary: '#8b8f98',
  textMuted: '#3d4148',
  silver: '#9ba3af',
  red: '#ef4444',
  redDeep: '#dc2626',
  white: '#ffffff',
}

// ─── Markdown renderer ────────────────────────────────────────────────────────
const MD = {
  p: ({ children }) => <p style={{ marginBottom: '0.75rem', lineHeight: 1.85 }}>{children}</p>,
  strong: ({ children }) => <strong style={{ fontWeight: 700, color: C.textPrimary }}>{children}</strong>,
  h1: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.2rem', fontWeight: 700, margin: '1.25rem 0 0.5rem', color: C.textPrimary }}>{children}</div>,
  h2: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1rem', fontWeight: 700, margin: '1rem 0 0.4rem', color: C.textPrimary }}>{children}</div>,
  h3: ({ children }) => <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.62rem', letterSpacing: '0.15em', color: C.silver, margin: '0.75rem 0 0.35rem', textTransform: 'uppercase' }}>{children}</div>,
  li: ({ children }) => (
    <div style={{ display: 'flex', gap: '0.65rem', marginBottom: '0.45rem', alignItems: 'flex-start' }}>
      <span style={{ color: C.silver, flexShrink: 0, fontSize: '0.5rem', marginTop: '0.45rem' }}>◆</span>
      <span>{children}</span>
    </div>
  ),
  ul: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  ol: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  hr: () => <div style={{ borderTop: `1px solid ${C.border}`, margin: '1.25rem 0' }} />,
  table: ({ children }) => (
    <div style={{ overflowX: 'auto', margin: '0.75rem 0' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>{children}</table>
    </div>
  ),
  th: ({ children }) => <th style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.silver, padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.borderMid}`, textAlign: 'left', letterSpacing: '0.08em' }}>{children}</th>,
  td: ({ children }) => <td style={{ padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.border}`, color: C.textSecondary }}>{children}</td>,
  blockquote: ({ children }) => <div style={{ borderLeft: `2px solid ${C.silver}`, paddingLeft: '1rem', color: C.textSecondary, fontStyle: 'italic', margin: '0.75rem 0' }}>{children}</div>,
}

// ─── Deep dive full page ──────────────────────────────────────────────────────
function DeepDive({ title, query, onClose }) {
  const [content, setContent] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const data = await sendQuery(query)
        setContent(data.answer)
      } catch {
        setContent('Error generating analysis.')
      } finally {
        setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [query])

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <style>{`@keyframes slideIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }`}</style>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — DEEP ANALYSIS</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 740, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Intelligence Analysis</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{title}</h1>
        {loading && (
          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', padding: '2rem 0' }}>
            {[0, 1, 2].map(i => <div key={i} style={{ width: 6, height: 6, borderRadius: '50%', background: C.textMuted, animation: `pulse 1.2s ease ${i * 0.2}s infinite` }} />)}
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary, marginLeft: '0.5rem', letterSpacing: '0.1em' }}>ANALYZING...</span>
          </div>
        )}
        {content && !loading && (
          <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', lineHeight: 1.85, color: C.textSecondary }}>
            <ReactMarkdown components={MD}>{content}</ReactMarkdown>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Briefing full page ───────────────────────────────────────────────────────
function BriefingPage({ topic, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  const SECTIONS = [
    'SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS',
    'SIGNAL vs NOISE', 'PREDICTIONS', 'DEEPER CONTEXT',
    'WHAT TO WATCH', 'SOURCE CONTRADICTIONS'
  ]

  function parseBriefing(text) {
    const result = {}
    SECTIONS.forEach((section, i) => {
      const next = SECTIONS[i + 1]
      const escaped = section.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      const regex = new RegExp(
        `(?:#{1,3}\\s*)?${escaped}:?\\s*([\\s\\S]*?)${next ? `(?=(?:#{1,3}\\s*)?${next.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:?)` : '$'}`, 'i'
      )
      const match = text.match(regex)
      result[section] = match ? match[1].trim() : ''
    })
    return result
  }

  useEffect(() => {
    fetchBriefing(topic.id).then(setData).catch(console.error).finally(() => setLoading(false))
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [topic.id])

  const parsed = data ? parseBriefing(data.briefing) : {}

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — {topic.tag} BRIEFING</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 900, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && (
          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', padding: '2rem 0' }}>
            {[0, 1, 2].map(i => <div key={i} style={{ width: 6, height: 6, borderRadius: '50%', background: C.textMuted, animation: `pulse 1.2s ease ${i * 0.2}s infinite` }} />)}
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary, marginLeft: '0.5rem', letterSpacing: '0.1em' }}>LOADING BRIEFING...</span>
          </div>
        )}

        {!loading && data && (
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '3rem', alignItems: 'start' }}>
            <div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>INTELLIGENCE BRIEFING — {topic.tag}</div>
              <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.5rem' }}>{topic.label}</h1>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
                {new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })} — {data.article_count} sources
              </div>
              {['SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS', 'SIGNAL vs NOISE', 'DEEPER CONTEXT', 'SOURCE CONTRADICTIONS'].map(section => (
                parsed[section] ? (
                  <div key={section} style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: section === 'SOURCE CONTRADICTIONS' ? C.red : C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{section}</div>
                      <div style={{ flex: 1, height: '1px', background: C.border }} />
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.8, color: C.textSecondary }}>
                      <ReactMarkdown components={MD}>{parsed[section]}</ReactMarkdown>
                    </div>
                  </div>
                ) : null
              ))}
            </div>
            <div style={{ position: 'sticky', top: '80px', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
              {parsed['PREDICTIONS'] && (
                <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.25rem', background: C.bgRaised }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>PREDICTIONS</div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}>
                    <ReactMarkdown components={MD}>{parsed['PREDICTIONS']}</ReactMarkdown>
                  </div>
                </div>
              )}
              {parsed['WHAT TO WATCH'] && (
                <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>WHAT TO WATCH</div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}>
                    <ReactMarkdown components={MD}>{parsed['WHAT TO WATCH']}</ReactMarkdown>
                  </div>
                </div>
              )}
              <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SOURCES</div>
                {data.sources.map((s, i) => (
                  <a key={i} href={s.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.6rem', paddingBottom: '0.6rem', borderBottom: i < data.sources.length - 1 ? `1px solid ${C.border}` : 'none' }}>
                    <div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.4, marginBottom: '0.15rem', transition: 'color 0.15s' }}
                      onMouseEnter={e => e.target.style.color = C.textPrimary}
                      onMouseLeave={e => e.target.style.color = C.textSecondary}
                    >{s.title}</div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>{s.source}</div>
                  </a>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Timeline full page ───────────────────────────────────────────────────────
function TimelinePage({ query, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const result = await fetchTimeline(query)
        setData(result)
      } catch {
        setData({ error: true })
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
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — TIMELINE</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 800, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && (
          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', padding: '2rem 0' }}>
            {[0, 1, 2].map(i => <div key={i} style={{ width: 6, height: 6, borderRadius: '50%', background: C.textMuted, animation: `pulse 1.2s ease ${i * 0.2}s infinite` }} />)}
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary, marginLeft: '0.5rem', letterSpacing: '0.1em' }}>BUILDING TIMELINE...</span>
          </div>
        )}

        {data && !loading && !data.error && (
          <div style={{ animation: 'fadeIn 0.4s ease' }}>
            <div style={{ marginBottom: '3rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>CHRONOLOGICAL INTELLIGENCE TIMELINE</div>
              <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{data.title}</h1>
              <p style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', color: C.textSecondary, lineHeight: 1.6, fontStyle: 'italic' }}>{data.summary}</p>
              <div style={{ display: 'flex', gap: '1.5rem', marginTop: '1.5rem', paddingTop: '1rem', borderTop: `1px solid ${C.border}` }}>
                {[['HIGH', 'Major event'], ['MEDIUM', 'Significant development'], ['LOW', 'Background event']].map(([sig, label]) => (
                  <div key={sig} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <div style={{ width: significanceDot[sig], height: significanceDot[sig], borderRadius: '50%', background: significanceColor[sig], flexShrink: 0 }} />
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{label}</span>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ position: 'relative' }}>
              <div style={{ position: 'absolute', left: 120, top: 0, bottom: 0, width: '1px', background: C.border }} />
              {data.events?.map((event, i) => (
                <div key={i} style={{ display: 'flex', gap: 0, marginBottom: '2.5rem', animation: `fadeUp 0.4s ease ${i * 0.06}s both` }}>
                  <div style={{ width: 120, flexShrink: 0, paddingRight: '1.25rem', textAlign: 'right', paddingTop: '0.15rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, lineHeight: 1.4, letterSpacing: '0.05em' }}>
                      {(() => {
                        try {
                          const d = new Date(event.date)
                          return <><div style={{ color: C.textPrimary, fontWeight: 500 }}>{d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div><div>{d.getFullYear()}</div></>
                        } catch { return <div>{event.date}</div> }
                      })()}
                    </div>
                  </div>
                  <div style={{ position: 'relative', flexShrink: 0, display: 'flex', alignItems: 'flex-start', paddingTop: '0.2rem' }}>
                    <div style={{ width: significanceDot[event.significance] || 7, height: significanceDot[event.significance] || 7, borderRadius: '50%', background: significanceColor[event.significance] || C.textMuted, position: 'relative', zIndex: 1, transform: 'translateX(-50%)', boxShadow: event.significance === 'HIGH' ? `0 0 12px ${C.red}60` : 'none', flexShrink: 0 }} />
                  </div>
                  <div style={{ flex: 1, paddingLeft: '1.25rem' }}>
                    <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: event.significance === 'HIGH' ? '1.05rem' : '0.9rem', fontWeight: 700, color: event.significance === 'HIGH' ? C.textPrimary : C.textSecondary, lineHeight: 1.3, marginBottom: '0.4rem' }}>{event.headline}</div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.7, marginBottom: '0.35rem' }}>{event.description}</div>
                    {event.source && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{event.source}</div>}
                  </div>
                </div>
              ))}
              <div style={{ display: 'flex', alignItems: 'center', paddingLeft: 120, gap: '0.75rem' }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', border: `1px solid ${C.borderMid}`, transform: 'translateX(-50%)', flexShrink: 0 }} />
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, letterSpacing: '0.1em', paddingLeft: '1.25rem' }}>ONGOING</div>
              </div>
            </div>
          </div>
        )}

        {data?.error && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem', color: C.textSecondary }}>Not enough archived articles on this topic yet.</div>}
      </div>
    </div>
  )
}

// ─── Main App ─────────────────────────────────────────────────────────────────
export default function App() {
  const [time, setTime] = useState(new Date())
  const [headlines, setHeadlines] = useState([])
  const [headlinesLoading, setHeadlinesLoading] = useState(false)
  const [headlinesLoaded, setHeadlinesLoaded] = useState(false)
  const [entitySignals, setEntitySignals] = useState(null)
  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [headerVisible, setHeaderVisible] = useState(true)
  const [timelinePage, setTimelinePage] = useState(null)
  const lastScrollY = useRef(0)

  const TOPICS = [
    { id: 'geopolitics', label: 'Geopolitical Briefing', tag: 'WORLD', shortLabel: 'Geopolitics' },
    { id: 'economics', label: 'Economic Briefing', tag: 'MARKETS', shortLabel: 'Economics' },
  ]

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(timer)
  }, [])

  useEffect(() => {
    function handleScroll() {
      const currentY = window.scrollY
      if (currentY < 60) { setHeaderVisible(true); lastScrollY.current = currentY; return }
      setHeaderVisible(currentY < lastScrollY.current)
      lastScrollY.current = currentY
    }
    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  useEffect(() => {
    fetchEntitySignals().then(setEntitySignals).catch(console.error)
    loadHeadlines()
  }, [])

  function getGreeting() {
    const h = time.getHours()
    if (h < 12) return 'Good morning'
    if (h < 17) return 'Good afternoon'
    if (h < 21) return 'Good evening'
    return 'Good night'
  }

  async function loadHeadlines() {
    setHeadlinesLoading(true)
    setHeadlinesLoaded(false)
    try {
      const data = await fetchHeadlines()
      setHeadlines(data.stories || [])
      setHeadlinesLoaded(true)
    } catch (err) {
      console.error(err)
    } finally {
      setHeadlinesLoading(false)
    }
  }

  const dateStr = time.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })
  const timeStr = time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=Source+Serif+4:ital,opsz,wght@0,8..60,400;0,8..60,600;1,8..60,400&family=JetBrains+Mono:wght@400;500&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 3px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.borderMid}; border-radius: 2px; }
        @keyframes fadeUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
        @keyframes pulse { 0%,100% { opacity: 0.2; } 50% { opacity: 1; } }
        @keyframes slideIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        .headline-item:hover .headline-text { color: ${C.textPrimary} !important; }
        .entity-item:hover { background: ${C.bgHover} !important; cursor: pointer; }
        .briefing-pill:hover { background: ${C.textPrimary} !important; color: ${C.bg} !important; }
        .timeline-row:hover { background: ${C.bgHover} !important; }
      `}</style>

      {/* ── Floating header ── */}
      <div style={{ position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100, transform: headerVisible ? 'translateY(0)' : 'translateY(-100%)', transition: 'transform 0.3s ease', background: `${C.bg}e8`, backdropFilter: 'blur(16px)', borderBottom: `1px solid ${C.border}`, padding: '0.85rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.1rem', fontWeight: 700, letterSpacing: '-0.01em', color: C.textPrimary }}>OTHELLO</div>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.1em' }}>{timeStr}</div>
      </div>

      {/* ── Main content ── */}
      <div style={{ maxWidth: 760, margin: '0 auto', padding: '0 2rem' }}>

        {/* ── Section 1: Greeting ── */}
        <div style={{ paddingTop: '18vh', paddingBottom: '5vh', animation: 'fadeUp 0.6s ease both' }}>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(2rem, 5vw, 3rem)', fontWeight: 700, letterSpacing: '-0.03em', lineHeight: 1.1, color: C.textPrimary, marginBottom: '0.5rem' }}>
            {getGreeting()}.
          </div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.62rem', color: C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
            {dateStr} — {timeStr}
          </div>
        </div>

        {/* ── Section 2: Briefing pills ── */}
        <div style={{ paddingBottom: '5vh', display: 'flex', gap: '0.5rem', animation: 'fadeUp 0.6s ease 0.1s both' }}>
          {TOPICS.map(topic => (
            <button
              key={topic.id}
              className="briefing-pill"
              onClick={() => setBriefingPage(topic)}
              style={{
                background: 'none',
                border: `1px solid ${C.borderMid}`,
                color: C.textSecondary,
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '0.62rem',
                letterSpacing: '0.08em',
                padding: '0.5rem 1.1rem',
                cursor: 'pointer',
                transition: 'all 0.15s',
                textTransform: 'uppercase',
                borderRadius: '999px',
              }}
            >
              {topic.shortLabel} →
            </button>
          ))}
        </div>

        {/* ── Section 2.5: Timelines ── */}
        <div style={{ paddingBottom: '5vh', animation: 'fadeUp 0.6s ease 0.15s both' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '0.75rem' }}>
            Situation Timelines
          </div>
          <div style={{ height: '1px', background: C.border, marginBottom: '0.25rem' }} />
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            {[
              { label: 'US–Iran Military Conflict', query: 'US Iran military conflict war strikes' },
              { label: 'Russia–Ukraine War', query: 'Russia Ukraine war conflict' },
              { label: 'Federal Reserve & Interest Rates', query: 'Federal Reserve interest rates monetary policy' },
              { label: 'China–Taiwan Tensions', query: 'China Taiwan tensions military' },
            ].map((item, i) => (
              <div
                key={i}
                className="timeline-row"
                onClick={() => setTimelinePage(item.query)}
                style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.8rem 0.4rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', transition: 'background 0.15s', borderRadius: 2 }}
              >
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textSecondary }}>{item.label}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>VIEW →</div>
              </div>
            ))}
          </div>
          <div style={{ marginTop: '0.75rem', display: 'flex', gap: '0.5rem' }}>
            <input
              id="custom-timeline-input"
              placeholder="Custom timeline: type any topic..."
              style={{ flex: 1, background: C.bgRaised, border: `1px solid ${C.border}`, color: C.textPrimary, fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', padding: '0.6rem 0.9rem', outline: 'none', borderRadius: 2, transition: 'border-color 0.15s' }}
              onFocus={e => e.target.style.borderColor = C.silver}
              onBlur={e => e.target.style.borderColor = C.border}
              onKeyDown={e => { if (e.key === 'Enter' && e.target.value.trim()) { setTimelinePage(e.target.value.trim()); e.target.value = '' } }}
            />
            <button
              onClick={() => { const input = document.getElementById('custom-timeline-input'); if (input.value.trim()) { setTimelinePage(input.value.trim()); input.value = '' } }}
              style={{ background: 'none', border: `1px solid ${C.border}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', letterSpacing: '0.1em', padding: '0.6rem 1rem', cursor: 'pointer', transition: 'all 0.15s', borderRadius: 2 }}
              onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
              onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.color = C.textSecondary }}
            >
              GENERATE →
            </button>
          </div>
        </div>

        {/* ── Section 3: Breaking stories ── */}
        <div style={{ paddingBottom: '5vh', animation: 'fadeUp 0.6s ease 0.2s both' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} />
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Breaking Stories</div>
            </div>
            {headlinesLoaded && (
              <button onClick={loadHeadlines} style={{ background: 'none', border: 'none', color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', letterSpacing: '0.1em', cursor: 'pointer', padding: 0, transition: 'color 0.15s' }}
                onMouseEnter={e => e.target.style.color = C.textSecondary}
                onMouseLeave={e => e.target.style.color = C.textMuted}>
                ↺ Refresh
              </button>
            )}
          </div>
          <div style={{ height: '1px', background: C.border, marginBottom: '0.25rem' }} />

          {headlinesLoading && (
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', padding: '1.5rem 0' }}>
              {[0, 1, 2].map(i => <div key={i} style={{ width: 5, height: 5, borderRadius: '50%', background: C.textMuted, animation: `pulse 1.2s ease ${i * 0.2}s infinite` }} />)}
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, marginLeft: '0.5rem', letterSpacing: '0.08em' }}>Identifying top stories...</span>
            </div>
          )}

          {headlines.map((story, i) => (
            <div
              key={i}
              className="headline-item"
              onClick={() => setDeepDive({ title: story.headline, query: `Give me a comprehensive intelligence deep-dive on this story: "${story.headline}". Cover: what is actually happening beyond the surface narrative, key actors and their motivations, what mainstream media is missing or underreporting, historical parallels, geopolitical implications, and your probability assessments for how this develops. Be direct, analytical, and specific.` })}
              style={{ padding: '1.1rem 0.4rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', animation: `fadeUp 0.4s ease ${i * 0.08}s both`, borderRadius: 2, transition: 'background 0.15s' }}
              onMouseEnter={e => e.currentTarget.style.background = C.bgHover}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
            >
              <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '1rem' }}>
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
                    {i === 0 && <div style={{ width: 5, height: 5, borderRadius: '50%', background: C.red, flexShrink: 0 }} />}
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: i === 0 ? C.red : C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
                      {story.topic?.replace('_', ' ')}
                    </div>
                  </div>
                  <div className="headline-text" style={{ fontFamily: "'Libre Baskerville', serif", fontSize: i === 0 ? '1.25rem' : '0.98rem', fontWeight: 700, lineHeight: 1.25, letterSpacing: '-0.01em', color: C.textSecondary, marginBottom: '0.4rem', transition: 'color 0.15s' }}>
                    {story.headline}
                  </div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', color: C.textMuted, lineHeight: 1.6, fontStyle: 'italic' }}>
                    {story.summary}
                  </div>
                </div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, flexShrink: 0, marginTop: '0.2rem' }}>→</div>
              </div>
            </div>
          ))}
        </div>

        {/* ── Section 4: Entity tracker ── */}
        <div style={{ paddingBottom: '8vh', animation: 'fadeUp 0.6s ease 0.3s both' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '0.75rem' }}>
            Tracked Entities
          </div>
          <div style={{ height: '1px', background: C.border, marginBottom: '0.25rem' }} />

          {!entitySignals && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary, padding: '1rem 0' }}>Accumulating data...</div>}

          {entitySignals && (
            <div>
              {entitySignals.spikes?.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 5).length > 0 && (
                <div style={{ marginBottom: '1.5rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', padding: '0.6rem 0.4rem', marginBottom: '0.15rem' }}>
                    <div style={{ width: 5, height: 5, borderRadius: '50%', background: C.red }} />
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.red, letterSpacing: '0.15em' }}>SURGING</div>
                  </div>
                  {entitySignals.spikes.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 5).map((e, i) => (
                    <div
                      key={i}
                      className="entity-item"
                      onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they playing in current geopolitical events, why are they suddenly getting increased attention in the news, what are their motivations and capabilities, and what should we expect from them in the coming weeks? Be specific and analytical.` })}
                      style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.55rem 0.4rem', borderBottom: `1px solid ${C.border}`, transition: 'background 0.15s', borderRadius: 2 }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <span style={{ fontSize: '0.9rem', color: C.textPrimary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, letterSpacing: '0.08em' }}>{e.type}</span>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver }}>
                          {e.trend === 'NEW' ? 'NEW' : `${e.spike_ratio}×`}
                        </span>
                        <span style={{ color: C.textMuted, fontSize: '0.7rem' }}>→</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {entitySignals.top_entities?.length > 0 && (
                <div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, letterSpacing: '0.15em', padding: '0.6rem 0.4rem', marginBottom: '0.15rem' }}>MOST DISCUSSED THIS WEEK</div>
                  {entitySignals.top_entities.slice(0, 8).map((e, i) => (
                    <div
                      key={i}
                      className="entity-item"
                      onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they currently playing in world events, what are their key actions and motivations right now, and what should we be watching for? Be direct and analytically precise.` })}
                      style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.4rem', borderBottom: `1px solid ${C.border}`, transition: 'background 0.15s', borderRadius: 2 }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, width: '1rem' }}>{i + 1}</span>
                        <span style={{ fontSize: '0.88rem', color: C.textSecondary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>{e.mentions}</span>
                        <span style={{ color: C.textMuted, fontSize: '0.7rem' }}>→</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {deepDive && <DeepDive title={deepDive.title} query={deepDive.query} onClose={() => setDeepDive(null)} />}
      {briefingPage && <BriefingPage topic={briefingPage} onClose={() => setBriefingPage(null)} />}
      {timelinePage && <TimelinePage query={timelinePage} onClose={() => setTimelinePage(null)} />}
    </div>
  )
}