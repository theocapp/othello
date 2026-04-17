import { useEffect, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { fetchEntityReference, sendQuery } from '../api'
import { C } from '../constants/theme'
import { formatDateTime, friendlyErrorMessage } from '../lib/formatters'
import { MD } from '../lib/markdown'

export default function DeepDive({ title, query, entityName, queryTopic, regionContext, hotspotId, storyEventId, sourceUrls, attentionWindow, onClose }) {
  const [content, setContent] = useState(null)
  const [sources, setSources] = useState([])
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [entityReference, setEntityReference] = useState(null)
  const [referenceLoading, setReferenceLoading] = useState(Boolean(entityName))
  const [referenceError, setReferenceError] = useState(null)

  useEffect(() => {
    setLoading(true)
    async function load() {
      try {
        const data = await sendQuery(query, {
          topic: queryTopic || undefined,
          regionContext: regionContext || undefined,
          hotspotId: hotspotId || undefined,
          storyEventId: storyEventId || undefined,
          attentionWindow: attentionWindow || undefined,
          sourceUrls: sourceUrls?.length ? sourceUrls : undefined,
        })
        setContent(data.answer)
        setSources(data.sources || [])
        setMeta(data)
        setError(null)
      } catch (err) {
        setError(friendlyErrorMessage(err, 'analysis'))
        setContent('Error generating analysis.')
        setSources([])
        setMeta(null)
      } finally {
        setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [attentionWindow, hotspotId, query, queryTopic, regionContext, sourceUrls, storyEventId])

  useEffect(() => {
    let cancelled = false
    async function loadReference() {
      if (!entityName) {
        setEntityReference(null)
        setReferenceLoading(false)
        setReferenceError(null)
        return
      }
      setReferenceLoading(true)
      try {
        const data = await fetchEntityReference(entityName)
        if (!cancelled) {
          setEntityReference(data)
          setReferenceError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setReferenceError(friendlyErrorMessage(err, 'entity reference'))
          setEntityReference(null)
        }
      } finally {
        if (!cancelled) setReferenceLoading(false)
      }
    }
    loadReference()
    return () => { cancelled = true }
  }, [entityName])

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'static', background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — DEEP ANALYSIS</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 740, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Intelligence Analysis</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{title}</h1>
        {loading && <div>{[100, 85, 92, 70, 88, 95, 60, 80].map((w, i) => <div key={i} className="skeleton" style={{ height: i === 0 ? '1.1rem' : '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />)}</div>}
        {content && !loading && <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 280px', gap: '2rem' }} className="briefing-layout">
          <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', lineHeight: 1.85, color: C.textSecondary }}>
            {error && <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>{error}</div>}
            <ReactMarkdown components={MD}>{content}</ReactMarkdown>
          </div>
          <aside style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }} className="briefing-sidebar">
            {entityName && <div style={{ border: `1px solid ${C.border}`, padding: '1rem', background: C.bgRaised }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>REFERENCE CONTEXT</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6, marginBottom: '0.8rem' }}>Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.</div>
              {referenceLoading && <div><div className="skeleton" style={{ height: '0.8rem', width: '68%', marginBottom: '0.6rem' }} /><div className="skeleton" style={{ height: '0.7rem', width: '100%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.7rem', width: '92%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.7rem', width: '84%' }} /></div>}
              {!referenceLoading && referenceError && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, lineHeight: 1.6 }}>{referenceError}</div>}
              {!referenceLoading && !referenceError && entityReference?.status === 'ok' && <div>
                {entityReference.thumbnail_url && <img src={entityReference.thumbnail_url} alt={entityReference.title || entityName} style={{ width: '100%', borderRadius: 4, marginBottom: '0.85rem', border: `1px solid ${C.border}` }} />}
                <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.92rem', color: C.textPrimary, lineHeight: 1.35, marginBottom: '0.65rem' }}>{entityReference.title || entityName}</div>
                {entityReference.summary && <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.65, marginBottom: '0.85rem' }}>{entityReference.summary}</div>}
                {entityReference.url && <a href={entityReference.url} target="_blank" rel="noreferrer" style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.silver, letterSpacing: '0.1em', textDecoration: 'none' }}>OPEN WIKIPEDIA →</a>}
              </div>}
            </div>}
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem', background: C.bgRaised }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>ANALYSIS STATUS</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, lineHeight: 1.8 }}>
                <div>SOURCES: {meta?.source_count || sources.length}</div>
                <div>ARCHIVE: {meta?.historical_sources || 0}</div>
                <div>LIVE FETCH: {meta?.live_sources || 0}</div>
              </div>
            </div>
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SUPPORTING REPORTING</div>
              {sources.slice(0, 8).map((source, index) => <a key={`${source.url}-${index}`} href={source.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.7rem', paddingBottom: '0.7rem', borderBottom: index < Math.min(sources.length, 8) - 1 ? `1px solid ${C.border}` : 'none' }}><div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.2rem' }}>{source.title}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6 }}><div>{source.source}</div><div>{formatDateTime(source.published_at)}</div></div></a>)}
            </div>
          </aside>
        </div>}
      </div>
    </div>
  )
}
