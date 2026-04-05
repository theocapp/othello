import { C } from '../constants/theme'
import { formatDateTime } from '../lib/formatters'
import { totalNarrativeFlags } from '../lib/hotspots'

export default function ContradictionOverlay({ event, onClose }) {
  function parseSourceLabel(label) {
    const text = (label || '').trim()
    const match = text.match(/^(.*?)(?:\s+\((\d{4}-\d{2}-\d{2}T[^)]+)\))?$/)
    if (!match) return { source: text, published_at: null }
    return { source: (match[1] || text).trim(), published_at: match[2] || null }
  }

  function sourceRecordFor(item, index) {
    const direct = item?.source_records?.[index]
    if (direct?.url || direct?.source) return direct
    const label = (item?.sources_in_conflict || [])[index]
    const parsed = parseSourceLabel(label)
    return (event.articles || []).find(article => article.source === parsed.source && (!parsed.published_at || article.published_at === parsed.published_at))
      || (event.articles || []).find(article => article.source === parsed.source)
      || null
  }

  function mostCredibleRecordFor(item) {
    if (item?.most_credible_record?.url || item?.most_credible_record?.source) return item.most_credible_record
    const parsed = parseSourceLabel(item?.most_credible_source)
    return (event.articles || []).find(article => article.source === parsed.source && (!parsed.published_at || article.published_at === parsed.published_at))
      || (event.articles || []).find(article => article.source === parsed.source)
      || null
  }

  function SourceLabel({ record, fallback }) {
    const fallbackSource = parseSourceLabel(fallback).source || fallback
    const displaySource = parseSourceLabel(record?.source || fallbackSource).source || fallbackSource
    if (record?.url) return <a href={record.url} target="_blank" rel="noreferrer" style={{ color: C.silver, textDecoration: 'underline', textUnderlineOffset: '2px' }}>{displaySource}</a>
    return <span>{displaySource}</span>
  }

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 220, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — CONTRADICTIONS</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '1rem' }}>Narrative Fracture Review</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{event.label}</h1>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{event.topic?.toUpperCase()}</div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{formatDateTime(event.latest_update)}</div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{event.source_count} SOURCES</div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.08em' }}>{totalNarrativeFlags(event)} FLAGS</div>
        </div>
        <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: '2rem', alignItems: 'start' }}>
          <div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', color: C.textSecondary, lineHeight: 1.8, marginBottom: '2rem' }}>{event.summary}</div>
            {(event.contradictions || []).map((item, index) => {
              const leftRecord = sourceRecordFor(item, 0)
              const rightRecord = sourceRecordFor(item, 1)
              const mostCredible = mostCredibleRecordFor(item)
              return <div key={index} style={{ border: `1px solid ${C.borderMid}`, background: C.bgRaised, padding: '1.25rem', marginBottom: '1rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.8rem', flexWrap: 'wrap' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase' }}>{item.conflict_type || 'fact conflict'}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>{Math.round((item.confidence || 0) * 100)}% confidence</div></div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '0.8rem' }}>
                  <div style={{ borderLeft: `2px solid ${C.red}`, paddingLeft: '0.8rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}><SourceLabel record={leftRecord} fallback={(item.sources_in_conflict || [])[0] || 'Source A'} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>{item.claim_a || 'No claim captured.'}</div></div>
                  <div style={{ borderLeft: `2px solid ${C.borderMid}`, paddingLeft: '0.8rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}><SourceLabel record={rightRecord} fallback={(item.sources_in_conflict || [])[1] || 'Source B'} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>{item.claim_b || 'No conflicting claim captured.'}</div></div>
                </div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textMuted, lineHeight: 1.65 }}><strong style={{ color: C.textSecondary }}>Assessment:</strong> {item.reasoning || 'No reasoning provided.'}</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, marginTop: '0.7rem' }}>Most credible source: <SourceLabel record={mostCredible} fallback={item.most_credible_source || 'unresolved'} /></div>
              </div>
            })}
          </div>
          <aside className="briefing-sidebar" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', position: 'sticky', top: '88px' }}>
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>ENTITY FOCUS</div>{(event.entity_focus || []).map((entity, index) => <div key={index} style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, marginBottom: '0.4rem' }}>{entity}</div>)}</div>
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SOURCE PACK</div>{(event.articles || []).slice(0, 8).map((article, index) => <a key={`${article.url}-${index}`} href={article.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.7rem', paddingBottom: '0.7rem', borderBottom: index < Math.min((event.articles || []).length, 8) - 1 ? `1px solid ${C.border}` : 'none' }}><div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.2rem' }}>{article.title}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6 }}><div>{article.source}</div><div>{formatDateTime(article.published_at)}</div></div></a>)}</div>
          </aside>
        </div>
      </div>
    </div>
  )
}
