import { useEffect, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { fetchBriefing } from '../api'
import { C } from '../constants/theme'
import { formatDateTime, friendlyErrorMessage, parseBriefingSections, truncateText } from '../lib/formatters'
import { formatAttentionShare, formatWindowLabel, totalNarrativeFlags } from '../lib/hotspots'
import { MD } from '../lib/markdown'

export default function ConflictBriefingPage({ topic, hotspot, hotspots, contradictionEvents, windowId, onClose, onOpenContradiction }) {
  const [briefData, setBriefData] = useState(null)
  const [briefLoading, setBriefLoading] = useState(true)
  const [briefError, setBriefError] = useState(null)

  useEffect(() => {
    setBriefLoading(true)
    fetchBriefing('conflict')
      .then(result => {
        setBriefData(result)
        setBriefError(null)
      })
      .catch(err => {
        setBriefData(null)
        setBriefError(friendlyErrorMessage(err, 'conflict briefing'))
      })
      .finally(() => setBriefLoading(false))
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [])

  const focusHotspot = hotspot || hotspots[0] || null
  const topHotspots = (hotspots || []).slice(0, 6)
  const topFractures = (contradictionEvents || []).slice(0, 5)
  const watchItems = [
    focusHotspot ? `Watch whether ${focusHotspot.location || focusHotspot.label} sustains its current incident tempo over the ${formatWindowLabel(windowId).toLowerCase()} window.` : null,
    topHotspots[1] ? `Track whether ${topHotspots[1].country} displaces ${topHotspots[0]?.country || 'the current lead hotspot'} in total conflict attention.` : null,
    topFractures[0] ? `Monitor narrative divergence around ${topFractures[0].label}, where reporting is already fragmenting across sources.` : null,
  ].filter(Boolean)
  const parsedBrief = briefData ? parseBriefingSections(briefData.briefing) : {}

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>← BACK</button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — {topic.tag} INTELLIGENCE</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: '3rem', alignItems: 'start' }}>
          <div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.18em', marginBottom: '0.75rem' }}>CONFLICT · GENERATED BRIEFING + MAP CONTEXT</div>
            <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.6rem' }}>{topic.label}</h1>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{formatWindowLabel(windowId)} window · {hotspots.length} active conflict clouds{briefData?.article_count != null ? ` · ${briefData.article_count} corpus sources in conflict briefing` : ''}</div>
            {briefLoading && <div style={{ marginBottom: '2rem' }}>{[50, 100, 75, 88, 80, 92].map((w, i) => <div key={i} className="skeleton" style={{ height: '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />)}</div>}
            {!briefLoading && briefError && <div style={{ marginBottom: '2rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.55 }}>{briefError}</div>}
            {!briefLoading && briefData && ['SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS', 'SIGNAL vs NOISE', 'DEEPER CONTEXT', 'SOURCE CONTRADICTIONS'].map(section => parsedBrief[section] ? <div key={section} style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: section === 'SOURCE CONTRADICTIONS' ? C.red : C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{section}</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.8, color: C.textSecondary }}><ReactMarkdown components={MD}>{parsedBrief[section]}</ReactMarkdown></div></div> : null)}
            {focusHotspot && <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.red, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Map · Situation Snapshot</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', lineHeight: 1.8, color: C.textSecondary }}>{truncateText(focusHotspot.sample_events?.[0]?.summary || `${focusHotspot.location || focusHotspot.label} remains the focal conflict hotspot in ${focusHotspot.country}.`, 420)}</div></div>}
            <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Active Hotspots</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div>{topHotspots.map((item, index) => <div key={item.hotspot_id} style={{ padding: '0.95rem 0', borderBottom: index < topHotspots.length - 1 ? `1px solid ${C.border}` : 'none' }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.3rem' }}><div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.35 }}>{item.location || item.label}, {item.country}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.red, flexShrink: 0 }}>{formatAttentionShare(item.attention_share)}</div></div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}><div>{item.event_count} incidents · {item.fatality_total} fatalities · {item.event_types?.join(' · ')}</div><div>{item.sample_locations?.slice(0, 4).join(' · ')}</div></div></div>)}</div>
            <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Narrative Fractures</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div>{topFractures.length > 0 ? topFractures.map((event, index) => <div key={event.event_id || index} role={onOpenContradiction ? 'button' : undefined} tabIndex={onOpenContradiction ? 0 : undefined} onClick={() => onOpenContradiction?.(event)} style={{ padding: '0.95rem 0', borderBottom: index < topFractures.length - 1 ? `1px solid ${C.border}` : 'none', cursor: onOpenContradiction ? 'pointer' : 'default' }}><div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.35, marginBottom: '0.25rem' }}>{event.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}><div>{totalNarrativeFlags(event)} flags · {event.source_count} sources · {formatDateTime(event.latest_update)}</div><div>{truncateText(event.summary, 180)}</div>{onOpenContradiction && <div style={{ marginTop: '0.35rem', color: C.silver }}>OPEN FRACTURE DETAIL →</div>}</div></div>) : <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>No contradiction-rich clusters are active in the current window.</div>}</div>
            <div><div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>What To Watch</div><div style={{ flex: 1, height: '1px', background: C.border }} /></div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.75, color: C.textSecondary }}>{watchItems.map(item => <div key={item} style={{ marginBottom: '0.6rem' }}>{item}</div>)}</div></div>
          </div>
          <aside className="briefing-sidebar" style={{ position: 'sticky', top: '80px', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.1rem', background: C.bgRaised }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>Conflict Indices</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, lineHeight: 1.8 }}><div>HOTSPOTS: {hotspots.length}</div><div>ACTIVE WINDOW: {formatWindowLabel(windowId).toUpperCase()}</div><div>PRIMARY CLOUD: {focusHotspot?.location || '—'}</div><div>FATALITIES: {hotspots.reduce((sum, item) => sum + (item.fatality_total || 0), 0)}</div></div></div>
            {parsedBrief.PREDICTIONS && <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.1rem', background: C.bgRaised }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.65rem' }}>PREDICTIONS</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', lineHeight: 1.7, color: C.textSecondary }}><ReactMarkdown components={MD}>{parsedBrief.PREDICTIONS}</ReactMarkdown></div></div>}
            {briefData && (Array.isArray(briefData.sources) ? briefData.sources : []).length > 0 && <div style={{ border: `1px solid ${C.border}`, padding: '1.1rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>BRIEFING SOURCES</div>{(Array.isArray(briefData.sources) ? briefData.sources : []).map((s, i, list) => <a key={i} href={s.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.55rem', paddingBottom: '0.55rem', borderBottom: i < list.length - 1 ? `1px solid ${C.border}` : 'none' }}><div style={{ fontSize: '0.72rem', color: C.textSecondary, lineHeight: 1.4, marginBottom: '0.12rem' }}>{s.title}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.55 }}><div>{s.source}</div><div>{formatDateTime(s.published_at)}</div></div></a>)}</div>}
          </aside>
        </div>
      </div>
    </div>
  )
}
