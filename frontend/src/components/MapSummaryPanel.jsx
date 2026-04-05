import { C } from '../constants/theme'
import { formatDateTime, truncateText } from '../lib/formatters'
import {
  HOTSPOT_TYPE_PALETTE,
  formatAttentionShare,
  formatWindowLabel,
  getHotspotAspect,
  hotspotDisplayHeadline,
  hotspotEventDescription,
  hotspotEventTitle,
  mapAspectToQueryTopic,
} from '../lib/hotspots'

export default function MapSummaryPanel({ data, hotspot, onOpenBriefing, onAnalyzeCluster }) {
  const aspect = hotspot ? getHotspotAspect(hotspot) : 'default'
  const palette = HOTSPOT_TYPE_PALETTE[aspect] || HOTSPOT_TYPE_PALETTE.default
  const leadEvent = hotspot?.sample_events?.[0]
  const moreEvents = (hotspot?.sample_events || []).slice(1, 4)
  const windowLabel = data?.window ? formatWindowLabel(data.window) : '—'
  const isStoryHotspot = hotspot?.source_kind === 'story'
  const leadTitle = leadEvent ? hotspotEventTitle(leadEvent) : ''
  const leadBody = leadEvent ? hotspotEventDescription(leadEvent) : ''
  const showLeadBody = leadBody && leadBody !== leadTitle
  const storySourceLinks = isStoryHotspot ? (hotspot?.sample_events || []).map(ev => ({ url: ev?.event_id, title: hotspotEventTitle(ev) })).filter(item => item.url && item.url.startsWith('http')).slice(0, 4) : []
  const briefingTopicId = aspect === 'conflict' ? 'conflict' : aspect === 'economic' ? 'economics' : 'geopolitics'
  const queryTopic = mapAspectToQueryTopic(aspect)

  return (
    <div style={{ border: `1px solid ${C.border}`, background: `linear-gradient(180deg, ${C.bgRaised}, rgba(19,22,26,0.98))`, overflow: 'hidden' }}>
      <div style={{ padding: '0.8rem 1rem', background: 'rgba(9,11,15,0.68)', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Click near a hotspot centroid to select. Hover to preview. Filter by type using the map legend.</div>
      <div style={{ padding: '1rem 1rem 1.1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.55rem', marginBottom: '0.65rem' }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: palette.core, boxShadow: `0 0 12px ${palette.core}` }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: aspect === 'conflict' ? C.red : C.silver, letterSpacing: '0.14em', textTransform: 'uppercase' }}>{isStoryHotspot ? (aspect === 'conflict' ? 'Conflict Story Cluster' : aspect === 'economic' ? 'Economic Story Cluster' : aspect === 'political' ? 'Geopolitical Story Cluster' : 'Story Cluster') : (aspect === 'conflict' ? 'Conflict Hotspot' : aspect === 'political' ? 'Political Hotspot' : aspect === 'economic' ? 'Economic Hotspot' : 'Active Hotspot')}</div>
        </div>
        {hotspot ? <>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.35rem', color: C.textPrimary, lineHeight: 1.2, marginBottom: '0.2rem' }}>{hotspotDisplayHeadline(hotspot)}</div>
          {hotspot.location && hotspotDisplayHeadline(hotspot) !== hotspot.location ? <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>{hotspot.location}</div> : null}
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: '0.9rem' }}>{hotspot.admin1 ? `${hotspot.admin1} · ${hotspot.country}` : hotspot.country}</div>
          {leadEvent && (leadTitle || leadBody) && <div style={{ border: `1px solid ${C.borderMid}`, background: 'rgba(9,12,17,0.56)', padding: '0.85rem 0.9rem', marginBottom: '0.95rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.45rem' }}>{isStoryHotspot ? 'Story snapshot' : 'What happened'}</div>
            {showLeadBody && leadTitle && <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.95rem', color: C.textPrimary, lineHeight: 1.45, marginBottom: '0.5rem' }}>{truncateText(leadTitle, 160)}</div>}
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.65 }}>{truncateText(showLeadBody ? leadBody : (leadTitle || leadBody), 280)}</div>
          </div>}
          {moreEvents.length > 0 && <div style={{ marginBottom: '0.95rem' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>{isStoryHotspot ? 'Other stories in this cluster' : 'Other incidents in this cluster'}</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.55rem' }}>{moreEvents.map((ev, idx) => <div key={ev?.event_id || idx} style={{ border: `1px solid ${C.border}`, background: 'rgba(18,21,27,0.5)', padding: '0.55rem 0.65rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: C.textMuted, marginBottom: '0.25rem' }}>{ev?.event_date ? formatDateTime(ev.event_date) : '—'}{ev?.fatalities ? ` · ${ev.fatalities} fatalities` : ''}</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.8rem', color: C.textSecondary, lineHeight: 1.55 }}>{truncateText(hotspotEventDescription(ev), 180)}</div></div>)}</div>
          </div>}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.65rem', marginBottom: '0.95rem' }}>
            {[[isStoryHotspot ? 'Articles' : 'Incidents', hotspot.event_count], [isStoryHotspot ? 'Sources' : 'Fatalities', isStoryHotspot ? hotspot.source_count : hotspot.fatality_total], ['Attention', formatAttentionShare(hotspot.attention_share)], ['Updated', hotspot.latest_event_date ? formatDateTime(hotspot.latest_event_date) : '—']].map(([label, value]) => <div key={label} style={{ border: `1px solid ${C.border}`, padding: '0.7rem 0.75rem', background: 'rgba(18,21,27,0.62)' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.35rem' }}>{label}</div><div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: label === 'Updated' ? '0.8rem' : '1rem', color: C.textSecondary, lineHeight: 1.35 }}>{value}</div></div>)}
          </div>
          {(hotspot.event_types || []).length > 0 && <div style={{ marginBottom: '0.9rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Scenario Indexes</div><div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>{(hotspot.event_types || []).slice(0, 4).map(item => <div key={item} style={{ border: `1px solid ${C.borderMid}`, background: `${C.bgRaised}c8`, padding: '0.35rem 0.5rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', color: C.textSecondary, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{item}</div>)}</div></div>}
          {(hotspot.sample_locations || []).length > 0 && <div style={{ marginBottom: '0.9rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.45rem' }}>Nearby Locations</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.6 }}>{(hotspot.sample_locations || []).slice(0, 5).join(' · ')}</div></div>}
          {storySourceLinks.length > 0 && <div style={{ marginBottom: '0.9rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>Source Articles</div><div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>{storySourceLinks.map((item, idx) => <a key={idx} href={item.url} target="_blank" rel="noopener noreferrer" style={{ display: 'block', fontFamily: "'Source Serif 4', serif", fontSize: '0.8rem', color: C.silver, lineHeight: 1.45, textDecoration: 'none', borderLeft: `2px solid ${palette.core}`, paddingLeft: '0.55rem', opacity: 0.85 }}>{truncateText(item.title, 100)}</a>)}</div></div>}
          {onOpenBriefing && <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem', marginTop: '0.15rem' }}>
            <button onClick={() => onOpenBriefing(({ conflict: { id: 'conflict', kind: 'conflict', label: 'Conflict Briefing', tag: 'Conflict', accent: '#ef4444', description: '' }, geopolitics: { id: 'geopolitics', kind: 'briefing', label: 'Political Briefing', tag: 'Political', accent: '#60a5fa', description: '' }, economics: { id: 'economics', kind: 'briefing', label: 'Economic Briefing', tag: 'Economic', accent: '#fbbf24', description: '' } })[briefingTopicId] || { id: 'geopolitics', kind: 'briefing', label: 'Political Briefing', tag: 'Political', accent: '#60a5fa', description: '' })} style={{ width: '100%', background: `${palette.core}14`, border: `1px solid ${palette.core}40`, color: palette.core, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.49rem', letterSpacing: '0.12em', textTransform: 'uppercase', padding: '0.65rem 1rem', cursor: 'pointer', textAlign: 'left', transition: 'background 0.15s' }}>Global {aspect === 'conflict' ? 'conflict' : aspect === 'economic' ? 'economic' : 'political'} briefing →</button>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: C.textMuted, letterSpacing: '0.06em', lineHeight: 1.5 }}>Corpus-wide generated briefing (not tied to this dot). Use cluster analysis for this location.</div>
            {onAnalyzeCluster && <button onClick={() => onAnalyzeCluster(hotspot, queryTopic)} style={{ width: '100%', background: 'rgba(18,21,27,0.62)', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.49rem', letterSpacing: '0.12em', textTransform: 'uppercase', padding: '0.65rem 1rem', cursor: 'pointer', textAlign: 'left' }}>Analyze this cluster →</button>}
          </div>}
        </> : <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>No hotspot is selected yet. Click a dot on the map or hover to preview.</div>}
        <div style={{ marginTop: '1rem', paddingTop: '0.9rem', borderTop: `1px solid ${C.border}` }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}><span>Window {windowLabel}</span><span>{data?.hotspot_count || 0} hotspots</span><span>{data?.total_events || 0} mapped signals</span></div></div>
      </div>
    </div>
  )
}
