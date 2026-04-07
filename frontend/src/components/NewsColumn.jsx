import { C } from '../constants/theme'
import { formatDateTime, formatRegionLabel, truncateText } from '../lib/formatters'

export default function NewsColumn({
  headlines,
  headlinesLoading,
  headlinesLoaded,
  headlinesError,
  headlineSort,
  headlineRegion,
  headlineRegions,
  onChangeSort,
  onChangeRegion,
  onRefresh,
  onOpenStory,
  onOpenEventDebug,
}) {
  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised }}>
      <div style={{ padding: '0.95rem 1rem 0.8rem', borderBottom: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.7rem', marginBottom: '0.85rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red, boxShadow: `0 0 10px ${C.red}` }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase' }}>Recent Analysis</div>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <select value={headlineSort} onChange={event => onChangeSort(event.target.value)} style={{ flex: 1, minWidth: 150, background: C.bg, border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.08em', padding: '0.5rem 0.6rem', borderRadius: 2 }}>
            <option value="relevance">Sort: Most Covered + Recent</option>
            <option value="region">Sort: Region</option>
          </select>
          <select value={headlineRegion} onChange={event => onChangeRegion(event.target.value)} style={{ flex: 1, minWidth: 120, background: C.bg, border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.08em', padding: '0.5rem 0.6rem', borderRadius: 2 }}>
            <option value="all">Region: All</option>
            {headlineRegions.map(region => <option key={region} value={region}>{`Region: ${formatRegionLabel(region)}`}</option>)}
          </select>
          {headlinesLoaded && <button onClick={onRefresh} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.1em', cursor: 'pointer', padding: '0.5rem 0.7rem', borderRadius: 2 }}>REFRESH</button>}
        </div>
      </div>

      {headlinesLoading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.95rem 1rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.5rem', width: '5rem', marginBottom: '0.55rem' }} /><div className="skeleton" style={{ height: i === 0 ? '1rem' : '0.9rem', width: '88%', marginBottom: '0.4rem' }} /><div className="skeleton" style={{ height: '0.72rem', width: '62%' }} /></div>)}</div>}
      {!headlinesLoading && headlinesError && <div style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>{headlinesError}</div>}
      {!headlinesLoading && !headlinesError && headlines.length === 0 && <div style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>No stories match the current region filter.</div>}
      {!headlinesLoading && !headlinesError && headlines.map((story, index) => (
        <div key={`${story.event_id || story.headline}-${index}`} className="headline-item" onClick={() => onOpenStory(story)} style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', marginBottom: '0.35rem', flexWrap: 'wrap' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: index === 0 ? C.red : C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>{story.topic?.replace('_', ' ')}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{formatRegionLabel(story.dominant_region)}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.08em' }}>{formatDateTime(story.latest_update || story.sources?.[0]?.published_at)}</div>
          </div>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: index === 0 ? '1rem' : '0.88rem', color: C.textSecondary, lineHeight: 1.28, marginBottom: '0.35rem' }}>{story.headline}</div>
          <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.76rem', color: C.textMuted, lineHeight: 1.55 }}>{truncateText(story.summary, 160)}</div>
          {story.event_id && (
            <div style={{ marginTop: '0.55rem' }}>
              <button
                onClick={event => {
                  event.stopPropagation()
                  onOpenEventDebug?.(story)
                }}
                style={{
                  background: 'none',
                  border: `1px solid ${C.borderMid}`,
                  color: C.silver,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.46rem',
                  letterSpacing: '0.11em',
                  padding: '0.35rem 0.52rem',
                  borderRadius: 2,
                  cursor: 'pointer',
                  textTransform: 'uppercase',
                }}
              >
                Open debug
              </button>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
