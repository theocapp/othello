import { useMemo, useState } from 'react'
import { C } from '../constants/theme'
import useCanonicalEvents from '../hooks/useCanonicalEvents'
import { formatDateTime, formatRegionLabel, friendlyErrorMessage, truncateText } from '../lib/formatters'

// Themed select/button base styles for the Recent Analysis controls
function _svgArrowDataUri(color) {
  const svg = `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'><path d='M7 10l5 5 5-5z' fill='${color}'/></svg>`
  return `url("data:image/svg+xml;utf8,${encodeURIComponent(svg)}")`
}

function getSelectBase() {
  return {
    background: C.bg,
    border: `1px solid ${C.borderMid}`,
    color: C.textSecondary,
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: '0.5rem',
    letterSpacing: '0.08em',
    padding: '0.5rem 0.6rem',
    borderRadius: 6,
    appearance: 'none',
    WebkitAppearance: 'none',
    MozAppearance: 'none',
    // arrow is rendered as a separate overlay element to avoid tiling issues
    paddingRight: '1.2rem',
  }
}

export default function NewsColumn({
  onOpenStory,
  onOpenEventDebug,
}) {
  const [headlineSort, setHeadlineSort] = useState('relevance')
  const [headlineRegion, setHeadlineRegion] = useState('all')
  const {
    data: canonicalEventsData,
    error: canonicalEventsError,
    isLoading: headlinesLoading,
  } = useCanonicalEvents({ topic: null, limit: 160 })

  const canonicalEvents = canonicalEventsData?.events || []

  const headlineRegions = useMemo(() => {
    return Array.from(
      new Set(
        canonicalEvents
          .map(event => {
            const payload = event?.payload || {}
            return String(
              event?.geo_region ||
              event?.geo_country ||
              payload?.dominant_region ||
              ''
            ).trim().toLowerCase()
          })
          .filter(region => region && region !== 'global')
      )
    ).sort((left, right) => left.localeCompare(right))
  }, [canonicalEvents])

  const headlines = useMemo(() => {
    const stories = canonicalEvents.map(event => {
      const payload = event?.payload || {}
      const region = (
        event?.geo_region ||
        event?.geo_country ||
        payload?.dominant_region ||
        'global'
      )
      const summary =
        event?.neutral_summary ||
        payload?.summary ||
        (event?.importance_reasons || [])[0] ||
        'Coverage is still developing.'

      return {
        event_id: event?.event_id,
        headline: event?.label || event?.event_id,
        summary,
        topic: event?.topic || 'geopolitics',
        dominant_region: String(region || 'global').toLowerCase(),
        latest_update: event?.last_updated_at || event?.computed_at || event?.first_reported_at,
        source_count: Number(event?.source_count || 0),
        article_count: Number(event?.article_count || 0),
        contradiction_count: Number(event?.contradiction_count || 0),
        importance_score: Number(event?.importance_score || 0),
        sources: [],
      }
    })

    const filtered = headlineRegion === 'all'
      ? stories
      : stories.filter(story => story?.dominant_region === headlineRegion)

    if (headlineSort === 'most_covered') {
      return [...filtered].sort((left, right) => {
        const sourceDelta = Number(right?.source_count || 0) - Number(left?.source_count || 0)
        if (sourceDelta !== 0) return sourceDelta
        const articleDelta = Number(right?.article_count || 0) - Number(left?.article_count || 0)
        if (articleDelta !== 0) return articleDelta
        return String(right?.latest_update || '').localeCompare(String(left?.latest_update || ''))
      })
    }

    if (headlineSort === 'recent') {
      return [...filtered].sort((left, right) => String(right?.latest_update || '').localeCompare(String(left?.latest_update || '')))
    }

    return [...filtered].sort((left, right) => {
      const importanceDelta = Number(right?.importance_score || 0) - Number(left?.importance_score || 0)
      if (importanceDelta !== 0) return importanceDelta
      const contradictionDelta = Number(right?.contradiction_count || 0) - Number(left?.contradiction_count || 0)
      if (contradictionDelta !== 0) return contradictionDelta
      return String(right?.latest_update || '').localeCompare(String(left?.latest_update || ''))
    })
  }, [canonicalEvents, headlineRegion, headlineSort])

  const headlinesError = canonicalEventsError
    ? friendlyErrorMessage(canonicalEventsError, 'canonical event feed')
    : null

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised }}>
      <div style={{ padding: '0.95rem 1rem 0.8rem', borderBottom: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.7rem', marginBottom: '0.85rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red, boxShadow: `0 0 10px ${C.red}` }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase' }}>Recent Analysis</div>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <div style={{ position: 'relative', flex: 1, minWidth: 150 }}>
            <select
              value={headlineSort}
              onChange={event => setHeadlineSort(event.target.value)}
              style={{ ...getSelectBase(), width: '100%', borderRadius: 999, padding: '0.42rem 0.9rem' }}
            >
              <option value="most_covered">Most Covered</option>
              <option value="relevance">Relevant</option>
              <option value="recent">Recent</option>
            </select>
            <span style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', display: 'inline-flex', alignItems: 'center' }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M7 10l5 5 5-5" stroke="#000" strokeOpacity="0.35" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </span>
          </div>

          <div style={{ position: 'relative', flex: 1, minWidth: 120 }}>
            <select
              value={headlineRegion}
              onChange={event => setHeadlineRegion(event.target.value)}
              style={{ ...getSelectBase(), width: '100%', borderRadius: 999, padding: '0.42rem 0.9rem' }}
            >
              <option value="all">Region: All</option>
              {headlineRegions.map(region => <option key={region} value={region}>{`Region: ${formatRegionLabel(region)}`}</option>)}
            </select>
            <span style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', pointerEvents: 'none', display: 'inline-flex', alignItems: 'center' }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M7 10l5 5 5-5" stroke="#000" strokeOpacity="0.35" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </span>
          </div>
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
                  background: C.bgRaised,
                  border: `1px solid ${C.borderMid}`,
                  color: C.silver,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.46rem',
                  letterSpacing: '0.11em',
                  padding: '0.35rem 0.6rem',
                  borderRadius: 6,
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
