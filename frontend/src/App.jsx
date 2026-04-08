import { Suspense, lazy, useEffect, useRef, useState, useMemo } from 'react'
import useHealth from './hooks/useHealth'
import HomeDashboard from './components/HomeDashboard'
import { applyTheme, buildAppStyles, C } from './constants/theme'
import { friendlyErrorMessage, formatRegionLabel, parseDateValue } from './lib/formatters'
import {
  buildHotspotClusterAnalysisQuery,
  collectHotspotSourceUrls,
  normalizeStoryTopicForQuery,
} from './lib/hotspots'
import useContradictions from './hooks/useContradictions'
import useEntitySignals from './hooks/useEntitySignals'
import useMapAttention from './hooks/useMapAttention'
import useInstability from './hooks/useInstability'
import useCorrelations from './hooks/useCorrelations'
import usePredictionLedger from './hooks/usePredictionLedger'
import useBeforeNewsArchive from './hooks/useBeforeNewsArchive'
import useCanonicalEvents from './hooks/useCanonicalEvents'

const DeepDive = lazy(() => import('./pages/DeepDive'))
const BriefingPage = lazy(() => import('./pages/BriefingPage'))
const ConflictBriefingPage = lazy(() => import('./pages/ConflictBriefingPage'))
const ContradictionOverlay = lazy(() => import('./pages/ContradictionOverlay'))
const TimelinePage = lazy(() => import('./pages/TimelinePage'))
const ForesightPage = lazy(() => import('./pages/ForesightPage'))
const EventDebugOverlay = lazy(() => import('./pages/EventDebugOverlay'))

const TOPICS = [
  {
    id: 'geopolitics',
    kind: 'briefing',
    label: 'Political Briefing',
    tag: 'Political',
    accent: '#60a5fa',
    description: 'Power shifts, state moves, pressure campaigns, and diplomatic signaling.',
  },
  {
    id: 'economics',
    kind: 'briefing',
    label: 'Economic Briefing',
    tag: 'Economic',
    accent: '#fbbf24',
    description: 'Markets, sanctions, supply chains, and economic coercion shaping the story.',
  },
  {
    id: 'conflict',
    kind: 'conflict',
    label: 'Conflict Briefing',
    tag: 'Conflict',
    accent: '#ef4444',
    description: 'Hotspots, incident tempo, fatalities, and the fractures forming around live conflict zones.',
  },
]

const THEATERS = [
  { label: 'US–Iran Military Conflict', query: 'US Iran military conflict war strikes' },
  { label: 'Russia–Ukraine War', query: 'Russia Ukraine war conflict' },
  { label: 'Federal Reserve & Interest Rates', query: 'Federal Reserve interest rates monetary policy' },
  { label: 'China–Taiwan Tensions', query: 'China Taiwan tensions military' },
]

function OverlayFallback() {
  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: C.bg,
        zIndex: 200,
      }}
    />
  )
}

export default function App() {
  const [themeMode, setThemeMode] = useState(() => {
    if (typeof window === 'undefined') return 'dark'
    return window.localStorage.getItem('othello-theme-mode') === 'light' ? 'light' : 'dark'
  })
  const [time, setTime] = useState(new Date())
  const [headerVisible, setHeaderVisible] = useState(true)
  const { data: healthSnapshot, error: healthFetchError } = useHealth()

  useEffect(() => {
    applyTheme(themeMode)
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('othello-theme-mode', themeMode)
    }
  }, [themeMode])

  function toErrorText(error, label) {
    if (!error) return null
    if (typeof error === 'string') return error
    if (error instanceof Error && error.message) return error.message
    if (error?.response?.data?.detail) return String(error.response.data.detail)
    if (label) return `Unable to load ${label}.`
    return String(error)
  }

  const lastUpdated = useMemo(() => {
    try {
      return healthSnapshot?.runtime?.corpus?.latest_published_at
        ? parseDateValue(healthSnapshot.runtime.corpus.latest_published_at)
        : null
    } catch {
      return null
    }
  }, [healthSnapshot])

  const [headlineSort, setHeadlineSort] = useState('relevance')
  const [headlineRegion, setHeadlineRegion] = useState('all')

  const [mapAttentionWindow, setMapAttentionWindow] = useState('24h')
  const [selectedMapHotspot, setSelectedMapHotspot] = useState(null)

  const { data: entitySignals, error: entitySignalsError } = useEntitySignals()

  const {
    data: contradictionEvents = [],
    error: contradictionsError,
    isLoading: contradictionsLoading,
  } = useContradictions(6)

  const {
    data: mapAttention,
    error: mapAttentionError,
    isLoading: mapAttentionLoading,
    refetch: refetchMapAttention,
  } = useMapAttention(mapAttentionWindow)
  const mapHotspots = useMemo(
    () => (Array.isArray(mapAttention?.hotspots) ? mapAttention.hotspots : []),
    [mapAttention]
  )

  const {
    data: instabilityData,
    error: instabilityError,
    isLoading: instabilityLoading,
  } = useInstability(3)

  const {
    data: correlationData,
    error: correlationError,
    isLoading: correlationLoading,
  } = useCorrelations(3)

  const { data: predictionLedgerResp, error: predictionLedgerError } = usePredictionLedger()
  const predictionLedger = predictionLedgerResp?.predictions || []

  const { data: beforeNewsData, error: beforeNewsError } = useBeforeNewsArchive()
  const beforeNewsArchive = beforeNewsData?.records || []

  const {
    data: canonicalEventsData,
    error: canonicalEventsError,
    isLoading: canonicalEventsLoading,
    refetch: refetchCanonicalEvents,
  } = useCanonicalEvents({ topic: null, limit: 160 })
  const canonicalEvents = useMemo(() => canonicalEventsData?.events ?? [], [canonicalEventsData])

  const canonicalStories = useMemo(() => {
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

    if (headlineSort === 'region') {
      return [...filtered].sort((left, right) => {
        const regionCompare = String(left?.dominant_region || '').localeCompare(String(right?.dominant_region || ''))
        if (regionCompare !== 0) return regionCompare
        const importanceDelta = Number(right?.importance_score || 0) - Number(left?.importance_score || 0)
        if (importanceDelta !== 0) return importanceDelta
        return String(right?.latest_update || '').localeCompare(String(left?.latest_update || ''))
      })
    }

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

  const headlines = canonicalStories
  const headlinesLoaded = headlines.length > 0
  const headlinesLoading = canonicalEventsLoading
  const headlinesError = canonicalEventsError
    ? friendlyErrorMessage(canonicalEventsError, 'canonical event feed')
    : null

  const healthFetchErrorText = toErrorText(healthFetchError, 'health status')
  const mapAttentionErrorText = toErrorText(mapAttentionError, 'map attention')
  const entitySignalsErrorText = toErrorText(entitySignalsError, 'entity signals')
  const instabilityErrorText = toErrorText(instabilityError, 'instability index')
  const correlationErrorText = toErrorText(correlationError, 'signal convergence')
  const predictionLedgerErrorText = toErrorText(predictionLedgerError, 'prediction ledger')
  const beforeNewsErrorText = toErrorText(beforeNewsError, 'before-news archive')
  const canonicalEventsErrorText = toErrorText(canonicalEventsError, 'canonical event feed')
  const contradictionsErrorText = toErrorText(contradictionsError, 'narrative fractures')

  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [selectedContradiction, setSelectedContradiction] = useState(null)
  const [timelinePage, setTimelinePage] = useState(null)
  const [foresightPage, setForesightPage] = useState(null)
  const [eventDebugPage, setEventDebugPage] = useState(null)

  const lastScrollY = useRef(0)
  const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

  useEffect(() => {
    const handleWindowError = event => {
      console.error('Unhandled frontend error', event.error || event.message)
    }

    const handleUnhandledRejection = event => {
      console.error('Unhandled promise rejection', event.reason)
    }

    window.addEventListener('error', handleWindowError)
    window.addEventListener('unhandledrejection', handleUnhandledRejection)

    return () => {
      window.removeEventListener('error', handleWindowError)
      window.removeEventListener('unhandledrejection', handleUnhandledRejection)
    }
  }, [])

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(timer)
  }, [])

  useEffect(() => {
    function handleScroll() {
      const currentY = window.scrollY
      if (currentY < 60) {
        setHeaderVisible(true)
        lastScrollY.current = currentY
        return
      }
      setHeaderVisible(currentY < lastScrollY.current)
      lastScrollY.current = currentY
    }

    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  function loadHeadlines() {
    // Expose a compatible API for components: trigger canonical feed refresh.
    refetchCanonicalEvents()
  }

  async function loadMapAttention(windowParam = mapAttentionWindow) {
    setMapAttentionWindow(windowParam)
    try {
      const res = await refetchMapAttention()
      const data = res?.data ?? res
      const hotspots = Array.isArray(data?.hotspots) ? data.hotspots : []
      setSelectedMapHotspot(current =>
        current && hotspots.some(item => item.hotspot_id === current)
          ? current
          : hotspots[0]?.hotspot_id || null
      )
    } catch {
      // mapAttentionError is provided by the hook; no local error state needed
    }
  }

  useEffect(() => {
    if (!selectedMapHotspot && mapHotspots.length) {
      setSelectedMapHotspot(mapHotspots[0]?.hotspot_id || null)
    }
  }, [mapHotspots, selectedMapHotspot])

  function openStoryDeepDive(story) {
    const sourceUrls = (story.sources || [])
      .map(s => s?.url)
      .filter(u => typeof u === 'string' && u.startsWith('http'))
      .slice(0, 12)

    const qt = normalizeStoryTopicForQuery(story.topic)

    setDeepDive({
      title: story.headline,
      query: `Give me a comprehensive intelligence deep-dive on this story: "${story.headline}". Cover: what is actually happening beyond the surface narrative, key actors and their motivations, what mainstream media is missing or underreporting, historical parallels, geopolitical implications, and your probability assessments for how this develops. Be direct, analytical, and specific.`,
      queryTopic: qt || undefined,
      storyEventId: story.event_id || undefined,
      sourceUrls: sourceUrls.length ? sourceUrls : undefined,
      regionContext: story.dominant_region ? formatRegionLabel(story.dominant_region) : undefined,
    })
  }

  function openEventDebug(story) {
    const eventId = (story?.event_id || '').trim()
    if (!eventId) return
    setEventDebugPage({
      eventId,
      label: story?.headline || story?.label || eventId,
    })
  }

  function openHotspotClusterAnalysis(hotspot, queryTopic) {
    if (!hotspot) return

    const urls = collectHotspotSourceUrls(hotspot)

    setDeepDive({
      title: `Cluster: ${hotspot.location || hotspot.label || 'Hotspot'}`,
      query: buildHotspotClusterAnalysisQuery(hotspot),
      queryTopic,
      regionContext: [hotspot.location || hotspot.label, hotspot.admin1, hotspot.country]
        .filter(Boolean)
        .join(', ') || undefined,
      hotspotId: hotspot.hotspot_id || undefined,
      attentionWindow: mapAttentionWindow,
      sourceUrls: urls.length ? urls : undefined,
    })
  }

  const selectedHotspot =
    mapHotspots.find(item => item.hotspot_id === selectedMapHotspot) ||
    mapHotspots[0] ||
    null

  function toggleThemeMode() {
    setThemeMode(current => (current === 'dark' ? 'light' : 'dark'))
  }

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
      <style>{buildAppStyles()}</style>

      <HomeDashboard
        time={time}
        headerVisible={headerVisible}
        localTimeZone={localTimeZone}
        lastUpdated={lastUpdated}
        healthFetchError={healthFetchErrorText}
        healthSnapshot={healthSnapshot}
        mapAttention={mapAttention}
        mapAttentionError={mapAttentionErrorText}
        mapAttentionLoading={mapAttentionLoading}
        selectedMapHotspot={selectedMapHotspot}
        selectedHotspot={selectedHotspot}
        loadMapAttention={loadMapAttention}
        handleMapHotspotSelect={setSelectedMapHotspot}
        setBriefingPage={setBriefingPage}
        openHotspotClusterAnalysis={openHotspotClusterAnalysis}
        headlines={headlines}
        headlinesLoading={headlinesLoading}
        headlinesLoaded={headlinesLoaded}
        headlinesError={headlinesError}
        headlineSort={headlineSort}
        headlineRegion={headlineRegion}
        headlineRegions={headlineRegions}
        setHeadlineSort={setHeadlineSort}
        setHeadlineRegion={setHeadlineRegion}
        loadHeadlines={loadHeadlines}
        openStoryDeepDive={openStoryDeepDive}
        openEventDebug={openEventDebug}
        canonicalEvents={canonicalEvents}
        canonicalEventsError={canonicalEventsErrorText}
        canonicalEventsLoading={canonicalEventsLoading}
        topics={TOPICS}
        setForesightPage={setForesightPage}
        instabilityData={instabilityData}
        instabilityLoading={instabilityLoading}
        instabilityError={instabilityErrorText}
        correlationData={correlationData}
        correlationLoading={correlationLoading}
        correlationError={correlationErrorText}
        setDeepDive={setDeepDive}
        contradictionEvents={contradictionEvents}
        contradictionsLoading={contradictionsLoading}
        contradictionsError={contradictionsErrorText}
        setSelectedContradiction={setSelectedContradiction}
        theaters={THEATERS}
        setTimelinePage={setTimelinePage}
        entitySignals={entitySignals}
        entitySignalsError={entitySignalsErrorText}
        themeMode={themeMode}
        onToggleThemeMode={toggleThemeMode}
      />

      {deepDive && (
        <Suspense fallback={<OverlayFallback />}>
          <DeepDive
            {...deepDive}
            entityName={deepDive.entityName || deepDive.entity}
            onClose={() => setDeepDive(null)}
          />
        </Suspense>
      )}

      {briefingPage && (
        <Suspense fallback={<OverlayFallback />}>
          {briefingPage.kind === 'conflict' ? (
            <ConflictBriefingPage
              topic={briefingPage}
              hotspot={selectedHotspot}
              hotspots={mapHotspots}
              contradictionEvents={contradictionEvents}
              windowId={mapAttentionWindow}
              onClose={() => setBriefingPage(null)}
              onOpenContradiction={event => setSelectedContradiction(event)}
            />
          ) : (
            <BriefingPage topic={briefingPage} onClose={() => setBriefingPage(null)} />
          )}
        </Suspense>
      )}

      {selectedContradiction && (
        <Suspense fallback={<OverlayFallback />}>
          <ContradictionOverlay
            event={selectedContradiction}
            onClose={() => setSelectedContradiction(null)}
          />
        </Suspense>
      )}

      {timelinePage && (
        <Suspense fallback={<OverlayFallback />}>
          <TimelinePage query={timelinePage} onClose={() => setTimelinePage(null)} />
        </Suspense>
      )}

      {foresightPage && (
        <Suspense fallback={<OverlayFallback />}>
          <ForesightPage
            mode={foresightPage}
            records={foresightPage === 'predictions' ? predictionLedger : beforeNewsArchive}
            error={foresightPage === 'predictions' ? predictionLedgerErrorText : beforeNewsErrorText}
            onClose={() => setForesightPage(null)}
          />
        </Suspense>
      )}

      {eventDebugPage && (
        <Suspense fallback={<OverlayFallback />}>
          <EventDebugOverlay
            eventId={eventDebugPage.eventId}
            onClose={() => setEventDebugPage(null)}
          />
        </Suspense>
      )}
    </div>
  )
}