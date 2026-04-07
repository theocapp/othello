import { Suspense, lazy, useEffect, useRef, useState, useMemo } from 'react'
import {
  fetchBeforeNewsArchive,
  fetchCorrelations,
  fetchInstability,
  fetchPredictionLedger,
} from './api'
import useHealth from './hooks/useHealth'
import useHeadlines from './hooks/useHeadlines'
import HomeDashboard from './components/HomeDashboard'
import { buildAppStyles, C } from './constants/theme'
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
  const [time, setTime] = useState(new Date())
  const [headerVisible, setHeaderVisible] = useState(true)
  const { data: healthSnapshot, error: healthFetchError } = useHealth()
  const lastUpdated = useMemo(() => {
    try {
      return healthSnapshot?.runtime?.corpus?.latest_published_at
        ? parseDateValue(healthSnapshot.runtime.corpus.latest_published_at)
        : null
    } catch (e) {
      return null
    }
  }, [healthSnapshot])

  const [headlineSort, setHeadlineSort] = useState('relevance')
  const [headlineRegion, setHeadlineRegion] = useState('all')
  const {
    data: headlinesData,
    error: headlinesError,
    isLoading: headlinesLoading,
    refetch: refetchHeadlines,
  } = useHeadlines(headlineSort, headlineRegion)
  const headlines = headlinesData?.stories || []
  const headlineRegions = headlinesData?.available_regions || []
  const headlinesLoaded = headlines.length > 0

  const [mapAttentionWindow, setMapAttentionWindow] = useState('24h')
  const [selectedMapHotspot, setSelectedMapHotspot] = useState(null)

  const { data: entitySignals, error: entitySignalsError } = useEntitySignals()

  const {
    data: contradictionEvents = [],
    error: contradictionsError,
    isLoading: contradictionsLoading,
    refetch: refetchContradictions,
  } = useContradictions(6)

  const {
    data: mapAttention,
    error: mapAttentionError,
    isLoading: mapAttentionLoading,
    refetch: refetchMapAttention,
  } = useMapAttention(mapAttentionWindow)

  const {
    data: instabilityData,
    error: instabilityError,
    isLoading: instabilityLoading,
    refetch: refetchInstability,
  } = useInstability(3)

  const {
    data: correlationData,
    error: correlationError,
    isLoading: correlationLoading,
    refetch: refetchCorrelations,
  } = useCorrelations(3)

  const { data: predictionLedgerResp, error: predictionLedgerError } = usePredictionLedger()
  const predictionLedger = predictionLedgerResp?.predictions || []

  const { data: beforeNewsData, error: beforeNewsError } = useBeforeNewsArchive()
  const beforeNewsArchive = beforeNewsData?.records || []

  const {
    data: canonicalEventsData,
    error: canonicalEventsError,
    isLoading: canonicalEventsLoading,
  } = useCanonicalEvents({ topic: 'geopolitics', limit: 120 })
  const canonicalEvents = canonicalEventsData?.events || []

  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [selectedContradiction, setSelectedContradiction] = useState(null)
  const [timelinePage, setTimelinePage] = useState(null)
  const [foresightPage, setForesightPage] = useState(null)
  const [eventDebugPage, setEventDebugPage] = useState(null)

  const lastScrollY = useRef(0)
  const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

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

  useEffect(() => {
    // entity signals and contradictions are handled by react-query hooks

    // prediction ledger and before-news archive are handled by react-query hooks

    // Health and headlines are fetched via react-query hooks.
    loadMapAttention('24h')
    loadInstability()
    loadCorrelations()
  }, [])

  async function loadInstability() {
    try {
      await refetchInstability()
    } catch (err) {
      // instabilityError is provided by the hook
    }
  }

  async function loadCorrelations() {
    try {
      await refetchCorrelations()
    } catch (err) {
      // correlationError is provided by the hook
    }
  }

  function loadHeadlines(next = {}) {
    // Expose a compatible API for components: trigger a manual refetch.
    refetchHeadlines()
  }

  async function loadMapAttention(window = mapAttentionWindow) {
    setMapAttentionWindow(window)
    try {
      const res = await refetchMapAttention()
      const data = res?.data ?? res
      setSelectedMapHotspot(current =>
        current && (data?.hotspots || []).some(item => item.hotspot_id === current)
          ? current
          : data?.hotspots?.[0]?.hotspot_id || null
      )
    } catch (err) {
      // mapAttentionError is provided by the hook; no local error state needed
    }
  }

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
    (mapAttention?.hotspots || []).find(item => item.hotspot_id === selectedMapHotspot) ||
    mapAttention?.hotspots?.[0] ||
    null

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
      <style>{buildAppStyles()}</style>

      <HomeDashboard
        time={time}
        headerVisible={headerVisible}
        localTimeZone={localTimeZone}
        lastUpdated={lastUpdated}
        healthFetchError={healthFetchError}
        healthSnapshot={healthSnapshot}
        mapAttention={mapAttention}
        mapAttentionError={mapAttentionError}
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
        canonicalEventsError={canonicalEventsError}
        canonicalEventsLoading={canonicalEventsLoading}
        topics={TOPICS}
        setForesightPage={setForesightPage}
        instabilityData={instabilityData}
        instabilityLoading={instabilityLoading}
        instabilityError={instabilityError}
        correlationData={correlationData}
        correlationLoading={correlationLoading}
        correlationError={correlationError}
        setDeepDive={setDeepDive}
        contradictionEvents={contradictionEvents}
        contradictionsLoading={contradictionsLoading}
        contradictionsError={contradictionsError}
        setSelectedContradiction={setSelectedContradiction}
        theaters={THEATERS}
        setTimelinePage={setTimelinePage}
        entitySignals={entitySignals}
        entitySignalsError={entitySignalsError}
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
              hotspots={mapAttention?.hotspots || []}
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
            error={foresightPage === 'predictions' ? predictionLedgerError : beforeNewsError}
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