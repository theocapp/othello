import { Suspense, lazy, useEffect, useRef, useState } from 'react'
import {
  fetchBeforeNewsArchive,
  fetchCorrelations,
  fetchEntitySignals,
  fetchEvents,
  fetchHeadlines,
  fetchHealth,
  fetchInstability,
  fetchPredictionLedger,
  fetchRegionAttention,
} from './api'
import HomeDashboard from './components/HomeDashboard'
import { buildAppStyles, C } from './constants/theme'
import { friendlyErrorMessage, formatRegionLabel, parseDateValue } from './lib/formatters'
import {
  buildHotspotClusterAnalysisQuery,
  collectHotspotSourceUrls,
  normalizeStoryTopicForQuery,
  totalNarrativeFlags,
} from './lib/hotspots'

const DeepDive = lazy(() => import('./pages/DeepDive'))
const BriefingPage = lazy(() => import('./pages/BriefingPage'))
const ConflictBriefingPage = lazy(() => import('./pages/ConflictBriefingPage'))
const ContradictionOverlay = lazy(() => import('./pages/ContradictionOverlay'))
const TimelinePage = lazy(() => import('./pages/TimelinePage'))
const ForesightPage = lazy(() => import('./pages/ForesightPage'))

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
  const [lastUpdated, setLastUpdated] = useState(null)
  const [healthSnapshot, setHealthSnapshot] = useState(null)
  const [healthFetchError, setHealthFetchError] = useState(null)

  const [headlines, setHeadlines] = useState([])
  const [headlineRegions, setHeadlineRegions] = useState([])
  const [headlinesLoading, setHeadlinesLoading] = useState(false)
  const [headlinesLoaded, setHeadlinesLoaded] = useState(false)
  const [headlinesError, setHeadlinesError] = useState(null)
  const [headlineSort, setHeadlineSort] = useState('relevance')
  const [headlineRegion, setHeadlineRegion] = useState('all')

  const [mapAttention, setMapAttention] = useState(null)
  const [mapAttentionLoading, setMapAttentionLoading] = useState(true)
  const [mapAttentionError, setMapAttentionError] = useState(null)
  const [mapAttentionWindow, setMapAttentionWindow] = useState('24h')
  const [selectedMapHotspot, setSelectedMapHotspot] = useState(null)

  const [entitySignals, setEntitySignals] = useState(null)
  const [entitySignalsError, setEntitySignalsError] = useState(null)

  const [contradictionEvents, setContradictionEvents] = useState([])
  const [contradictionsLoading, setContradictionsLoading] = useState(true)
  const [contradictionsError, setContradictionsError] = useState(null)

  const [instabilityData, setInstabilityData] = useState(null)
  const [instabilityLoading, setInstabilityLoading] = useState(true)
  const [instabilityError, setInstabilityError] = useState(null)

  const [correlationData, setCorrelationData] = useState(null)
  const [correlationLoading, setCorrelationLoading] = useState(true)
  const [correlationError, setCorrelationError] = useState(null)

  const [predictionLedger, setPredictionLedger] = useState([])
  const [predictionLedgerError, setPredictionLedgerError] = useState(null)

  const [beforeNewsArchive, setBeforeNewsArchive] = useState([])
  const [beforeNewsError, setBeforeNewsError] = useState(null)

  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [selectedContradiction, setSelectedContradiction] = useState(null)
  const [timelinePage, setTimelinePage] = useState(null)
  const [foresightPage, setForesightPage] = useState(null)

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
    fetchEntitySignals()
      .then(data => {
        setEntitySignals(data)
        setEntitySignalsError(null)
      })
      .catch(err => {
        setEntitySignals(null)
        setEntitySignalsError(friendlyErrorMessage(err, 'entity signals'))
      })

    setContradictionsLoading(true)
    fetchEvents()
      .then(data => {
        const ranked = (data.events || [])
          .filter(event => totalNarrativeFlags(event) > 0)
          .sort((a, b) => totalNarrativeFlags(b) - totalNarrativeFlags(a))
        setContradictionEvents(ranked.slice(0, 6))
        setContradictionsError(null)
      })
      .catch(err => {
        setContradictionEvents([])
        setContradictionsError(friendlyErrorMessage(err, 'narrative fractures'))
      })
      .finally(() => setContradictionsLoading(false))

    fetchPredictionLedger()
      .then(data => {
        setPredictionLedger(data.predictions || [])
        setPredictionLedgerError(null)
      })
      .catch(err => {
        setPredictionLedger([])
        setPredictionLedgerError(friendlyErrorMessage(err, 'prediction ledger'))
      })

    fetchBeforeNewsArchive()
      .then(data => {
        setBeforeNewsArchive(data.records || [])
        setBeforeNewsError(null)
      })
      .catch(err => {
        setBeforeNewsArchive([])
        setBeforeNewsError(friendlyErrorMessage(err, 'before-it-was-news archive'))
      })

    fetchHealth()
      .then(data => {
        setHealthSnapshot(data)
        setHealthFetchError(null)
        if (data?.runtime?.corpus?.latest_published_at) {
          setLastUpdated(parseDateValue(data.runtime.corpus.latest_published_at))
        }
      })
      .catch(err => {
        setHealthSnapshot(null)
        setHealthFetchError(friendlyErrorMessage(err, 'API health'))
      })

    loadMapAttention('24h')
    loadHeadlines()
    loadInstability()
    loadCorrelations()
  }, [])

  async function loadInstability() {
    setInstabilityLoading(true)
    setInstabilityError(null)
    try {
      setInstabilityData(await fetchInstability(3))
    } catch (err) {
      setInstabilityError(friendlyErrorMessage(err, 'instability index'))
    } finally {
      setInstabilityLoading(false)
    }
  }

  async function loadCorrelations() {
    setCorrelationLoading(true)
    setCorrelationError(null)
    try {
      setCorrelationData(await fetchCorrelations(3))
    } catch (err) {
      setCorrelationError(friendlyErrorMessage(err, 'signal correlations'))
    } finally {
      setCorrelationLoading(false)
    }
  }

  async function loadHeadlines(next = {}) {
    const sortBy = next.sortBy || headlineSort
    const region = next.region || headlineRegion

    setHeadlinesLoading(true)
    setHeadlinesLoaded(false)
    setHeadlinesError(null)

    try {
      const data = await fetchHeadlines({ sortBy, region })
      const stories = data.stories || []

      setHeadlineRegions(data.available_regions || [])
      setHeadlines(stories)
      setHeadlinesLoaded(true)

      const latestStoryUpdate =
        stories
          .map(story => story.latest_update || story.sources?.[0]?.published_at)
          .map(value => parseDateValue(value))
          .filter(Boolean)
          .sort((a, b) => b.getTime() - a.getTime())[0] || null

      setLastUpdated(current => current || latestStoryUpdate)
    } catch (err) {
      setHeadlines([])
      setHeadlinesError(friendlyErrorMessage(err, 'headlines'))
    } finally {
      setHeadlinesLoading(false)
    }
  }

  async function loadMapAttention(window = mapAttentionWindow) {
    setMapAttentionLoading(true)
    setMapAttentionError(null)

    try {
      const data = await fetchRegionAttention(window)
      setMapAttention(data)
      setMapAttentionWindow(data.window || window)
      setSelectedMapHotspot(current =>
        current && (data.hotspots || []).some(item => item.hotspot_id === current)
          ? current
          : data.hotspots?.[0]?.hotspot_id || null
      )
    } catch (err) {
      setMapAttention(null)
      setMapAttentionError(friendlyErrorMessage(err, 'incident cloud map'))
    } finally {
      setMapAttentionLoading(false)
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
    </div>
  )
}