import { Suspense, lazy, useEffect, useMemo, useRef, useState } from 'react'
import {
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useParams,
  useSearchParams,
} from 'react-router-dom'
import HomeDashboard from './components/HomeDashboard'
import { applyTheme, buildAppStyles, C } from './constants/theme'
import { AppContext } from './context/AppContext'
import useHealth from './hooks/useHealth'
import { parseDateValue } from './lib/formatters'

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
    description: 'Hotspots, incident tempo, fatalities, and fractures around active conflict zones.',
  },
]

const THEATERS = [
  { label: 'US-Iran Military Conflict', query: 'US Iran military conflict war strikes' },
  { label: 'Russia-Ukraine War', query: 'Russia Ukraine war conflict' },
  { label: 'Federal Reserve & Interest Rates', query: 'Federal Reserve interest rates monetary policy' },
  { label: 'China-Taiwan Tensions', query: 'China Taiwan tensions military' },
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

function goBackOrHome(navigate) {
  if (window.history.length > 1) {
    navigate(-1)
    return
  }
  navigate('/')
}

function BriefingRoute() {
  const navigate = useNavigate()
  const location = useLocation()
  const { topic } = useParams()
  const selectedTopic = TOPICS.find(item => item.id === topic)

  if (!selectedTopic) return <Navigate to="/" replace />

  if (selectedTopic.kind === 'conflict') {
    const state = location.state || {}
    return (
      <ConflictBriefingPage
        topic={selectedTopic}
        hotspot={state.hotspot || null}
        hotspots={Array.isArray(state.hotspots) ? state.hotspots : []}
        contradictionEvents={Array.isArray(state.contradictionEvents) ? state.contradictionEvents : []}
        windowId={state.windowId || '24h'}
        onClose={() => goBackOrHome(navigate)}
        onOpenContradiction={event => navigate('/contradiction', { state: { event } })}
      />
    )
  }

  return <BriefingPage topic={selectedTopic} onClose={() => goBackOrHome(navigate)} />
}

function DeepDiveRoute() {
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const state = location.state || {}

  const payload = {
    title: state.title || searchParams.get('title') || 'Intelligence Analysis',
    query: state.query || searchParams.get('query') || 'Analyze current geopolitical developments.',
    entityName: state.entityName || state.entity || searchParams.get('entity') || undefined,
    queryTopic: state.queryTopic || searchParams.get('topic') || undefined,
    regionContext: state.regionContext || searchParams.get('region') || undefined,
    hotspotId: state.hotspotId || searchParams.get('hotspotId') || undefined,
    storyEventId: state.storyEventId || searchParams.get('storyEventId') || undefined,
    sourceUrls: Array.isArray(state.sourceUrls) ? state.sourceUrls : undefined,
    attentionWindow: state.attentionWindow || searchParams.get('window') || undefined,
  }

  return <DeepDive {...payload} onClose={() => goBackOrHome(navigate)} />
}

function TimelineRoute() {
  const navigate = useNavigate()
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const query = location.state?.query || searchParams.get('query') || 'global geopolitics timeline'

  return <TimelinePage query={query} onClose={() => goBackOrHome(navigate)} />
}

function ForesightRoute() {
  const navigate = useNavigate()
  const { mode } = useParams()
  const resolvedMode = mode === 'archive' ? 'before-news' : 'predictions'

  return <ForesightPage mode={resolvedMode} onClose={() => goBackOrHome(navigate)} />
}

function ContradictionRoute() {
  const navigate = useNavigate()
  const location = useLocation()
  const event = location.state?.event

  if (!event) {
    return (
      <div style={{ minHeight: '100vh', background: C.bg, color: C.textPrimary, padding: '2rem' }}>
        <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, maxWidth: 780, margin: '0 auto', padding: '1.2rem' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, marginBottom: '0.7rem' }}>
            No contradiction payload was provided for this route.
          </div>
          <button
            type="button"
            onClick={() => goBackOrHome(navigate)}
            style={{
              border: `1px solid ${C.borderMid}`,
              background: 'transparent',
              color: C.textSecondary,
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.56rem',
              padding: '0.42rem 0.8rem',
              cursor: 'pointer',
            }}
          >
            Back
          </button>
        </div>
      </div>
    )
  }

  return <ContradictionOverlay event={event} onClose={() => goBackOrHome(navigate)} />
}

function EventDebugRoute() {
  const navigate = useNavigate()
  const { eventId } = useParams()

  if (!eventId) return <Navigate to="/" replace />
  return <EventDebugOverlay eventId={eventId} onClose={() => goBackOrHome(navigate)} />
}

export default function App() {
  const location = useLocation()
  const [themeMode, setThemeMode] = useState(() => {
    if (typeof window === 'undefined') return 'dark'
    return window.localStorage.getItem('othello-theme-mode') === 'light' ? 'light' : 'dark'
  })
  const [time, setTime] = useState(new Date())
  const [headerVisible, setHeaderVisible] = useState(true)
  const [animationPhase, setAnimationPhase] = useState(() => {
    if (typeof window === 'undefined') return 'title'
    return window.sessionStorage.getItem('othello-splashed') ? 'complete' : 'title'
  })
  const { data: healthSnapshot, error: healthFetchError } = useHealth()

  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [selectedContradiction, setSelectedContradiction] = useState(null)
  const [timelinePage, setTimelinePage] = useState(null)
  const [foresightPage, setForesightPage] = useState(null)
  const [eventDebugPage, setEventDebugPage] = useState(null)

  const lastScrollY = useRef(0)
  const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

  useEffect(() => {
    applyTheme(themeMode)
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('othello-theme-mode', themeMode)
    }
  }, [themeMode])

  useEffect(() => {
    if (animationPhase === 'title') {
      const timer = setTimeout(() => setAnimationPhase('date'), 1000)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'date') {
      const timer = setTimeout(() => setAnimationPhase('moving'), 1000)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'moving') {
      const timer = setTimeout(() => setAnimationPhase('complete'), 1400)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'complete' && typeof window !== 'undefined') {
      window.sessionStorage.setItem('othello-splashed', '1')
    }
  }, [animationPhase])

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
    const state = location.state || null
    if (location.pathname.startsWith('/deep-dive')) setDeepDive(state)
    if (location.pathname.startsWith('/briefing/')) setBriefingPage(state)
    if (location.pathname === '/contradiction') setSelectedContradiction(state?.event || null)
    if (location.pathname === '/timeline') setTimelinePage(state)
    if (location.pathname.startsWith('/foresight/')) setForesightPage(state)
    if (location.pathname.startsWith('/debug/')) setEventDebugPage(state)
  }, [location.pathname, location.state])

  const contextValue = useMemo(
    () => ({
      deepDive,
      setDeepDive,
      briefingPage,
      setBriefingPage,
      selectedContradiction,
      setSelectedContradiction,
      timelinePage,
      setTimelinePage,
      foresightPage,
      setForesightPage,
      eventDebugPage,
      setEventDebugPage,
    }),
    [
      briefingPage,
      deepDive,
      eventDebugPage,
      foresightPage,
      selectedContradiction,
      timelinePage,
    ]
  )

  const lastUpdated = useMemo(() => {
    try {
      return healthSnapshot?.runtime?.corpus?.latest_published_at
        ? parseDateValue(healthSnapshot.runtime.corpus.latest_published_at)
        : null
    } catch {
      return null
    }
  }, [healthSnapshot])

  function toggleThemeMode() {
    setThemeMode(current => (current === 'dark' ? 'light' : 'dark'))
  }

  function toErrorText(error, fallback) {
    if (!error) return null
    if (typeof error === 'string') return error
    if (error instanceof Error && error.message) return error.message
    if (error?.response?.data?.detail) return String(error.response.data.detail)
    return fallback || 'An unexpected error occurred.'
  }

  return (
    <AppContext.Provider value={contextValue}>
      <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
        <style>{buildAppStyles()}</style>

        <Routes>
          <Route
            path="/"
            element={
              <HomeDashboard
                time={time}
                headerVisible={headerVisible}
                animationPhase={animationPhase}
                localTimeZone={localTimeZone}
                lastUpdated={lastUpdated}
                healthSnapshot={healthSnapshot}
                healthFetchError={toErrorText(healthFetchError, 'Unable to load health status.')}
                themeMode={themeMode}
                onToggleThemeMode={toggleThemeMode}
              />
            }
          />

          <Route
            path="/briefing/:topic"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <BriefingRoute />
              </Suspense>
            }
          />

          <Route
            path="/deep-dive"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <DeepDiveRoute />
              </Suspense>
            }
          />

          <Route
            path="/timeline"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <TimelineRoute />
              </Suspense>
            }
          />

          <Route
            path="/foresight/:mode"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <ForesightRoute />
              </Suspense>
            }
          />

          <Route
            path="/contradiction"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <ContradictionRoute />
              </Suspense>
            }
          />

          <Route
            path="/debug/:eventId"
            element={
              <Suspense fallback={<OverlayFallback />}>
                <EventDebugRoute />
              </Suspense>
            }
          />

          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </div>
    </AppContext.Provider>
  )
}
