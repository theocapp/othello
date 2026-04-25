import { Suspense, lazy, useMemo } from 'react'
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
import { buildAppStyles, C } from './constants/theme'
import { TOPICS, THEATERS } from './constants/topics'
import useHealth from './hooks/useHealth'
import useAppChrome from './hooks/useAppChrome'
import { parseDateValue } from './lib/formatters'

const DeepDive = lazy(() => import('./pages/DeepDive'))
const BriefingPage = lazy(() => import('./pages/BriefingPage'))
const ConflictBriefingPage = lazy(() => import('./pages/ConflictBriefingPage'))
const ContradictionOverlay = lazy(() => import('./pages/ContradictionOverlay'))
const TimelinePage = lazy(() => import('./pages/TimelinePage'))
const ForesightPage = lazy(() => import('./pages/ForesightPage'))
const EventDebugOverlay = lazy(() => import('./pages/EventDebugOverlay'))

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
  const chrome = useAppChrome()
  const { data: healthSnapshot, error: healthFetchError } = useHealth()

  const lastUpdated = useMemo(() => {
    try {
      return healthSnapshot?.runtime?.corpus?.latest_published_at
        ? parseDateValue(healthSnapshot.runtime.corpus.latest_published_at)
        : null
    } catch {
      return null
    }
  }, [healthSnapshot])

  function toErrorText(error, fallback) {
    if (!error) return null
    if (typeof error === 'string') return error
    if (error instanceof Error && error.message) return error.message
    if (error?.response?.data?.detail) return String(error.response.data.detail)
    return fallback || 'An unexpected error occurred.'
  }

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
      <style>{buildAppStyles()}</style>

      <Routes>
          <Route
            path="/"
            element={
              <HomeDashboard
                {...chrome}
                lastUpdated={lastUpdated}
                healthSnapshot={healthSnapshot}
                healthFetchError={toErrorText(healthFetchError, 'Unable to load health status.')}
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
  )
}
