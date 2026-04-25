import { Suspense, lazy, useMemo, useState } from 'react'
import BriefingLaunchPanel from './BriefingLaunchPanel'
import ContradictionsPanel from './ContradictionsPanel'
import CorrelationPanel from './CorrelationPanel'
import DecisionSurfacePanel from './DecisionSurfacePanel'
import EntitySignalsPanel from './EntitySignalsPanel'
import InstabilityPanel from './InstabilityPanel'
import MapSummaryPanel from './MapSummaryPanel'
import NewsColumn from './NewsColumn'
import { C } from '../constants/theme'
import { TOPICS, THEATERS } from '../constants/topics'
import { formatClock, formatDateLabel, formatRelativeUpdate } from '../lib/formatters'
import useMapAttention from '../hooks/useMapAttention'
import useHomeNavigation from '../hooks/useHomeNavigation'

const WorldHotspotMap = lazy(() => import('./WorldHotspotMap'))

const navBtnStyle = {
  border: 'none',
  background: 'transparent',
  fontFamily: "'JetBrains Mono', monospace",
  fontSize: '0.5rem',
  letterSpacing: '0.05em',
  textTransform: 'uppercase',
  padding: '0.35rem 0.65rem',
  cursor: 'pointer',
  textDecoration: 'none',
}

const briefingById = {
  conflict: { id: 'conflict', kind: 'conflict', label: 'Conflict Briefing', tag: 'Conflict', accent: '#ef4444', description: '' },
  geopolitics: { id: 'geopolitics', kind: 'briefing', label: 'Political Briefing', tag: 'Political', accent: '#60a5fa', description: '' },
  economics: { id: 'economics', kind: 'briefing', label: 'Economic Briefing', tag: 'Economic', accent: '#fbbf24', description: '' },
}

export default function HomeDashboard({
  time,
  headerVisible,
  animationPhase,
  localTimeZone,
  lastUpdated,
  healthFetchError,
  healthSnapshot,
  themeMode,
  toggleThemeMode,
}) {
  const [mapAttentionWindow, setMapAttentionWindow] = useState('24h')

  const {
    openBriefingByTopic,
    openForesight,
    openTimeline,
    openContradiction,
    openDeepDive,
    openStoryDeepDive,
    openEventDebug,
    openHotspotClusterAnalysis,
  } = useHomeNavigation({ mapAttentionWindow })
  const [selectedMapHotspot, setSelectedMapHotspot] = useState(null)
  const {
    data: mapAttention,
    error: mapAttentionError,
    isLoading: mapAttentionLoading,
  } = useMapAttention(mapAttentionWindow)

  const mapHotspots = useMemo(
    () => (Array.isArray(mapAttention?.hotspots) ? mapAttention.hotspots : []),
    [mapAttention]
  )
  const selectedHotspot = useMemo(
    () => mapHotspots.find(item => item.hotspot_id === selectedMapHotspot) || mapHotspots[0] || null,
    [mapHotspots, selectedMapHotspot]
  )

  const dateStr = formatDateLabel(time)
  const timeStr = formatClock(time, localTimeZone)
  const lastUpdatedStr = formatRelativeUpdate(lastUpdated, localTimeZone)

  const worldClocks = [
    { label: 'New York', zone: 'America/New_York' },
    { label: 'London', zone: 'Europe/London' },
    { label: 'Moscow', zone: 'Europe/Moscow' },
    { label: 'Dubai', zone: 'Asia/Dubai' },
    { label: 'Tokyo', zone: 'Asia/Tokyo' },
  ]

  const themeToggleStyle = {
    width: 40,
    height: 40,
    borderRadius: '50%',
    border: `1px solid ${C.borderMid}`,
    background: C.bgRaised,
    color: C.textSecondary,
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: 0,
    cursor: 'pointer',
    boxShadow: '0 8px 18px rgba(2, 6, 23, 0.12)',
    transition: 'background 160ms ease, border-color 160ms ease, transform 160ms ease, box-shadow 160ms ease',
  }

  function scrollToId(id) {
    const el = document.getElementById(id)
    if (!el) return
    el.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  return (
    <>
      {animationPhase !== 'complete' && (
        <>
          <div
            style={{
              position: 'fixed',
              inset: 0,
              zIndex: 996,
              background: C.bg,
              opacity: animationPhase === 'moving' ? 0.96 : 1,
              pointerEvents: 'none',
              transition: animationPhase === 'moving' ? 'opacity 1.4s ease' : 'none',
            }}
          />

          <div
            style={{
              position: 'fixed',
              zIndex: 998,
              fontFamily: "'Libre Baskerville', serif",
              fontWeight: 800,
              letterSpacing: '-0.01em',
              lineHeight: 1,
              color: C.textPrimary,
              top: animationPhase === 'moving' ? '56px' : '50%',
              left: 0,
              right: 0,
              width: '100%',
              height: animationPhase === 'moving' ? '64px' : 'auto',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transform: animationPhase === 'moving' ? 'none' : 'translateY(-50%)',
              fontSize: animationPhase === 'moving' ? '2.6rem' : '1.85rem',
              WebkitFontSmoothing: 'antialiased',
              MozOsxFontSmoothing: 'grayscale',
              transition: animationPhase === 'moving' ? 'all 1.4s cubic-bezier(0.4, 0, 0.2, 1)' : 'none',
              pointerEvents: 'none',
            }}
          >
            othello
          </div>

          <div
            style={{
              position: 'fixed',
              zIndex: 998,
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: animationPhase === 'moving' ? '0.54rem' : '0.75rem',
              color: C.textSecondary,
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              top: animationPhase === 'moving' ? '64px' : 'calc(50% + 2.15rem)',
              left: 0,
              right: 0,
              width: '100%',
              height: animationPhase === 'moving' ? '64px' : 'auto',
              display: 'flex',
              alignItems: 'center',
              justifyContent: animationPhase === 'moving' ? 'flex-start' : 'center',
              paddingLeft: animationPhase === 'moving' ? '1rem' : 0,
              opacity: animationPhase === 'title' ? 0 : 1,
              transition: animationPhase === 'moving' ? 'all 1.4s cubic-bezier(0.4, 0, 0.2, 1)' : 'opacity 0.3s ease',
              pointerEvents: 'none',
            }}
          >
            {dateStr.toUpperCase()} - {timeStr}
          </div>
        </>
      )}

      <div
        style={{
          position: 'relative',
          height: 48,
          zIndex: 110,
          background: `${C.bg}f0`,
          backdropFilter: 'blur(6px)',
          borderBottom: `1px solid ${C.border}`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'flex-start',
          padding: '0 1rem',
          opacity: animationPhase === 'complete' ? 1 : 0,
          transition: animationPhase === 'moving' ? 'opacity 1.4s ease 0.2s' : 'none',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          {worldClocks.map((clock, index) => (
            <div key={clock.zone} style={{ display: 'flex', alignItems: 'center', gap: '0.45rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{clock.label}</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textPrimary, fontWeight: 600 }}>{formatClock(time, clock.zone)}</div>
              {index < worldClocks.length - 1 && <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, marginLeft: '0.4rem' }} />}
            </div>
          ))}
        </div>
      </div>

      <div
        style={{
          position: 'relative',
          zIndex: 105,
          background: `${C.bg}e8`,
          backdropFilter: 'blur(12px)',
          padding: '0.7rem 1.25rem',
          minHeight: 64,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          opacity: animationPhase === 'complete' ? 1 : 0,
          transitionProperty: 'opacity',
          transitionDuration: '0.3s',
        }}
      >
        <div style={{ position: 'absolute', left: '1rem', display: 'flex', alignItems: 'center', gap: '0.6rem', flexWrap: 'wrap', opacity: animationPhase === 'complete' ? 1 : 0, transition: 'opacity 0.3s ease' }}>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{dateStr.toUpperCase()} - {timeStr}</div>
          <div style={{ width: '1px', height: '0.65rem', background: C.borderMid }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted, letterSpacing: '0.08em' }}>LAST UPDATE: <span style={{ color: C.textSecondary }}>{lastUpdatedStr}</span></div>
        </div>

        <div style={{ position: 'absolute', left: 0, right: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', pointerEvents: 'none' }}>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '2.6rem', fontWeight: 800, color: C.textPrimary, pointerEvents: 'auto', letterSpacing: '-0.01em', lineHeight: 1, opacity: animationPhase === 'complete' ? 1 : 0, transition: 'opacity 0.3s ease' }}>othello</div>
        </div>

        <div style={{ position: 'absolute', right: '1rem', display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <button
            type="button"
            onClick={toggleThemeMode}
            style={themeToggleStyle}
            aria-label={themeMode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
            title={themeMode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          >
            {themeMode === 'dark' ? (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
                <circle cx="12" cy="12" r="4" stroke={C.textSecondary} strokeWidth="1.8" />
                <path d="M12 2.5V5.2M12 18.8V21.5M21.5 12H18.8M5.2 12H2.5M18.72 5.28L16.81 7.19M7.19 16.81L5.28 18.72M18.72 18.72L16.81 16.81M7.19 7.19L5.28 5.28" stroke={C.textSecondary} strokeWidth="1.8" strokeLinecap="round" />
              </svg>
            ) : (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
                <path d="M20.5 14.3C19.3 14.9 18 15.2 16.6 15.2C11.8 15.2 8 11.4 8 6.6C8 5.2 8.3 3.9 8.9 2.7C5.2 4 2.5 7.5 2.5 11.6C2.5 16.9 6.8 21.2 12.1 21.2C16.2 21.2 19.7 18.5 21 14.8C20.9 14.7 20.7 14.5 20.5 14.3Z" stroke={C.textSecondary} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            )}
          </button>
        </div>
      </div>

      <div
        style={{
          position: 'relative',
          zIndex: 104,
          background: `${C.bg}dd`,
          backdropFilter: 'blur(10px)',
          minHeight: 34,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '0.35rem 1rem',
          overflowX: 'auto',
          opacity: animationPhase === 'complete' ? 1 : 0,
          transitionProperty: 'opacity, transform',
          transitionDuration: '0.3s',
          transform: headerVisible ? 'translateY(0)' : 'translateY(-8px)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.2rem', whiteSpace: 'nowrap' }}>
          <a
            href="#hotspot-map"
            onClick={event => {
              event.preventDefault()
              scrollToId('hotspot-map')
            }}
            style={{ ...navBtnStyle, color: C.textMuted }}
          >
            Map
          </a>
          <a
            href="#news-column"
            onClick={event => {
              event.preventDefault()
              scrollToId('news-column')
            }}
            style={{ ...navBtnStyle, color: C.textMuted }}
          >
            News
          </a>
          <a
            href="#briefings"
            onClick={event => {
              event.preventDefault()
              scrollToId('briefings')
            }}
            style={{ ...navBtnStyle, color: C.textMuted }}
          >
            Briefings
          </a>
          <a
            href="#narrative-fractures"
            onClick={event => {
              event.preventDefault()
              scrollToId('narrative-fractures')
            }}
            style={{ ...navBtnStyle, color: C.textMuted }}
          >
            Fractures
          </a>
          <a
            href="#tracked-entities"
            onClick={event => {
              event.preventDefault()
              scrollToId('tracked-entities')
            }}
            style={{ ...navBtnStyle, color: C.textMuted }}
          >
            Entities
          </a>

          <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, margin: '0 0.25rem' }} />

          <button type="button" onClick={() => openBriefingByTopic('geopolitics')} style={{ ...navBtnStyle, color: C.textSecondary }}>Political Briefing ↗</button>
          <button type="button" onClick={() => openBriefingByTopic('economics')} style={{ ...navBtnStyle, color: C.textSecondary }}>Economic Briefing ↗</button>
          <button
            type="button"
            onClick={() => openBriefingByTopic('conflict', {
              hotspot: selectedHotspot,
              hotspots: mapHotspots,
              contradictionEvents: [],
              windowId: mapAttentionWindow,
            })}
            style={{ ...navBtnStyle, color: C.textSecondary }}
          >
            Conflict Briefing ↗
          </button>
          <button type="button" onClick={() => openForesight('predictions')} style={{ ...navBtnStyle, color: C.textSecondary }}>Foresight ↗</button>
          <button type="button" onClick={() => openTimeline(THEATERS[0]?.query || 'global geopolitics timeline')} style={{ ...navBtnStyle, color: C.textSecondary }}>Timelines ↗</button>
        </div>
      </div>

      <div className="main-padding" style={{ padding: '1.5rem', opacity: animationPhase === 'complete' ? 1 : 0, transition: 'opacity 0.3s ease' }}>
        {healthFetchError && (
          <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', animation: 'fadeUp 0.4s ease both' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.3rem' }}>API health check failed</div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.5 }}>{healthFetchError}</div>
          </div>
        )}

        {!healthFetchError && healthSnapshot?.runtime && (!healthSnapshot.runtime.llm_ready || !healthSnapshot.runtime.contradiction_ready) && (
          <div style={{ marginBottom: '1rem', border: '1px solid rgba(251,191,36,0.35)', background: 'rgba(251,191,36,0.06)', padding: '0.75rem 1rem', animation: 'fadeUp 0.4s ease both' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: '#fbbf24', letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.28rem' }}>Partial capability</div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.55 }}>
              {!healthSnapshot.runtime.llm_ready && 'LLM-backed answers and briefings may use fallbacks (set GROQ_API_KEY on the API). '}
              {!healthSnapshot.runtime.contradiction_ready && 'Narrative fracture mining is limited without ANTHROPIC_API_KEY. '}
            </div>
          </div>
        )}

        <div className="home-shell" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.45fr) 360px', gap: '1.25rem', alignItems: 'start', paddingBottom: '10vh' }}>
          <div className="home-primary-column" style={{ display: 'grid', gap: '1.25rem', alignContent: 'start' }}>
            <section id="hotspot-map" style={{ animation: 'fadeUp 0.6s ease 0.08s both' }}>
              <Suspense
                fallback={
                  <div
                    style={{
                      minHeight: 520,
                      border: `1px solid ${C.border}`,
                      background: C.bgRaised,
                    }}
                  />
                }
              >
                <WorldHotspotMap
                  data={mapAttention}
                  error={mapAttentionError}
                  loading={mapAttentionLoading}
                  selectedHotspotId={selectedMapHotspot}
                  onWindowChange={windowId => setMapAttentionWindow(windowId)}
                  onSelectHotspot={setSelectedMapHotspot}
                />
              </Suspense>
            </section>

            <section style={{ animation: 'fadeUp 0.6s ease 0.2s both' }}>
              <div id="briefings" style={{ marginBottom: '1rem' }}>
                <BriefingLaunchPanel
                  topics={TOPICS}
                  onOpenBriefing={topic => {
                    if (!topic?.id) return
                    openBriefingByTopic(topic.id, {
                      hotspot: selectedHotspot,
                      hotspots: mapHotspots,
                      contradictionEvents: [],
                      windowId: mapAttentionWindow,
                    })
                  }}
                  onOpenForesight={mode => openForesight(mode === 'before-news' ? 'archive' : 'predictions')}
                />
              </div>

              <DecisionSurfacePanel onOpenEventDebug={openEventDebug} />

              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '1.25rem', marginBottom: '1rem' }}>
                <InstabilityPanel
                  onAnalyze={country =>
                    openDeepDive({
                      title: `Instability Analysis: ${country.label}`,
                      query: `Analyze the current instability situation in ${country.label}. The country scores ${country.score}/100 on the instability index (level: ${country.level}). Break down: what conflict events are occurring, what is driving media attention, whether narratives diverge across sources, which entities are most active, and what to watch in coming days. Components: conflict=${country.components?.conflict}, media=${country.components?.media_attention}, contradictions=${country.components?.contradiction}, severity=${country.components?.event_severity}. Be analytically precise.`,
                      queryTopic: 'geopolitics',
                      regionContext: country.country,
                    })
                  }
                />
                <CorrelationPanel
                  onAnalyze={card =>
                    openDeepDive({
                      title: `Signal Convergence: ${card.label}`,
                      query: `Analyze the signal convergence detected in ${card.label} (score: ${card.score}/100, type: ${String(card.convergence_type || '').replace(/_/g, ' ')}). Active domains: ${(card.active_domains || []).join(', ')}. Domain scores: ${Object.entries(card.domain_scores || {}).map(([key, value]) => `${key}=${value}`).join(', ')}. What is driving this multi-domain convergence? What does the intersection suggest about the developing situation? What should analysts watch for? Be specific and analytical.`,
                      queryTopic: 'geopolitics',
                      regionContext: card.country,
                    })
                  }
                />
              </div>

              <div className="lower-grid" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.06fr) minmax(0, 0.94fr)', gap: '1.25rem', alignItems: 'start' }}>
                <ContradictionsPanel onOpenContradiction={openContradiction} />

                <div style={{ display: 'grid', gap: '1.25rem' }}>
                  <div id="timelines" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
                      <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.silver }} />
                      <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Timelines</h2>
                    </div>
                    <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                    {THEATERS.map((item, i) => (
                      <div
                        key={item.query || i}
                        className="theater-row"
                        onClick={() => openTimeline(item.query)}
                        style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}
                      >
                        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.4 }}>{item.label}</div>
                        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0, marginLeft: '1rem' }}>VIEW -&gt;</div>
                      </div>
                    ))}
                  </div>

                  <EntitySignalsPanel
                    onOpenEntityAnalysis={(entity, query) =>
                      openDeepDive({
                        title: `Intelligence Analysis: ${entity}`,
                        query,
                        entityName: entity,
                        queryTopic: 'geopolitics',
                      })
                    }
                  />
                </div>
              </div>
            </section>
          </div>

          <aside className="home-sidebar" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', animation: 'fadeUp 0.6s ease 0.14s both' }}>
            <MapSummaryPanel
              data={mapAttention}
              hotspot={selectedHotspot}
              onOpenBriefing={topic => {
                if (!topic?.id) return
                openBriefingByTopic(topic.id, {
                  hotspot: selectedHotspot,
                  hotspots: mapHotspots,
                  contradictionEvents: [],
                  windowId: mapAttentionWindow,
                })
              }}
              onAnalyzeCluster={openHotspotClusterAnalysis}
            />

            <div id="news-column">
              <NewsColumn onOpenStory={openStoryDeepDive} onOpenEventDebug={openEventDebug} />
            </div>
          </aside>
        </div>
      </div>
    </>
  )
}
