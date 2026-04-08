import { Suspense, lazy, useState, useRef, useEffect } from 'react'
import BriefingLaunchPanel from './BriefingLaunchPanel'
import CorrelationPanel from './CorrelationPanel'
import DecisionSurfacePanel from './DecisionSurfacePanel'
import InstabilityPanel from './InstabilityPanel'
import MapSummaryPanel from './MapSummaryPanel'
import NewsColumn from './NewsColumn'
import { C } from '../constants/theme'
import { formatClock, formatDateLabel, formatDateTime, formatRelativeUpdate } from '../lib/formatters'
import { totalNarrativeFlags } from '../lib/hotspots'

const WorldHotspotMap = lazy(() => import('./WorldHotspotMap'))


export default function HomeDashboard({
  time,
  headerVisible,
  localTimeZone,
  lastUpdated,
  healthFetchError,
  healthSnapshot,
  mapAttention,
  mapAttentionError,
  mapAttentionLoading,
  selectedMapHotspot,
  selectedHotspot,
  loadMapAttention,
  handleMapHotspotSelect,
  setBriefingPage,
  openHotspotClusterAnalysis,
  headlines,
  headlinesLoading,
  headlinesLoaded,
  headlinesError,
  headlineSort,
  headlineRegion,
  headlineRegions,
  setHeadlineSort,
  setHeadlineRegion,
  loadHeadlines,
  openStoryDeepDive,
  openEventDebug,
  canonicalEvents,
  canonicalEventsLoading,
  canonicalEventsError,
  topics,
  setForesightPage,
  instabilityData,
  instabilityLoading,
  instabilityError,
  correlationData,
  correlationLoading,
  correlationError,
  setDeepDive,
  contradictionEvents,
  contradictionsLoading,
  contradictionsError,
  setSelectedContradiction,
  theaters,
  setTimelinePage,
  entitySignals,
  entitySignalsError,
  themeMode,
  onToggleThemeMode,
}) {
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
  const authButtonStyle = {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: '0.8rem',
    fontWeight: 700,
    color: C.bg,
    background: C.gold,
    border: `1px solid ${C.gold}`,
    borderRadius: '999px',
    padding: '0.62rem 1rem',
    cursor: 'pointer',
    lineHeight: 1,
    minWidth: 104,
    minHeight: 40,
    boxShadow: '0 10px 24px rgba(212, 175, 55, 0.18)',
    transition: 'background 160ms ease, border-color 160ms ease, transform 160ms ease, box-shadow 160ms ease',
  }
  const loginButtonStyle = {
    ...authButtonStyle,
    color: C.textPrimary,
    background: 'linear-gradient(135deg, rgba(97,165,255,0.2), rgba(60,120,255,0.14))',
    border: `1px solid rgba(97,165,255,0.65)`,
    boxShadow: '0 10px 24px rgba(60, 120, 255, 0.14)',
  }
  const entitySpikes = Array.isArray(entitySignals?.spikes) ? entitySignals.spikes : []
  const topEntities = Array.isArray(entitySignals?.top_entities) ? entitySignals.top_entities : []
  const surgingEntities = entitySpikes.filter(e => e?.trend === 'RISING' || e?.trend === 'NEW').slice(0, 4)

  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef(null)

  // Slideout animation state: mounted vs visible so we can animate in/out
  const [panelActive, setPanelActive] = useState(false)
  const [panelVisible, setPanelVisible] = useState(false)

  useEffect(() => {
    if (menuOpen) {
      setPanelActive(true)
      // allow mount to commit, then trigger visible for CSS transition
      requestAnimationFrame(() => setPanelVisible(true))
    } else {
      // start hide animation, then unmount after transition
      setPanelVisible(false)
      const id = setTimeout(() => setPanelActive(false), 260)
      return () => clearTimeout(id)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [menuOpen])

  useEffect(() => {
    function handleDocClick(e) {
      if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false)
    }
    function handleKey(e) {
      if (e.key === 'Escape') setMenuOpen(false)
    }
    document.addEventListener('mousedown', handleDocClick)
    document.addEventListener('keydown', handleKey)
    return () => {
      document.removeEventListener('mousedown', handleDocClick)
      document.removeEventListener('keydown', handleKey)
    }
  }, [])

  return (
    <>
      {/* Top skinny stripe: clocks + market tickers */}
      <div style={{ position: 'fixed', top: 0, left: 0, right: 0, height: 48, zIndex: 110, background: `${C.bg}f0`, backdropFilter: 'blur(6px)', borderBottom: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          {worldClocks.map((clock, index) => (
            <div key={clock.zone} style={{ display: 'flex', alignItems: 'center', gap: '0.45rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{clock.label}</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textPrimary, fontWeight: 600 }}>{formatClock(time, clock.zone)}</div>
              {index < worldClocks.length - 1 && <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, marginLeft: '0.4rem' }} />}
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', flex: 1, justifyContent: 'center', overflow: 'hidden' }}>
          {/* Market tickers (static placeholders) - expanded to fill bar */}
          {[
            { id: 'sp', label: 'S&P 500', value: 5283.73, pct: 0.62 },
            { id: 'nas', label: 'NASDAQ', value: 15523.12, pct: -0.41 },
            { id: 'dow', label: 'Dow Jones', value: 40012.55, pct: 0.12 },
            { id: 'rut', label: 'Russell 2000', value: 1836.42, pct: -0.33 },
            { id: 'ftse', label: 'FTSE 100', value: 7531.22, pct: 0.04 },
            { id: 'dax', label: 'DAX', value: 15230.44, pct: -0.22 },
            { id: 'nik', label: 'Nikkei 225', value: 34901.11, pct: 0.77 },
            { id: 'hang', label: 'Hang Seng', value: 18325.77, pct: -0.98 },
            { id: 'ssec', label: 'SSE Composite', value: 3310.67, pct: 0.11 },
            { id: 'vix', label: 'VIX', value: 12.34, pct: -1.25 },
            { id: 'vxus', label: 'VXUS', value: 62.11, pct: 0.18 },
            { id: 'gold', label: 'Gold', value: 2348.25, pct: -0.14 },
          ].map(t => {
            const up = Number(t.pct) >= 0
            const color = up ? '#16a34a' : C.red
            const arrow = up ? '▲' : '▼'
            return (
              <div key={t.id} style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', padding: '0 0.5rem' }}>
                <div style={{ color: C.textMuted, fontSize: '0.48rem' }}>{t.label}</div>
                <div style={{ color: C.textSecondary, fontWeight: 700 }}>{t.value}</div>
                <div style={{ color, fontWeight: 800 }}>{arrow} {Math.abs(t.pct).toFixed(2)}%</div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Main header: centered site title, hamburger left, auth links right */}
      <div style={{ position: 'fixed', top: 48, left: 0, right: 0, zIndex: 105, transform: headerVisible ? 'translateY(0)' : 'translateY(-100%)', transition: 'transform 0.3s ease', background: `${C.bg}e8`, backdropFilter: 'blur(12px)', padding: '0.7rem 1.25rem', minHeight: 64, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{ position: 'absolute', left: '1rem', display: 'flex', alignItems: 'center' }}>
          <button aria-label="Open menu" onClick={() => setMenuOpen(!menuOpen)} style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '0.25rem' }}>
            <svg width="24" height="18" viewBox="0 0 32 22" fill="none" xmlns="http://www.w3.org/2000/svg"><rect y="3" width="32" height="3" rx="1" fill={C.textPrimary} /><rect y="9.5" width="32" height="3" rx="1" fill={C.textPrimary} /><rect y="16" width="32" height="3" rx="1" fill={C.textPrimary} /></svg>
          </button>

          {panelActive && (
            <>
              <div onClick={() => setMenuOpen(false)} style={{ position: 'fixed', inset: 0, zIndex: 220, background: panelVisible ? 'rgba(0,0,0,0.36)' : 'rgba(0,0,0,0)', transition: 'background 220ms ease' }} />
              <nav role="dialog" aria-label="Main menu" style={{ position: 'fixed', top: 0, left: 0, bottom: 0, zIndex: 230, width: 320, maxWidth: '85vw', transform: panelVisible ? 'translateX(0)' : 'translateX(-100%)', transition: 'transform 220ms ease', background: 'transparent', padding: '1rem', display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-start' }}>
                <div ref={menuRef} style={{ width: '100%', maxHeight: '100vh', overflowY: 'auto' }}>
                  <div style={{ background: C.bgRaised, border: `1px solid ${C.border}`, borderRadius: 10, boxShadow: '0 10px 30px rgba(2,6,23,0.12)', padding: '0.75rem', display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.25rem' }}>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.9rem', fontWeight: 700, color: C.textPrimary }}>Menu</div>
                      <button aria-label="Close menu" onClick={() => setMenuOpen(false)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.textPrimary, fontSize: '1.05rem' }}>✕</button>
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                      <button onClick={() => { const el = document.getElementById('hotspot-map'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Map — Hotspots</button>
                      {topics && topics.length > 0 && <>
                        <button onClick={() => { setBriefingPage(topics[0]); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>{topics[0].label}</button>
                        <button onClick={() => { setBriefingPage(topics[1] || topics[0]); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>{(topics[1] || topics[0]).label}</button>
                        <button onClick={() => { setBriefingPage(topics[2] || topics[0]); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>{(topics[2] || topics[0]).label}</button>
                      </>}
                      <button onClick={() => { setForesightPage && setForesightPage('predictions'); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Foresight — Predictions</button>
                      <button onClick={() => { if (theaters && theaters.length) setTimelinePage && setTimelinePage(theaters[0].query); else { const el = document.getElementById('timelines'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) } setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Timelines</button>
                      <button onClick={() => { const el = document.getElementById('briefings'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Briefings</button>
                      <button onClick={() => { const el = document.getElementById('news-column'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>News</button>
                      <button onClick={() => { const el = document.getElementById('narrative-fractures'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Narrative Fractures</button>
                      <button onClick={() => { const el = document.getElementById('tracked-entities'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); setMenuOpen(false) }} style={{ width: '100%', textAlign: 'left', background: 'transparent', border: 'none', padding: '0.65rem', cursor: 'pointer', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace" }}>Tracked Entities</button>
                    </div>
                  </div>
                </div>
              </nav>
            </>
          )}
        </div>
        <div style={{ position: 'absolute', left: 0, right: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', pointerEvents: 'none' }}>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.9rem', fontWeight: 800, color: C.textPrimary, pointerEvents: 'auto', letterSpacing: '-0.01em' }}>othello</div>
        </div>
        <div style={{ position: 'absolute', right: '1rem', display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <button
            type="button"
            onClick={() => { window.location.hash = 'signup' }}
            style={authButtonStyle}
          >
            Sign up
          </button>
          <button
            type="button"
            onClick={() => { window.location.hash = 'login' }}
            style={loginButtonStyle}
          >
            Log in
          </button>
        </div>
      </div>
      <div className="main-padding" style={{ padding: '120px 1.5rem 0 1.5rem' }}>
        <header className="header-section" style={{ paddingTop: '1rem', paddingBottom: '1.2rem', animation: 'fadeUp 0.6s ease' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{dateStr.toUpperCase()} — {timeStr}</div>
            <div style={{ width: '1px', height: '0.8rem', background: C.borderMid }} />
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textMuted, letterSpacing: '0.08em' }}>LAST UPDATE: <span style={{ color: C.textSecondary }}>{lastUpdatedStr}</span></div>
          </div>
        </header>
        {healthFetchError && <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', animation: 'fadeUp 0.4s ease both' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.3rem' }}>API health check failed</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.5 }}>{healthFetchError}</div></div>}
        {!healthFetchError && healthSnapshot?.runtime && (!healthSnapshot.runtime.llm_ready || !healthSnapshot.runtime.contradiction_ready) && <div style={{ marginBottom: '1rem', border: `1px solid rgba(251,191,36,0.35)`, background: 'rgba(251,191,36,0.06)', padding: '0.75rem 1rem', animation: 'fadeUp 0.4s ease both' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: '#fbbf24', letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.28rem' }}>Partial capability</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.55 }}>{!healthSnapshot.runtime.llm_ready && 'LLM-backed answers and briefings may use fallbacks (set GROQ_API_KEY on the API). '}{!healthSnapshot.runtime.contradiction_ready && 'Narrative fracture mining is limited without ANTHROPIC_API_KEY. '}</div></div>}
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
      onWindowChange={loadMapAttention}
      onSelectHotspot={handleMapHotspotSelect}
    />
  </Suspense>
</section>
            <section style={{ animation: 'fadeUp 0.6s ease 0.2s both' }}>
            <DecisionSurfacePanel
              canonicalEvents={canonicalEvents}
              canonicalEventsLoading={canonicalEventsLoading}
              canonicalEventsError={canonicalEventsError}
              onOpenEventDebug={openEventDebug}
            />
            <div id="briefings" style={{ marginBottom: '1rem' }}><BriefingLaunchPanel topics={topics} onOpenBriefing={setBriefingPage} onOpenForesight={setForesightPage} /></div>
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '1.25rem', marginBottom: '1rem' }}>
              <InstabilityPanel data={instabilityData} loading={instabilityLoading} error={instabilityError} onAnalyze={country => setDeepDive({ title: `Instability Analysis: ${country.label}`, query: `Analyze the current instability situation in ${country.label}. The country scores ${country.score}/100 on the instability index (level: ${country.level}). Break down: what conflict events are occurring, what's driving media attention, are there contradictory narratives across sources, what entities are most active, and what should we watch for in the coming days? Components: conflict=${country.components?.conflict}, media=${country.components?.media_attention}, contradictions=${country.components?.contradiction}, severity=${country.components?.event_severity}. Be analytically precise.`, queryTopic: 'geopolitics', regionContext: country.country })} />
              <CorrelationPanel data={correlationData} loading={correlationLoading} error={correlationError} onAnalyze={card => setDeepDive({ title: `Signal Convergence: ${card.label}`, query: `Analyze the signal convergence detected in ${card.label} (score: ${card.score}/100, type: ${card.convergence_type.replace(/_/g, ' ')}). Active domains: ${card.active_domains.join(', ')}. Domain scores: ${Object.entries(card.domain_scores).map(([k, v]) => `${k}=${v}`).join(', ')}. What is driving this multi-domain convergence? What does the intersection of these signals suggest about the developing situation? What should analysts watch for? Be specific and analytical.`, queryTopic: 'geopolitics', regionContext: card.country })} />
            </div>
            <div className="lower-grid" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.06fr) minmax(0, 0.94fr)', gap: '1.25rem', alignItems: 'start' }}>
              <div id="narrative-fractures" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}><div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} /><h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Narrative Fractures</h2></div>
                <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                {contradictionsLoading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '88%' : '74%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.55rem', width: '45%' }} /></div>)}</div>}
                {!contradictionsLoading && contradictionEvents.length === 0 && <div style={{ padding: '0.95rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>{contradictionsError || 'No stored contradiction-rich events are currently surfaced in the active corpus.'}</div>}
                {!contradictionsLoading && contradictionEvents.map((event, i) => <div key={event.event_id || i} className="theater-row" onClick={() => setSelectedContradiction(event)} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.25rem' }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.94rem', color: C.textSecondary, lineHeight: 1.5 }}>{event.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>{totalNarrativeFlags(event)} FLAGS</div></div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.6 }}><div>{formatDateTime(event.latest_update)}</div><div>{event.source_count} sources split across the cluster{(event.narrative_fracture_count || 0) > 0 ? ` · ${event.narrative_fracture_count} framing fractures` : ''}</div></div></div>)}
              </div>
              <div style={{ display: 'grid', gap: '1.25rem' }}>
                <div id="timelines" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}><div style={{ width: 6, height: 6, borderRadius: '50%', background: C.silver }} /><h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Timelines</h2></div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                  {theaters.map((item, i) => <div key={i} className="theater-row" onClick={() => setTimelinePage(item.query)} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.4 }}>{item.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0, marginLeft: '1rem' }}>VIEW →</div></div>)}
                </div>
                <div id="tracked-entities" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Tracked Entities</div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />
                  {!entitySignals && (entitySignalsError ? <div style={{ padding: '0.8rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>{entitySignalsError}</div> : <div>{[0, 1, 2, 3, 4].map(i => <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.75rem', width: '55%' }} /><div className="skeleton" style={{ height: '0.75rem', width: '15%' }} /></div>)}</div>)}
                  {entitySignals && <div>
                    {surgingEntities.length > 0 && <div style={{ marginBottom: '1.25rem' }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}><div style={{ width: 4, height: 4, borderRadius: '50%', background: C.red }} /><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, letterSpacing: '0.15em' }}>SURGING</div></div>{surgingEntities.map((e, i) => <div key={i} className="entity-item" onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they playing in current geopolitical events, why are they suddenly getting increased attention in the news, what are their motivations and capabilities, and what should we expect from them in the coming weeks? Be specific and analytical.`, entity: e.entity, queryTopic: 'geopolitics' })} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><span style={{ fontSize: '0.85rem', color: C.textPrimary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>{e.type}</span></div><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, flexShrink: 0 }}>{e.trend === 'NEW' ? 'NEW' : `${e.spike_ratio}×`}</span></div>)}</div>}
                    {topEntities.length > 0 && <div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.12em', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}>MOST DISCUSSED</div>{topEntities.slice(0, 6).map((e, i) => <div key={i} className="entity-item" onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they currently playing in world events, what are their key actions and motivations right now, and what should we be watching for? Be direct and analytically precise.`, entity: e.entity, queryTopic: 'geopolitics' })} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, width: '0.9rem' }}>{i + 1}</span><span style={{ fontSize: '0.85rem', color: C.textSecondary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span></div><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0 }}>{e.mentions}</span></div>)}</div>}
                  </div>}
                </div>
              </div>
            </div>
            </section>
          </div>
          <aside className="home-sidebar" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', animation: 'fadeUp 0.6s ease 0.14s both' }}>
            <MapSummaryPanel data={mapAttention} hotspot={selectedHotspot} onOpenBriefing={setBriefingPage} onAnalyzeCluster={openHotspotClusterAnalysis} />
            <div id="news-column">
              <NewsColumn headlines={headlines} headlinesLoading={headlinesLoading} headlinesLoaded={headlinesLoaded} headlinesError={headlinesError} headlineSort={headlineSort} headlineRegion={headlineRegion} headlineRegions={headlineRegions} onChangeSort={async value => { setHeadlineSort(value); await loadHeadlines({ sortBy: value, region: headlineRegion }) }} onChangeRegion={async value => { setHeadlineRegion(value); await loadHeadlines({ sortBy: headlineSort, region: value }) }} onRefresh={() => loadHeadlines()} onOpenStory={openStoryDeepDive} onOpenEventDebug={openEventDebug} />
            </div>
          </aside>
        </div>
      </div>
    </>
  )
}
