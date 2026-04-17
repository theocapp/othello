import { Suspense, lazy } from 'react'
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
  animationPhase,
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
  const entitySpikes = Array.isArray(entitySignals?.spikes) ? entitySignals.spikes : []
  const topEntities = Array.isArray(entitySignals?.top_entities) ? entitySignals.top_entities : []
  const surgingEntities = entitySpikes.filter(e => e?.trend === 'RISING' || e?.trend === 'NEW').slice(0, 4)

  return (
    <>
      {/* Animated splash elements */}
      {animationPhase !== 'complete' && (
        <>
          {/* Splash background overlay */}
          <div style={{
            position: 'fixed',
            inset: 0,
            zIndex: 996,
            background: C.bg,
            opacity: animationPhase === 'moving' ? 0.96 : 1,
            pointerEvents: 'none',
            transition: animationPhase === 'moving' ? 'opacity 1.4s ease' : 'none',
          }} />

          {/* Animated title */}
          <div style={{
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
          }}>
            othello
          </div>

          {/* Animated date/time */}
          <div style={{
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
          }}>
            {dateStr.toUpperCase()} - {timeStr}
          </div>
        </>
      )}

      {/* Top skinny stripe: clocks + market tickers */}
      <div style={{ position: 'relative', height: 48, zIndex: 110, background: `${C.bg}f0`, backdropFilter: 'blur(6px)', borderBottom: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 1rem', opacity: animationPhase === 'complete' ? 1 : 0, transition: animationPhase === 'moving' ? 'opacity 1.4s ease 0.2s' : 'none' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          {worldClocks.map((clock, index) => (
            <div key={clock.zone} style={{ display: 'flex', alignItems: 'center', gap: '0.45rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.06em', textTransform: 'uppercase' }}>{clock.label}</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textPrimary, fontWeight: 600 }}>{formatClock(time, clock.zone)}</div>
              {index < worldClocks.length - 1 && <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, marginLeft: '0.4rem' }} />}
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', flex: 1, justifyContent: 'flex-end', overflow: 'hidden', marginRight: '0.5rem' }}>
          {/* Top six market tickers */}
          {[
            { id: 'sp', label: 'S&P 500', value: 5283.73, pct: 0.62 },
            { id: 'nas', label: 'NASDAQ', value: 15523.12, pct: -0.41 },
            { id: 'dow', label: 'Dow Jones', value: 40012.55, pct: 0.12 },
            { id: 'rut', label: 'Russell 2000', value: 1836.42, pct: -0.33 },
            { id: 'vix', label: 'VIX', value: 12.34, pct: -1.25 },
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

      {/* Main header: metadata left, centered title, auth right */}
      <div style={{ position: 'relative', zIndex: 105, background: `${C.bg}e8`, backdropFilter: 'blur(12px)', padding: '0.7rem 1.25rem', minHeight: 64, display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: animationPhase === 'complete' ? 1 : 0, transitionProperty: 'opacity', transitionDuration: '0.3s' }}>
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
            onClick={onToggleThemeMode}
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

      <div style={{ position: 'relative', zIndex: 104, background: `${C.bg}dd`, backdropFilter: 'blur(10px)', minHeight: 34, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '0.35rem 1rem', overflowX: 'auto', opacity: animationPhase === 'complete' ? 1 : 0, transitionProperty: 'opacity', transitionDuration: '0.3s' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', whiteSpace: 'nowrap' }}>
          <button onClick={() => { const el = document.getElementById('hotspot-map'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Map</button>
          {topics && topics.length > 0 && (
            <>
              <button onClick={() => setBriefingPage(topics[0])} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >{topics[0].label}</button>
              <button onClick={() => setBriefingPage(topics[1] || topics[0])} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >{(topics[1] || topics[0]).label}</button>
              <button onClick={() => setBriefingPage(topics[2] || topics[0])} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >{(topics[2] || topics[0]).label}</button>
            </>
          )}
          <button onClick={() => { setForesightPage && setForesightPage('predictions') }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Foresight</button>
          <button onClick={() => { if (theaters && theaters.length) setTimelinePage && setTimelinePage(theaters[0].query); else { const el = document.getElementById('timelines'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) } }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Timelines</button>
          <button onClick={() => { const el = document.getElementById('briefings'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Briefings</button>
          <button onClick={() => { const el = document.getElementById('news-column'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >News</button>
          <button onClick={() => { const el = document.getElementById('narrative-fractures'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Fractures</button>
          <button onClick={() => { const el = document.getElementById('tracked-entities'); if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' }) }} style={{ border: 'none', background: 'transparent', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.05em', textTransform: 'uppercase', padding: '0.35rem 0.65rem', cursor: 'pointer', borderBottom: '2px solid transparent', transition: 'color 160ms ease, border-color 160ms ease' }} onMouseEnter={(e) => { e.target.style.borderColor = C.gold; e.target.style.color = C.textPrimary }} onMouseLeave={(e) => { e.target.style.borderColor = 'transparent'; e.target.style.color = C.textSecondary }} >Entities</button>
        </div>
      </div>

      <div className="main-padding" style={{ padding: '1.5rem', opacity: animationPhase === 'complete' ? 1 : 0, transition: 'opacity 0.3s ease' }}>
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
            <div id="briefings" style={{ marginBottom: '1rem' }}><BriefingLaunchPanel topics={topics} onOpenBriefing={setBriefingPage} onOpenForesight={setForesightPage} /></div>
            <DecisionSurfacePanel
              canonicalEvents={canonicalEvents}
              canonicalEventsLoading={canonicalEventsLoading}
              canonicalEventsError={canonicalEventsError}
              onOpenEventDebug={openEventDebug}
            />
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '1.25rem', marginBottom: '1rem' }}>
              <InstabilityPanel data={instabilityData} loading={instabilityLoading} error={instabilityError} onAnalyze={country => setDeepDive({ title: `Instability Analysis: ${country.label}`, query: `Analyze the current instability situation in ${country.label}. The country scores ${country.score}/100 on the instability index (level: ${country.level}). Break down: what conflict events are occurring, what's driving media attention, are there contradictory narratives across sources, what entities are most active, and what should we watch for in the coming days? Components: conflict=${country.components?.conflict}, media=${country.components?.media_attention}, contradictions=${country.components?.contradiction}, severity=${country.components?.event_severity}. Be analytically precise.`, queryTopic: 'geopolitics', regionContext: country.country })} />
              <CorrelationPanel data={correlationData} loading={correlationLoading} error={correlationError} onAnalyze={card => setDeepDive({ title: `Signal Convergence: ${card.label}`, query: `Analyze the signal convergence detected in ${card.label} (score: ${card.score}/100, type: ${card.convergence_type.replace(/_/g, ' ')}). Active domains: ${card.active_domains.join(', ')}. Domain scores: ${Object.entries(card.domain_scores).map(([k, v]) => `${k}=${v}`).join(', ')}. What is driving this multi-domain convergence? What does the intersection of these signals suggest about the developing situation? What should analysts watch for? Be specific and analytical.`, queryTopic: 'geopolitics', regionContext: card.country })} />
            </div>
            <div className="lower-grid" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.06fr) minmax(0, 0.94fr)', gap: '1.25rem', alignItems: 'start' }}>
              <div id="narrative-fractures" style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}><div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} /><h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Narrative Fractures</h2></div>
                <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                {contradictionsLoading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '88%' : '74%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.55rem', width: '45%' }} /></div>)}</div>}
                {!contradictionsLoading && contradictionEvents.length === 0 && <div style={{ padding: '0.95rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>{contradictionsError || 'No stored framing-divergence events are currently surfaced in the active corpus; this view highlights sources using different framing for the same event.'}</div>}
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
