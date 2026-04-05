import { Suspense, lazy } from 'react'
import BriefingLaunchPanel from './BriefingLaunchPanel'
import CorrelationPanel from './CorrelationPanel'
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

  return (
    <>
      <div style={{ position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100, transform: headerVisible ? 'translateY(0)' : 'translateY(-100%)', transition: 'transform 0.3s ease', background: `${C.bg}e8`, backdropFilter: 'blur(16px)', borderBottom: `1px solid ${C.border}`, padding: '0.85rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1.5rem' }}>
        <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.1rem', fontWeight: 700, letterSpacing: '-0.01em', color: C.textPrimary, flexShrink: 0 }}>OTHELLO</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          {worldClocks.map((clock, index) => <div key={clock.zone} style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{clock.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, letterSpacing: '0.03em' }}>{formatClock(time, clock.zone)}</div>{index < worldClocks.length - 1 && <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, marginLeft: '0.1rem' }} />}</div>)}
        </div>
      </div>
      <div className="main-padding" style={{ padding: '0 1.5rem' }}>
        <header className="header-section" style={{ paddingTop: '9vh', paddingBottom: '1.2rem', animation: 'fadeUp 0.6s ease' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{dateStr.toUpperCase()} — {timeStr}</div>
            <div style={{ width: '1px', height: '0.8rem', background: C.borderMid }} />
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textMuted, letterSpacing: '0.08em' }}>LAST UPDATE: <span style={{ color: C.textSecondary }}>{lastUpdatedStr}</span></div>
          </div>
        </header>
        {healthFetchError && <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', animation: 'fadeUp 0.4s ease both' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.3rem' }}>API health check failed</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.5 }}>{healthFetchError}</div></div>}
        {!healthFetchError && healthSnapshot?.runtime && (!healthSnapshot.runtime.llm_ready || !healthSnapshot.runtime.contradiction_ready) && <div style={{ marginBottom: '1rem', border: `1px solid rgba(251,191,36,0.35)`, background: 'rgba(251,191,36,0.06)', padding: '0.75rem 1rem', animation: 'fadeUp 0.4s ease both' }}><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: '#fbbf24', letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.28rem' }}>Partial capability</div><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.55 }}>{!healthSnapshot.runtime.llm_ready && 'LLM-backed answers and briefings may use fallbacks (set GROQ_API_KEY on the API). '}{!healthSnapshot.runtime.contradiction_ready && 'Narrative fracture mining is limited without ANTHROPIC_API_KEY. '}</div></div>}
        <div className="home-shell" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.45fr) 360px', gridTemplateAreas: '"map sidebar" "lower sidebar"', gap: '1.25rem', alignItems: 'start', paddingBottom: '10vh' }}>
          <section style={{ gridArea: 'map', animation: 'fadeUp 0.6s ease 0.08s both' }}>
            <Suspense fallback={<div style={{ minHeight: 520, border: `1px solid ${C.border}`, background: C.bgRaised }} />}>
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
          <aside style={{ gridArea: 'sidebar', display: 'flex', flexDirection: 'column', gap: '1rem', animation: 'fadeUp 0.6s ease 0.14s both' }}>
            <MapSummaryPanel data={mapAttention} hotspot={selectedHotspot} onOpenBriefing={setBriefingPage} onAnalyzeCluster={openHotspotClusterAnalysis} />
            <NewsColumn headlines={headlines} headlinesLoading={headlinesLoading} headlinesLoaded={headlinesLoaded} headlinesError={headlinesError} headlineSort={headlineSort} headlineRegion={headlineRegion} headlineRegions={headlineRegions} onChangeSort={async value => { setHeadlineSort(value); await loadHeadlines({ sortBy: value, region: headlineRegion }) }} onChangeRegion={async value => { setHeadlineRegion(value); await loadHeadlines({ sortBy: headlineSort, region: value }) }} onRefresh={() => loadHeadlines()} onOpenStory={openStoryDeepDive} />
          </aside>
          <section style={{ gridArea: 'lower', animation: 'fadeUp 0.6s ease 0.2s both' }}>
            <div style={{ marginBottom: '1rem' }}><BriefingLaunchPanel topics={topics} onOpenBriefing={setBriefingPage} onOpenForesight={setForesightPage} /></div>
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '1.25rem', marginBottom: '1rem' }}>
              <InstabilityPanel data={instabilityData} loading={instabilityLoading} error={instabilityError} onAnalyze={country => setDeepDive({ title: `Instability Analysis: ${country.label}`, query: `Analyze the current instability situation in ${country.label}. The country scores ${country.score}/100 on the instability index (level: ${country.level}). Break down: what conflict events are occurring, what's driving media attention, are there contradictory narratives across sources, what entities are most active, and what should we watch for in the coming days? Components: conflict=${country.components?.conflict}, media=${country.components?.media_attention}, contradictions=${country.components?.contradiction}, severity=${country.components?.event_severity}. Be analytically precise.`, queryTopic: 'geopolitics', regionContext: country.country })} />
              <CorrelationPanel data={correlationData} loading={correlationLoading} error={correlationError} onAnalyze={card => setDeepDive({ title: `Signal Convergence: ${card.label}`, query: `Analyze the signal convergence detected in ${card.label} (score: ${card.score}/100, type: ${card.convergence_type.replace(/_/g, ' ')}). Active domains: ${card.active_domains.join(', ')}. Domain scores: ${Object.entries(card.domain_scores).map(([k, v]) => `${k}=${v}`).join(', ')}. What is driving this multi-domain convergence? What does the intersection of these signals suggest about the developing situation? What should analysts watch for? Be specific and analytical.`, queryTopic: 'geopolitics', regionContext: card.country })} />
            </div>
            <div className="lower-grid" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.06fr) minmax(0, 0.94fr)', gap: '1.25rem', alignItems: 'start' }}>
              <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}><div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} /><h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Narrative Fractures</h2></div>
                <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                {contradictionsLoading && <div>{[0, 1, 2].map(i => <div key={i} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '88%' : '74%', marginBottom: '0.35rem' }} /><div className="skeleton" style={{ height: '0.55rem', width: '45%' }} /></div>)}</div>}
                {!contradictionsLoading && contradictionEvents.length === 0 && <div style={{ padding: '0.95rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>{contradictionsError || 'No stored contradiction-rich events are currently surfaced in the active corpus.'}</div>}
                {!contradictionsLoading && contradictionEvents.map((event, i) => <div key={event.event_id || i} className="theater-row" onClick={() => setSelectedContradiction(event)} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.25rem' }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.94rem', color: C.textSecondary, lineHeight: 1.5 }}>{event.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>{totalNarrativeFlags(event)} FLAGS</div></div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.6 }}><div>{formatDateTime(event.latest_update)}</div><div>{event.source_count} sources split across the cluster{(event.narrative_fracture_count || 0) > 0 ? ` · ${event.narrative_fracture_count} framing fractures` : ''}</div></div></div>)}
              </div>
              <div style={{ display: 'grid', gap: '1.25rem' }}>
                <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}><div style={{ width: 6, height: 6, borderRadius: '50%', background: C.silver }} /><h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Timelines</h2></div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                  {theaters.map((item, i) => <div key={i} className="theater-row" onClick={() => setTimelinePage(item.query)} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.4 }}>{item.label}</div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0, marginLeft: '1rem' }}>VIEW →</div></div>)}
                </div>
                <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Tracked Entities</div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />
                  {!entitySignals && (entitySignalsError ? <div style={{ padding: '0.8rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>{entitySignalsError}</div> : <div>{[0, 1, 2, 3, 4].map(i => <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}` }}><div className="skeleton" style={{ height: '0.75rem', width: '55%' }} /><div className="skeleton" style={{ height: '0.75rem', width: '15%' }} /></div>)}</div>)}
                  {entitySignals && <div>
                    {entitySignals.spikes?.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 4).length > 0 && <div style={{ marginBottom: '1.25rem' }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}><div style={{ width: 4, height: 4, borderRadius: '50%', background: C.red }} /><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, letterSpacing: '0.15em' }}>SURGING</div></div>{entitySignals.spikes.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 4).map((e, i) => <div key={i} className="entity-item" onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they playing in current geopolitical events, why are they suddenly getting increased attention in the news, what are their motivations and capabilities, and what should we expect from them in the coming weeks? Be specific and analytical.`, entity: e.entity, queryTopic: 'geopolitics' })} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><span style={{ fontSize: '0.85rem', color: C.textPrimary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>{e.type}</span></div><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, flexShrink: 0 }}>{e.trend === 'NEW' ? 'NEW' : `${e.spike_ratio}×`}</span></div>)}</div>}
                    {entitySignals.top_entities?.length > 0 && <div><div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.12em', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}>MOST DISCUSSED</div>{entitySignals.top_entities.slice(0, 6).map((e, i) => <div key={i} className="entity-item" onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they currently playing in world events, what are their key actions and motivations right now, and what should we be watching for? Be direct and analytically precise.`, entity: e.entity, queryTopic: 'geopolitics' })} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}><div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, width: '0.9rem' }}>{i + 1}</span><span style={{ fontSize: '0.85rem', color: C.textSecondary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span></div><span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0 }}>{e.mentions}</span></div>)}</div>}
                  </div>}
                </div>
              </div>
            </div>
          </section>
        </div>
      </div>
    </>
  )
}
