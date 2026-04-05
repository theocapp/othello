import { useCallback, useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import * as topojson from 'topojson-client'
import { C } from '../constants/theme'
import {
  ATTENTION_WINDOWS,
  HOTSPOT_TYPE_PALETTE,
  getHotspotAspect,
  getHotspotPalette,
  hashToken,
  hotspotDisplayHeadline,
  hotspotEventDescription,
  seededUnit,
} from '../lib/hotspots'
import { truncateText } from '../lib/formatters'

export default function WorldHotspotMap({ data, error, loading, selectedHotspotId, onWindowChange, onSelectHotspot }) {
  const svgRef = useRef(null)
  const zoomGRef = useRef(null)
  const projectionRef = useRef(null)
  const [worldData, setWorldData] = useState(null)
  const [transform, setTransform] = useState(d3.zoomIdentity)
  const [hoveredHotspot, setHoveredHotspot] = useState(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [activeAspects, setActiveAspects] = useState(null)

  const W = 1000
  const H = 520
  const MAP_HIT_RADIUS = 52
  const allHotspots = data?.hotspots || []
  const hotspots = activeAspects ? allHotspots.filter(h => activeAspects.has(getHotspotAspect(h))) : allHotspots
  const selected = hotspots.find(h => h.hotspot_id === selectedHotspotId) || hotspots[0] || null

  useEffect(() => {
    fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json')
      .then(r => r.json())
      .then(topo => setWorldData({ land: topojson.feature(topo, topo.objects.land), borders: topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b) }))
      .catch(() => {})
  }, [])

  useEffect(() => {
    projectionRef.current = d3.geoNaturalEarth1().scale(153).translate([W / 2, H / 2])
  }, [])

  useEffect(() => {
    if (!svgRef.current) return
    const zoom = d3.zoom()
      .scaleExtent([1.5, 10])
      .translateExtent([[-W * 0, -H * 0], [W * 1, H * 1]])
      .filter(event => (event.type === 'wheel' ? event.ctrlKey : !event.button))
      .on('zoom', event => setTransform(event.transform))

    d3.select(svgRef.current).call(zoom)
    d3.select(svgRef.current).call(zoom.transform, d3.zoomIdentity.translate(W / 2, H / 2).scale(1.2).translate(-W / 2, -H / 2))
    return () => d3.select(svgRef.current).on('.zoom', null)
  }, [])

  useEffect(() => {
    const el = svgRef.current
    if (!el) return
    const prevent = e => { if (e.ctrlKey) e.preventDefault() }
    el.addEventListener('wheel', prevent, { passive: false })
    return () => el.removeEventListener('wheel', prevent)
  }, [])

  const project = useCallback((lat, lng) => {
    if (!projectionRef.current) return { x: 0, y: 0 }
    const [px, py] = projectionRef.current([Number(lng), Number(lat)]) || [0, 0]
    return { x: transform.x + px * transform.k, y: transform.y + py * transform.k }
  }, [transform])

  const clientToSvg = useCallback((clientX, clientY) => {
    const svg = svgRef.current
    if (!svg) return null
    const ctm = svg.getScreenCTM()
    if (!ctm) return null
    const pt = svg.createSVGPoint()
    pt.x = clientX
    pt.y = clientY
    return pt.matrixTransform(ctm.inverse())
  }, [])

  const pickHotspotAtSvgPoint = useCallback((cx, cy) => {
    if (!hotspots.length || !projectionRef.current) return null
    const effectiveRadius = MAP_HIT_RADIUS / Math.max(1, transform.k)
    let best = null
    let bestD = Infinity
    const tieEps = 0.5
    for (const h of hotspots) {
      const p = project(h.latitude, h.longitude)
      const d = Math.hypot(p.x - cx, p.y - cy)
      if (d > effectiveRadius) continue
      if (d < bestD - tieEps) {
        bestD = d
        best = h
      } else if (best && Math.abs(d - bestD) <= tieEps && Number(h?.attention_score || 0) > Number(best?.attention_score || 0)) {
        best = h
      }
    }
    return best
  }, [hotspots, project, transform.k])

  const handleMapSvgClick = useCallback(e => {
    if (loading || error || !hotspots.length) return
    const loc = clientToSvg(e.clientX, e.clientY)
    if (!loc) return
    const hit = pickHotspotAtSvgPoint(loc.x, loc.y)
    if (hit) {
      e.preventDefault()
      e.stopPropagation()
      onSelectHotspot(hit.hotspot_id)
    }
  }, [clientToSvg, error, hotspots.length, loading, onSelectHotspot, pickHotspotAtSvgPoint])

  const handleMapSvgMouseMove = useCallback(e => {
    if (loading || error || !hotspots.length) {
      setHoveredHotspot(null)
      return
    }
    const loc = clientToSvg(e.clientX, e.clientY)
    if (!loc) {
      setHoveredHotspot(null)
      return
    }
    const hit = pickHotspotAtSvgPoint(loc.x, loc.y)
    setHoveredHotspot(hit || null)
    const rect = svgRef.current?.getBoundingClientRect()
    if (rect) setTooltipPos({ x: e.clientX - rect.left + 14, y: e.clientY - rect.top - 10 })
  }, [clientToSvg, error, hotspots.length, loading, pickHotspotAtSvgPoint])

  function buildParticles(hotspot, palette) {
    const seed = hashToken(hotspot.hotspot_id)
    const radius = (hotspot.cloud_radius || 32) * 0.7
    const count = 7 + Math.round((hotspot.cloud_density || 0.45) * 6)
    return Array.from({ length: count }, (_, i) => {
      const a = seededUnit(seed + i * 13)
      const b = seededUnit(seed + i * 29)
      const c = seededUnit(seed + i * 47)
      const angle = a * Math.PI * 2
      const dist = Math.pow(b, 0.6) * radius
      return {
        dx: Math.cos(angle) * dist,
        dy: Math.sin(angle) * dist * 0.7,
        r: radius * 0.18 + c * radius * 0.22,
        opacity: 0.12 + (1 - dist / Math.max(radius, 1)) * 0.28,
        fill: palette.cloud,
      }
    })
  }

  const pathGen = projectionRef.current ? d3.geoPath().projection(projectionRef.current) : null

  return (
    <div>
      <div style={{ border: `1px solid ${C.border}`, background: `linear-gradient(180deg, ${C.bgRaised}, ${C.bg})`, padding: '1.1rem', position: 'relative', overflow: 'hidden' }}>
        <div style={{ position: 'absolute', inset: 0, background: 'radial-gradient(circle at 20% 20%, rgba(239,68,68,0.06), transparent 32%), radial-gradient(circle at 78% 24%, rgba(195,202,211,0.06), transparent 28%)', pointerEvents: 'none', zIndex: 2 }} />
        <div style={{ position: 'relative', height: 470, border: `1px solid ${C.border}`, background: 'linear-gradient(180deg, rgba(8,11,16,0.99), rgba(13,17,22,0.99))', overflow: 'hidden' }}>
          <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet" onClick={handleMapSvgClick} onMouseMove={handleMapSvgMouseMove} onMouseLeave={() => setHoveredHotspot(null)} style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', cursor: hoveredHotspot ? 'pointer' : 'grab' }}>
            <defs>
              <filter id="coreGlow" x="-100%" y="-100%" width="300%" height="300%"><feGaussianBlur stdDeviation="3" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
              <filter id="cloudBlur" x="-80%" y="-80%" width="260%" height="260%"><feGaussianBlur stdDeviation="12" /></filter>
              <filter id="ringBlur" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="5" /></filter>
              <clipPath id="mapClip"><rect x="0" y="0" width={W} height={H} /></clipPath>
            </defs>
            <g clipPath="url(#mapClip)">
              <g ref={zoomGRef} transform={`translate(${transform.x},${transform.y}) scale(${transform.k})`}>
                {pathGen && <path d={pathGen({ type: 'Sphere' })} fill="rgba(8,12,18,0.0)" stroke="rgba(195,202,211,0.08)" strokeWidth={0.8 / transform.k} />}
                {pathGen && worldData && <path d={pathGen(worldData.land)} fill="rgba(166,177,190,0.06)" stroke="none" />}
                {pathGen && worldData && <path d={pathGen(worldData.borders)} fill="none" stroke="rgba(195,202,211,0.10)" strokeWidth={0.4 / transform.k} />}
              </g>
              {!loading && !error && hotspots.map(hotspot => {
                const palette = getHotspotPalette(hotspot)
                const pt = project(hotspot.latitude, hotspot.longitude)
                const isSelected = hotspot.hotspot_id === selected?.hotspot_id
                const isHovered = !isSelected && hotspot.hotspot_id === hoveredHotspot?.hotspot_id
                const particles = buildParticles(hotspot, palette)
                const baseCloud = hotspot.cloud_radius || 32
                const cloudRadius = baseCloud / Math.max(1, transform.k * 0.65)
                const selRingRadius = cloudRadius * 0.9
                return (
                  <g key={hotspot.hotspot_id} style={{ pointerEvents: 'none' }}>
                    <ellipse cx={pt.x} cy={pt.y} rx={cloudRadius * 1.6} ry={cloudRadius * 0.95} fill={palette.ring} filter="url(#cloudBlur)" opacity={isSelected ? 0.7 : isHovered ? 0.6 : 0.45} />
                    {particles.map((p, idx) => <circle key={idx} cx={pt.x + p.dx / Math.max(1, transform.k * 0.65)} cy={pt.y + p.dy / Math.max(1, transform.k * 0.65)} r={p.r / Math.max(1, transform.k * 0.65)} fill={palette.ring} opacity={p.opacity * (isSelected ? 1.4 : isHovered ? 1.2 : 1)} filter="url(#cloudBlur)" />)}
                    <circle cx={pt.x} cy={pt.y} r={(8 + (hotspot.intensity || 0.3) * 10) * (isSelected ? 1.2 : isHovered ? 1.05 : 1)} fill={palette.ring} filter="url(#ringBlur)" opacity={isSelected ? 0.6 : isHovered ? 0.5 : 0.35} />
                    <circle cx={pt.x} cy={pt.y} r={isSelected ? 3.8 : isHovered ? 3.2 : 2.2} fill={palette.core} filter="url(#coreGlow)" opacity={1} />
                    {isSelected && <>
                      <circle cx={pt.x} cy={pt.y} r={selRingRadius} fill="none" stroke={palette.core} strokeWidth="0.8" strokeDasharray="4 6" opacity={0.5} />
                      <text x={pt.x + 10} y={pt.y - selRingRadius * 0.85} fill={palette.core} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '10px', letterSpacing: '0.12em', textTransform: 'uppercase' }}>{truncateText(hotspotDisplayHeadline(hotspot), 42)}</text>
                    </>}
                  </g>
                )
              })}
            </g>
          </svg>
          {loading && <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}><div style={{ width: '78%', maxWidth: 520 }}>{[0, 1, 2].map(i => <div key={i} className="skeleton" style={{ height: i === 1 ? '5rem' : '3.5rem', width: i === 0 ? '56%' : i === 1 ? '72%' : '44%', margin: '0 auto 1rem' }} />)}</div></div>}
          {!loading && error && <div style={{ position: 'absolute', left: '1rem', right: '1rem', bottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}1c`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>{error}</div>}
          {!loading && !error && hotspots.length === 0 && <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, letterSpacing: '0.08em' }}>No mapped incident or story hotspots yet for this time window.</div>}
          {hoveredHotspot && <div style={{ position: 'absolute', left: tooltipPos.x, top: tooltipPos.y, zIndex: 10, pointerEvents: 'none', maxWidth: 240, border: `1px solid ${C.borderMid}`, background: 'rgba(9,11,15,0.93)', backdropFilter: 'blur(12px)', padding: '0.6rem 0.75rem' }}>
            <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.88rem', color: C.textPrimary, lineHeight: 1.3, marginBottom: '0.35rem' }}>{hotspotDisplayHeadline(hoveredHotspot)}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.43rem', color: HOTSPOT_TYPE_PALETTE[getHotspotAspect(hoveredHotspot)]?.core || C.silver, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.25rem' }}>{hoveredHotspot.admin1 ? `${hoveredHotspot.admin1} · ${hoveredHotspot.country}` : hoveredHotspot.country}</div>
            {hoveredHotspot.location && hotspotDisplayHeadline(hoveredHotspot) !== hoveredHotspot.location ? <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.06em', marginBottom: hoveredHotspot.sample_events?.[0] ? '0.35rem' : 0 }}>{hoveredHotspot.location}</div> : null}
            {hoveredHotspot.sample_events?.[0] && <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.5 }}>{truncateText(hotspotEventDescription(hoveredHotspot.sample_events[0]), 120)}</div>}
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.08em', marginTop: '0.35rem' }}>{hoveredHotspot.event_count} events · click to select</div>
          </div>}
          <div style={{ position: 'absolute', left: '1rem', top: '1rem', display: 'flex', gap: '0.6rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.55rem 0.75rem', border: `1px solid ${C.border}`, background: 'rgba(9,11,15,0.78)', backdropFilter: 'blur(10px)', zIndex: 3 }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Filter</div>
            {[['conflict', '#ef4444', 'Conflict'], ['political', '#60a5fa', 'Political'], ['economic', '#fbbf24', 'Economic'], ['default', C.silver, 'Other']].map(([key, color, label]) => {
              const isActive = !activeAspects || activeAspects.has(key)
              return <button key={key} onClick={() => setActiveAspects(prev => {
                const all = new Set(['conflict', 'political', 'economic', 'default'])
                const current = prev ?? new Set(all)
                const next = new Set(current)
                if (next.has(key) && next.size > 1) next.delete(key)
                else if (!next.has(key)) {
                  next.add(key)
                  if (next.size === all.size) return null
                }
                return next.size === all.size ? null : next
              })} style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', background: 'none', border: 'none', cursor: 'pointer', padding: '0.1rem 0', opacity: isActive ? 1 : 0.35, transition: 'opacity 0.15s' }}>
                <div style={{ width: 5, height: 5, borderRadius: '50%', background: color, flexShrink: 0 }} />
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{label}</span>
              </button>
            })}
            {activeAspects && <button onClick={() => setActiveAspects(null)} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', letterSpacing: '0.08em', padding: '0.2rem 0.4rem', cursor: 'pointer' }}>ALL</button>}
          </div>
          <div style={{ position: 'absolute', left: '1rem', right: '1rem', bottom: '1rem', display: 'flex', gap: '0.45rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.65rem 0.75rem', border: `1px solid ${C.border}`, background: 'rgba(9,11,15,0.78)', backdropFilter: 'blur(10px)', zIndex: 3 }}>
            {ATTENTION_WINDOWS.map(item => {
              const active = data?.window === item.id
              return <button key={item.id} onClick={() => onWindowChange(item.id)} style={{ background: active ? `${C.red}18` : 'rgba(19,22,26,0.92)', border: `1px solid ${active ? C.red : C.borderMid}`, color: active ? C.textPrimary : C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.49rem', letterSpacing: '0.08em', padding: '0.45rem 0.6rem', cursor: 'pointer', borderRadius: 999, transition: 'all 0.15s ease' }}>{item.label}</button>
            })}
          </div>
        </div>
      </div>
    </div>
  )
}
