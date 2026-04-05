import { useState, useEffect, useRef, useCallback } from 'react'
import { fetchBeforeNewsArchive, fetchBriefing, fetchCorrelations, fetchEntityReference, fetchEntitySignals, fetchEvents, fetchHeadlines, fetchHealth, fetchInstability, fetchPredictionLedger, fetchRegionAttention, fetchTimeline, sendQuery } from './api'
import ReactMarkdown from 'react-markdown'
import * as d3 from 'd3'
import * as topojson from 'topojson-client'

const C = {
  bg: '#13161a',
  bgRaised: '#1a1e24',
  bgHover: '#1e2329',
  border: '#1e2228',
  borderMid: '#2a2f38',
  textPrimary: '#f6f7f9',
  textSecondary: '#b3bac4',
  textMuted: '#7d8794',
  silver: '#c3cad3',
  red: '#ef4444',
  redDeep: '#dc2626',
  white: '#ffffff',
}

const MD = {
  p: ({ children }) => <p style={{ marginBottom: '0.75rem', lineHeight: 1.85 }}>{children}</p>,
  strong: ({ children }) => <strong style={{ fontWeight: 700, color: C.textPrimary }}>{children}</strong>,
  h1: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.2rem', fontWeight: 700, margin: '1.25rem 0 0.5rem', color: C.textPrimary }}>{children}</div>,
  h2: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1rem', fontWeight: 700, margin: '1rem 0 0.4rem', color: C.textPrimary }}>{children}</div>,
  h3: ({ children }) => <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.62rem', letterSpacing: '0.15em', color: C.silver, margin: '0.75rem 0 0.35rem', textTransform: 'uppercase' }}>{children}</div>,
  li: ({ children }) => (
    <div style={{ display: 'flex', gap: '0.65rem', marginBottom: '0.45rem', alignItems: 'flex-start' }}>
      <span style={{ color: C.silver, flexShrink: 0, fontSize: '0.5rem', marginTop: '0.45rem' }}>◆</span>
      <span>{children}</span>
    </div>
  ),
  ul: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  ol: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  hr: () => <div style={{ borderTop: `1px solid ${C.border}`, margin: '1.25rem 0' }} />,
  table: ({ children }) => (
    <div style={{ overflowX: 'auto', margin: '0.75rem 0' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>{children}</table>
    </div>
  ),
  th: ({ children }) => <th style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.silver, padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.borderMid}`, textAlign: 'left', letterSpacing: '0.08em' }}>{children}</th>,
  td: ({ children }) => <td style={{ padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.border}`, color: C.textSecondary }}>{children}</td>,
  blockquote: ({ children }) => <div style={{ borderLeft: `2px solid ${C.silver}`, paddingLeft: '1rem', color: C.textSecondary, fontStyle: 'italic', margin: '0.75rem 0' }}>{children}</div>,
}

function formatDateTime(value) {
  if (!value) return 'Undated'
  try {
    const parsed = parseDateValue(value)
    if (!parsed) return String(value)
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(parsed)
  } catch {
    return value
  }
}

function parseDateValue(value) {
  if (!value) return null
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value
  if (typeof value !== 'string') {
    const parsed = new Date(value)
    return Number.isNaN(parsed.getTime()) ? null : parsed
  }

  const text = value.trim()
  const compact = text.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/)
  if (compact) {
    const [, year, month, day, hour, minute, second] = compact
    return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`)
  }

  const parsed = new Date(text)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

function formatClock(value, timeZone) {
  try {
    return new Intl.DateTimeFormat('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone,
    }).format(value)
  } catch {
    return '—'
  }
}

function formatDateLabel(value) {
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  }).format(value)
}

function formatRelativeUpdate(value, timeZone) {
  const parsed = parseDateValue(value)
  if (!parsed) return '—'

  const nowText = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date())
  const targetText = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(parsed)

  const nowDate = new Date(`${nowText}T00:00:00`)
  const targetDate = new Date(`${targetText}T00:00:00`)
  const dayDiff = Math.round((nowDate.getTime() - targetDate.getTime()) / 86400000)
  const clock = formatClock(parsed, timeZone)

  if (dayDiff === 0) return `Today, ${clock}`
  if (dayDiff === 1) return `Yesterday, ${clock}`
  return `${targetText}, ${clock}`
}

function totalNarrativeFlags(event) {
  return (event?.contradiction_count || 0) + (event?.narrative_fracture_count || 0)
}

function friendlyErrorMessage(err, label) {
  if (err?.code === 'ECONNABORTED') return `Timed out while loading ${label}. The backend may be slow or unavailable.`
  if (!err?.response) return `Unable to reach the backend for ${label}.`
  if (err.response?.data?.detail) return `Unable to load ${label}: ${err.response.data.detail}`
  return `Unable to load ${label} right now.`
}

function formatRegionLabel(region) {
  if (!region) return 'Global'
  return region
    .split('-')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

const ATTENTION_WINDOWS = [
  { id: '24h', label: '24 Hours' },
  { id: '7d', label: 'This Week' },
  { id: '30d', label: 'This Month' },
  { id: '90d', label: 'Past 3 Months' },
  { id: '180d', label: 'Past 6 Months' },
  { id: '365d', label: 'Past Year' },
  { id: '1825d', label: 'Past 5 Years' },
]

const HOTSPOT_TYPE_PALETTE = {
  conflict:  { core: 'rgba(239,68,68,0.95)',   ring: 'rgba(239,68,68,0.18)',   cloud: 'rgba(239,68,68,0.09)'  },
  political: { core: 'rgba(96,165,250,0.95)',  ring: 'rgba(96,165,250,0.18)',  cloud: 'rgba(96,165,250,0.09)' },
  economic:  { core: 'rgba(251,191,36,0.95)',  ring: 'rgba(251,191,36,0.18)',  cloud: 'rgba(251,191,36,0.09)' },
  default:   { core: 'rgba(195,202,211,0.90)', ring: 'rgba(195,202,211,0.15)', cloud: 'rgba(195,202,211,0.07)'},
}

function getHotspotAspect(hotspot) {
  const aspect = String(hotspot?.aspect || '').toLowerCase()
  if (aspect === 'conflict' || aspect === 'political' || aspect === 'economic') return aspect
  const types = (hotspot.event_types || []).map(t => t.toLowerCase())
  if (types.some(t => t.includes('conflict') || t.includes('battle') || t.includes('violence') || t.includes('explosion') || t.includes('attack') || t.includes('airstrike') || t.includes('strike') || t.includes('missile') || t.includes('drone') || t.includes('war') || t.includes('military') || t.includes('clash'))) return 'conflict'
  if (types.some(t => t.includes('politic') || t.includes('riot') || t.includes('protest') || t.includes('government'))) return 'political'
  if (types.some(t => t.includes('econom') || t.includes('market') || t.includes('sanction') || t.includes('trade'))) return 'economic'
  if (hotspot?.source_kind === 'story') {
    const topic = String(hotspot?.topic || '').toLowerCase()
    if (topic === 'economics') return 'economic'
    if (topic === 'geopolitics') return 'political'
  }
  return 'default'
}

function getHotspotPalette(hotspot) {
  return HOTSPOT_TYPE_PALETTE[getHotspotAspect(hotspot)] || HOTSPOT_TYPE_PALETTE.default
}

function formatAttentionShare(value) {
  return `${Math.round((value || 0) * 100)}%`
}

function formatWindowLabel(windowId) {
  return ATTENTION_WINDOWS.find(item => item.id === windowId)?.label || windowId
}

/** Prefer backend development-style headline when present (semantic clusters / materialized stories). */
function hotspotDisplayHeadline(hotspot) {
  const explicit = String(hotspot?.headline || hotspot?.development_title || '').trim()
  if (explicit) return explicit
  return String(hotspot?.label || '').trim()
}

function truncateText(text, limit = 180) {
  const clean = String(text || '').trim()
  if (!clean) return ''
  if (clean.length <= limit) return clean
  return `${clean.slice(0, limit - 1).trimEnd()}…`
}

const BRIEFING_SECTION_KEYS = [
  'SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS',
  'SIGNAL vs NOISE', 'PREDICTIONS', 'DEEPER CONTEXT',
  'WHAT TO WATCH', 'SOURCE CONTRADICTIONS',
]

function parseBriefingSections(text) {
  const result = {}
  BRIEFING_SECTION_KEYS.forEach((section, i) => {
    const next = BRIEFING_SECTION_KEYS[i + 1]
    const escaped = section.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const regex = new RegExp(
      `(?:#{1,3}\\s*)?${escaped}:?\\s*([\\s\\S]*?)${next ? `(?=(?:#{1,3}\\s*)?${next.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:?)` : '$'}`, 'i'
    )
    const match = text.match(regex)
    result[section] = match ? match[1].trim() : ''
  })
  return result
}

function mapAspectToQueryTopic(aspect) {
  if (aspect === 'economic') return 'economics'
  return 'geopolitics'
}

function buildHotspotClusterAnalysisQuery(hotspot) {
  const place = [hotspot.location || hotspot.label, hotspot.admin1, hotspot.country].filter(Boolean).join(', ')
  const samples = (hotspot.sample_events || []).slice(0, 5).map(ev => hotspotEventDescription(ev)).filter(Boolean)
  const sampleBlock = samples.length ? ` Representative cluster reporting includes: ${samples.join(' · ')}` : ''
  return `Focus on this mapped cluster${place ? ` in ${place}` : ''}.${sampleBlock} Give a concise intelligence briefing: current situation, key actors, escalation risks, and what to watch next. Anchor analysis to this geography and cluster rather than generic global themes.`
}

function collectHotspotSourceUrls(hotspot) {
  if (!hotspot?.sample_events?.length) return []
  const urls = []
  const seen = new Set()
  for (const event of hotspot.sample_events) {
    const candidates = [
      ...(Array.isArray(event?.source_urls) ? event.source_urls : []),
      event?.event_id,
    ]
    for (const candidate of candidates) {
      if (typeof candidate !== 'string' || !candidate.startsWith('http') || seen.has(candidate)) continue
      seen.add(candidate)
      urls.push(candidate)
      if (urls.length >= 12) return urls
    }
  }
  return urls
}

function normalizeStoryTopicForQuery(topic) {
  const s = String(topic || '').toLowerCase().replace(/_/g, '')
  if (s === 'economics' || s === 'economic') return 'economics'
  if (s === 'geopolitics' || s === 'political') return 'geopolitics'
  return null
}

function hashToken(value) {
  let hash = 2166136261
  for (let index = 0; index < String(value || '').length; index += 1) {
    hash ^= String(value).charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return Math.abs(hash >>> 0)
}

function seededUnit(seed) {
  const x = Math.sin(seed * 12.9898) * 43758.5453
  return x - Math.floor(x)
}

function WorldHotspotMap({ data, error, loading, selectedHotspotId, onWindowChange, onSelectHotspot }) {
  const svgRef = useRef(null)
  const zoomGRef = useRef(null)
  const projectionRef = useRef(null)
  const [worldData, setWorldData] = useState(null)
  const [transform, setTransform] = useState(d3.zoomIdentity)
  const [hoveredHotspot, setHoveredHotspot] = useState(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  // null means "all active"; set contains aspect strings that are visible
  const [activeAspects, setActiveAspects] = useState(null)

  const W = 1000
  const H = 520
  /**
   * Max hit distance in base SVG viewBox units (before zoom).
   * Divided by transform.k at query time so zooming in gives tighter selection.
   */
  const MAP_HIT_RADIUS = 52

  const allHotspots = data?.hotspots || []
  // Apply type filter
  const hotspots = activeAspects
    ? allHotspots.filter(h => activeAspects.has(getHotspotAspect(h)))
    : allHotspots
  const selected = hotspots.find(h => h.hotspot_id === selectedHotspotId) || hotspots[0] || null

  // Load world topology
  useEffect(() => {
    fetch('/world/countries-110m.json')
      .then(r => r.json())
      .then(topo => {
        setWorldData({
          land: topojson.feature(topo, topo.objects.land),
          borders: topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b),
        })
      })
      .catch(() => {})
  }, [])

  // Set up projection once
  useEffect(() => {
    projectionRef.current = d3.geoNaturalEarth1()
      .scale(153)
      .translate([W / 2, H / 2])
  }, [])

  // Set up zoom — pinch only (block scroll wheel)
  useEffect(() => {
    if (!svgRef.current) return
    const zoom = d3.zoom()
    .scaleExtent([1.5, 10])
    .translateExtent([[-W * 0, -H * 0], [W * 1, H * 1]])
    .filter(event => {
    if (event.type === 'wheel') return event.ctrlKey
    return !event.button
    })
    .on('zoom', event => setTransform(event.transform))

    d3.select(svgRef.current).call(zoom)
    d3.select(svgRef.current).call(
    zoom.transform,
    d3.zoomIdentity
      .translate(W / 2, H / 2)
      .scale(1.2)
      .translate(-W / 2, -H / 2)
      )
    return () => d3.select(svgRef.current).on('.zoom', null)
    }, [])
  
  useEffect(() => {
    const el = svgRef.current
    if (!el) return
    const prevent = e => { if (e.ctrlKey) e.preventDefault() }
    el.addEventListener('wheel', prevent, { passive: false })
    return () => el.removeEventListener('wheel', prevent)
    }, [])

  // Project lat/lng → screen coords through current zoom (memoized to stay stable in callbacks)
  const project = useCallback(
    (lat, lng) => {
      if (!projectionRef.current) return { x: 0, y: 0 }
      const [px, py] = projectionRef.current([Number(lng), Number(lat)]) || [0, 0]
      return { x: transform.x + px * transform.k, y: transform.y + py * transform.k }
    },
    [transform],
  )

  // Convert a native client-space pointer to SVG viewBox coordinates
  const clientToSvg = useCallback(
    (clientX, clientY) => {
      const svg = svgRef.current
      if (!svg) return null
      const ctm = svg.getScreenCTM()
      if (!ctm) return null
      const pt = svg.createSVGPoint()
      pt.x = clientX
      pt.y = clientY
      return pt.matrixTransform(ctm.inverse())
    },
    [],
  )

  // Find the nearest hotspot within an effective radius that scales with zoom
  const pickHotspotAtSvgPoint = useCallback(
    (cx, cy) => {
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
        } else if (best && Math.abs(d - bestD) <= tieEps) {
          const score = x => Number(x?.attention_score || 0)
          if (score(h) > score(best)) best = h
        }
      }
      return best
    },
    [hotspots, transform, project],
  )

  const handleMapSvgClick = useCallback(
    e => {
      if (loading || error || !hotspots.length) return
      const loc = clientToSvg(e.clientX, e.clientY)
      if (!loc) return
      const hit = pickHotspotAtSvgPoint(loc.x, loc.y)
      if (hit) {
        e.preventDefault()
        e.stopPropagation()
        onSelectHotspot(hit.hotspot_id)
      }
    },
    [loading, error, hotspots.length, clientToSvg, pickHotspotAtSvgPoint, onSelectHotspot],
  )

  const handleMapSvgMouseMove = useCallback(
    e => {
      if (loading || error || !hotspots.length) {
        setHoveredHotspot(null)
        return
      }
      const loc = clientToSvg(e.clientX, e.clientY)
      if (!loc) { setHoveredHotspot(null); return }
      const hit = pickHotspotAtSvgPoint(loc.x, loc.y)
      setHoveredHotspot(hit || null)
      // tooltip position: offset from the SVG container top-left in CSS pixels
      const rect = svgRef.current?.getBoundingClientRect()
      if (rect) setTooltipPos({ x: e.clientX - rect.left + 14, y: e.clientY - rect.top - 10 })
    },
    [loading, error, hotspots.length, clientToSvg, pickHotspotAtSvgPoint],
  )

  const pathGen = projectionRef.current ? d3.geoPath().projection(projectionRef.current) : null

  // Build tight particle cloud — fewer, denser
  function buildParticles(hotspot, palette) {
    const seed = hashToken(hotspot.hotspot_id)
    const radius = (hotspot.cloud_radius || 32) * 0.7 // tighter
    const count = 7 + Math.round((hotspot.cloud_density || 0.45) * 6) // fewer
    const particles = []
    for (let i = 0; i < count; i++) {
      const a = seededUnit(seed + i * 13)
      const b = seededUnit(seed + i * 29)
      const c = seededUnit(seed + i * 47)
      const angle = a * Math.PI * 2
      const dist = Math.pow(b, 0.6) * radius
      particles.push({
        dx: Math.cos(angle) * dist,
        dy: Math.sin(angle) * dist * 0.7,
        r: radius * 0.18 + c * radius * 0.22,
        opacity: 0.12 + (1 - dist / Math.max(radius, 1)) * 0.28,
        fill: palette.cloud,
      })
    }
    return particles
  }

  return (
    <div>
      <div style={{ border: `1px solid ${C.border}`, background: `linear-gradient(180deg, ${C.bgRaised}, ${C.bg})`, padding: '1.1rem', position: 'relative', overflow: 'hidden' }}>
        <div style={{ position: 'absolute', inset: 0, background: 'radial-gradient(circle at 20% 20%, rgba(239,68,68,0.06), transparent 32%), radial-gradient(circle at 78% 24%, rgba(195,202,211,0.06), transparent 28%)', pointerEvents: 'none', zIndex: 2 }} />

        <div style={{ position: 'relative', height: 470, border: `1px solid ${C.border}`, background: 'linear-gradient(180deg, rgba(8,11,16,0.99), rgba(13,17,22,0.99))', overflow: 'hidden' }}>
          <svg
            ref={svgRef}
            viewBox={`0 0 ${W} ${H}`}
            preserveAspectRatio="xMidYMid meet"
            onClick={handleMapSvgClick}
            onMouseMove={handleMapSvgMouseMove}
            onMouseLeave={() => setHoveredHotspot(null)}
            style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', cursor: hoveredHotspot ? 'pointer' : 'grab' }}
          >
            <defs>
              <filter id="coreGlow" x="-100%" y="-100%" width="300%" height="300%">
                <feGaussianBlur stdDeviation="3" result="blur" />
                <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
              </filter>
              <filter id="cloudBlur" x="-80%" y="-80%" width="260%" height="260%">
                <feGaussianBlur stdDeviation="12" />
              </filter>
              <filter id="ringBlur" x="-60%" y="-60%" width="220%" height="220%">
                <feGaussianBlur stdDeviation="5" />
              </filter>
              <clipPath id="mapClip">
                <rect x="0" y="0" width={W} height={H} />
              </clipPath>
            </defs>

            <g clipPath="url(#mapClip)">
              {/* Geo layer — transforms with zoom/pan */}
              <g ref={zoomGRef} transform={`translate(${transform.x},${transform.y}) scale(${transform.k})`}>
                {/* Ocean base */}
                {pathGen && (
                  <path
                    d={pathGen({ type: 'Sphere' })}
                    fill="rgba(8,12,18,0.0)"
                    stroke="rgba(195,202,211,0.08)"
                    strokeWidth={0.8 / transform.k}
                  />
                )}
                {/* Land mass — very subtle fill */}
                {pathGen && worldData && (
                  <path
                    d={pathGen(worldData.land)}
                    fill="rgba(166,177,190,0.06)"
                    stroke="none"
                  />
                )}
                {/* Country borders — very faint */}
                {pathGen && worldData && (
                  <path
                    d={pathGen(worldData.borders)}
                    fill="none"
                    stroke="rgba(195,202,211,0.10)"
                    strokeWidth={0.4 / transform.k}
                  />
                )}
              </g>

              {/* Hotspot layer — NOT geo-transformed, projected manually */}
              {!loading && !error && hotspots.map(hotspot => {
                const palette = getHotspotPalette(hotspot)
                const pt = project(hotspot.latitude, hotspot.longitude)
                const isSelected = hotspot.hotspot_id === selected?.hotspot_id
                const isHovered = !isSelected && hotspot.hotspot_id === hoveredHotspot?.hotspot_id
                const particles = buildParticles(hotspot, palette)
                // Cloud is geographically fixed — shrink proportionally when zooming in
                const baseCloud = hotspot.cloud_radius || 32
                const cloudRadius = baseCloud / Math.max(1, transform.k * 0.65)
                const selRingRadius = baseCloud / Math.max(1, transform.k * 0.65) * 0.9

                return (
                  <g key={hotspot.hotspot_id} style={{ pointerEvents: 'none' }}>
                    {/* Outer soft cloud */}
                    <ellipse
                      cx={pt.x} cy={pt.y}
                      rx={cloudRadius * 1.6} ry={cloudRadius * 0.95}
                      fill={palette.ring}
                      filter="url(#cloudBlur)"
                      opacity={isSelected ? 0.7 : isHovered ? 0.6 : 0.45}
                    />
                    {/* Dense inner particles */}
                    {particles.map((p, idx) => (
                      <circle
                        key={idx}
                        cx={pt.x + p.dx / Math.max(1, transform.k * 0.65)}
                        cy={pt.y + p.dy / Math.max(1, transform.k * 0.65)}
                        r={p.r / Math.max(1, transform.k * 0.65)}
                        fill={palette.ring}
                        opacity={p.opacity * (isSelected ? 1.4 : isHovered ? 1.2 : 1)}
                        filter="url(#cloudBlur)"
                      />
                    ))}
                    {/* Sharp ring */}
                    <circle
                      cx={pt.x} cy={pt.y}
                      r={(8 + (hotspot.intensity || 0.3) * 10) * (isSelected ? 1.2 : isHovered ? 1.05 : 1)}
                      fill={palette.ring}
                      filter="url(#ringBlur)"
                      opacity={isSelected ? 0.6 : isHovered ? 0.5 : 0.35}
                    />
                    {/* Sharp core dot */}
                    <circle
                      cx={pt.x} cy={pt.y}
                      r={isSelected ? 3.8 : isHovered ? 3.2 : 2.2}
                      fill={palette.core}
                      filter="url(#coreGlow)"
                      opacity={1}
                    />
                    {/* Selection ring */}
                    {isSelected && (
                      <>
                        <circle
                          cx={pt.x} cy={pt.y}
                          r={selRingRadius}
                          fill="none"
                          stroke={palette.core}
                          strokeWidth="0.8"
                          strokeDasharray="4 6"
                          opacity={0.5}
                        />
                        <text
                          x={pt.x + 10}
                          y={pt.y - selRingRadius * 0.85}
                          fill={palette.core}
                          style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '10px', letterSpacing: '0.12em', textTransform: 'uppercase' }}
                        >
                          {truncateText(hotspotDisplayHeadline(hotspot), 42)}
                        </text>
                      </>
                    )}
                  </g>
                )
              })}
            </g>
          </svg>

          {loading && (
            <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <div style={{ width: '78%', maxWidth: 520 }}>
                {[0, 1, 2].map(i => (
                  <div key={i} className="skeleton" style={{ height: i === 1 ? '5rem' : '3.5rem', width: i === 0 ? '56%' : i === 1 ? '72%' : '44%', margin: '0 auto 1rem' }} />
                ))}
              </div>
            </div>
          )}

          {!loading && error && (
            <div style={{ position: 'absolute', left: '1rem', right: '1rem', bottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}1c`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>
              {error}
            </div>
          )}

          {!loading && !error && hotspots.length === 0 && (
            <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, letterSpacing: '0.08em' }}>
              No mapped incident or story hotspots yet for this time window.
            </div>
          )}

          {/* Hover tooltip */}
          {hoveredHotspot && (
            <div
              style={{
                position: 'absolute',
                left: tooltipPos.x,
                top: tooltipPos.y,
                zIndex: 10,
                pointerEvents: 'none',
                maxWidth: 240,
                border: `1px solid ${C.borderMid}`,
                background: 'rgba(9,11,15,0.93)',
                backdropFilter: 'blur(12px)',
                padding: '0.6rem 0.75rem',
              }}
            >
              <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.88rem', color: C.textPrimary, lineHeight: 1.3, marginBottom: '0.35rem' }}>
                {hotspotDisplayHeadline(hoveredHotspot)}
              </div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.43rem', color: HOTSPOT_TYPE_PALETTE[getHotspotAspect(hoveredHotspot)]?.core || C.silver, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.25rem' }}>
                {hoveredHotspot.admin1 ? `${hoveredHotspot.admin1} · ${hoveredHotspot.country}` : hoveredHotspot.country}
              </div>
              {hoveredHotspot.location && hotspotDisplayHeadline(hoveredHotspot) !== hoveredHotspot.location ? (
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.06em', marginBottom: hoveredHotspot.sample_events?.[0] ? '0.35rem' : 0 }}>
                  {hoveredHotspot.location}
                </div>
              ) : null}
              {hoveredHotspot.sample_events?.[0] && (
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.5 }}>
                  {truncateText(hotspotEventDescription(hoveredHotspot.sample_events[0]), 120)}
                </div>
              )}
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.08em', marginTop: '0.35rem' }}>
                {hoveredHotspot.event_count} events · click to select
              </div>
            </div>
          )}

          <div style={{ position: 'absolute', left: '1rem', top: '1rem', display: 'flex', gap: '0.6rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.55rem 0.75rem', border: `1px solid ${C.border}`, background: 'rgba(9,11,15,0.78)', backdropFilter: 'blur(10px)', zIndex: 3 }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
              Filter
            </div>
            {[['conflict', '#ef4444', 'Conflict'], ['political', '#60a5fa', 'Political'], ['economic', '#fbbf24', 'Economic'], ['default', C.silver, 'Other']].map(([key, color, label]) => {
              const isActive = !activeAspects || activeAspects.has(key)
              return (
                <button
                  key={key}
                  onClick={() => {
                    setActiveAspects(prev => {
                      const all = new Set(['conflict', 'political', 'economic', 'default'])
                      const current = prev ?? new Set(all)
                      const next = new Set(current)
                      if (next.has(key) && next.size > 1) next.delete(key)
                      else if (!next.has(key)) {
                        next.add(key)
                        if (next.size === all.size) return null // all = no filter
                      }
                      return next.size === all.size ? null : next
                    })
                  }}
                  style={{
                    display: 'flex', alignItems: 'center', gap: '0.35rem',
                    background: 'none', border: 'none', cursor: 'pointer', padding: '0.1rem 0',
                    opacity: isActive ? 1 : 0.35, transition: 'opacity 0.15s',
                  }}
                >
                  <div style={{ width: 5, height: 5, borderRadius: '50%', background: color, flexShrink: 0 }} />
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{label}</span>
                </button>
              )
            })}
            {activeAspects && (
              <button
                onClick={() => setActiveAspects(null)}
                style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', letterSpacing: '0.08em', padding: '0.2rem 0.4rem', cursor: 'pointer' }}
              >
                ALL
              </button>
            )}
          </div>

          <div style={{ position: 'absolute', left: '1rem', right: '1rem', bottom: '1rem', display: 'flex', gap: '0.45rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.65rem 0.75rem', border: `1px solid ${C.border}`, background: 'rgba(9,11,15,0.78)', backdropFilter: 'blur(10px)', zIndex: 3 }}>
            {ATTENTION_WINDOWS.map(item => {
              const active = data?.window === item.id
              return (
                <button key={item.id} onClick={() => onWindowChange(item.id)} style={{
                  background: active ? `${C.red}18` : 'rgba(19,22,26,0.92)',
                  border: `1px solid ${active ? C.red : C.borderMid}`,
                  color: active ? C.textPrimary : C.textMuted,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.49rem', letterSpacing: '0.08em',
                  padding: '0.45rem 0.6rem', cursor: 'pointer', borderRadius: 999, transition: 'all 0.15s ease',
                }}>{item.label}</button>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  )
}

function hotspotEventTitle(ev) {
  const t = (ev?.title || '').trim()
  if (t) return t
  const et = (ev?.event_type || 'Event').trim()
  const sub = (ev?.sub_event_type || '').trim()
  const place = (ev?.location || ev?.admin1 || ev?.country || '').trim()
  const action = (sub && sub.toLowerCase() !== et.toLowerCase()) ? sub : et
  const a1 = (ev?.actor_primary || '').trim()
  const a2 = (ev?.actor_secondary || '').trim()
  if (a1 && a2 && place) return `${action}: ${a1} vs ${a2} — ${place}`
  if (a1 && place) return `${action} involving ${a1} — ${place}`
  return place ? `${action} — ${place}` : action
}

function hotspotEventDescription(ev) {
  const s = (ev?.summary || '').trim()
  if (s && s.length > 20) return s
  // Build a meaningful description from available fields
  const et = (ev?.event_type || 'Event').trim()
  const sub = (ev?.sub_event_type || '').trim()
  const action = (sub && sub.toLowerCase() !== et.toLowerCase()) ? sub : et
  const place = (ev?.location || ev?.admin1 || ev?.country || '').trim()
  const a1 = (ev?.actor_primary || '').trim()
  const a2 = (ev?.actor_secondary || '').trim()
  const date = (ev?.event_date || '').trim()
  const fatalities = ev?.fatalities || 0
  let desc = `${action} reported`
  if (place) desc += ` in ${place}`
  if (a1 && a2) desc += ` involving ${a1} and ${a2}`
  else if (a1) desc += ` involving ${a1}`
  if (date) desc += ` on ${date}`
  desc += '.'
  if (fatalities) desc += ` ${fatalities} fatalities reported.`
  if (s) desc += ` ${s}`
  return desc
}

function MapSummaryPanel({ data, hotspot, onOpenBriefing, onAnalyzeCluster }) {
  const aspect = hotspot ? getHotspotAspect(hotspot) : 'default'
  const palette = HOTSPOT_TYPE_PALETTE[aspect] || HOTSPOT_TYPE_PALETTE.default
  const leadEvent = hotspot?.sample_events?.[0]
  const moreEvents = (hotspot?.sample_events || []).slice(1, 4)
  const windowLabel = data?.window ? formatWindowLabel(data.window) : '—'
  const isStoryHotspot = hotspot?.source_kind === 'story'
  const leadTitle = leadEvent ? hotspotEventTitle(leadEvent) : ''
  const leadBody = leadEvent ? hotspotEventDescription(leadEvent) : ''
  const showLeadBody = leadBody && leadBody !== leadTitle

  // For story hotspots the event_id is the article URL
  const storySourceLinks = isStoryHotspot
    ? (hotspot?.sample_events || [])
        .map(ev => ({ url: ev?.event_id, title: hotspotEventTitle(ev) }))
        .filter(item => item.url && item.url.startsWith('http'))
        .slice(0, 4)
    : []

  // Pick the matching topic for the global briefing button
  const briefingTopicId = aspect === 'conflict' ? 'conflict' : aspect === 'economic' ? 'economics' : 'geopolitics'
  const queryTopic = mapAspectToQueryTopic(aspect)

  return (
    <div style={{ border: `1px solid ${C.border}`, background: `linear-gradient(180deg, ${C.bgRaised}, rgba(19,22,26,0.98))`, overflow: 'hidden' }}>
      <div style={{ padding: '0.8rem 1rem', background: 'rgba(9,11,15,0.68)', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
        Click near a hotspot centroid to select. Hover to preview. Filter by type using the map legend.
      </div>
      <div style={{ padding: '1rem 1rem 1.1rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.55rem', marginBottom: '0.65rem' }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: palette.core, boxShadow: `0 0 12px ${palette.core}` }} />
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: aspect === 'conflict' ? C.red : C.silver, letterSpacing: '0.14em', textTransform: 'uppercase' }}>
            {isStoryHotspot
              ? (aspect === 'conflict' ? 'Conflict Story Cluster' : aspect === 'economic' ? 'Economic Story Cluster' : aspect === 'political' ? 'Geopolitical Story Cluster' : 'Story Cluster')
              : (aspect === 'conflict' ? 'Conflict Hotspot' : aspect === 'political' ? 'Political Hotspot' : aspect === 'economic' ? 'Economic Hotspot' : 'Active Hotspot')}
            </div>
          </div>

        {hotspot ? (
          <>
            <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.35rem', color: C.textPrimary, lineHeight: 1.2, marginBottom: '0.2rem' }}>
              {hotspotDisplayHeadline(hotspot)}
            </div>
            {hotspot.location && hotspotDisplayHeadline(hotspot) !== hotspot.location ? (
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>
                {hotspot.location}
              </div>
            ) : null}
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: '0.9rem' }}>
              {hotspot.admin1 ? `${hotspot.admin1} · ${hotspot.country}` : hotspot.country}
            </div>

            {leadEvent && (leadTitle || leadBody) && (
              <div style={{ border: `1px solid ${C.borderMid}`, background: 'rgba(9,12,17,0.56)', padding: '0.85rem 0.9rem', marginBottom: '0.95rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.45rem' }}>
                  {isStoryHotspot ? 'Story snapshot' : 'What happened'}
                </div>
                {showLeadBody && leadTitle && (
                  <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.95rem', color: C.textPrimary, lineHeight: 1.45, marginBottom: '0.5rem' }}>
                    {truncateText(leadTitle, 160)}
                  </div>
                )}
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.65 }}>
                  {truncateText(showLeadBody ? leadBody : (leadTitle || leadBody), 280)}
                </div>
              </div>
            )}

            {moreEvents.length > 0 && (
              <div style={{ marginBottom: '0.95rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>
                  {isStoryHotspot ? 'Other stories in this cluster' : 'Other incidents in this cluster'}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.55rem' }}>
                  {moreEvents.map((ev, idx) => (
                    <div
                      key={ev?.event_id || idx}
                      style={{ border: `1px solid ${C.border}`, background: 'rgba(18,21,27,0.5)', padding: '0.55rem 0.65rem' }}
                    >
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: C.textMuted, marginBottom: '0.25rem' }}>
                        {ev?.event_date ? formatDateTime(ev.event_date) : '—'}
                        {ev?.fatalities ? ` · ${ev.fatalities} fatalities` : ''}
                      </div>
                      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.8rem', color: C.textSecondary, lineHeight: 1.55 }}>
                        {truncateText(hotspotEventDescription(ev), 180)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '0.65rem', marginBottom: '0.95rem' }}>
              {[
                [isStoryHotspot ? 'Articles' : 'Incidents', hotspot.event_count],
                [isStoryHotspot ? 'Sources' : 'Fatalities', isStoryHotspot ? hotspot.source_count : hotspot.fatality_total],
                ['Attention', formatAttentionShare(hotspot.attention_share)],
                ['Updated', hotspot.latest_event_date ? formatDateTime(hotspot.latest_event_date) : '—'],
              ].map(([label, value]) => (
                <div key={label} style={{ border: `1px solid ${C.border}`, padding: '0.7rem 0.75rem', background: 'rgba(18,21,27,0.62)' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.35rem' }}>
                    {label}
                  </div>
                  <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: label === 'Updated' ? '0.8rem' : '1rem', color: C.textSecondary, lineHeight: 1.35 }}>
                    {value}
                  </div>
                </div>
              ))}
            </div>

            {(hotspot.event_types || []).length > 0 && (
              <div style={{ marginBottom: '0.9rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>
                  Scenario Indexes
                </div>
                <div style={{ display: 'flex', gap: '0.45rem', flexWrap: 'wrap' }}>
                  {(hotspot.event_types || []).slice(0, 4).map(item => (
                    <div key={item} style={{ border: `1px solid ${C.borderMid}`, background: `${C.bgRaised}c8`, padding: '0.35rem 0.5rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', color: C.textSecondary, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
                      {item}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {(hotspot.sample_locations || []).length > 0 && (
              <div style={{ marginBottom: '0.9rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.45rem' }}>
                  Nearby Locations
                </div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.6 }}>
                  {(hotspot.sample_locations || []).slice(0, 5).join(' · ')}
                </div>
              </div>
            )}

            {storySourceLinks.length > 0 && (
              <div style={{ marginBottom: '0.9rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.5rem' }}>
                  Source Articles
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
                  {storySourceLinks.map((item, idx) => (
                    <a
                      key={idx}
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{
                        display: 'block',
                        fontFamily: "'Source Serif 4', serif",
                        fontSize: '0.8rem',
                        color: C.silver,
                        lineHeight: 1.45,
                        textDecoration: 'none',
                        borderLeft: `2px solid ${palette.core}`,
                        paddingLeft: '0.55rem',
                        opacity: 0.85,
                      }}
                      onMouseEnter={e => { e.currentTarget.style.opacity = '1'; e.currentTarget.style.color = C.textPrimary }}
                      onMouseLeave={e => { e.currentTarget.style.opacity = '0.85'; e.currentTarget.style.color = C.silver }}
                    >
                      {truncateText(item.title, 100)}
                    </a>
                  ))}
                </div>
              </div>
            )}

            {onOpenBriefing && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem', marginTop: '0.15rem' }}>
                <button
                  onClick={() => {
                    const topicMap = {
                      conflict: { id: 'conflict', kind: 'conflict', label: 'Conflict Briefing', tag: 'Conflict', accent: '#ef4444', description: '' },
                      geopolitics: { id: 'geopolitics', kind: 'briefing', label: 'Political Briefing', tag: 'Political', accent: '#60a5fa', description: '' },
                      economics: { id: 'economics', kind: 'briefing', label: 'Economic Briefing', tag: 'Economic', accent: '#fbbf24', description: '' },
                    }
                    onOpenBriefing(topicMap[briefingTopicId] || topicMap.geopolitics)
                  }}
                  style={{
                    width: '100%',
                    background: `${palette.core}14`,
                    border: `1px solid ${palette.core}40`,
                    color: palette.core,
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '0.49rem',
                    letterSpacing: '0.12em',
                    textTransform: 'uppercase',
                    padding: '0.65rem 1rem',
                    cursor: 'pointer',
                    textAlign: 'left',
                    transition: 'background 0.15s',
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = `${palette.core}22` }}
                  onMouseLeave={e => { e.currentTarget.style.background = `${palette.core}14` }}
                >
                  Global {aspect === 'conflict' ? 'conflict' : aspect === 'economic' ? 'economic' : 'political'} briefing →
                </button>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: C.textMuted, letterSpacing: '0.06em', lineHeight: 1.5 }}>
                  Corpus-wide generated briefing (not tied to this dot). Use cluster analysis for this location.
                </div>
                {onAnalyzeCluster && (
                  <button
                    onClick={() => onAnalyzeCluster(hotspot, queryTopic)}
                    style={{
                      width: '100%',
                      background: 'rgba(18,21,27,0.62)',
                      border: `1px solid ${C.borderMid}`,
                      color: C.textSecondary,
                      fontFamily: "'JetBrains Mono', monospace",
                      fontSize: '0.49rem',
                      letterSpacing: '0.12em',
                      textTransform: 'uppercase',
                      padding: '0.65rem 1rem',
                      cursor: 'pointer',
                      textAlign: 'left',
                      transition: 'background 0.15s, border-color 0.15s',
                    }}
                    onMouseEnter={e => { e.currentTarget.style.background = C.bgHover; e.currentTarget.style.borderColor = C.silver }}
                    onMouseLeave={e => { e.currentTarget.style.background = 'rgba(18,21,27,0.62)'; e.currentTarget.style.borderColor = C.borderMid }}
                  >
                    Analyze this cluster →
                  </button>
                )}
              </div>
            )}
          </>
        ) : (
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>
            No hotspot is selected yet. Click a dot on the map or hover to preview.
          </div>
        )}

        <div style={{ marginTop: '1rem', paddingTop: '0.9rem', borderTop: `1px solid ${C.border}` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
            <span>Window {windowLabel}</span>
            <span>{data?.hotspot_count || 0} hotspots</span>
            <span>{data?.total_events || 0} mapped signals</span>
          </div>
        </div>
      </div>
    </div>
  )
}

const LEVEL_COLORS = { critical: '#ef4444', high: '#f97316', elevated: '#fbbf24', low: '#4ade80' }
const TREND_ARROWS = { rising: '▲', falling: '▼', stable: '—', new: '●' }
const TREND_COLORS = { rising: '#ef4444', falling: '#4ade80', stable: '#7d8794', new: '#60a5fa' }

function InstabilityPanel({ data, loading, error, onAnalyze }) {
  const countries = data?.countries || []
  const topCountries = countries.slice(0, 12)

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#f97316' }} />
          <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Country Instability Index</h2>
        </div>
        {data && (
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>
            {data.country_count} COUNTRIES · {data.window_days}D WINDOW
          </div>
        )}
      </div>
      <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />

      {loading && (
        <div>
          {[0, 1, 2, 3, 4].map(i => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.55rem 0.2rem', borderBottom: `1px solid ${C.border}` }}>
              <div className="skeleton" style={{ height: '0.75rem', width: '50%' }} />
              <div className="skeleton" style={{ height: '0.75rem', width: '12%' }} />
            </div>
          ))}
        </div>
      )}

      {!loading && error && (
        <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          {error}
        </div>
      )}

      {!loading && !error && topCountries.length === 0 && (
        <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          No instability data available yet — structured events may still be ingesting.
        </div>
      )}

      {!loading && !error && topCountries.map((country, i) => (
        <div
          key={country.country}
          className="theater-row"
          onClick={() => onAnalyze && onAnalyze(country)}
          style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.55rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', minWidth: 0 }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.textMuted, width: '1rem', textAlign: 'right', flexShrink: 0 }}>{i + 1}</div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{country.label}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: LEVEL_COLORS[country.level] || C.textMuted, letterSpacing: '0.1em', textTransform: 'uppercase', flexShrink: 0 }}>{country.level}</div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', flexShrink: 0 }}>
            <div style={{ width: 60, height: 4, background: C.bg, borderRadius: 2, overflow: 'hidden' }}>
              <div style={{ width: `${Math.min(100, country.score)}%`, height: '100%', background: LEVEL_COLORS[country.level] || C.textMuted, borderRadius: 2 }} />
            </div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textPrimary, width: '2rem', textAlign: 'right' }}>{country.score}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: TREND_COLORS[country.trend] || C.textMuted, width: '0.8rem', textAlign: 'center' }}>
              {TREND_ARROWS[country.trend] || '—'}
            </div>
          </div>
        </div>
      ))}

      {!loading && !error && countries.length > 12 && (
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, padding: '0.6rem 0.2rem', letterSpacing: '0.06em' }}>
          + {countries.length - 12} more countries tracked
        </div>
      )}
    </div>
  )
}

const CONVERGENCE_COLORS = {
  crisis_escalation: '#ef4444',
  military_escalation: '#f97316',
  information_crisis: '#a855f7',
  conflict_spotlight: '#ef4444',
  emerging_situation: '#fbbf24',
  narrative_instability: '#8b5cf6',
  multi_signal: '#60a5fa',
}

function CorrelationPanel({ data, loading, error, onAnalyze }) {
  const cards = data?.cards || []

  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#a855f7' }} />
          <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Signal Convergence</h2>
        </div>
        {data && (
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>
            {data.card_count} CONVERGENCES
          </div>
        )}
      </div>
      <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />

      {loading && (
        <div>
          {[0, 1, 2].map(i => (
            <div key={i} style={{ padding: '0.75rem 0.2rem', borderBottom: `1px solid ${C.border}` }}>
              <div className="skeleton" style={{ height: '0.8rem', width: '70%', marginBottom: '0.35rem' }} />
              <div className="skeleton" style={{ height: '0.55rem', width: '90%' }} />
            </div>
          ))}
        </div>
      )}

      {!loading && error && (
        <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          {error}
        </div>
      )}

      {!loading && !error && cards.length === 0 && (
        <div style={{ padding: '0.8rem 0.2rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          No cross-domain convergences detected in the current window.
        </div>
      )}

      {!loading && !error && cards.slice(0, 8).map((card) => (
        <div
          key={card.country}
          className="theater-row"
          onClick={() => onAnalyze && onAnalyze(card)}
          style={{ padding: '0.7rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.3rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <span style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textPrimary }}>{card.label}</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.42rem', color: CONVERGENCE_COLORS[card.convergence_type] || C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
                {card.convergence_type.replace(/_/g, ' ')}
              </span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', flexShrink: 0 }}>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textPrimary }}>{card.score}</span>
              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: TREND_COLORS[card.trend] || C.textMuted }}>
                {TREND_ARROWS[card.trend] || '—'}
              </span>
            </div>
          </div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, lineHeight: 1.6 }}>
            {card.domain_count} domains active: {card.active_domains.map(d => d.replace(/_/g, ' ')).join(' · ')}
          </div>
          {card.convergence_description && (
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.78rem', color: C.textSecondary, marginTop: '0.25rem', lineHeight: 1.5 }}>
              {card.convergence_description}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

function BriefingLaunchPanel({ topics, onOpenBriefing, onOpenForesight }) {
  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '0.85rem' }}>
        Generate Briefings
      </div>
      <div className="briefing-launch-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.75rem' }}>
        {topics.map(topic => (
          <button
            key={topic.id}
            className="briefing-btn"
            onClick={() => onOpenBriefing(topic)}
            style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", padding: '1.05rem 1.05rem', borderRadius: 4, minHeight: 88, borderTop: `2px solid ${topic.accent || C.borderMid}` }}
          >
            <div style={{ fontSize: '0.46rem', letterSpacing: '0.14em', color: topic.accent || C.silver, textTransform: 'uppercase', marginBottom: '0.45rem' }}>
              {topic.tag}
            </div>
            <div style={{ fontSize: '0.9rem', letterSpacing: '0.03em', color: C.textPrimary, marginBottom: '0.35rem' }}>
              {topic.label}
            </div>
            <div style={{ fontSize: '0.5rem', letterSpacing: '0.06em', color: C.textMuted, lineHeight: 1.55 }}>
              {topic.description}
            </div>
          </button>
        ))}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.6rem', marginTop: '0.9rem' }} className="briefing-tools-grid">
        <button
          className="briefing-btn"
          onClick={() => onOpenForesight('predictions')}
          style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', padding: '0.7rem 0.8rem', borderRadius: 4, minHeight: 58 }}
        >
          PREDICTION LEDGER
        </button>
        <button
          className="briefing-btn"
          onClick={() => onOpenForesight('before-news')}
          style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', padding: '0.7rem 0.8rem', borderRadius: 4, minHeight: 58 }}
        >
          BEFORE IT WAS NEWS
        </button>
      </div>
    </div>
  )
}

function NewsColumn({
  headlines,
  headlinesLoading,
  headlinesLoaded,
  headlinesError,
  headlineSort,
  headlineRegion,
  headlineRegions,
  onChangeSort,
  onChangeRegion,
  onRefresh,
  onOpenStory,
}) {
  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised }}>
      <div style={{ padding: '0.95rem 1rem 0.8rem', borderBottom: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.7rem', marginBottom: '0.85rem' }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red, boxShadow: `0 0 10px ${C.red}` }} />
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase' }}>
            Recent Analysis
          </div>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <select
            value={headlineSort}
            onChange={event => onChangeSort(event.target.value)}
            style={{ flex: 1, minWidth: 150, background: C.bg, border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.08em', padding: '0.5rem 0.6rem', borderRadius: 2 }}
          >
            <option value="relevance">Sort: Most Covered + Recent</option>
            <option value="region">Sort: Region</option>
          </select>
          <select
            value={headlineRegion}
            onChange={event => onChangeRegion(event.target.value)}
            style={{ flex: 1, minWidth: 120, background: C.bg, border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.08em', padding: '0.5rem 0.6rem', borderRadius: 2 }}
          >
            <option value="all">Region: All</option>
            {headlineRegions.map(region => (
              <option key={region} value={region}>{`Region: ${formatRegionLabel(region)}`}</option>
            ))}
          </select>
          {headlinesLoaded && (
            <button
              onClick={onRefresh}
              style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', letterSpacing: '0.1em', cursor: 'pointer', padding: '0.5rem 0.7rem', borderRadius: 2 }}
            >
              REFRESH
            </button>
          )}
        </div>
      </div>

      {headlinesLoading && (
        <div>
          {[0, 1, 2].map(i => (
            <div key={i} style={{ padding: '0.95rem 1rem', borderBottom: `1px solid ${C.border}` }}>
              <div className="skeleton" style={{ height: '0.5rem', width: '5rem', marginBottom: '0.55rem' }} />
              <div className="skeleton" style={{ height: i === 0 ? '1rem' : '0.9rem', width: '88%', marginBottom: '0.4rem' }} />
              <div className="skeleton" style={{ height: '0.72rem', width: '62%' }} />
            </div>
          ))}
        </div>
      )}

      {!headlinesLoading && headlinesError && (
        <div style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          {headlinesError}
        </div>
      )}

      {!headlinesLoading && !headlinesError && headlines.length === 0 && (
        <div style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
          No stories match the current region filter.
        </div>
      )}

      {!headlinesLoading && !headlinesError && headlines.map((story, index) => (
        <div
          key={`${story.event_id || story.headline}-${index}`}
          className="headline-item"
          onClick={() => onOpenStory(story)}
          style={{ padding: '1rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer' }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.45rem', marginBottom: '0.35rem', flexWrap: 'wrap' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: index === 0 ? C.red : C.textSecondary, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
              {story.topic?.replace('_', ' ')}
            </div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
              {formatRegionLabel(story.dominant_region)}
            </div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.08em' }}>
              {formatDateTime(story.latest_update || story.sources?.[0]?.published_at)}
            </div>
          </div>
          <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: index === 0 ? '1rem' : '0.88rem', color: C.textSecondary, lineHeight: 1.28, marginBottom: '0.35rem' }}>
            {story.headline}
          </div>
          <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.76rem', color: C.textMuted, lineHeight: 1.55 }}>
            {truncateText(story.summary, 160)}
          </div>
        </div>
      ))}
    </div>
  )
}

function DeepDive({ title, query, entityName, queryTopic, regionContext, hotspotId, storyEventId, sourceUrls, attentionWindow, onClose }) {
  const [content, setContent] = useState(null)
  const [sources, setSources] = useState([])
  const [meta, setMeta] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [entityReference, setEntityReference] = useState(null)
  const [referenceLoading, setReferenceLoading] = useState(Boolean(entityName))
  const [referenceError, setReferenceError] = useState(null)

  useEffect(() => {
    setLoading(true)
    async function load() {
      try {
        const data = await sendQuery(query, {
          topic: queryTopic || undefined,
          regionContext: regionContext || undefined,
          hotspotId: hotspotId || undefined,
          storyEventId: storyEventId || undefined,
          attentionWindow: attentionWindow || undefined,
          sourceUrls: sourceUrls?.length ? sourceUrls : undefined,
        })
        setContent(data.answer)
        setSources(data.sources || [])
        setMeta(data)
        setError(null)
      } catch (err) {
        setError(friendlyErrorMessage(err, 'analysis'))
        setContent('Error generating analysis.')
        setSources([])
        setMeta(null)
      } finally {
        setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [query, queryTopic, regionContext, hotspotId, storyEventId, sourceUrls])

  useEffect(() => {
    let cancelled = false

    async function loadReference() {
      if (!entityName) {
        setEntityReference(null)
        setReferenceLoading(false)
        setReferenceError(null)
        return
      }

      setReferenceLoading(true)
      try {
        const data = await fetchEntityReference(entityName)
        if (!cancelled) {
          setEntityReference(data)
          setReferenceError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setReferenceError(friendlyErrorMessage(err, 'entity reference'))
          setEntityReference(null)
        }
      } finally {
        if (!cancelled) {
          setReferenceLoading(false)
        }
      }
    }

    loadReference()
    return () => {
      cancelled = true
    }
  }, [entityName])

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — DEEP ANALYSIS</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 740, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Intelligence Analysis</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>{title}</h1>
        {loading && (
          <div>
            {[100, 85, 92, 70, 88, 95, 60, 80].map((w, i) => (
              <div key={i} className="skeleton" style={{ height: i === 0 ? '1.1rem' : '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />
            ))}
          </div>
        )}
        {content && !loading && (
          <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 280px', gap: '2rem' }} className="briefing-layout">
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', lineHeight: 1.85, color: C.textSecondary }}>
              {error && (
                <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
                  {error}
                </div>
              )}
              <ReactMarkdown components={MD}>{content}</ReactMarkdown>
            </div>
            <aside style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }} className="briefing-sidebar">
              {entityName && (
                <div style={{ border: `1px solid ${C.border}`, padding: '1rem', background: C.bgRaised }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>REFERENCE CONTEXT</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6, marginBottom: '0.8rem' }}>
                    Wikipedia background only. Not used in Othello analysis, scoring, or contradiction detection.
                  </div>
                  {referenceLoading && (
                    <div>
                      <div className="skeleton" style={{ height: '0.8rem', width: '68%', marginBottom: '0.6rem' }} />
                      <div className="skeleton" style={{ height: '0.7rem', width: '100%', marginBottom: '0.35rem' }} />
                      <div className="skeleton" style={{ height: '0.7rem', width: '92%', marginBottom: '0.35rem' }} />
                      <div className="skeleton" style={{ height: '0.7rem', width: '84%' }} />
                    </div>
                  )}
                  {!referenceLoading && referenceError && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, lineHeight: 1.6 }}>
                      {referenceError}
                    </div>
                  )}
                  {!referenceLoading && !referenceError && entityReference?.status === 'ok' && (
                    <div>
                      {entityReference.thumbnail_url && (
                        <img
                          src={entityReference.thumbnail_url}
                          alt={entityReference.title || entityName}
                          style={{ width: '100%', borderRadius: 4, marginBottom: '0.85rem', border: `1px solid ${C.border}` }}
                        />
                      )}
                      <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.92rem', color: C.textPrimary, lineHeight: 1.35, marginBottom: '0.65rem' }}>
                        {entityReference.title || entityName}
                      </div>
                      {entityReference.summary && (
                        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.84rem', color: C.textSecondary, lineHeight: 1.65, marginBottom: '0.85rem' }}>
                          {entityReference.summary}
                        </div>
                      )}
                      {entityReference.url && (
                        <a
                          href={entityReference.url}
                          target="_blank"
                          rel="noreferrer"
                          style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.silver, letterSpacing: '0.1em', textDecoration: 'none' }}
                        >
                          OPEN WIKIPEDIA →
                        </a>
                      )}
                    </div>
                  )}
                  {!referenceLoading && !referenceError && (!entityReference || entityReference.status !== 'ok') && (
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, lineHeight: 1.6 }}>
                      No Wikipedia reference found for this entity yet.
                    </div>
                  )}
                </div>
              )}
              <div style={{ border: `1px solid ${C.border}`, padding: '1rem', background: C.bgRaised }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>ANALYSIS STATUS</div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, lineHeight: 1.8 }}>
                  <div>SOURCES: {meta?.source_count || sources.length}</div>
                  <div>ARCHIVE: {meta?.historical_sources || 0}</div>
                  <div>LIVE FETCH: {meta?.live_sources || 0}</div>
                </div>
              </div>
              <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SUPPORTING REPORTING</div>
                {sources.slice(0, 8).map((source, index) => (
                  <a key={`${source.url}-${index}`} href={source.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.7rem', paddingBottom: '0.7rem', borderBottom: index < Math.min(sources.length, 8) - 1 ? `1px solid ${C.border}` : 'none' }}>
                    <div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.2rem' }}>{source.title}</div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6 }}>
                      <div>{source.source}</div>
                      <div>{formatDateTime(source.published_at)}</div>
                    </div>
                  </a>
                ))}
              </div>
            </aside>
          </div>
        )}
      </div>
    </div>
  )
}

function ContradictionOverlay({ event, onClose }) {
  const overlayZ = 220

  function parseSourceLabel(label) {
    const text = (label || '').trim()
    const match = text.match(/^(.*?)(?:\s+\((\d{4}-\d{2}-\d{2}T[^)]+)\))?$/)
    if (!match) return { source: text, published_at: null }
    return {
      source: (match[1] || text).trim(),
      published_at: match[2] || null,
    }
  }

  function sourceRecordFor(item, index) {
    const direct = item?.source_records?.[index]
    if (direct?.url || direct?.source) return direct
    const label = (item?.sources_in_conflict || [])[index]
    const parsed = parseSourceLabel(label)
    return (event.articles || []).find(article => (
      article.source === parsed.source
        && (!parsed.published_at || article.published_at === parsed.published_at)
    )) || (event.articles || []).find(article => article.source === parsed.source) || null
  }

  function mostCredibleRecordFor(item) {
    if (item?.most_credible_record?.url || item?.most_credible_record?.source) return item.most_credible_record
    const label = item?.most_credible_source
    const parsed = parseSourceLabel(label)
    return (event.articles || []).find(article => (
      article.source === parsed.source
        && (!parsed.published_at || article.published_at === parsed.published_at)
    )) || (event.articles || []).find(article => article.source === parsed.source) || null
  }

  function SourceLabel({ record, fallback }) {
    const fallbackSource = parseSourceLabel(fallback).source || fallback
    const displaySource = parseSourceLabel(record?.source || fallbackSource).source || fallbackSource
    if (record?.url) {
      return (
        <a
          href={record.url}
          target="_blank"
          rel="noreferrer"
          style={{ color: C.silver, textDecoration: 'underline', textUnderlineOffset: '2px' }}
        >
          {displaySource}
        </a>
      )
    }
    return <span>{displaySource}</span>
  }

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: overlayZ, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — CONTRADICTIONS</div>
        <div style={{ width: 80 }} />
      </div>
      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '1rem' }}>Narrative Fracture Review</div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{event.label}</h1>
          <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap', marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{event.topic?.toUpperCase()}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{formatDateTime(event.latest_update)}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{event.source_count} SOURCES</div>
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.08em' }}>{totalNarrativeFlags(event)} FLAGS</div>
        </div>

        <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: '2rem', alignItems: 'start' }}>
          <div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', color: C.textSecondary, lineHeight: 1.8, marginBottom: '2rem' }}>{event.summary}</div>
            {(event.contradictions || []).map((item, index) => (
              <div key={index} style={{ border: `1px solid ${C.borderMid}`, background: C.bgRaised, padding: '1.25rem', marginBottom: '1rem' }}>
                {(() => {
                  const leftRecord = sourceRecordFor(item, 0)
                  const rightRecord = sourceRecordFor(item, 1)
                  const mostCredible = mostCredibleRecordFor(item)
                  return (
                    <>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.8rem', flexWrap: 'wrap' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase' }}>{item.conflict_type || 'fact conflict'}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>{Math.round((item.confidence || 0) * 100)}% confidence</div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '0.8rem' }}>
                  <div style={{ borderLeft: `2px solid ${C.red}`, paddingLeft: '0.8rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}>
                      <SourceLabel record={leftRecord} fallback={(item.sources_in_conflict || [])[0] || 'Source A'} />
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>{item.claim_a || 'No claim captured.'}</div>
                  </div>
                  <div style={{ borderLeft: `2px solid ${C.borderMid}`, paddingLeft: '0.8rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}>
                      <SourceLabel record={rightRecord} fallback={(item.sources_in_conflict || [])[1] || 'Source B'} />
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>{item.claim_b || 'No conflicting claim captured.'}</div>
                  </div>
                </div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textMuted, lineHeight: 1.65 }}>
                  <strong style={{ color: C.textSecondary }}>Assessment:</strong> {item.reasoning || 'No reasoning provided.'}
                </div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, marginTop: '0.7rem' }}>
                  Most credible source: <SourceLabel record={mostCredible} fallback={item.most_credible_source || 'unresolved'} />
                </div>
                    </>
                  )
                })()}
              </div>
            ))}
            {(event.narrative_fractures || []).map((item, index) => (
              <div key={`fracture-${index}`} style={{ border: `1px solid ${C.borderMid}`, background: C.bgRaised, padding: '1.25rem', marginBottom: '1rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.8rem', flexWrap: 'wrap' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase' }}>framing fracture</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted }}>{Math.round((item.confidence || 0) * 100)}% confidence</div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '0.8rem' }}>
                  <div style={{ borderLeft: `2px solid ${C.red}`, paddingLeft: '0.8rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}>
                      {item.label_a}
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>
                      {(item.source_records_a || []).map((record, idx) => (
                        <div key={idx} style={{ marginBottom: '0.3rem' }}><SourceLabel record={record} fallback={(item.sources_a || [])[idx] || 'Source'} /></div>
                      ))}
                    </div>
                  </div>
                  <div style={{ borderLeft: `2px solid ${C.borderMid}`, paddingLeft: '0.8rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.silver, marginBottom: '0.35rem' }}>
                      {item.label_b}
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, lineHeight: 1.65 }}>
                      {(item.source_records_b || []).map((record, idx) => (
                        <div key={idx} style={{ marginBottom: '0.3rem' }}><SourceLabel record={record} fallback={(item.sources_b || [])[idx] || 'Source'} /></div>
                      ))}
                    </div>
                  </div>
                </div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textMuted, lineHeight: 1.65 }}>
                  <strong style={{ color: C.textSecondary }}>Assessment:</strong> {item.reasoning}
                </div>
              </div>
            ))}
            {(!(event.contradictions || []).length && !(event.narrative_fractures || []).length) && (
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.62rem', color: C.textSecondary }}>No structured contradictions or framing fractures were captured for this event cluster.</div>
            )}
          </div>

          <aside className="briefing-sidebar" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', position: 'sticky', top: '88px' }}>
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>ENTITY FOCUS</div>
              {(event.entity_focus || []).map((entity, index) => (
                <div key={index} style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.9rem', color: C.textSecondary, marginBottom: '0.4rem' }}>{entity}</div>
              ))}
            </div>
            <div style={{ border: `1px solid ${C.border}`, padding: '1rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SOURCE PACK</div>
              {(event.articles || []).slice(0, 8).map((article, index) => (
                <a key={`${article.url}-${index}`} href={article.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.7rem', paddingBottom: '0.7rem', borderBottom: index < Math.min((event.articles || []).length, 8) - 1 ? `1px solid ${C.border}` : 'none' }}>
                  <div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.2rem' }}>{article.title}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.6 }}>
                    <div>{article.source}</div>
                    <div>{formatDateTime(article.published_at)}</div>
                  </div>
                </a>
              ))}
            </div>
          </aside>
        </div>
      </div>
    </div>
  )
}

function BriefingPage({ topic, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetchBriefing(topic.id)
      .then(result => {
        setData(result)
        setError(null)
      })
      .catch(err => {
        console.error(err)
        setData(null)
        setError(friendlyErrorMessage(err, `${topic.label.toLowerCase()}`))
      })
      .finally(() => setLoading(false))
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [topic.id, topic.label])

  const parsed = data ? parseBriefingSections(data.briefing) : {}

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — {topic.tag} BRIEFING</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 900, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && (
          <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '3rem' }}>
            <div>
              {[50, 100, 75, 88, 80, 92, 68, 85, 78, 90].map((w, i) => (
                <div key={i} className="skeleton" style={{ height: i === 0 ? '0.6rem' : i === 1 ? '2rem' : '0.85rem', width: `${w}%`, marginBottom: i === 1 ? '1.5rem' : '0.6rem' }} />
              ))}
            </div>
            <div>
              {[100, 80, 90, 70, 85, 75].map((w, i) => (
                <div key={i} className="skeleton" style={{ height: '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />
              ))}
            </div>
          </div>
        )}

        {!loading && data && (
          <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '3rem', alignItems: 'start' }}>
            <div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>INTELLIGENCE BRIEFING — {topic.tag}</div>
              <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.5rem' }}>{topic.label}</h1>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
                {new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })} — {data.article_count} sources
              </div>
              {['SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS', 'SIGNAL vs NOISE', 'DEEPER CONTEXT', 'SOURCE CONTRADICTIONS'].map(section => (
                parsed[section] ? (
                  <div key={section} style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: section === 'SOURCE CONTRADICTIONS' ? C.red : C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{section}</div>
                      <div style={{ flex: 1, height: '1px', background: C.border }} />
                    </div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.8, color: C.textSecondary }}>
                      <ReactMarkdown components={MD}>{parsed[section]}</ReactMarkdown>
                    </div>
                  </div>
                ) : null
              ))}
            </div>
            <div className="briefing-sidebar" style={{ position: 'sticky', top: '80px', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
              {parsed['PREDICTIONS'] && (
                <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.25rem', background: C.bgRaised }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>PREDICTIONS</div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}>
                    <ReactMarkdown components={MD}>{parsed['PREDICTIONS']}</ReactMarkdown>
                  </div>
                </div>
              )}
              {parsed['WHAT TO WATCH'] && (
                <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>WHAT TO WATCH</div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', lineHeight: 1.75, color: C.textSecondary }}>
                    <ReactMarkdown components={MD}>{parsed['WHAT TO WATCH']}</ReactMarkdown>
                  </div>
                </div>
              )}
              <div style={{ border: `1px solid ${C.border}`, padding: '1.25rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, letterSpacing: '0.15em', marginBottom: '0.75rem' }}>SOURCES</div>
                {(Array.isArray(data.sources) ? data.sources : []).map((s, i, list) => (
                  <a key={i} href={s.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.6rem', paddingBottom: '0.6rem', borderBottom: i < list.length - 1 ? `1px solid ${C.border}` : 'none' }}>
                    <div style={{ fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.4, marginBottom: '0.15rem', transition: 'color 0.15s' }}
                      onMouseEnter={e => e.target.style.color = C.textPrimary}
                      onMouseLeave={e => e.target.style.color = C.textSecondary}
                    >{s.title}</div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, lineHeight: 1.6 }}>
                      <div>{s.source}</div>
                      <div>{formatDateTime(s.published_at)}</div>
                    </div>
                  </a>
                ))}
              </div>
            </div>
          </div>
        )}

        {!loading && !data && error && (
          <div style={{ border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '1rem 1.1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>
            {error}
          </div>
        )}
      </div>
    </div>
  )
}

function ConflictBriefingPage({ topic, hotspot, hotspots, contradictionEvents, windowId, onClose, onOpenContradiction }) {
  const [briefData, setBriefData] = useState(null)
  const [briefLoading, setBriefLoading] = useState(true)
  const [briefError, setBriefError] = useState(null)

  useEffect(() => {
    setBriefLoading(true)
    fetchBriefing('conflict')
      .then(result => {
        setBriefData(result)
        setBriefError(null)
      })
      .catch(err => {
        console.error(err)
        setBriefData(null)
        setBriefError(friendlyErrorMessage(err, 'conflict briefing'))
      })
      .finally(() => setBriefLoading(false))
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [])

  const focusHotspot = hotspot || hotspots[0] || null
  const topHotspots = (hotspots || []).slice(0, 6)
  const topFractures = (contradictionEvents || []).slice(0, 5)
  const watchItems = [
    focusHotspot ? `Watch whether ${focusHotspot.location || focusHotspot.label} sustains its current incident tempo over the ${formatWindowLabel(windowId).toLowerCase()} window.` : null,
    topHotspots[1] ? `Track whether ${topHotspots[1].country} displaces ${topHotspots[0]?.country || 'the current lead hotspot'} in total conflict attention.` : null,
    topFractures[0] ? `Monitor narrative divergence around ${topFractures[0].label}, where reporting is already fragmenting across sources.` : null,
  ].filter(Boolean)

  const parsedBrief = briefData ? parseBriefingSections(briefData.briefing) : {}

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — {topic.tag} INTELLIGENCE</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div className="briefing-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: '3rem', alignItems: 'start' }}>
          <div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.red, letterSpacing: '0.18em', marginBottom: '0.75rem' }}>
              CONFLICT · GENERATED BRIEFING + MAP CONTEXT
            </div>
            <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.6rem' }}>
              {topic.label}
            </h1>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.textSecondary, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
              {formatWindowLabel(windowId)} window · {hotspots.length} active conflict clouds
              {briefData?.article_count != null ? ` · ${briefData.article_count} corpus sources in conflict briefing` : ''}
            </div>

            {briefLoading && (
              <div style={{ marginBottom: '2rem' }}>
                {[50, 100, 75, 88, 80, 92].map((w, i) => (
                  <div key={i} className="skeleton" style={{ height: '0.85rem', width: `${w}%`, marginBottom: '0.6rem' }} />
                ))}
              </div>
            )}

            {!briefLoading && briefError && (
              <div style={{ marginBottom: '2rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.55 }}>
                {briefError}
              </div>
            )}

            {!briefLoading && briefData && ['SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS', 'SIGNAL vs NOISE', 'DEEPER CONTEXT', 'SOURCE CONTRADICTIONS'].map(section => (
              parsedBrief[section] ? (
                <div key={section} style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: section === 'SOURCE CONTRADICTIONS' ? C.red : C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>{section}</div>
                    <div style={{ flex: 1, height: '1px', background: C.border }} />
                  </div>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.8, color: C.textSecondary }}>
                    <ReactMarkdown components={MD}>{parsedBrief[section]}</ReactMarkdown>
                  </div>
                </div>
              ) : null
            ))}

            {focusHotspot && (
              <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.red, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Map · Situation Snapshot</div>
                  <div style={{ flex: 1, height: '1px', background: C.border }} />
                </div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', lineHeight: 1.8, color: C.textSecondary }}>
                  {truncateText(focusHotspot.sample_events?.[0]?.summary || `${focusHotspot.location || focusHotspot.label} remains the focal conflict hotspot in ${focusHotspot.country}.`, 420)}
                </div>
              </div>
            )}

            <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Active Hotspots</div>
                <div style={{ flex: 1, height: '1px', background: C.border }} />
              </div>
              {topHotspots.map((item, index) => (
                <div key={item.hotspot_id} style={{ padding: '0.95rem 0', borderBottom: index < topHotspots.length - 1 ? `1px solid ${C.border}` : 'none' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.3rem' }}>
                    <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.35 }}>
                      {item.location || item.label}, {item.country}
                    </div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', color: C.red, flexShrink: 0 }}>
                      {formatAttentionShare(item.attention_share)}
                    </div>
                  </div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}>
                    <div>{item.event_count} incidents · {item.fatality_total} fatalities · {item.event_types?.join(' · ')}</div>
                    <div>{item.sample_locations?.slice(0, 4).join(' · ')}</div>
                  </div>
                </div>
              ))}
            </div>

            <div style={{ marginBottom: '2rem', paddingBottom: '2rem', borderBottom: `1px solid ${C.border}` }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>Narrative Fractures</div>
                <div style={{ flex: 1, height: '1px', background: C.border }} />
              </div>
              {topFractures.length > 0 ? topFractures.map((event, index) => (
                <div
                  key={event.event_id || index}
                  role={onOpenContradiction ? 'button' : undefined}
                  tabIndex={onOpenContradiction ? 0 : undefined}
                  onClick={() => onOpenContradiction?.(event)}
                  onKeyDown={onOpenContradiction ? e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpenContradiction(event) } } : undefined}
                  style={{
                    padding: '0.95rem 0',
                    borderBottom: index < topFractures.length - 1 ? `1px solid ${C.border}` : 'none',
                    cursor: onOpenContradiction ? 'pointer' : 'default',
                    borderRadius: onOpenContradiction ? 2 : 0,
                  }}
                  className={onOpenContradiction ? 'theater-row' : undefined}
                >
                  <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.35, marginBottom: '0.25rem' }}>
                    {event.label}
                  </div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}>
                    <div>{totalNarrativeFlags(event)} flags · {event.source_count} sources · {formatDateTime(event.latest_update)}</div>
                    <div>{truncateText(event.summary, 180)}</div>
                    {onOpenContradiction && <div style={{ marginTop: '0.35rem', color: C.silver }}>OPEN FRACTURE DETAIL →</div>}
                  </div>
                </div>
              )) : (
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary }}>
                  No contradiction-rich clusters are active in the current window.
                </div>
              )}
            </div>

            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.2em', color: C.textSecondary, textTransform: 'uppercase', whiteSpace: 'nowrap' }}>What To Watch</div>
                <div style={{ flex: 1, height: '1px', background: C.border }} />
              </div>
              <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', lineHeight: 1.75, color: C.textSecondary }}>
                {watchItems.map(item => (
                  <div key={item} style={{ marginBottom: '0.6rem' }}>{item}</div>
                ))}
              </div>
            </div>
          </div>

          <aside className="briefing-sidebar" style={{ position: 'sticky', top: '80px', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.1rem', background: C.bgRaised }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>Conflict Indices</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, lineHeight: 1.8 }}>
                <div>HOTSPOTS: {hotspots.length}</div>
                <div>ACTIVE WINDOW: {formatWindowLabel(windowId).toUpperCase()}</div>
                <div>PRIMARY CLOUD: {focusHotspot?.location || '—'}</div>
                <div>FATALITIES: {hotspots.reduce((sum, item) => sum + (item.fatality_total || 0), 0)}</div>
              </div>
            </div>
            {parsedBrief.PREDICTIONS && (
              <div style={{ border: `1px solid ${C.borderMid}`, borderTop: `2px solid ${C.red}`, padding: '1.1rem', background: C.bgRaised }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.15em', marginBottom: '0.65rem' }}>PREDICTIONS</div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', lineHeight: 1.7, color: C.textSecondary }}>
                  <ReactMarkdown components={MD}>{parsedBrief.PREDICTIONS}</ReactMarkdown>
                </div>
              </div>
            )}
            {parsedBrief['WHAT TO WATCH'] && (
              <div style={{ border: `1px solid ${C.border}`, padding: '1.1rem', background: C.bgRaised }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.65rem' }}>WHAT TO WATCH</div>
                <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.82rem', lineHeight: 1.7, color: C.textSecondary }}>
                  <ReactMarkdown components={MD}>{parsedBrief['WHAT TO WATCH']}</ReactMarkdown>
                </div>
              </div>
            )}
            {briefData && (Array.isArray(briefData.sources) ? briefData.sources : []).length > 0 && (
              <div style={{ border: `1px solid ${C.border}`, padding: '1.1rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>BRIEFING SOURCES</div>
                {(Array.isArray(briefData.sources) ? briefData.sources : []).map((s, i, list) => (
                  <a key={i} href={s.url} target="_blank" rel="noreferrer" style={{ display: 'block', textDecoration: 'none', marginBottom: '0.55rem', paddingBottom: '0.55rem', borderBottom: i < list.length - 1 ? `1px solid ${C.border}` : 'none' }}>
                    <div style={{ fontSize: '0.72rem', color: C.textSecondary, lineHeight: 1.4, marginBottom: '0.12rem' }}>{s.title}</div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.55 }}>
                      <div>{s.source}</div>
                      <div>{formatDateTime(s.published_at)}</div>
                    </div>
                  </a>
                ))}
              </div>
            )}
            {focusHotspot && (
              <div style={{ border: `1px solid ${C.border}`, padding: '1.1rem' }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>Focus Cloud</div>
                <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.35, marginBottom: '0.45rem' }}>
                  {focusHotspot.location || focusHotspot.label}
                </div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}>
                  <div>{focusHotspot.country}</div>
                  <div>{focusHotspot.latitude?.toFixed(2)}, {focusHotspot.longitude?.toFixed(2)}</div>
                  <div>{focusHotspot.sample_locations?.slice(0, 4).join(' · ')}</div>
                </div>
              </div>
            )}
            <div style={{ border: `1px solid ${C.border}`, padding: '1.1rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.15em', marginBottom: '0.7rem' }}>Recent Incident Samples</div>
              {(focusHotspot?.sample_events || []).slice(0, 4).map((event, index, list) => (
                <div key={`${event.event_id}-${index}`} style={{ paddingBottom: '0.7rem', marginBottom: '0.7rem', borderBottom: index < list.length - 1 ? `1px solid ${C.border}` : 'none' }}>
                  <div style={{ fontSize: '0.74rem', color: C.textSecondary, lineHeight: 1.45, marginBottom: '0.15rem' }}>{event.location || event.country}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.6 }}>
                    <div>{event.event_type}</div>
                    <div>{event.fatalities} fatalities · {event.event_date}</div>
                  </div>
                </div>
              ))}
            </div>
          </aside>
        </div>
      </div>
    </div>
  )
}

function TimelinePage({ query, onClose }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const result = await fetchTimeline(query)
        setData(result)
        setError(null)
      } catch (err) {
        setData({ error: true })
        setError(friendlyErrorMessage(err, 'timeline'))
      } finally {
        setLoading(false)
      }
    }
    load()
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [query])

  const significanceColor = { HIGH: C.red, MEDIUM: C.silver, LOW: C.textMuted }
  const significanceDot = { HIGH: 10, MEDIUM: 7, LOW: 5 }

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px', transition: 'all 0.15s' }}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = C.borderMid; e.currentTarget.style.color = C.textSecondary }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — TIMELINE</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 800, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        {loading && (
          <div>
            <div className="skeleton" style={{ height: '2rem', width: '70%', marginBottom: '1rem' }} />
            <div className="skeleton" style={{ height: '0.85rem', width: '90%', marginBottom: '2rem' }} />
            {[0, 1, 2, 3, 4].map(i => (
              <div key={i} style={{ display: 'flex', gap: 0, marginBottom: '2rem' }}>
                <div style={{ width: 120, flexShrink: 0, paddingRight: '1.25rem', display: 'flex', justifyContent: 'flex-end' }}>
                  <div className="skeleton" style={{ height: '0.7rem', width: '80%' }} />
                </div>
                <div style={{ flex: 1, paddingLeft: '1.5rem' }}>
                  <div className="skeleton" style={{ height: '0.9rem', width: '75%', marginBottom: '0.4rem' }} />
                  <div className="skeleton" style={{ height: '0.75rem', width: '95%', marginBottom: '0.3rem' }} />
                  <div className="skeleton" style={{ height: '0.75rem', width: '60%' }} />
                </div>
              </div>
            ))}
          </div>
        )}

        {data && !loading && !data.error && (
          <div style={{ animation: 'fadeIn 0.4s ease' }}>
            <div style={{ marginBottom: '3rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', marginBottom: '0.75rem' }}>CHRONOLOGICAL INTELLIGENCE TIMELINE</div>
              <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>{data.title}</h1>
              <p style={{ fontFamily: "'Source Serif 4', serif", fontSize: '1rem', color: C.textSecondary, lineHeight: 1.6, fontStyle: 'italic' }}>{data.summary}</p>
              <div style={{ display: 'flex', gap: '1.5rem', marginTop: '1.5rem', paddingTop: '1rem', borderTop: `1px solid ${C.border}`, flexWrap: 'wrap' }}>
                {[['HIGH', 'Major event'], ['MEDIUM', 'Significant development'], ['LOW', 'Background event']].map(([sig, label]) => (
                  <div key={sig} style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <div style={{ width: significanceDot[sig], height: significanceDot[sig], borderRadius: '50%', background: significanceColor[sig], flexShrink: 0 }} />
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, letterSpacing: '0.08em' }}>{label}</span>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ position: 'relative' }}>
              <div style={{ position: 'absolute', left: 120, top: 0, bottom: 0, width: '1px', background: C.border }} />
              {data.events?.map((event, i) => (
                <div key={i} style={{ display: 'flex', gap: 0, marginBottom: '2.5rem', animation: `fadeUp 0.4s ease ${i * 0.06}s both` }}>
                  <div style={{ width: 120, flexShrink: 0, paddingRight: '1.25rem', textAlign: 'right', paddingTop: '0.15rem' }}>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, lineHeight: 1.4, letterSpacing: '0.05em' }}>
                      {(() => {
                        try {
                          const d = new Date(event.date)
                          return <><div style={{ color: C.textPrimary, fontWeight: 500 }}>{d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div><div>{d.getFullYear()}</div><div>{d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })}</div></>
                        } catch { return <div>{event.date}</div> }
                      })()}
                    </div>
                  </div>
                  <div style={{ position: 'relative', flexShrink: 0, display: 'flex', alignItems: 'flex-start', paddingTop: '0.2rem' }}>
                    <div style={{ width: significanceDot[event.significance] || 7, height: significanceDot[event.significance] || 7, borderRadius: '50%', background: significanceColor[event.significance] || C.textMuted, position: 'relative', zIndex: 1, transform: 'translateX(-50%)', boxShadow: event.significance === 'HIGH' ? `0 0 12px ${C.red}60` : 'none', flexShrink: 0 }} />
                  </div>
                  <div style={{ flex: 1, paddingLeft: '1.25rem' }}>
                    <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: event.significance === 'HIGH' ? '1.05rem' : '0.9rem', fontWeight: 700, color: event.significance === 'HIGH' ? C.textPrimary : C.textSecondary, lineHeight: 1.3, marginBottom: '0.4rem' }}>{event.headline}</div>
                    <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.7, marginBottom: '0.35rem' }}>{event.description}</div>
                    {event.source && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>{event.source}</div>}
                  </div>
                </div>
              ))}
              <div style={{ display: 'flex', alignItems: 'center', paddingLeft: 120, gap: '0.75rem' }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', border: `1px solid ${C.borderMid}`, transform: 'translateX(-50%)', flexShrink: 0 }} />
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textMuted, letterSpacing: '0.1em', paddingLeft: '1.25rem' }}>ONGOING</div>
              </div>
            </div>
          </div>
        )}

        {data?.error && <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem', color: C.textSecondary }}>{error || 'Not enough archived articles on this topic yet.'}</div>}
      </div>
    </div>
  )
}

function ForesightPage({ mode, records, error, onClose }) {
  const isPredictions = mode === 'predictions'
  const title = isPredictions ? 'Prediction Ledger' : 'Before It Was News'
  const subtitle = isPredictions
    ? 'Timestamped Othello forecasts and their tracked outcome status.'
    : 'Events Othello surfaced before major-source pickup, including open leads still awaiting broader confirmation.'

  return (
    <div style={{ position: 'fixed', inset: 0, background: C.bg, zIndex: 200, overflowY: 'auto', animation: 'slideIn 0.3s ease' }}>
      <div style={{ position: 'sticky', top: 0, background: `${C.bg}ee`, backdropFilter: 'blur(12px)', borderBottom: `1px solid ${C.border}`, padding: '1rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', zIndex: 10 }}>
        <button onClick={onClose} style={{ background: 'none', border: `1px solid ${C.borderMid}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', letterSpacing: '0.1em', padding: '0.4rem 0.8rem', cursor: 'pointer', borderRadius: '4px' }}>
          ← BACK
        </button>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em' }}>OTHELLO — FORESIGHT</div>
        <div style={{ width: 80 }} />
      </div>

      <div style={{ maxWidth: 980, margin: '0 auto', padding: '3rem 2rem 6rem' }}>
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '1rem' }}>
          {isPredictions ? 'Forecast Audit Trail' : 'Early Signal Archive'}
        </div>
        <h1 style={{ fontFamily: "'Libre Baskerville', serif", fontSize: 'clamp(1.5rem, 4vw, 2.5rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.02em', color: C.textPrimary, marginBottom: '0.75rem' }}>
          {title}
        </h1>
        <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.98rem', color: C.textSecondary, lineHeight: 1.7, marginBottom: '2rem', paddingBottom: '1.5rem', borderBottom: `1px solid ${C.border}` }}>
          {subtitle}
        </div>

        {error && (
          <div style={{ border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '1rem 1.1rem', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, marginBottom: '1rem' }}>
            {error}
          </div>
        )}

        {!error && records.length === 0 && (
          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.6rem', color: C.textSecondary }}>
            {isPredictions ? 'No briefing-derived predictions have been logged yet.' : 'No early-signal records are available yet.'}
          </div>
        )}

        {!error && records.map((item, i) => (
          <div key={item.prediction_key || item.event_key || i} style={{ padding: '1rem 0.4rem', borderBottom: `1px solid ${C.border}` }}>
            {isPredictions ? (
              <>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.35rem' }}>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.5 }}>{item.prediction_text}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: item.status === 'resolved_hit' ? C.silver : item.status === 'resolved_miss' ? C.red : C.textMuted, flexShrink: 0 }}>
                    {(item.status || 'pending').replaceAll('_', ' ').toUpperCase()}
                  </div>
                </div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}>
                  <div>{item.topic} · {item.prediction_horizon_days} day horizon · logged {formatDateTime(new Date(item.created_at * 1000))}</div>
                  <div>{item.outcome_summary || 'Awaiting resolution from later reporting.'}</div>
                </div>
              </>
            ) : (
              <>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.35rem' }}>
                  <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.95rem', color: C.textSecondary, lineHeight: 1.5 }}>{item.event_label}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>
                    +{Math.round(item.lead_time_hours || 0)}H
                  </div>
                </div>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, lineHeight: 1.7 }}>
                  <div>{item.status === 'awaiting_major_pickup' ? 'OPEN LEAD' : 'CONFIRMED LEAD'} · {item.topic}</div>
                  <div>{item.earliest_source || 'Unknown source'} before {item.earliest_major_source || 'major pickup not yet observed'}</div>
                  <div>Othello first saw it {formatDateTime(new Date(item.first_othello_seen_at * 1000))}</div>
                </div>
              </>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

export default function App() {
  const [time, setTime] = useState(new Date())
  const [headlines, setHeadlines] = useState([])
  const [mapAttention, setMapAttention] = useState(null)
  const [mapAttentionLoading, setMapAttentionLoading] = useState(true)
  const [mapAttentionError, setMapAttentionError] = useState(null)
  const [mapAttentionWindow, setMapAttentionWindow] = useState('24h')
  const [selectedMapHotspot, setSelectedMapHotspot] = useState(null)
  const [headlineRegions, setHeadlineRegions] = useState([])
  const [headlinesLoading, setHeadlinesLoading] = useState(false)
  const [headlinesLoaded, setHeadlinesLoaded] = useState(false)
  const [headlinesError, setHeadlinesError] = useState(null)
  const [headlineSort, setHeadlineSort] = useState('relevance')
  const [headlineRegion, setHeadlineRegion] = useState('all')
  const [lastUpdated, setLastUpdated] = useState(null)
  const [entitySignals, setEntitySignals] = useState(null)
  const [entitySignalsError, setEntitySignalsError] = useState(null)
  const [contradictionEvents, setContradictionEvents] = useState([])
  const [contradictionsLoading, setContradictionsLoading] = useState(true)
  const [contradictionsError, setContradictionsError] = useState(null)
  const [selectedContradiction, setSelectedContradiction] = useState(null)
  const [deepDive, setDeepDive] = useState(null)
  const [briefingPage, setBriefingPage] = useState(null)
  const [headerVisible, setHeaderVisible] = useState(true)
  const [timelinePage, setTimelinePage] = useState(null)
  const [predictionLedger, setPredictionLedger] = useState([])
  const [predictionLedgerError, setPredictionLedgerError] = useState(null)
  const [beforeNewsArchive, setBeforeNewsArchive] = useState([])
  const [beforeNewsError, setBeforeNewsError] = useState(null)
  const [foresightPage, setForesightPage] = useState(null)
  const [healthSnapshot, setHealthSnapshot] = useState(null)
  const [healthFetchError, setHealthFetchError] = useState(null)
  const [instabilityData, setInstabilityData] = useState(null)
  const [instabilityLoading, setInstabilityLoading] = useState(true)
  const [instabilityError, setInstabilityError] = useState(null)
  const [correlationData, setCorrelationData] = useState(null)
  const [correlationLoading, setCorrelationLoading] = useState(true)
  const [correlationError, setCorrelationError] = useState(null)
  const lastScrollY = useRef(0)
  const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

  const TOPICS = [
    { id: 'geopolitics', kind: 'briefing', label: 'Political Briefing', tag: 'Political', accent: '#60a5fa', description: 'Power shifts, state moves, pressure campaigns, and diplomatic signaling.' },
    { id: 'economics', kind: 'briefing', label: 'Economic Briefing', tag: 'Economic', accent: '#fbbf24', description: 'Markets, sanctions, supply chains, and economic coercion shaping the story.' },
    { id: 'conflict', kind: 'conflict', label: 'Conflict Briefing', tag: 'Conflict', accent: '#ef4444', description: 'Hotspots, incident tempo, fatalities, and the fractures forming around live conflict zones.' },
  ]

  const THEATERS = [
    { label: 'US–Iran Military Conflict', query: 'US Iran military conflict war strikes' },
    { label: 'Russia–Ukraine War', query: 'Russia Ukraine war conflict' },
    { label: 'Federal Reserve & Interest Rates', query: 'Federal Reserve interest rates monetary policy' },
    { label: 'China–Taiwan Tensions', query: 'China Taiwan tensions military' },
  ]

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(timer)
  }, [])

  useEffect(() => {
    function handleScroll() {
      const currentY = window.scrollY
      if (currentY < 60) { setHeaderVisible(true); lastScrollY.current = currentY; return }
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
        console.error(err)
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
        console.error(err)
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
        console.error(err)
        setPredictionLedger([])
        setPredictionLedgerError(friendlyErrorMessage(err, 'prediction ledger'))
      })
    fetchBeforeNewsArchive()
      .then(data => {
        setBeforeNewsArchive(data.records || [])
        setBeforeNewsError(null)
      })
      .catch(err => {
        console.error(err)
        setBeforeNewsArchive([])
        setBeforeNewsError(friendlyErrorMessage(err, 'before-it-was-news archive'))
      })
    fetchHealth()
      .then(data => {
        setHealthSnapshot(data)
        setHealthFetchError(null)
        const corpusLatest = data?.runtime?.corpus?.latest_published_at
        if (corpusLatest) {
          setLastUpdated(parseDateValue(corpusLatest))
        }
      })
      .catch(err => {
        console.error(err)
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
      const data = await fetchInstability(3)
      setInstabilityData(data)
    } catch (err) {
      console.error(err)
      setInstabilityError(friendlyErrorMessage(err, 'instability index'))
    } finally {
      setInstabilityLoading(false)
    }
  }

  async function loadCorrelations() {
    setCorrelationLoading(true)
    setCorrelationError(null)
    try {
      const data = await fetchCorrelations(3)
      setCorrelationData(data)
    } catch (err) {
      console.error(err)
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
      const latestStoryUpdate = stories
        .map(story => story.latest_update || story.sources?.[0]?.published_at)
        .map(value => parseDateValue(value))
        .filter(value => value && !Number.isNaN(value.getTime()))
        .sort((a, b) => b.getTime() - a.getTime())[0] || null
      setHeadlines(stories)
      setHeadlinesLoaded(true)
      setLastUpdated(current => current || latestStoryUpdate)
    } catch (err) {
      console.error(err)
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
      setSelectedMapHotspot(current => {
        const preferred = current
        if (preferred && (data.hotspots || []).some(item => item.hotspot_id === preferred)) {
          return preferred
        }
        return data.hotspots?.[0]?.hotspot_id || null
      })
    } catch (err) {
      console.error(err)
      setMapAttention(null)
      setMapAttentionError(friendlyErrorMessage(err, 'incident cloud map'))
    } finally {
      setMapAttentionLoading(false)
    }
  }

  function handleMapHotspotSelect(hotspotId) {
    setSelectedMapHotspot(hotspotId)
  }

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
  const selectedHotspot = (mapAttention?.hotspots || []).find(item => item.hotspot_id === selectedMapHotspot) || mapAttention?.hotspots?.[0] || null

  function openStoryDeepDive(story) {
    const sourceUrls = (story.sources || []).map(s => s?.url).filter(u => typeof u === 'string' && u.startsWith('http')).slice(0, 12)
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
      regionContext: [hotspot.location || hotspot.label, hotspot.admin1, hotspot.country].filter(Boolean).join(', ') || undefined,
      hotspotId: hotspot.hotspot_id || undefined,
      attentionWindow: mapAttentionWindow,
      sourceUrls: urls.length ? urls : undefined,
    })
  }

  return (
    <div style={{ background: C.bg, minHeight: '100vh', color: C.textPrimary }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=Source+Serif+4:ital,opsz,wght@0,8..60,400;0,8..60,600;1,8..60,400&family=JetBrains+Mono:wght@400;500&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 3px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.borderMid}; border-radius: 2px; }
        @keyframes fadeUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
        @keyframes slideIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes shimmer { 0% { background-position: -600px 0; } 100% { background-position: 600px 0; } }
        .headline-item { transition: background 0.15s ease; }
        .headline-item:hover { background: ${C.bgHover}; }
        .theater-row:hover { background: ${C.bgHover}; }
        .briefing-btn { border: 1px solid ${C.borderMid}; background: none; transition: all 0.2s; cursor: pointer; }
        .briefing-btn:hover { border-color: ${C.silver}; background: ${C.bgHover} !important; transform: translateY(-1px); }
        .entity-item:hover { background: ${C.bgHover}; }
        .skeleton {
          background: linear-gradient(90deg, ${C.bgRaised} 25%, ${C.bgHover} 50%, ${C.bgRaised} 75%);
          background-size: 600px 100%;
          animation: shimmer 1.4s infinite;
          border-radius: 3px;
          display: block;
        }
        .query-input { caret-color: ${C.silver}; }
        .query-input:focus { outline: none; border-color: ${C.silver} !important; }
        .query-input::placeholder { color: ${C.textMuted}; font-style: italic; }
        @media (max-width: 1180px) {
          .home-shell { grid-template-columns: 1fr !important; grid-template-areas: "map" "sidebar" "lower" !important; }
          .lower-grid { grid-template-columns: 1fr !important; }
        }
        @media (max-width: 960px) {
          .sidebar-sticky { position: static !important; top: auto !important; }
        }
        @media (max-width: 700px) {
          .briefing-btn { text-align: left !important; }
          .header-section { padding-top: 8vh !important; padding-bottom: 2vh !important; }
          .main-padding { padding: 0 1rem !important; }
          .briefing-layout { grid-template-columns: 1fr !important; }
          .briefing-sidebar { position: static !important; }
          .coverage-summary { grid-template-columns: 1fr !important; }
          .briefing-launch-grid { grid-template-columns: 1fr !important; }
          .briefing-tools-grid { grid-template-columns: 1fr !important; }
        }
      `}</style>

      <div style={{ position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100, transform: headerVisible ? 'translateY(0)' : 'translateY(-100%)', transition: 'transform 0.3s ease', background: `${C.bg}e8`, backdropFilter: 'blur(16px)', borderBottom: `1px solid ${C.border}`, padding: '0.85rem 2rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1.5rem' }}>
        <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.1rem', fontWeight: 700, letterSpacing: '-0.01em', color: C.textPrimary, flexShrink: 0 }}>OTHELLO</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          {worldClocks.map((clock, index) => (
            <div key={clock.zone} style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.47rem', color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
                {clock.label}
              </div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.textSecondary, letterSpacing: '0.03em' }}>
                {formatClock(time, clock.zone)}
              </div>
              {index < worldClocks.length - 1 && <div style={{ width: '1px', height: '0.7rem', background: C.borderMid, marginLeft: '0.1rem' }} />}
            </div>
          ))}
        </div>
      </div>

      <div className="main-padding" style={{ padding: '0 1.5rem' }}>
        <header className="header-section" style={{ paddingTop: '9vh', paddingBottom: '1.2rem', animation: 'fadeUp 0.6s ease' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', flexWrap: 'wrap' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary, letterSpacing: '0.08em' }}>
              {dateStr.toUpperCase()} — {timeStr}
            </div>
            <div style={{ width: '1px', height: '0.8rem', background: C.borderMid }} />
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textMuted, letterSpacing: '0.08em' }}>
              LAST UPDATE: <span style={{ color: C.textSecondary }}>{lastUpdatedStr}</span>
            </div>
          </div>
        </header>

        {healthFetchError && (
          <div style={{ marginBottom: '1rem', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}18`, padding: '0.85rem 1rem', animation: 'fadeUp 0.4s ease both' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.red, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.3rem' }}>
              API health check failed
            </div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.88rem', color: C.textSecondary, lineHeight: 1.5 }}>
              {healthFetchError}
            </div>
          </div>
        )}
        {!healthFetchError && healthSnapshot?.runtime && (!healthSnapshot.runtime.llm_ready || !healthSnapshot.runtime.contradiction_ready) && (
          <div style={{ marginBottom: '1rem', border: `1px solid rgba(251,191,36,0.35)`, background: 'rgba(251,191,36,0.06)', padding: '0.75rem 1rem', animation: 'fadeUp 0.4s ease both' }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: '#fbbf24', letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.28rem' }}>
              Partial capability
            </div>
            <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.86rem', color: C.textSecondary, lineHeight: 1.55 }}>
              {!healthSnapshot.runtime.llm_ready && 'LLM-backed answers and briefings may use fallbacks (set GROQ_API_KEY on the API). '}
              {!healthSnapshot.runtime.contradiction_ready && 'Narrative fracture mining is limited without ANTHROPIC_API_KEY. '}
            </div>
          </div>
        )}

        <div
          className="home-shell"
          style={{
            display: 'grid',
            gridTemplateColumns: 'minmax(0, 1.45fr) 360px',
            gridTemplateAreas: '"map sidebar" "lower sidebar"',
            gap: '1.25rem',
            alignItems: 'start',
            paddingBottom: '10vh',
          }}
        >
          <section style={{ gridArea: 'map', animation: 'fadeUp 0.6s ease 0.08s both' }}>
            <WorldHotspotMap
              data={mapAttention}
              error={mapAttentionError}
              loading={mapAttentionLoading}
              selectedHotspotId={selectedMapHotspot}
              onWindowChange={loadMapAttention}
              onSelectHotspot={handleMapHotspotSelect}
            />
          </section>

          <aside style={{ gridArea: 'sidebar', display: 'flex', flexDirection: 'column', gap: '1rem', animation: 'fadeUp 0.6s ease 0.14s both' }}>
            <MapSummaryPanel
              data={mapAttention}
              hotspot={selectedHotspot}
              onOpenBriefing={setBriefingPage}
              onAnalyzeCluster={openHotspotClusterAnalysis}
            />
            <NewsColumn
              headlines={headlines}
              headlinesLoading={headlinesLoading}
              headlinesLoaded={headlinesLoaded}
              headlinesError={headlinesError}
              headlineSort={headlineSort}
              headlineRegion={headlineRegion}
              headlineRegions={headlineRegions}
              onChangeSort={async value => {
                setHeadlineSort(value)
                await loadHeadlines({ sortBy: value, region: headlineRegion })
              }}
              onChangeRegion={async value => {
                setHeadlineRegion(value)
                await loadHeadlines({ sortBy: headlineSort, region: value })
              }}
              onRefresh={() => loadHeadlines()}
              onOpenStory={openStoryDeepDive}
            />
          </aside>

          <section style={{ gridArea: 'lower', animation: 'fadeUp 0.6s ease 0.2s both' }}>
            <div style={{ marginBottom: '1rem' }}>
              <BriefingLaunchPanel topics={TOPICS} onOpenBriefing={setBriefingPage} onOpenForesight={setForesightPage} />
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '1.25rem', marginBottom: '1rem' }}>
              <InstabilityPanel
                data={instabilityData}
                loading={instabilityLoading}
                error={instabilityError}
                onAnalyze={(country) => setDeepDive({
                  title: `Instability Analysis: ${country.label}`,
                  query: `Analyze the current instability situation in ${country.label}. The country scores ${country.score}/100 on the instability index (level: ${country.level}). Break down: what conflict events are occurring, what's driving media attention, are there contradictory narratives across sources, what entities are most active, and what should we watch for in the coming days? Components: conflict=${country.components?.conflict}, media=${country.components?.media_attention}, contradictions=${country.components?.contradiction}, severity=${country.components?.event_severity}. Be analytically precise.`,
                  queryTopic: 'geopolitics',
                  regionContext: country.country,
                })}
              />
              <CorrelationPanel
                data={correlationData}
                loading={correlationLoading}
                error={correlationError}
                onAnalyze={(card) => setDeepDive({
                  title: `Signal Convergence: ${card.label}`,
                  query: `Analyze the signal convergence detected in ${card.label} (score: ${card.score}/100, type: ${card.convergence_type.replace(/_/g, ' ')}). Active domains: ${card.active_domains.join(', ')}. Domain scores: ${Object.entries(card.domain_scores).map(([k, v]) => `${k}=${v}`).join(', ')}. What is driving this multi-domain convergence? What does the intersection of these signals suggest about the developing situation? What should analysts watch for? Be specific and analytical.`,
                  queryTopic: 'geopolitics',
                  regionContext: card.country,
                })}
              />
            </div>
            <div className="lower-grid" style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.06fr) minmax(0, 0.94fr)', gap: '1.25rem', alignItems: 'start' }}>
              <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
                  <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.red }} />
                  <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Narrative Fractures</h2>
                </div>
                <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                {contradictionsLoading && (
                  <div>
                    {[0, 1, 2].map(i => (
                      <div key={i} style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}` }}>
                        <div className="skeleton" style={{ height: '0.8rem', width: i === 0 ? '88%' : '74%', marginBottom: '0.35rem' }} />
                        <div className="skeleton" style={{ height: '0.55rem', width: '45%' }} />
                      </div>
                    ))}
                  </div>
                )}
                {!contradictionsLoading && contradictionEvents.length === 0 && (
                  <div style={{ padding: '0.95rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.textSecondary }}>
                    {contradictionsError || 'No stored contradiction-rich events are currently surfaced in the active corpus.'}
                  </div>
                )}
                {!contradictionsLoading && contradictionEvents.map((event, i) => (
                  <div
                    key={event.event_id || i}
                    className="theater-row"
                    onClick={() => setSelectedContradiction(event)}
                    style={{ padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', marginBottom: '0.25rem' }}>
                      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.94rem', color: C.textSecondary, lineHeight: 1.5 }}>{event.label}</div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, flexShrink: 0 }}>{totalNarrativeFlags(event)} FLAGS</div>
                    </div>
                    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, lineHeight: 1.6 }}>
                      <div>{formatDateTime(event.latest_update)}</div>
                      <div>
                        {event.source_count} sources split across the cluster
                        {(event.narrative_fracture_count || 0) > 0 ? ` · ${event.narrative_fracture_count} framing fractures` : ''}
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              <div style={{ display: 'grid', gap: '1.25rem' }}>
                <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
                    <div style={{ width: 6, height: 6, borderRadius: '50%', background: C.silver }} />
                    <h2 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase' }}>Timelines</h2>
                  </div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.35rem' }} />
                  {THEATERS.map((item, i) => (
                    <div
                      key={i}
                      className="theater-row"
                      onClick={() => setTimelinePage(item.query)}
                      style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.9rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', borderRadius: 2 }}
                    >
                      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.92rem', color: C.textSecondary, lineHeight: 1.4 }}>{item.label}</div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0, marginLeft: '1rem' }}>VIEW →</div>
                    </div>
                  ))}
                  <div style={{ marginTop: '1rem', display: 'flex', gap: '0.5rem' }}>
                    <input
                      id="custom-timeline-input"
                      placeholder="Custom timeline..."
                      style={{ flex: 1, background: C.bg, border: `1px solid ${C.border}`, color: C.textPrimary, fontFamily: "'Source Serif 4', serif", fontSize: '0.85rem', padding: '0.6rem 0.9rem', outline: 'none', borderRadius: 2, minWidth: 0 }}
                      onFocus={e => e.target.style.borderColor = C.silver}
                      onBlur={e => e.target.style.borderColor = C.border}
                      onKeyDown={e => { if (e.key === 'Enter' && e.target.value.trim()) { setTimelinePage(e.target.value.trim()); e.target.value = '' } }}
                    />
                    <button
                      onClick={() => { const input = document.getElementById('custom-timeline-input'); if (input.value.trim()) { setTimelinePage(input.value.trim()); input.value = '' } }}
                      style={{ background: 'none', border: `1px solid ${C.border}`, color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', letterSpacing: '0.1em', padding: '0.6rem 1rem', cursor: 'pointer', borderRadius: 2, flexShrink: 0 }}
                      onMouseEnter={e => { e.currentTarget.style.borderColor = C.silver; e.currentTarget.style.color = C.textPrimary }}
                      onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.color = C.textSecondary }}
                    >
                      GO →
                    </button>
                  </div>
                </div>

                <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.55rem', color: C.silver, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: '1rem' }}>Tracked Entities</div>
                  <div style={{ height: '1px', background: C.border, marginBottom: '0.5rem' }} />
                  {!entitySignals && (
                    entitySignalsError ? (
                      <div style={{ padding: '0.8rem 0.2rem', borderBottom: `1px solid ${C.border}`, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', color: C.textSecondary, lineHeight: 1.6 }}>
                        {entitySignalsError}
                      </div>
                    ) : (
                      <div>
                        {[0, 1, 2, 3, 4].map(i => (
                          <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}` }}>
                            <div className="skeleton" style={{ height: '0.75rem', width: '55%' }} />
                            <div className="skeleton" style={{ height: '0.75rem', width: '15%' }} />
                          </div>
                        ))}
                      </div>
                    )
                  )}
                  {entitySignals && (
                    <div>
                      {entitySignals.spikes?.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 4).length > 0 && (
                        <div style={{ marginBottom: '1.25rem' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}>
                            <div style={{ width: 4, height: 4, borderRadius: '50%', background: C.red }} />
                            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.red, letterSpacing: '0.15em' }}>SURGING</div>
                          </div>
                          {entitySignals.spikes.filter(e => e.trend === 'RISING' || e.trend === 'NEW').slice(0, 4).map((e, i) => (
                            <div
                              key={i}
                              className="entity-item"
                              onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they playing in current geopolitical events, why are they suddenly getting increased attention in the news, what are their motivations and capabilities, and what should we expect from them in the coming weeks? Be specific and analytical.`, entity: e.entity, queryTopic: 'geopolitics' })}
                              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', transition: 'background 0.15s', borderRadius: 2 }}
                            >
                              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <span style={{ fontSize: '0.85rem', color: C.textPrimary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span>
                                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.44rem', color: C.textMuted, letterSpacing: '0.06em' }}>{e.type}</span>
                              </div>
                              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, flexShrink: 0 }}>
                                {e.trend === 'NEW' ? 'NEW' : `${e.spike_ratio}×`}
                              </span>
                            </div>
                          ))}
                        </div>
                      )}
                      {entitySignals.top_entities?.length > 0 && (
                        <div>
                          <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.12em', padding: '0.4rem 0.2rem', marginBottom: '0.1rem' }}>MOST DISCUSSED</div>
                          {entitySignals.top_entities.slice(0, 6).map((e, i) => (
                            <div
                              key={i}
                              className="entity-item"
                              onClick={() => setDeepDive({ title: `Intelligence Analysis: ${e.entity}`, query: `Give me a comprehensive intelligence analysis of ${e.entity}. Who or what are they, what role are they currently playing in world events, what are their key actions and motivations right now, and what should we be watching for? Be direct and analytically precise.`, entity: e.entity, queryTopic: 'geopolitics' })}
                              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.5rem 0.2rem', borderBottom: `1px solid ${C.border}`, cursor: 'pointer', transition: 'background 0.15s', borderRadius: 2 }}
                            >
                              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textMuted, width: '0.9rem' }}>{i + 1}</span>
                                <span style={{ fontSize: '0.85rem', color: C.textSecondary, fontFamily: "'Source Serif 4', serif" }}>{e.entity}</span>
                              </div>
                              <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted, flexShrink: 0 }}>{e.mentions}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </section>
        </div>
      </div>

      {deepDive && (
        <DeepDive
          title={deepDive.title}
          query={deepDive.query}
          entityName={deepDive.entity}
          queryTopic={deepDive.queryTopic}
          regionContext={deepDive.regionContext}
          hotspotId={deepDive.hotspotId}
          storyEventId={deepDive.storyEventId}
          attentionWindow={deepDive.attentionWindow}
          sourceUrls={deepDive.sourceUrls}
          onClose={() => setDeepDive(null)}
        />
      )}
      {briefingPage && (
        briefingPage.kind === 'conflict' ? (
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
        )
      )}
      {selectedContradiction && <ContradictionOverlay event={selectedContradiction} onClose={() => setSelectedContradiction(null)} />}
      {timelinePage && <TimelinePage query={timelinePage} onClose={() => setTimelinePage(null)} />}
      {foresightPage && (
        <ForesightPage
          mode={foresightPage}
          records={foresightPage === 'predictions' ? predictionLedger : beforeNewsArchive}
          error={foresightPage === 'predictions' ? predictionLedgerError : beforeNewsError}
          onClose={() => setForesightPage(null)}
        />
      )}
    </div>
  )
}
