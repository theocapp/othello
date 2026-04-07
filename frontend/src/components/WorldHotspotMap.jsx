import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
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
  const [worldData, setWorldData] = useState(null)
  const [transform, setTransform] = useState(d3.zoomIdentity)
  const [hoveredHotspot, setHoveredHotspot] = useState(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })
  const [activeAspects, setActiveAspects] = useState(null)

  const W = 1000
  const H = 520
  const MAP_HIT_RADIUS = 52
  const projection = useMemo(
    () => d3.geoNaturalEarth1().scale(153).translate([W / 2, H / 2]),
    [W, H]
  )
  const pathGen = useMemo(() => d3.geoPath().projection(projection), [projection])
  // global map constants (used for initial fit and pan-to behavior)
  const MAX_SCALE = 10
  const ZOOM_IN_FACTOR = 1.25
  const defaultMinScale = 0.1
  const CENTER_X_FRACTION = 0.43
  const CENTER_Y_FRACTION = 0.57

  const zoomRef = useRef(null)

  const allHotspots = Array.isArray(data?.hotspots) ? data.hotspots : []

  // search + continent filter state
  const [countryQuery, setCountryQuery] = useState('')
  const [selectedContinents, setSelectedContinents] = useState(null)

  // only allow these continents (remove Antarctica)
  const CONTINENTS = ['Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania']

  // improved continent bounding boxes (lon/lat) tuned to better fill viewport
  const CONTINENT_BBOXES = {
    'Africa': { minLat: -35, maxLat: 38, minLon: -20, maxLon: 55 },
    // narrower Asia box to avoid extreme eastern longitudes that make fit tiny
    'Asia': { minLat: -10, maxLat: 60, minLon: 25, maxLon: 150 },
    // focus Europe around western/central europe to fill frame better
    'Europe': { minLat: 34, maxLat: 66, minLon: -10, maxLon: 40 },
    // continental North America (exclude far Aleutians / pacific longitudes)
    'North America': { minLat: 5, maxLat: 72, minLon: -140, maxLon: -50 },
    'South America': { minLat: -56, maxLat: 13, minLon: -82, maxLon: -34 },
    // Oceania focused box (Australia + island nations)
    'Oceania': { minLat: -50, maxLat: 10, minLon: 110, maxLon: 160 },
  }

  // per-continent zoom multipliers to make smaller/narrow continents fill viewport better
  const CONTINENT_ZOOM = {
    'Africa': 1.0,
    'South America': 1.0,
    'Asia': 1.25,
    'Europe': 1.25,
    'North America': 1.3,
    'Oceania': 1.35,
  }

  // basic country name list for autocomplete (common country names)
  const COUNTRIES = [
    'Afghanistan','Albania','Algeria','Andorra','Angola','Antigua and Barbuda','Argentina','Armenia','Australia','Austria','Azerbaijan',
    'Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bhutan','Bolivia','Bosnia and Herzegovina','Botswana','Brazil','Brunei','Bulgaria','Burkina Faso','Burundi',
    'Cote d\'Ivoire','Cabo Verde','Cambodia','Cameroon','Canada','Central African Republic','Chad','Chile','China','Colombia','Comoros','Costa Rica','Croatia','Cuba','Cyprus','Czech Republic',
    'Democratic Republic of the Congo','Denmark','Djibouti','Dominica','Dominican Republic','Ecuador','Egypt','El Salvador','Equatorial Guinea','Eritrea','Estonia','Eswatini','Ethiopia',
    'Fiji','Finland','France','Gabon','Gambia','Georgia','Germany','Ghana','Greece','Grenada','Guatemala','Guinea','Guinea-Bissau','Guyana','Haiti','Honduras','Hungary',
    'Iceland','India','Indonesia','Iran','Iraq','Ireland','Israel','Italy','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kiribati','Kosovo','Kuwait','Kyrgyzstan',
    'Laos','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Luxembourg','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall Islands','Mauritania','Mauritius',
    'Mexico','Micronesia','Moldova','Monaco','Mongolia','Montenegro','Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','New Zealand','Nicaragua','Niger','Nigeria',
    'North Korea','North Macedonia','Norway','Oman','Pakistan','Palau','Panama','Papua New Guinea','Paraguay','Peru','Philippines','Poland','Portugal','Qatar','Romania','Russia','Rwanda',
    'Saint Kitts and Nevis','Saint Lucia','Saint Vincent and the Grenadines','Samoa','San Marino','Sao Tome and Principe','Saudi Arabia','Senegal','Serbia','Seychelles','Sierra Leone','Singapore','Slovakia','Slovenia','Solomon Islands','Somalia','South Africa','South Korea','South Sudan','Spain','Sri Lanka','Sudan','Suriname','Sweden','Switzerland','Syria',
    'Taiwan','Tajikistan','Tanzania','Thailand','Timor-Leste','Togo','Tonga','Trinidad and Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United Arab Emirates','United Kingdom','United States','Uruguay','Uzbekistan','Vanuatu','Vatican City','Venezuela','Vietnam','Yemen','Zambia','Zimbabwe'
  ]

  // autocomplete state
  const [inputFocused, setInputFocused] = useState(false)
  const [activeSuggestion, setActiveSuggestion] = useState(-1)
  const suggestions = useMemo(() => {
    const q = String(countryQuery || '').trim().toLowerCase()
    if (!q) return []
    return COUNTRIES.filter(c => c.toLowerCase().includes(q)).slice(0, 8)
  }, [countryQuery])
  useEffect(() => { setActiveSuggestion(-1) }, [suggestions])

  function inferContinent(hotspot) {
    const lat = Number(hotspot?.latitude)
    const lon = Number(hotspot?.longitude)
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      if (lat < -60) return 'Antarctica'
      if ((lon >= 110 || lon <= -140) && lat <= 30) return 'Oceania'
      if (lon >= -82 && lon <= -34 && lat >= -56 && lat <= 13) return 'South America'
      if (lon >= -170 && lon <= -50 && lat >= 5 && lat <= 83) return 'North America'
      if (lon >= -20 && lon <= 55 && lat >= -35 && lat <= 38) return 'Africa'
      if (lon >= -25 && lon <= 60 && lat >= 34 && lat <= 72) return 'Europe'
      if (lat >= -10 && lon >= 26 && lon <= 180) return 'Asia'
    }
    const country = String(hotspot?.country || '').toLowerCase()
    if (!country) return null
    const asiaKeys = ['china', 'india', 'japan', 'korea', 'pakistan', 'iran', 'iraq', 'saudi', 'turkey', 'israel', 'bangladesh', 'indonesia', 'vietnam', 'thailand', 'philippines', 'malaysia']
    if (asiaKeys.some(k => country.includes(k))) return 'Asia'
    const europeKeys = ['france', 'germany', 'spain', 'italy', 'united kingdom', 'uk', 'russia', 'poland', 'ukraine', 'sweden', 'norway']
    if (europeKeys.some(k => country.includes(k))) return 'Europe'
    const africaKeys = ['egypt', 'nigeria', 'south africa', 'kenya', 'ethiopia', 'algeria', 'morocco', 'sudan', 'libya']
    if (africaKeys.some(k => country.includes(k))) return 'Africa'
    const naKeys = ['united states', 'usa', 'canada', 'mexico']
    if (naKeys.some(k => country.includes(k))) return 'North America'
    const saKeys = ['brazil', 'argentina', 'chile', 'colombia', 'peru', 'venezuela']
    if (saKeys.some(k => country.includes(k))) return 'South America'
    const ocKeys = ['australia', 'new zealand', 'fiji', 'samoa', 'papua']
    if (ocKeys.some(k => country.includes(k))) return 'Oceania'
    if (country.includes('antarctic')) return 'Antarctica'
    return null
  }

  const hotspots = useMemo(() => {
    const q = String(countryQuery || '').trim().toLowerCase()
    const continents = selectedContinents
    let list = allHotspots
    if (activeAspects) list = list.filter(h => activeAspects.has(getHotspotAspect(h)))
    if (continents && continents.size) {
      list = list.filter(h => {
        const cont = inferContinent(h)
        return cont && continents.has(cont)
      })
    }
    if (q) {
      list = list.filter(h => {
        const s = [h.country, h.label, h.location, h.admin1].filter(Boolean).join(' ').toLowerCase()
        return s.includes(q)
      })
    }
    return list
  }, [allHotspots, activeAspects, countryQuery, selectedContinents])

  const selected = hotspots.find(h => h.hotspot_id === selectedHotspotId) || hotspots[0] || null
  const isLightTheme = C.bg === '#f3f5f8'
  const mapSurfaceBg = isLightTheme
    ? 'linear-gradient(180deg, #f8fbff, #edf2f8)'
    : 'linear-gradient(180deg, rgba(8,11,16,0.99), rgba(13,17,22,0.99))'
  const panelOverlayBg = isLightTheme ? 'rgba(255,255,255,0.86)' : 'rgba(9,11,15,0.78)'
  const tooltipBg = isLightTheme ? 'rgba(255,255,255,0.96)' : 'rgba(9,11,15,0.93)'

  useEffect(() => {
    fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json')
      .then(r => r.json())
      .then(topo => setWorldData({ land: topojson.feature(topo, topo.objects.land), borders: topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b) }))
      .catch(() => {})
  }, [])

  useEffect(() => {
    const svgEl = svgRef.current
    if (!svgEl) return
    const MAX_SCALE = 10
    // d3 zoom instance (kept in ref for external pan/zoom control)
    zoomRef.current = null

    const zoom = d3.zoom()
      .translateExtent([[-W * 10, -H * 10], [W * 10, H * 10]])
      .filter(event => (event.type === 'wheel' ? event.ctrlKey : !event.button))
      .on('zoom', event => {
        const t = event.transform
        if (worldData && pathGen) {
          try {
            const b = pathGen.bounds(worldData.land)
            const x0 = b[0][0]
            const y0 = b[0][1]
            const x1 = b[1][0]
            const y1 = b[1][1]
            const k = t.k

            // compute tight clamps so land always fills the viewport
            let minTx = W - x1 * k
            let maxTx = -x0 * k
            let minTy = H - y1 * k
            let maxTy = -y0 * k

            if (minTx > maxTx) {
              const mid = (minTx + maxTx) / 2
              minTx = maxTx = mid
            }
            if (minTy > maxTy) {
              const mid = (minTy + maxTy) / 2
              minTy = maxTy = mid
            }

            let nx = t.x
            let ny = t.y
            if (nx < minTx) nx = minTx
            if (nx > maxTx) nx = maxTx
            if (ny < minTy) ny = minTy
            if (ny > maxTy) ny = maxTy
            if (nx !== t.x || ny !== t.y) {
              const clamped = d3.zoomIdentity.translate(nx, ny).scale(k)
              d3.select(svgEl).call(zoom.transform, clamped)
              return
            }
          } catch (e) {
            // if bounds calculation fails, fall back to default behavior
          }
        }
        setTransform(t)
      })

    d3.select(svgEl).call(zoom)
    zoomRef.current = zoom

    // initialize to tightly fit the land bounds if available
    let initialScale = defaultMinScale
    // default initial translate uses CENTER_X_FRACTION / CENTER_Y_FRACTION so the map center is nudged
    let initTx = W * CENTER_X_FRACTION
    let initTy = H * CENTER_Y_FRACTION
    if (worldData && pathGen) {
      try {
        const b = pathGen.bounds(worldData.land)
        const x0 = b[0][0]
        const y0 = b[0][1]
        const x1 = b[1][0]
        const y1 = b[1][1]
        const landW = Math.max(1, x1 - x0)
        const landH = Math.max(1, y1 - y0)
        const fitScale = Math.min(W / landW, H / landH)
        // apply zoom-in factor so the initial framing is more tightly cropped
        initialScale = Math.min(fitScale * ZOOM_IN_FACTOR, MAX_SCALE)
        zoom.scaleExtent([initialScale, MAX_SCALE])
        // center the land bounding box in the viewport using the computed scale
        // apply CENTER_X_FRACTION / CENTER_Y_FRACTION so the map's logical center is nudged
        const cx = (x0 + x1) / 2
        const cy = (y0 + y1) / 2
        initTx = W * CENTER_X_FRACTION - cx * initialScale
        initTy = H * CENTER_Y_FRACTION - cy * initialScale
      } catch (e) {
        initialScale = defaultMinScale
        initTx = W * CENTER_X_FRACTION
        initTy = H * CENTER_Y_FRACTION
      }
    }

    d3.select(svgEl).call(zoom.transform, d3.zoomIdentity.translate(initTx, initTy).scale(initialScale))
    return () => {
      zoomRef.current = null
      d3.select(svgEl).on('.zoom', null)
    }
  }, [worldData, pathGen])

  useEffect(() => {
    const el = svgRef.current
    if (!el) return
    const prevent = e => { if (e.ctrlKey) e.preventDefault() }
    el.addEventListener('wheel', prevent, { passive: false })
    return () => el.removeEventListener('wheel', prevent)
  }, [])

  const project = useCallback((lat, lng) => {
    const [px, py] = projection([Number(lng), Number(lat)]) || [0, 0]
    return { x: transform.x + px * transform.k, y: transform.y + py * transform.k }
  }, [projection, transform])

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

  const panToHotspots = useCallback((list, factor = ZOOM_IN_FACTOR) => {
    const svgEl = svgRef.current
    if (!svgEl || !list || !list.length) return
    const pts = list.map(h => {
      const lon = Number(h.longitude)
      const lat = Number(h.latitude)
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null
      return projection([lon, lat])
    }).filter(Boolean)
    if (!pts.length) return
    const xs = pts.map(p => p[0])
    const ys = pts.map(p => p[1])
    const x0 = Math.min(...xs)
    const x1 = Math.max(...xs)
    const y0 = Math.min(...ys)
    const y1 = Math.max(...ys)
    const width = Math.max(1, x1 - x0)
    const height = Math.max(1, y1 - y0)
    const fitScale = Math.min(W / width, H / height)
    const k = Math.min(Math.max(defaultMinScale, fitScale * factor), MAX_SCALE)
    const cx = (x0 + x1) / 2
    const cy = (y0 + y1) / 2
    const tx = W * CENTER_X_FRACTION - cx * k
    const ty = H * CENTER_Y_FRACTION - cy * k
    const t = d3.zoomIdentity.translate(tx, ty).scale(k)
    if (zoomRef.current) {
      d3.select(svgEl).transition().duration(600).call(zoomRef.current.transform, t)
    } else {
      d3.select(svgEl).call(d3.zoom().transform, t)
    }
  }, [projection])

  const panToLonLatBox = useCallback((minLon, minLat, maxLon, maxLat, factor = ZOOM_IN_FACTOR) => {
    const svgEl = svgRef.current
    if (!svgEl) return
    // normalize numbers
    minLon = Number(minLon)
    maxLon = Number(maxLon)
    minLat = Number(minLat)
    maxLat = Number(maxLat)
    if (!Number.isFinite(minLon) || !Number.isFinite(maxLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLat)) return

    // simple corner projection (handles most common cases)
    const corners = [[minLon, minLat], [minLon, maxLat], [maxLon, minLat], [maxLon, maxLat]]
    const pts = corners.map(c => projection([c[0], c[1]])).filter(Boolean)
    if (!pts.length) return
    const xs = pts.map(p => p[0])
    const ys = pts.map(p => p[1])
    const x0 = Math.min(...xs)
    const x1 = Math.max(...xs)
    const y0 = Math.min(...ys)
    const y1 = Math.max(...ys)
    const width = Math.max(1, x1 - x0)
    const height = Math.max(1, y1 - y0)
    const fitScale = Math.min(W / width, H / height)
    const k = Math.min(Math.max(defaultMinScale, fitScale * factor), MAX_SCALE)
    const cx = (x0 + x1) / 2
    const cy = (y0 + y1) / 2
    const tx = W * CENTER_X_FRACTION - cx * k
    const ty = H * CENTER_Y_FRACTION - cy * k
    const t = d3.zoomIdentity.translate(tx, ty).scale(k)
    if (zoomRef.current) {
      d3.select(svgEl).transition().duration(600).call(zoomRef.current.transform, t)
    } else {
      d3.select(svgEl).call(d3.zoom().transform, t)
    }
  }, [projection])

  const panToContinent = useCallback((continent, factor = 1.0) => {
    if (!continent) return
    const box = CONTINENT_BBOXES[continent]
    if (!box) return
    panToLonLatBox(box.minLon, box.minLat, box.maxLon, box.maxLat, factor)
  }, [panToLonLatBox])

  const panToCountryByName = useCallback(async (name) => {
    if (!name) return false
    try {
      const q = encodeURIComponent(String(name || '').trim())
      const url = `https://nominatim.openstreetmap.org/search?q=${q}&format=json&limit=1&polygon_geojson=0&addressdetails=0`
      const res = await fetch(url, { headers: { Accept: 'application/json' } })
      const json = await res.json()
      if (!Array.isArray(json) || !json.length) return false
      const b = json[0].boundingbox
      if (!b || b.length < 4) return false
      // Nominatim boundingbox: [south, north, west, east]
      const latMin = Number(b[0])
      const latMax = Number(b[1])
      const lonMin = Number(b[2])
      const lonMax = Number(b[3])
      panToLonLatBox(lonMin, latMin, lonMax, latMax, 1.05)
      return true
    } catch (e) {
      return false
    }
  }, [panToLonLatBox])

  const pickHotspotAtSvgPoint = useCallback((cx, cy) => {
    if (!hotspots.length) return null
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
    // make particle clouds much smaller and more compact
    const radius = (hotspot.cloud_radius || 32) * 0.35
    const count = 7 + Math.round((hotspot.cloud_density || 0.45) * 6)
    return Array.from({ length: count }, (_, i) => {
      const a = seededUnit(seed + i * 13)
      const b = seededUnit(seed + i * 29)
      const c = seededUnit(seed + i * 47)
      const angle = a * Math.PI * 2
      const dist = Math.pow(b, 0.6) * radius
      return {
        dx: Math.cos(angle) * dist,
        // keep distribution circular (no vertical squash)
        dy: Math.sin(angle) * dist,
        r: radius * 0.18 + c * radius * 0.22,
        opacity: 0.12 + (1 - dist / Math.max(radius, 1)) * 0.28,
        fill: palette.cloud,
      }
    })
  }

  return (
    <div>
      <div style={{ border: `1px solid ${C.border}`, background: `linear-gradient(180deg, ${C.bgRaised}, ${C.bg})`, padding: '1.1rem', position: 'relative', overflow: 'hidden' }}>
        <div style={{ position: 'absolute', inset: 0, background: isLightTheme ? 'radial-gradient(circle at 20% 20%, rgba(215,38,61,0.06), transparent 32%), radial-gradient(circle at 78% 24%, rgba(47,59,75,0.06), transparent 28%)' : 'radial-gradient(circle at 20% 20%, rgba(239,68,68,0.06), transparent 32%), radial-gradient(circle at 78% 24%, rgba(195,202,211,0.06), transparent 28%)', pointerEvents: 'none', zIndex: 2 }} />
        <div style={{ position: 'relative', height: 470, border: `1px solid ${C.border}`, background: mapSurfaceBg, overflow: 'hidden' }}>
          <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet" onClick={handleMapSvgClick} onMouseMove={handleMapSvgMouseMove} onMouseLeave={() => setHoveredHotspot(null)} style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', cursor: hoveredHotspot ? 'pointer' : 'grab' }}>
            <defs>
              <filter id="coreGlow" x="-100%" y="-100%" width="300%" height="300%"><feGaussianBlur stdDeviation="3" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
              <filter id="cloudBlur" x="-80%" y="-80%" width="260%" height="260%"><feGaussianBlur stdDeviation="12" /></filter>
              <filter id="ringBlur" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="5" /></filter>
              <clipPath id="mapClip"><rect x="0" y="0" width={W} height={H} /></clipPath>
            </defs>
            <g clipPath="url(#mapClip)">
              <g ref={zoomGRef} transform={`translate(${transform.x},${transform.y}) scale(${transform.k})`}>
                {pathGen && worldData && <path d={pathGen(worldData.land)} fill={isLightTheme ? 'rgba(102,115,134,0.13)' : 'rgba(166,177,190,0.14)'} stroke="none" />}
                {pathGen && worldData && <path d={pathGen(worldData.borders)} fill="none" stroke={isLightTheme ? 'rgba(57,69,86,0.24)' : 'rgba(195,202,211,0.22)'} strokeWidth={0.4 / transform.k} />}
              </g>
              {!loading && !error && hotspots.map(hotspot => {
                const palette = getHotspotPalette(hotspot)
                const pt = project(hotspot.latitude, hotspot.longitude)
                const isSelected = hotspot.hotspot_id === selected?.hotspot_id
                const isHovered = !isSelected && hotspot.hotspot_id === hoveredHotspot?.hotspot_id
                const particles = buildParticles(hotspot, palette)
                const baseCloud = hotspot.cloud_radius || 32
                // make the visible cloud much smaller and keep it circular
                const cloudRadius = (baseCloud * 0.35) / Math.max(1, transform.k * 0.9)
                const selRingRadius = cloudRadius * 0.9
                return (
                  <g key={hotspot.hotspot_id} style={{ pointerEvents: 'none' }}>
                    <circle cx={pt.x} cy={pt.y} r={cloudRadius} fill={palette.ring} filter="url(#cloudBlur)" opacity={isSelected ? 0.7 : isHovered ? 0.6 : 0.45} />
                    {particles.map((p, idx) => <circle key={idx} cx={pt.x + p.dx / Math.max(1, transform.k * 0.9)} cy={pt.y + p.dy / Math.max(1, transform.k * 0.9)} r={p.r / Math.max(1, transform.k * 0.9)} fill={palette.ring} opacity={p.opacity * (isSelected ? 1.4 : isHovered ? 1.2 : 1)} filter="url(#cloudBlur)" />)}
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
          {hoveredHotspot && <div style={{ position: 'absolute', left: tooltipPos.x, top: tooltipPos.y, zIndex: 10, pointerEvents: 'none', maxWidth: 240, border: `1px solid ${C.borderMid}`, background: tooltipBg, backdropFilter: 'blur(12px)', padding: '0.6rem 0.75rem' }}>
            <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '0.88rem', color: C.textPrimary, lineHeight: 1.3, marginBottom: '0.35rem' }}>{hotspotDisplayHeadline(hoveredHotspot)}</div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.43rem', color: HOTSPOT_TYPE_PALETTE[getHotspotAspect(hoveredHotspot)]?.core || C.silver, letterSpacing: '0.12em', textTransform: 'uppercase', marginBottom: '0.25rem' }}>{hoveredHotspot.admin1 ? `${hoveredHotspot.admin1} · ${hoveredHotspot.country}` : hoveredHotspot.country}</div>
            {hoveredHotspot.location && hotspotDisplayHeadline(hoveredHotspot) !== hoveredHotspot.location ? <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.06em', marginBottom: hoveredHotspot.sample_events?.[0] ? '0.35rem' : 0 }}>{hoveredHotspot.location}</div> : null}
            {hoveredHotspot.sample_events?.[0] && <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.5 }}>{truncateText(hotspotEventDescription(hoveredHotspot.sample_events[0]), 120)}</div>}
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.4rem', color: C.textMuted, letterSpacing: '0.08em', marginTop: '0.35rem' }}>{hoveredHotspot.event_count} events · click to select</div>
          </div>}
          <div style={{ position: 'absolute', left: '1rem', top: '1rem', display: 'flex', gap: '0.6rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.55rem 0.75rem', border: `1px solid ${C.border}`, background: panelOverlayBg, backdropFilter: 'blur(10px)', zIndex: 3 }}>
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
          <div style={{ position: 'absolute', right: '1rem', top: '1rem', display: 'flex', flexDirection: 'row', gap: '0.5rem', alignItems: 'center', padding: '0.25rem 0.5rem', zIndex: 3 }}>
            <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap', justifyContent: 'flex-end', alignItems: 'center' }}>
              {CONTINENTS.map(cont => {
                const isActive = selectedContinents && selectedContinents.has(cont)
                return (
                  <button
                    key={cont}
                    onClick={() => {
                      const prev = selectedContinents
                      const currentlySelected = prev && prev.has(cont)
                      if (currentlySelected) {
                        // clear selection -> reset to world fit
                        setSelectedContinents(null)
                        if (pathGen && worldData) {
                          try {
                            const b = pathGen.bounds(worldData.land)
                            const x0 = b[0][0]
                            const y0 = b[0][1]
                            const x1 = b[1][0]
                            const y1 = b[1][1]
                            const landW = Math.max(1, x1 - x0)
                            const landH = Math.max(1, y1 - y0)
                            const fitScale = Math.min(W / landW, H / landH)
                            const initialScale = Math.min(fitScale * ZOOM_IN_FACTOR, MAX_SCALE)
                            const cx = (x0 + x1) / 2
                            const cy = (y0 + y1) / 2
                            const tx = W * CENTER_X_FRACTION - cx * initialScale
                            const ty = H * CENTER_Y_FRACTION - cy * initialScale
                            const t = d3.zoomIdentity.translate(tx, ty).scale(initialScale)
                            const svgEl = svgRef.current
                            if (svgEl) {
                              if (zoomRef.current) d3.select(svgEl).transition().duration(600).call(zoomRef.current.transform, t)
                              else d3.select(svgEl).call(d3.zoom().transform, t)
                            }
                          } catch (e) {
                            // ignore
                          }
                        } else {
                          panToHotspots(allHotspots, 1.0)
                        }
                      } else {
                        // single-select this continent and pan to its bbox
                        setSelectedContinents(new Set([cont]))
                        const factor = CONTINENT_ZOOM[cont] || 1.2
                        panToContinent(cont, factor)
                      }
                    }}
                    style={{
                      background: isActive ? `${C.red}18` : C.bgRaised,
                      border: `1px solid ${isActive ? C.red : C.borderMid}`,
                      color: isActive ? C.textPrimary : C.textMuted,
                      fontFamily: "'JetBrains Mono', monospace",
                      fontSize: '0.44rem',
                      padding: '0.32rem 0.6rem',
                      cursor: 'pointer',
                      borderRadius: 8,
                      transition: 'all 0.12s ease',
                    }}
                  >
                    {cont}
                  </button>
                )
              })}
            </div>
            <div style={{ position: 'relative', width: 190 }}>
              <input
                value={countryQuery}
                onChange={e => setCountryQuery(e.target.value)}
                onFocus={() => setInputFocused(true)}
                onBlur={() => setTimeout(() => setInputFocused(false), 150)}
                onKeyDown={e => {
                  if (e.key === 'ArrowDown') {
                    if (suggestions.length) {
                      e.preventDefault()
                      setActiveSuggestion(prev => Math.min(prev + 1, suggestions.length - 1))
                    }
                    return
                  }
                  if (e.key === 'ArrowUp') {
                    if (suggestions.length) {
                      e.preventDefault()
                      setActiveSuggestion(prev => Math.max(prev - 1, 0))
                    }
                    return
                  }
                  if (e.key === 'Enter') {
                    e.preventDefault()
                    if (activeSuggestion >= 0 && suggestions[activeSuggestion]) {
                      const name = suggestions[activeSuggestion]
                      setCountryQuery(name)
                      setInputFocused(false)
                      setActiveSuggestion(-1)
                      panToCountryByName(name)
                      return
                    }
                    const raw = String(countryQuery || '').trim()
                    if (!raw) return
                    ;(async () => {
                      const did = await panToCountryByName(raw)
                      if (!did) {
                        const q = raw.toLowerCase()
                        const matches = allHotspots.filter(h => {
                          const s = [h.country, h.label, h.location, h.admin1].filter(Boolean).join(' ').toLowerCase()
                          return s.includes(q)
                        })
                        if (matches.length) panToHotspots(matches)
                      }
                    })()
                    return
                  }
                  if (e.key === 'Escape') {
                    setInputFocused(false)
                    setActiveSuggestion(-1)
                  }
                }}
                placeholder="Search country or place"
                style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.46rem', padding: '0.45rem 0.6rem', border: `1px solid ${C.borderMid}`, borderRadius: 6, background: C.bgRaised, color: C.textPrimary, width: '100%' }}
              />
              {inputFocused && suggestions.length > 0 && (
                <div style={{ position: 'absolute', right: 0, top: 'calc(100% + 6px)', width: 320, maxWidth: 320, background: C.bgRaised, border: `1px solid ${C.borderMid}`, boxShadow: '0 6px 20px rgba(0,0,0,0.08)', borderRadius: 6, zIndex: 40, overflow: 'hidden' }}>
                  {suggestions.map((sugg, idx) => {
                    const active = idx === activeSuggestion
                    return (
                      <div
                        key={sugg}
                        onMouseDown={() => {
                          setCountryQuery(sugg)
                          setInputFocused(false)
                          setActiveSuggestion(-1)
                          panToCountryByName(sugg)
                        }}
                        onMouseEnter={() => setActiveSuggestion(idx)}
                        style={{ padding: '0.45rem 0.6rem', cursor: 'pointer', background: active ? C.bg : 'transparent', color: C.textPrimary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', borderBottom: `1px solid ${C.border}` }}
                      >
                        {sugg}
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          </div>
          <div style={{ position: 'absolute', left: '1rem', right: '1rem', bottom: '1rem', display: 'flex', gap: '0.45rem', flexWrap: 'wrap', alignItems: 'center', padding: '0.35rem 0.5rem', zIndex: 3 }}>
            {ATTENTION_WINDOWS.map(item => {
              const active = data?.window === item.id
              return <button key={item.id} onClick={() => onWindowChange(item.id)} style={{ background: active ? `${C.red}18` : C.bgRaised, border: `1px solid ${active ? C.red : C.borderMid}`, color: active ? C.textPrimary : C.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.49rem', letterSpacing: '0.08em', padding: '0.45rem 0.6rem', cursor: 'pointer', borderRadius: 999, transition: 'all 0.15s ease' }}>{item.label}</button>
            })}
          </div>
        </div>
      </div>
    </div>
  )
}
