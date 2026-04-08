export const ATTENTION_WINDOWS = [
  { id: '24h', label: '1 Day' },
  { id: '7d', label: '7 Days' },
  { id: '30d', label: '30 Days' },
  { id: '90d', label: '90 Days' },
  { id: '365d', label: '365 Days' },
  { id: 'custom', label: 'Custom' },
]

export const HOTSPOT_TYPE_PALETTE = {
  conflict: { core: 'rgba(239,68,68,0.95)', ring: 'rgba(239,68,68,0.18)', cloud: 'rgba(239,68,68,0.09)' },
  political: { core: 'rgba(96,165,250,0.95)', ring: 'rgba(96,165,250,0.18)', cloud: 'rgba(96,165,250,0.09)' },
  economic: { core: 'rgba(251,191,36,0.95)', ring: 'rgba(251,191,36,0.18)', cloud: 'rgba(251,191,36,0.09)' },
  default: { core: 'rgba(195,202,211,0.90)', ring: 'rgba(195,202,211,0.15)', cloud: 'rgba(195,202,211,0.07)' },
}

export function totalNarrativeFlags(event) {
  return (event?.contradiction_count || 0) + (event?.narrative_fracture_count || 0)
}

export function getHotspotAspect(hotspot) {
  const aspect = String(hotspot?.aspect || '').toLowerCase()
  if (aspect === 'conflict' || aspect === 'political' || aspect === 'economic') return aspect
  const types = (Array.isArray(hotspot?.event_types) ? hotspot.event_types : [])
    .map(t => String(t || '').toLowerCase())
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

export function getHotspotPalette(hotspot) {
  return HOTSPOT_TYPE_PALETTE[getHotspotAspect(hotspot)] || HOTSPOT_TYPE_PALETTE.default
}

export function formatAttentionShare(value) {
  return `${Math.round((value || 0) * 100)}%`
}

export function formatWindowLabel(windowId) {
  return ATTENTION_WINDOWS.find(item => item.id === windowId)?.label || windowId
}

export function hotspotDisplayHeadline(hotspot) {
  const explicit = String(hotspot?.headline || hotspot?.development_title || '').trim()
  if (explicit) return explicit
  return String(hotspot?.label || '').trim()
}

export function mapAspectToQueryTopic(aspect) {
  if (aspect === 'economic') return 'economics'
  return 'geopolitics'
}

export function buildHotspotClusterAnalysisQuery(hotspot) {
  const place = [hotspot.location || hotspot.label, hotspot.admin1, hotspot.country].filter(Boolean).join(', ')
  const samples = (hotspot.sample_events || []).slice(0, 5).map(ev => hotspotEventDescription(ev)).filter(Boolean)
  const sampleBlock = samples.length ? ` Representative cluster reporting includes: ${samples.join(' · ')}` : ''
  return `Focus on this mapped cluster${place ? ` in ${place}` : ''}.${sampleBlock} Give a concise intelligence briefing: current situation, key actors, escalation risks, and what to watch next. Anchor analysis to this geography and cluster rather than generic global themes.`
}

export function collectHotspotSourceUrls(hotspot) {
  const sampleEvents = Array.isArray(hotspot?.sample_events) ? hotspot.sample_events : []
  if (!sampleEvents.length) return []
  const urls = []
  const seen = new Set()
  for (const event of sampleEvents) {
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

export function normalizeStoryTopicForQuery(topic) {
  const s = String(topic || '').toLowerCase().replace(/_/g, '')
  if (s === 'economics' || s === 'economic') return 'economics'
  if (s === 'geopolitics' || s === 'political') return 'geopolitics'
  return null
}

export function hashToken(value) {
  let hash = 2166136261
  for (let index = 0; index < String(value || '').length; index += 1) {
    hash ^= String(value).charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return Math.abs(hash >>> 0)
}

export function seededUnit(seed) {
  const x = Math.sin(seed * 12.9898) * 43758.5453
  return x - Math.floor(x)
}

export function hotspotEventTitle(ev) {
  const t = (ev?.title || '').trim()
  if (t) return t
  const et = (ev?.event_type || 'Event').trim()
  const sub = (ev?.sub_event_type || '').trim()
  const place = (ev?.location || ev?.admin1 || ev?.country || '').trim()
  const action = sub && sub.toLowerCase() !== et.toLowerCase() ? sub : et
  const a1 = (ev?.actor_primary || '').trim()
  const a2 = (ev?.actor_secondary || '').trim()
  if (a1 && a2 && place) return `${action}: ${a1} vs ${a2} — ${place}`
  if (a1 && place) return `${action} involving ${a1} — ${place}`
  return place ? `${action} — ${place}` : action
}

export function hotspotEventDescription(ev) {
  const s = (ev?.summary || '').trim()
  if (s && s.length > 20) return s
  const et = (ev?.event_type || 'Event').trim()
  const sub = (ev?.sub_event_type || '').trim()
  const action = sub && sub.toLowerCase() !== et.toLowerCase() ? sub : et
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
