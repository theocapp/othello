import axios from 'axios'

function defaultApiBaseUrl() {
  if (typeof window === 'undefined') return 'http://127.0.0.1:8001'
  if (import.meta.env.DEV) return 'http://127.0.0.1:8001'
  return ''
}

const baseURL = (import.meta.env.VITE_API_BASE_URL ?? defaultApiBaseUrl()).replace(/\/+$/, '')

const api = axios.create({
  baseURL,
})

function compactParams(params) {
  return Object.fromEntries(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== '')
  )
}

export async function fetchBriefing(topic) {
  const response = await api.get(`/briefing/${encodeURIComponent(topic)}`)
  return response.data
}

export async function fetchHealth() {
  const response = await api.get('/health')
  return response.data
}

export async function fetchEvents(limit = 12) {
  const response = await api.get('/events', {
    params: compactParams({ limit }),
  })
  return response.data
}

export async function fetchCanonicalEventDebug(eventId) {
  const response = await api.get(`/events/canonical/${encodeURIComponent(eventId)}/debug`)
  return response.data
}

export async function fetchCanonicalEvents({ topic = null, status = null, limit = 80 } = {}) {
  const response = await api.get('/events/canonical', {
    params: compactParams({ topic, status, limit }),
  })
  return response.data
}

export async function fetchEvaluationScorecard({
  kind = null,
  topic = null,
  limitFiles = 80,
  includeErrorSamples = false,
} = {}) {
  const response = await api.get('/evaluation/scorecard', {
    params: compactParams({
      kind,
      topic,
      limit_files: limitFiles,
      include_error_samples: includeErrorSamples,
    }),
  })
  return response.data
}

export async function fetchRegionAttention(window = '24h') {
  const response = await api.get('/coverage/map', {
    params: compactParams({ window }),
  })
  return response.data
}

export async function fetchInstability(days = 3) {
  const response = await api.get('/instability', {
    params: compactParams({ days }),
  })
  return response.data
}

export async function fetchCorrelations(days = 3) {
  const response = await api.get('/correlations', {
    params: compactParams({ days }),
  })
  return response.data
}

export async function fetchPredictionLedger({ topic = null, refresh = false, limit = 100 } = {}) {
  const response = await api.get('/foresight/predictions', {
    params: compactParams({ topic, refresh, limit }),
  })
  return response.data
}

export async function fetchBeforeNewsArchive({ limit = 50, minimumGapHours = 0 } = {}) {
  const response = await api.get('/foresight/before-news', {
    params: compactParams({
      limit,
      minimum_gap_hours: minimumGapHours,
    }),
  })
  return response.data
}

export async function fetchEntitySignals(topic = null) {
  const url = topic
    ? `/entities/signals/${encodeURIComponent(topic)}`
    : '/entities/signals'

  const response = await api.get(url)
  return response.data
}

export async function fetchEntityReference(entity, { refresh = false } = {}) {
  const response = await api.get(`/entities/reference/${encodeURIComponent(entity)}`, {
    params: compactParams({ refresh }),
  })
  return response.data
}

export async function fetchCacheStatus() {
  const response = await api.get('/cache/status')
  return response.data
}

export async function fetchHeadlines(options = {}) {
  const { sortBy = 'relevance', region = 'all' } = options

  const response = await api.get('/headlines', {
    params: compactParams({
      sort_by: sortBy,
      region,
    }),
  })
  return response.data
}

export async function fetchTimeline(query) {
  const response = await api.post('/timeline', { question: query })
  return response.data
}

export async function sendQuery(question, options = null) {
  const opts =
    typeof options === 'string'
      ? { topic: options }
      : (options || {})

  const body = {
    question,
    topic: opts.topic ?? null,
    region_context: opts.region_context ?? opts.regionContext ?? null,
    hotspot_id: opts.hotspot_id ?? opts.hotspotId ?? null,
    story_event_id: opts.story_event_id ?? opts.storyEventId ?? null,
    source_urls: opts.source_urls ?? opts.sourceUrls ?? null,
    attention_window: opts.attention_window ?? opts.attentionWindow ?? null,
  }

  const response = await api.post('/query', body)
  return response.data
}
