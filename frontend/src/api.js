import axios from 'axios'

function defaultApiBaseUrl() {
  if (typeof window !== 'undefined') {
    const host = window.location.hostname
    if (host === 'localhost' || host === '127.0.0.1' || host === '::1') {
      return 'http://127.0.0.1:8001'
    }
  }
  return 'http://localhost:8001'
}

const baseURL = (import.meta.env.VITE_API_BASE_URL || defaultApiBaseUrl()).replace(/\/+$/, '')

const api = axios.create({
  baseURL,
  timeout: 20000,
})

export async function fetchBriefing(topic) {
  const response = await api.get(`/briefing/${topic}`)
  return response.data
}

export async function fetchEvents(topic = null) {
  const url = topic ? `/events/${topic}` : '/events'
  const response = await api.get(url)
  return response.data
}

export async function fetchHeadlines(options = {}) {
  const params = {}
  if (options.sortBy) params.sort_by = options.sortBy
  if (options.region && options.region !== 'all') params.region = options.region
  const response = await api.get('/headlines', { params })
  return response.data
}

export async function fetchRegionAttention(window = '24h') {
  const response = await api.get('/coverage/map', { params: { window }, timeout: 60000 })
  return response.data
}

export async function fetchEntitySignals(topic = null) {
  const url = topic ? `/entities/signals/${topic}` : '/entities/signals'
  const response = await api.get(url)
  return response.data
}

export async function fetchEntityReference(entity) {
  const response = await api.get(`/entities/reference/${encodeURIComponent(entity)}`)
  return response.data
}

export async function fetchPredictionLedger() {
  const response = await api.get('/foresight/predictions')
  return response.data
}

export async function fetchBeforeNewsArchive() {
  const response = await api.get('/foresight/before-news')
  return response.data
}

export async function sendQuery(question, options = {}) {
  const body = { question }
  if (options.topic) body.topic = options.topic
  if (options.regionContext) body.region_context = options.regionContext
  if (options.hotspotId) body.hotspot_id = options.hotspotId
  if (options.storyEventId) body.story_event_id = options.storyEventId
  if (options.attentionWindow) body.attention_window = options.attentionWindow
  if (options.sourceUrls?.length) body.source_urls = options.sourceUrls.slice(0, 12)
  const response = await api.post('/query', body)
  return response.data
}

export async function fetchTimeline(query) {
  const response = await api.get(`/timeline/${encodeURIComponent(query)}`)
  return response.data
}

export async function fetchHealth() {
  const response = await api.get('/health')
  return response.data
}

export async function fetchOverview() {
  const response = await api.get('/system/overview')
  return response.data
}

export async function triggerIngest(topic = null) {
  const response = await api.post('/ingest', null, { params: topic ? { topic } : {} })
  return response.data
}
