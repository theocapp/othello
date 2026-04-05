import axios from 'axios'

const api = axios.create({
  baseURL: 'http://localhost:8000',
})

export async function fetchBriefing(topic) {
  const response = await api.get(`/briefing/${topic}`)
  return response.data
}

export async function sendQuery(question, topic = null) {
  const body = { question }
  if (topic) body.topic = topic
  const response = await api.post('/query', body)
  return response.data
}

export async function fetchEntitySignals(topic = null) {
  const url = topic ? `/entities/signals/${topic}` : '/entities/signals'
  const response = await api.get(url)
  return response.data
}

export async function fetchCacheStatus() {
  const response = await api.get('/cache/status')
  return response.data
}

export async function fetchHeadlines() {
  const response = await api.get('/headlines')
  return response.data
}

export async function fetchTimeline(query) {
  const response = await api.post('/timeline', { question: query })
  return response.data
}

