import { useNavigate } from 'react-router-dom'
import { TOPICS } from '../constants/topics'
import {
  buildHotspotClusterAnalysisQuery,
  collectHotspotSourceUrls,
  mapAspectToQueryTopic,
  normalizeStoryTopicForQuery,
} from '../lib/hotspots'

export default function useHomeNavigation({ mapAttentionWindow }) {
  const navigate = useNavigate()

  // Create briefingById lookup for context
  const briefingById = TOPICS.reduce((acc, topic) => {
    acc[topic.id] = topic
    return acc
  }, {})

  function openBriefingByTopic(topicId, state = {}) {
    const topic = briefingById[topicId] || briefingById.geopolitics
    navigate(`/briefing/${topic.id}`, { state })
  }

  function openForesight(mode) {
    navigate(`/foresight/${mode}`)
  }

  function openTimeline(query) {
    navigate('/timeline', { state: { query } })
  }

  function openContradiction(event) {
    if (!event) return
    navigate('/contradiction', { state: { event } })
  }

  function openDeepDive(payload) {
    if (!payload) return
    navigate('/deep-dive', { state: payload })
  }

  function openStoryDeepDive(story) {
    const sourceUrls = (story.sources || [])
      .map(source => source?.url)
      .filter(url => typeof url === 'string' && url.startsWith('http'))
      .slice(0, 12)

    const queryTopic = normalizeStoryTopicForQuery(story.topic)

    openDeepDive({
      title: story.headline,
      query: `Give me a comprehensive intelligence deep-dive on this story: "${story.headline}". Cover: what is actually happening beyond the surface narrative, key actors and motivations, what mainstream media is missing or underreporting, historical parallels, geopolitical implications, and probability assessments for how this develops. Be direct, analytical, and specific.`,
      queryTopic: queryTopic || undefined,
      storyEventId: story.event_id || undefined,
      sourceUrls: sourceUrls.length ? sourceUrls : undefined,
      regionContext: story.dominant_region || undefined,
    })
  }

  function openEventDebug(eventLike) {
    const eventId = String(eventLike?.event_id || '').trim()
    if (!eventId) return
    const payload = { eventId, label: eventLike?.headline || eventLike?.label || eventId }
    navigate(`/debug/${eventId}`, { state: payload })
  }

  function openHotspotClusterAnalysis(hotspot) {
    if (!hotspot) return
    const queryTopic = mapAspectToQueryTopic(hotspot?.aspect || '')
    const sourceUrls = collectHotspotSourceUrls(hotspot)

    openDeepDive({
      title: `Cluster: ${hotspot.location || hotspot.label || 'Hotspot'}`,
      query: buildHotspotClusterAnalysisQuery(hotspot),
      queryTopic,
      regionContext: [hotspot.location || hotspot.label, hotspot.admin1, hotspot.country]
        .filter(Boolean)
        .join(', ') || undefined,
      hotspotId: hotspot.hotspot_id || undefined,
      attentionWindow: mapAttentionWindow,
      sourceUrls: sourceUrls.length ? sourceUrls : undefined,
    })
  }

  return {
    openBriefingByTopic,
    openForesight,
    openTimeline,
    openContradiction,
    openDeepDive,
    openStoryDeepDive,
    openEventDebug,
    openHotspotClusterAnalysis,
  }
}
