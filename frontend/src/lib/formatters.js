export function parseDateValue(value) {
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

export function formatDateTime(value) {
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

export function formatClock(value, timeZone) {
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

export function formatDateLabel(value) {
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  }).format(value)
}

export function formatRelativeUpdate(value, timeZone) {
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

export function friendlyErrorMessage(err, label) {
  if (err?.code === 'ECONNABORTED') return `Timed out while loading ${label}. The backend may be slow or unavailable.`
  if (!err?.response) return `Unable to reach the backend for ${label}.`
  if (err.response?.data?.detail) return `Unable to load ${label}: ${err.response.data.detail}`
  return `Unable to load ${label} right now.`
}

export function formatRegionLabel(region) {
  if (!region) return 'Global'
  return region
    .split('-')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

export function truncateText(text, limit = 180) {
  const clean = String(text || '').trim()
  if (!clean) return ''
  if (clean.length <= limit) return clean
  return `${clean.slice(0, limit - 1).trimEnd()}…`
}

export const BRIEFING_SECTION_KEYS = [
  'SITUATION REPORT', 'KEY DEVELOPMENTS', 'CRITICAL ACTORS',
  'SIGNAL vs NOISE', 'PREDICTIONS', 'DEEPER CONTEXT',
  'WHAT TO WATCH', 'SOURCE CONTRADICTIONS',
]

export function parseBriefingSections(text) {
  const source = typeof text === 'string' ? text : ''
  const result = {}
  BRIEFING_SECTION_KEYS.forEach((section, i) => {
    const next = BRIEFING_SECTION_KEYS[i + 1]
    const escaped = section.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const regex = new RegExp(
      `(?:#{1,3}\\s*)?${escaped}:?\\s*([\\s\\S]*?)${next ? `(?=(?:#{1,3}\\s*)?${next.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:?)` : '$'}`,
      'i',
    )
    const match = source.match(regex)
    result[section] = match ? match[1].trim() : ''
  })
  return result
}
