import { C } from '../constants/theme'

const BUCKET_STYLES = {
  critical: {
    border: `1px solid ${C.red}`,
    color: C.red,
  },
  high: {
    border: `1px solid ${C.silver}`,
    color: C.textPrimary,
  },
  medium: {
    border: `1px solid ${C.borderMid}`,
    color: C.textSecondary,
  },
  low: {
    border: `1px solid ${C.border}`,
    color: C.textMuted,
  },
}

function Pill({ children, style = {} }) {
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        padding: '0.2rem 0.38rem',
        borderRadius: 999,
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: '0.42rem',
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        ...style,
      }}
    >
      {children}
    </span>
  )
}

export default function StoryImportanceBadges({ story }) {
  if (!story) return null

  const bucket = String(story.importance_bucket || '').toLowerCase()
  const bucketStyle = BUCKET_STYLES[bucket] || BUCKET_STYLES.low
  const score = Number.isFinite(Number(story.importance_score))
    ? Math.round(Number(story.importance_score))
    : null
  const sourceCount = Number(story.source_count || 0)
  const contradictionCount = Number(story.contradiction_count || 0)

  if (!bucket && score === null && sourceCount <= 0 && contradictionCount <= 0) {
    return null
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', flexWrap: 'wrap' }}>
      {bucket ? <Pill style={bucketStyle}>{bucket}</Pill> : null}
      {score !== null ? <Pill style={{ border: `1px solid ${C.borderMid}`, color: C.textSecondary }}>{score} score</Pill> : null}
      {sourceCount > 0 ? <Pill style={{ border: `1px solid ${C.border}`, color: C.textMuted }}>{sourceCount} src</Pill> : null}
      {contradictionCount > 0 ? <Pill style={{ border: `1px solid ${C.border}`, color: C.textMuted }}>{contradictionCount} split</Pill> : null}
    </div>
  )
}
