import { useEffect, useState } from 'react'
import { fetchCanonicalEvent, mergeEvents, splitArticle } from '../api'
import { C } from '../constants/theme'
import { friendlyErrorMessage } from '../lib/formatters'

export default function EventCorrectionPanel({ eventId, payload }) {
  const [event, setEvent] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [mergeTargetId, setMergeTargetId] = useState('')
  const [mergePending, setMergePending] = useState(false)
  const [mergeStatus, setMergeStatus] = useState(null)
  const [splitPending, setSplitPending] = useState({})
  const [splitStatus, setSplitStatus] = useState({})

  useEffect(() => {
    let cancelled = false
    async function load() {
      if (!eventId) {
        setEvent(null)
        setLoading(false)
        return
      }
      setLoading(true)
      try {
        const data = await fetchCanonicalEvent(eventId)
        if (!cancelled) {
          setEvent(data)
          setError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setError(friendlyErrorMessage(err, 'canonical event'))
          setEvent(null)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [eventId])

  const articleUrls = event?.article_urls || []
  const articlesByUrl = {}
  for (const article of payload?.articles || []) {
    if (article.url) {
      articlesByUrl[article.url] = article
    }
  }

  const getArticleTitle = (url) => {
    const article = articlesByUrl[url]
    return article?.title || article?.source || url
  }

  const shortenUrl = (url) => {
    try {
      const u = new URL(url)
      return u.hostname
    } catch {
      return url.substring(0, 30) + (url.length > 30 ? '...' : '')
    }
  }

  const handleSplit = async (url) => {
    setSplitPending(prev => ({ ...prev, [url]: true }))
    setSplitStatus(prev => ({ ...prev, [url]: null }))
    try {
      await splitArticle(eventId, url)
      setSplitStatus(prev => ({ ...prev, [url]: 'Queued' }))
    } catch (err) {
      setSplitStatus(prev => ({ ...prev, [url]: `Error: ${err.message}` }))
    } finally {
      setSplitPending(prev => ({ ...prev, [url]: false }))
    }
  }

  const handleMerge = async () => {
    if (!mergeTargetId.trim()) return
    setMergePending(true)
    setMergeStatus(null)
    try {
      await mergeEvents(eventId, mergeTargetId.trim())
      setMergeStatus('Queued')
      setMergeTargetId('')
    } catch (err) {
      setMergeStatus(`Error: ${err.message}`)
    } finally {
      setMergePending(false)
    }
  }

  return (
    <section style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.14em', textTransform: 'uppercase', marginBottom: '0.75rem' }}>
        Analyst Corrections
      </div>

      {loading && (
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.textMuted }}>
          Loading event...
        </div>
      )}

      {!loading && error && (
        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.54rem', color: C.red }}>
          {error}
        </div>
      )}

      {!loading && !error && event && (
        <div style={{ display: 'grid', gap: '1rem' }}>
          {/* Split Articles Section */}
          <div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.08em', marginBottom: '0.5rem', textTransform: 'uppercase' }}>
              Articles ({articleUrls.length})
            </div>
            {articleUrls.length === 0 ? (
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.5rem', color: C.textMuted }}>
                No articles in this event.
              </div>
            ) : (
              <div style={{ display: 'grid', gap: '0.45rem', maxHeight: 240, overflowY: 'auto' }}>
                {articleUrls.map(url => (
                  <div key={url} style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', padding: '0.5rem 0.65rem', border: `1px solid ${C.borderMid}`, background: C.bg, borderRadius: '3px' }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontFamily: "'Source Serif 4', serif", fontSize: '0.75rem', color: C.textSecondary, lineHeight: 1.3, marginBottom: '0.15rem', wordBreak: 'break-word' }}>
                        {getArticleTitle(url)}
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.45rem', color: C.textMuted }}>
                        {shortenUrl(url)}
                      </div>
                    </div>
                    <button
                      onClick={() => handleSplit(url)}
                      disabled={splitPending[url]}
                      style={{
                        fontFamily: "'JetBrains Mono', monospace",
                        fontSize: '0.46rem',
                        color: C.textSecondary,
                        background: splitStatus[url] ? C.bgRaised : C.bg,
                        border: `1px solid ${C.borderMid}`,
                        padding: '0.35rem 0.55rem',
                        borderRadius: '3px',
                        cursor: splitPending[url] ? 'wait' : 'pointer',
                        whiteSpace: 'nowrap',
                        flexShrink: 0,
                      }}
                    >
                      {splitStatus[url] || (splitPending[url] ? '...' : 'Split')}
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Merge Section */}
          <div style={{ paddingTop: '0.5rem', borderTop: `1px solid ${C.border}` }}>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.48rem', color: C.textSecondary, letterSpacing: '0.08em', marginBottom: '0.5rem', textTransform: 'uppercase' }}>
              Merge with another event
            </div>
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <input
                type="text"
                placeholder="Event ID"
                value={mergeTargetId}
                onChange={e => setMergeTargetId(e.target.value)}
                disabled={mergePending}
                style={{
                  flex: 1,
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.52rem',
                  color: C.textPrimary,
                  background: C.bg,
                  border: `1px solid ${C.borderMid}`,
                  padding: '0.55rem 0.65rem',
                  borderRadius: '3px',
                  outline: 'none',
                }}
              />
              <button
                onClick={handleMerge}
                disabled={!mergeTargetId.trim() || mergePending}
                style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.46rem',
                  color: C.textSecondary,
                  background: mergeStatus ? C.bgRaised : C.bg,
                  border: `1px solid ${C.borderMid}`,
                  padding: '0.55rem 0.75rem',
                  borderRadius: '3px',
                  cursor: mergePending ? 'wait' : 'pointer',
                  whiteSpace: 'nowrap',
                  opacity: !mergeTargetId.trim() || mergePending ? 0.5 : 1,
                }}
              >
                {mergeStatus || (mergePending ? '...' : 'Merge')}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}
