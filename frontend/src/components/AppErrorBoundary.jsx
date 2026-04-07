import { Component } from 'react'
import { C } from '../constants/theme'

export default class AppErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null, errorInfo: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ errorInfo })
    console.error('App crashed', error, errorInfo)
  }

  render() {
    const { error, errorInfo } = this.state

    if (!error) {
      return this.props.children
    }

    return (
      <div style={{ minHeight: '100vh', background: C.bg, color: C.textPrimary, padding: '2rem', fontFamily: "'JetBrains Mono', monospace" }}>
        <div style={{ maxWidth: 900, margin: '0 auto', border: `1px solid ${C.redDeep}`, background: `${C.redDeep}12`, padding: '1.25rem' }}>
          <div style={{ color: C.red, letterSpacing: '0.16em', textTransform: 'uppercase', fontSize: '0.72rem', marginBottom: '0.75rem' }}>Frontend runtime error</div>
          <div style={{ marginBottom: '1rem', whiteSpace: 'pre-wrap', lineHeight: 1.6, color: C.textSecondary }}>{error?.message || String(error)}</div>
          {errorInfo?.componentStack && (
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: '0.48rem', lineHeight: 1.6, color: C.textMuted }}>{errorInfo.componentStack}</pre>
          )}
        </div>
      </div>
    )
  }
}