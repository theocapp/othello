import { C } from '../constants/theme'

export const MD = {
  p: ({ children }) => <p style={{ marginBottom: '0.75rem', lineHeight: 1.85 }}>{children}</p>,
  strong: ({ children }) => <strong style={{ fontWeight: 700, color: C.textPrimary }}>{children}</strong>,
  h1: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1.2rem', fontWeight: 700, margin: '1.25rem 0 0.5rem', color: C.textPrimary }}>{children}</div>,
  h2: ({ children }) => <div style={{ fontFamily: "'Libre Baskerville', serif", fontSize: '1rem', fontWeight: 700, margin: '1rem 0 0.4rem', color: C.textPrimary }}>{children}</div>,
  h3: ({ children }) => <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.62rem', letterSpacing: '0.15em', color: C.silver, margin: '0.75rem 0 0.35rem', textTransform: 'uppercase' }}>{children}</div>,
  li: ({ children }) => <div style={{ display: 'flex', gap: '0.65rem', marginBottom: '0.45rem', alignItems: 'flex-start' }}><span style={{ color: C.silver, flexShrink: 0, fontSize: '0.5rem', marginTop: '0.45rem' }}>◆</span><span>{children}</span></div>,
  ul: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  ol: ({ children }) => <div style={{ margin: '0.35rem 0' }}>{children}</div>,
  hr: () => <div style={{ borderTop: `1px solid ${C.border}`, margin: '1.25rem 0' }} />,
  table: ({ children }) => <div style={{ overflowX: 'auto', margin: '0.75rem 0' }}><table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>{children}</table></div>,
  th: ({ children }) => <th style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.58rem', color: C.silver, padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.borderMid}`, textAlign: 'left', letterSpacing: '0.08em' }}>{children}</th>,
  td: ({ children }) => <td style={{ padding: '0.4rem 0.6rem', borderBottom: `1px solid ${C.border}`, color: C.textSecondary }}>{children}</td>,
  blockquote: ({ children }) => <div style={{ borderLeft: `2px solid ${C.silver}`, paddingLeft: '1rem', color: C.textSecondary, fontStyle: 'italic', margin: '0.75rem 0' }}>{children}</div>,
}
