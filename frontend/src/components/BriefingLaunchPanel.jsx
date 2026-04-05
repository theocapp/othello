import { C } from '../constants/theme'

export default function BriefingLaunchPanel({ topics, onOpenBriefing, onOpenForesight }) {
  return (
    <div style={{ border: `1px solid ${C.border}`, background: C.bgRaised, padding: '1rem' }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.52rem', color: C.silver, letterSpacing: '0.18em', textTransform: 'uppercase', marginBottom: '0.85rem' }}>Generate Briefings</div>
      <div className="briefing-launch-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: '0.75rem' }}>
        {topics.map(topic => (
          <button key={topic.id} className="briefing-btn" onClick={() => onOpenBriefing(topic)} style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", padding: '1.05rem 1.05rem', borderRadius: 4, minHeight: 88, borderTop: `2px solid ${topic.accent || C.borderMid}` }}>
            <div style={{ fontSize: '0.46rem', letterSpacing: '0.14em', color: topic.accent || C.silver, textTransform: 'uppercase', marginBottom: '0.45rem' }}>{topic.tag}</div>
            <div style={{ fontSize: '0.9rem', letterSpacing: '0.03em', color: C.textPrimary, marginBottom: '0.35rem' }}>{topic.label}</div>
            <div style={{ fontSize: '0.5rem', letterSpacing: '0.06em', color: C.textMuted, lineHeight: 1.55 }}>{topic.description}</div>
          </button>
        ))}
      </div>
      <div className="briefing-tools-grid" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.6rem', marginTop: '0.9rem' }}>
        <button className="briefing-btn" onClick={() => onOpenForesight('predictions')} style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', padding: '0.7rem 0.8rem', borderRadius: 4, minHeight: 58 }}>PREDICTION LEDGER</button>
        <button className="briefing-btn" onClick={() => onOpenForesight('before-news')} style={{ textAlign: 'left', width: '100%', color: C.textSecondary, fontFamily: "'JetBrains Mono', monospace", fontSize: '0.56rem', padding: '0.7rem 0.8rem', borderRadius: 4, minHeight: 58 }}>BEFORE IT WAS NEWS</button>
      </div>
    </div>
  )
}
