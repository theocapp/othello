const THEMES = {
  dark: {
    bg: '#13161a',
    bgRaised: '#1a1e24',
    bgHover: '#1e2329',
    border: '#1e2228',
    borderMid: '#2a2f38',
    textPrimary: '#f6f7f9',
    textSecondary: '#b3bac4',
    textMuted: '#7d8794',
    silver: '#c3cad3',
    gold: '#c9a84c',
    accent: '#60a5fa',
    success: '#16a34a',
    red: '#ef4444',
    redDeep: '#dc2626',
    white: '#ffffff',
  },
  light: {
    bg: '#f3f5f8',
    bgRaised: '#ffffff',
    bgHover: '#edf2f7',
    border: '#d8dee8',
    borderMid: '#c5ceda',
    textPrimary: '#151b24',
    textSecondary: '#394556',
    textMuted: '#667386',
    silver: '#2f3b4b',
    gold: '#a07828',
    accent: '#2563eb',
    success: '#15803d',
    red: '#d7263d',
    redDeep: '#b91c2f',
    white: '#ffffff',
  },
}

export const C = { ...THEMES.dark }

export function applyTheme(mode) {
  const palette = THEMES[mode] || THEMES.dark
  Object.assign(C, palette)
}

export function buildAppStyles() {
  return `
    @import url('https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=Source+Serif+4:ital,opsz,wght@0,8..60,400;0,8..60,600;1,8..60,400&family=JetBrains+Mono:wght@400;500&display=swap');
    * { box-sizing: border-box; margin: 0; padding: 0; }
    ::-webkit-scrollbar { width: 3px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: ${C.borderMid}; border-radius: 2px; }
    @keyframes fadeUp { from { opacity: 0; transform: translateY(16px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
    @keyframes slideIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes shimmer { 0% { background-position: -600px 0; } 100% { background-position: 600px 0; } }
    .headline-item { transition: background 0.15s ease; }
    .headline-item:hover { background: ${C.bgHover}; }
    .theater-row:hover { background: ${C.bgHover}; }
    .briefing-btn { border: 1px solid ${C.borderMid}; background: none; transition: all 0.2s; cursor: pointer; }
    .briefing-btn:hover { border-color: ${C.silver}; background: ${C.bgHover} !important; transform: translateY(-1px); }
    .entity-item:hover { background: ${C.bgHover}; }
    .skeleton {
      background: linear-gradient(90deg, ${C.bgRaised} 25%, ${C.bgHover} 50%, ${C.bgRaised} 75%);
      background-size: 600px 100%;
      animation: shimmer 1.4s infinite;
      border-radius: 3px;
      display: block;
    }
    .query-input { caret-color: ${C.silver}; }
    .query-input:focus { outline: none; border-color: ${C.silver} !important; }
    .query-input::placeholder { color: ${C.textMuted}; font-style: italic; }
    @media (max-width: 1180px) {
      .home-shell { grid-template-columns: 1fr !important; }
      .home-sidebar { margin-top: 0 !important; }
      .lower-grid { grid-template-columns: 1fr !important; }
    }
    @media (max-width: 960px) {
      .sidebar-sticky { position: static !important; top: auto !important; }
    }
    @media (max-width: 700px) {
      .briefing-btn { text-align: left !important; }
      .header-section { padding-top: 0.4rem !important; padding-bottom: 0.8rem !important; }
      .main-padding { padding: 172px 1rem 0 !important; }
      .briefing-layout { grid-template-columns: 1fr !important; }
      .briefing-sidebar { position: static !important; }
      .coverage-summary { grid-template-columns: 1fr !important; }
      .briefing-launch-grid { grid-template-columns: 1fr !important; }
      .briefing-tools-grid { grid-template-columns: 1fr !important; }
    }
  `
}
