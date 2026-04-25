import { useEffect, useRef, useState } from 'react'
import { applyTheme } from '../constants/theme'

export default function useAppChrome() {
  const [themeMode, setThemeMode] = useState(() => {
    if (typeof window === 'undefined') return 'dark'
    return window.localStorage.getItem('othello-theme-mode') === 'light' ? 'light' : 'dark'
  })
  const [time, setTime] = useState(new Date())
  const [headerVisible, setHeaderVisible] = useState(true)
  const [animationPhase, setAnimationPhase] = useState(() => {
    if (typeof window === 'undefined') return 'title'
    return window.sessionStorage.getItem('othello-splashed') ? 'complete' : 'title'
  })

  const lastScrollY = useRef(0)
  const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC'

  useEffect(() => {
    applyTheme(themeMode)
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('othello-theme-mode', themeMode)
    }
  }, [themeMode])

  useEffect(() => {
    if (animationPhase === 'title') {
      const timer = setTimeout(() => setAnimationPhase('date'), 1000)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'date') {
      const timer = setTimeout(() => setAnimationPhase('moving'), 1000)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'moving') {
      const timer = setTimeout(() => setAnimationPhase('complete'), 1400)
      return () => clearTimeout(timer)
    }
    if (animationPhase === 'complete' && typeof window !== 'undefined') {
      window.sessionStorage.setItem('othello-splashed', '1')
    }
  }, [animationPhase])

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(timer)
  }, [])

  useEffect(() => {
    function handleScroll() {
      const currentY = window.scrollY
      if (currentY < 60) {
        setHeaderVisible(true)
        lastScrollY.current = currentY
        return
      }
      setHeaderVisible(currentY < lastScrollY.current)
      lastScrollY.current = currentY
    }

    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  function toggleThemeMode() {
    setThemeMode(current => (current === 'dark' ? 'light' : 'dark'))
  }

  return {
    themeMode,
    toggleThemeMode,
    time,
    headerVisible,
    animationPhase,
    localTimeZone,
  }
}
