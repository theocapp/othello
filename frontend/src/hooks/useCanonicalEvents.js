import { useQuery } from '@tanstack/react-query'
import { fetchCanonicalEvents } from '../api'

export default function useCanonicalEvents({ topic = null, status = null, limit = 80 } = {}) {
  return useQuery(['canonical-events', topic, status, limit], () => fetchCanonicalEvents({ topic, status, limit }), {
    keepPreviousData: true,
  })
}
