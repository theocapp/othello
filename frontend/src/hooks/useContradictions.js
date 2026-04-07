import { useQuery } from '@tanstack/react-query'
import { fetchEvents } from '../api'
import { totalNarrativeFlags } from '../lib/hotspots'

export default function useContradictions(limit = 6) {
  return useQuery({
    queryKey: ['contradictions', limit],
    queryFn: async () => {
      const data = await fetchEvents()
      const ranked = (data.events || [])
        .filter(event => totalNarrativeFlags(event) > 0)
        .sort((a, b) => totalNarrativeFlags(b) - totalNarrativeFlags(a))
      return ranked.slice(0, limit)
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
