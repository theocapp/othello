import { useQuery } from '@tanstack/react-query'
import { fetchEntitySignals } from '../api'

export default function useEntitySignals() {
  return useQuery(['entitySignals'], () => fetchEntitySignals(), {
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
