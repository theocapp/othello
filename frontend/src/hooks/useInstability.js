import { useQuery } from '@tanstack/react-query'
import { fetchInstability } from '../api'

export default function useInstability(days = 3) {
  return useQuery({
    queryKey: ['instability', days],
    queryFn: () => fetchInstability(days),
    staleTime: 5 * 60 * 1000,
  })
}
