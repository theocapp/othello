import { useQuery } from '@tanstack/react-query'
import { fetchRegionAttention } from '../api'

export default function useMapAttention(windowParam = '24h') {
  const key = typeof windowParam === 'string' ? windowParam : JSON.stringify(windowParam)
  return useQuery({
    queryKey: ['mapAttention', key],
    queryFn: () => fetchRegionAttention(windowParam),
    staleTime: 60 * 1000, // 1 minute
  })
}
