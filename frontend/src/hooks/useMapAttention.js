import { useQuery } from '@tanstack/react-query'
import { fetchRegionAttention } from '../api'

export default function useMapAttention(windowId = '24h') {
  return useQuery(['mapAttention', windowId], () => fetchRegionAttention(windowId), {
    staleTime: 60 * 1000, // 1 minute
  })
}
