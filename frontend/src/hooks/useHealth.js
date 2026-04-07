import { useQuery } from '@tanstack/react-query'
import { fetchHealth } from '../api'

export default function useHealth() {
  return useQuery({
    queryKey: ['health'],
    queryFn: fetchHealth,
  })
}
