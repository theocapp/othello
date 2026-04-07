import { useQuery } from '@tanstack/react-query'
import { fetchCorrelations } from '../api'

export default function useCorrelations(days = 3) {
  return useQuery(['correlations', days], () => fetchCorrelations(days), {
    staleTime: 5 * 60 * 1000,
  })
}
