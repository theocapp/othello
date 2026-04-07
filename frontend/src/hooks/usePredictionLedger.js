import { useQuery } from '@tanstack/react-query'
import { fetchPredictionLedger } from '../api'

export default function usePredictionLedger() {
  return useQuery({
    queryKey: ['predictionLedger'],
    queryFn: () => fetchPredictionLedger(),
    staleTime: 5 * 60 * 1000,
  })
}
