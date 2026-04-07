import { useQuery } from '@tanstack/react-query'
import { fetchHeadlines } from '../api'

export default function useHeadlines(sortBy = 'relevance', region = 'all') {
  return useQuery(['headlines', sortBy, region], () => fetchHeadlines({ sortBy, region }), {
    keepPreviousData: true,
  })
}
