import { useQuery } from '@tanstack/react-query'
import { fetchBeforeNewsArchive } from '../api'

export default function useBeforeNewsArchive() {
  return useQuery({
    queryKey: ['beforeNewsArchive'],
    queryFn: () => fetchBeforeNewsArchive(),
    staleTime: 10 * 60 * 1000,
  })
}
