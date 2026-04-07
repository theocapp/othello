import { useQuery } from '@tanstack/react-query'
import { fetchBeforeNewsArchive } from '../api'

export default function useBeforeNewsArchive() {
  return useQuery(['beforeNewsArchive'], () => fetchBeforeNewsArchive(), {
    staleTime: 10 * 60 * 1000,
  })
}
