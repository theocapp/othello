import { QueryClient } from '@tanstack/react-query'

// Single shared QueryClient for the app. Configure defaults here.
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 30,
      cacheTime: 1000 * 60 * 5,
      refetchOnWindowFocus: false,
    },
  },
})
