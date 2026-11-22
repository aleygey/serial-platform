import { useSyncExternalStore } from 'react'

const preference = typeof window === 'undefined' ? undefined : window.matchMedia?.('(prefers-reduced-motion: reduce)')
const read = () => preference?.matches ?? true
const server = () => true

function subscribe(update: () => void): () => void {
  preference?.addEventListener('change', update)
  return () => preference?.removeEventListener('change', update)
}

export function useReducedMotion(): boolean {
  return useSyncExternalStore(subscribe, read, server)
}
