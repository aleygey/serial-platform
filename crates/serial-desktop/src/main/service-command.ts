export function buildSpawnArgs(prefix: string[], endpoint: string): string[] {
  return [...prefix, '--bind', new URL(endpoint).host]
}
