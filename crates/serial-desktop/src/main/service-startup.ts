export interface StartingService {
  start(endpoint: string): Promise<void>
  markReady(): void
  stop(): Promise<void>
}

export async function startAndVerifyService(
  service: StartingService,
  endpoint: string,
  waitUntilReachable: () => Promise<void>
): Promise<void> {
  await service.start(endpoint)
  try {
    await waitUntilReachable()
    service.markReady()
  } catch (error) {
    await service.stop()
    throw error
  }
}
