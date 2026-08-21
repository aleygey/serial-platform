export interface ManagedChild {
  stdin: { end(): void } | null
  exitCode: number | null
  once(event: 'exit', listener: () => void): unknown
  kill(): boolean
}

export async function stopManagedChild(child: ManagedChild, graceMs = 3_000): Promise<void> {
  const exited = child.exitCode === null
    ? new Promise<void>((resolve) => child.once('exit', resolve))
    : Promise.resolve()
  child.stdin?.end()
  await Promise.race([
    exited,
    new Promise<void>((resolve) => setTimeout(resolve, graceMs))
  ])
  if (child.exitCode === null) child.kill()
}
