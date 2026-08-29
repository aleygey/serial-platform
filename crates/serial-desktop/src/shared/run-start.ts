import type { Actor, PendingRunStartApproval, PortSnapshot } from './contracts'

export function isApprovalActionable(
  port: PortSnapshot,
  approval: PendingRunStartApproval,
  actor: Actor | undefined,
  nowMs = Date.now()
): boolean {
  const control = port.control
  return Boolean(
    actor?.kind === 'human'
    && approval.required_approver.kind === 'human'
    && approval.required_approver.id === actor.id
    && approval.port === port.config.port
    && approval.daemon_epoch === port.daemon_epoch
    && approval.generation === port.generation
    && approval.expires_wall_time_ns / 1_000_000 > nowMs
    && control
    && control.owner.kind === 'human'
    && control.owner.id === actor.id
    && control.id === approval.expected_control_id
    && control.fence === approval.expected_fence
    && control.epoch === approval.daemon_epoch
    && control.generation === approval.generation
  )
}
