import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import type { Actor, PendingRunStartApproval, PortSnapshot } from '../../shared/contracts'
import { isApprovalActionable } from '../../shared/run-start'
import { RunStartApprovalModal } from './RunStartApprovalModal'

const human: Actor = { id: 'human-1', label: 'Operator', kind: 'human' }
const approval: PendingRunStartApproval = {
  id: 'approval-1',
  port: 'COM6',
  requester: { id: 'agent-1', label: 'Deploy Agent', kind: 'agent' },
  required_approver: human,
  label: '升级固件并验证重启',
  metadata: {},
  control_ttl_ms: 30_000,
  daemon_epoch: 'epoch',
  generation: 3,
  expected_control_id: 'control-1',
  expected_fence: 9,
  requested_wall_time_ns: 1_000_000_000,
  expires_wall_time_ns: 20_000_000_000
}
const port: PortSnapshot = {
  config: { port: 'COM6', enabled: true },
  daemon_epoch: 'epoch',
  head_seq: 20,
  generation: 3,
  endpoint_present: true,
  session_state: 'online',
  control: {
    id: 'control-1', owner: human, epoch: 'epoch', generation: 3, fence: 9,
    issued_wall_time_ns: 0, expires_wall_time_ns: 20_000_000_000
  },
  pending_run_start: approval
}

describe('RunStart Human approval', () => {
  it('is actionable only for the exact live Human holder and lease fence', () => {
    expect(isApprovalActionable(port, approval, human, 10_000)).toBe(true)
    expect(isApprovalActionable(port, approval, { ...human, id: 'other' }, 10_000)).toBe(false)
    expect(isApprovalActionable({ ...port, control: { ...port.control!, fence: 10 } }, approval, human, 10_000)).toBe(false)
    expect(isApprovalActionable(port, approval, human, 20_000)).toBe(false)
  })

  it('renders an accessible non-dismissible decision dialog with the requested context', () => {
    const markup = renderToStaticMarkup(
      <RunStartApprovalModal approval={approval} onDecide={vi.fn()} onExpired={vi.fn()} />
    )
    expect(markup).toContain('role="dialog"')
    expect(markup).toContain('aria-modal="true"')
    expect(markup).toContain('COM6')
    expect(markup).toContain('Deploy Agent')
    expect(markup).toContain('升级固件并验证重启')
    expect(markup).toContain('批准并交给 Agent')
    expect(markup).toContain('拒绝')
  })
})
