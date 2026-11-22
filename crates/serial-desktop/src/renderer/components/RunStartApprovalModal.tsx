import { Clock3 } from 'lucide-react'
import { motion } from 'motion/react'
import { useReducedMotion } from '../lib/use-reduced-motion'
import { useEffect, useRef, useState } from 'react'
import type { PendingRunStartApproval, RunStartDecision } from '../../shared/contracts'

interface Props {
  approval: PendingRunStartApproval
  onDecide: (decision: RunStartDecision) => Promise<boolean>
  onExpired: () => void
}

export function RunStartApprovalModal({ approval, onDecide, onExpired }: Props): React.JSX.Element {
  const reduced = useReducedMotion()
  const dialogRef = useRef<HTMLDivElement>(null)
  const expiredRef = useRef(false)
  const submittingRef = useRef(false)
  const [remainingMs, setRemainingMs] = useState(() => millisecondsRemaining(approval))
  const [submitting, setSubmitting] = useState<RunStartDecision>()
  const [decisionError, setDecisionError] = useState(false)

  useEffect(() => {
    const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const backdrop = dialogRef.current?.parentElement
    const siblings = backdrop?.parentElement
      ? [...backdrop.parentElement.children].filter((element): element is HTMLElement => (
          element instanceof HTMLElement && element !== backdrop
        ))
      : []
    const backgroundState = siblings.map((element) => ({
      element,
      inert: element.inert,
      ariaHidden: element.getAttribute('aria-hidden')
    }))
    for (const element of siblings) {
      element.inert = true
      element.setAttribute('aria-hidden', 'true')
    }
    dialogRef.current?.focus()
    return () => {
      for (const state of backgroundState) {
        state.element.inert = state.inert
        if (state.ariaHidden === null) state.element.removeAttribute('aria-hidden')
        else state.element.setAttribute('aria-hidden', state.ariaHidden)
      }
      if (previous?.isConnected && !('disabled' in previous && Boolean(previous.disabled))) previous.focus()
    }
  }, [approval.id])

  useEffect(() => {
    const update = (): void => {
      const remaining = millisecondsRemaining(approval)
      setRemainingMs(remaining)
      if (remaining <= 0 && !expiredRef.current) {
        expiredRef.current = true
        onExpired()
      }
    }
    update()
    const timer = setInterval(update, 250)
    return () => clearInterval(timer)
  }, [approval, onExpired])

  const decide = async (decision: RunStartDecision): Promise<void> => {
    if (submittingRef.current || remainingMs <= 0) return
    submittingRef.current = true
    setDecisionError(false)
    setSubmitting(decision)
    try {
      if (await onDecide(decision)) return
    } catch { /* The dialog remains actionable if the backend rejects a request. */ }
    submittingRef.current = false
    setSubmitting(undefined)
    setDecisionError(true)
  }

  return (
    <div className="approval-backdrop">
      <motion.div initial={reduced ? false : { opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.16 }}
        aria-describedby="run-start-approval-description"
        aria-labelledby="run-start-approval-title"
        aria-modal="true"
        className="approval-dialog"
        onKeyDown={(event) => trapModalFocus(event, dialogRef.current)}
        ref={dialogRef}
        role="dialog"
        tabIndex={-1}
      >
        <div className="approval-heading">
          <h2 id="run-start-approval-title">允许外部 Agent 接管串口？</h2>
          <p id="run-start-approval-description">当前串口由你持有。批准后，请求方将通过 MCP 获得控制权并执行这轮 Run。</p>
        </div>
        <dl className="approval-details">
          <div><dt>串口</dt><dd><code>{approval.port}</code></dd></div>
          <div><dt>Agent</dt><dd>{approval.requester.label}</dd></div>
          <div><dt>Run</dt><dd>{approval.label}</dd></div>
        </dl>
        <div role="timer" className={`approval-expiry ${remainingMs <= 5_000 ? 'is-urgent' : ''}`}>
          <Clock3 size={14} />
          {remainingMs > 0 ? `请求将在 ${formatRemaining(remainingMs)} 后自动失效` : '请求已失效'}
        </div>
        {decisionError && <p aria-live="assertive" className="approval-error">审批未完成，状态可能已变化；请核对提示后重试。</p>}
        <div className="approval-actions">
          <button disabled={Boolean(submitting) || remainingMs <= 0} onClick={() => void decide('deny')} type="button">
            {submitting === 'deny' ? '正在拒绝…' : '拒绝'}
          </button>
          <button className="is-primary" disabled={Boolean(submitting) || remainingMs <= 0} onClick={() => void decide('approve')} type="button">
            {submitting === 'approve' ? '正在批准…' : '批准并交给 Agent'}
          </button>
        </div>
      </motion.div>
    </div>
  )
}

export function millisecondsRemaining(approval: Pick<PendingRunStartApproval, 'expires_wall_time_ns'>, nowMs = Date.now()): number {
  return Math.max(0, approval.expires_wall_time_ns / 1_000_000 - nowMs)
}

function formatRemaining(milliseconds: number): string {
  const seconds = Math.max(1, Math.ceil(milliseconds / 1_000))
  return seconds >= 60 ? `${Math.floor(seconds / 60)}分${seconds % 60}秒` : `${seconds}秒`
}

function trapModalFocus(event: React.KeyboardEvent<HTMLDivElement>, dialog: HTMLDivElement | null): void {
  if (event.key === 'Escape') {
    event.preventDefault()
    event.stopPropagation()
    return
  }
  if (event.key !== 'Tab' || !dialog) return
  const controls = [...dialog.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), [tabindex="0"]')]
  if (controls.length === 0) {
    event.preventDefault()
    return
  }
  const active = document.activeElement
  const index = controls.indexOf(active as HTMLElement)
  const next = event.shiftKey
    ? index <= 0 ? controls.length - 1 : index - 1
    : index < 0 || index === controls.length - 1 ? 0 : index + 1
  event.preventDefault()
  controls[next].focus()
}
