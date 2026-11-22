// Rare UI Step Player track, adapted from the registry component. Copyright (c) 2026 Swami Malode. See RARE-UI-LICENSE.txt.
// Each dot is an observed step or a known sequence position. The renderer never advances it on a timer.
import { motion, useReducedMotion } from 'motion/react'
import type { StateTone } from '../../lib/console-workspace'

const WIDTH_SPRING = { type: 'spring', duration: 0.42, bounce: 0.14 } as const
export interface StepMark { index: number; tone: StateTone; label: string }

export function StepPlayer({ steps, total, label, paused }: {
  steps: StepMark[]; total?: number; label: string; paused?: boolean
}) {
  const reduced = useReducedMotion()
  const last = steps.at(-1)?.index ?? 0
  const count = Math.max(total ?? 0, steps.length ? last + 1 : 0)
  const first = Math.max(0, Math.min(last - 4, count - 9))
  const length = Math.min(count - first, 9)
  const known = new Map(steps.map(step => [step.index, step]))
  return <span data-slot="step-player" className="step-player" role="img" aria-label={label} title={label}>
    {first > 0 && <span aria-hidden="true">…</span>}
    <span data-slot="step-player-track" className="step-track" aria-hidden="true">
      {Array.from({ length }, (_, offset) => {
        const index = first + offset
        const step = known.get(index)
        const active = !!step && index === last
        return <motion.span key={index} data-slot="step-player-step" data-state={step?.tone ?? 'pending'}
          title={step?.label ?? `第 ${index + 1} 步尚无记录`} initial={false}
          animate={{ width: active ? 23 : 5 }} transition={paused || reduced ? { duration: 0 } : WIDTH_SPRING}>
          {active && <span data-slot="step-player-fill" />}
        </motion.span>
      })}
    </span>
    {first + length < count && <span aria-hidden="true">…</span>}
  </span>
}
