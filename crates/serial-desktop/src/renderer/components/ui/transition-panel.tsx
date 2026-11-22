// Motion Primitives · ibelick. Adapted from components/core/transition-panel.tsx.
// Copyright (c) 2024 ibelick. See MOTION-PRIMITIVES-LICENSE.txt.
import { motion, type Variants } from 'motion/react'
import { useReducedMotion } from '../../lib/use-reduced-motion'
import type { ReactNode } from 'react'

const panelVariants: Variants = {
  center: { opacity: 1, y: 0, display: 'block' },
  exit: { opacity: 0, y: 5, transitionEnd: { display: 'none' } }
}

/** Keep each form mounted across transitions so its edits and cursor survive navigation. */
export function TransitionPanel({ activeIndex, children, tabIds }: {
  activeIndex: number
  children: ReactNode[]
  tabIds: string[]
}): React.JSX.Element {
  const reduced = useReducedMotion()
  return <div className="settings-panels">
    {children.map((child, index) => {
      const selected = activeIndex === index
      return <motion.div key={tabIds[index]} role="tabpanel" aria-labelledby={tabIds[index]}
        id={`${tabIds[index]}-panel`} aria-hidden={!selected} inert={!selected} data-active={selected}
        initial={false} animate={selected ? 'center' : 'exit'} variants={panelVariants}
        transition={{ duration: reduced ? 0 : 0.17, ease: 'easeOut' }}>
        {child}
      </motion.div>
    })}
  </div>
}
