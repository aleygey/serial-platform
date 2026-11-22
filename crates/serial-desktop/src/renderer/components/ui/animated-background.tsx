// Motion Primitives · ibelick. Adapted from components/core/animated-background.tsx.
// Copyright (c) 2024 ibelick. See MOTION-PRIMITIVES-LICENSE.txt.
import { AnimatePresence, motion } from 'motion/react'
import { useReducedMotion } from '../../lib/use-reduced-motion'
import { Children, cloneElement, useId, type ReactElement, type ReactNode } from 'react'

type Item = ReactElement<{ 'data-id': string; 'data-checked'?: boolean; children: ReactNode }>

/** The application owns selection, including navigation initiated outside the tab bar. */
export function AnimatedBackground({ children, value, className }: {
  children: Item[]
  value: string
  className: string
}): React.JSX.Element {
  const groupId = useId()
  const reduced = useReducedMotion()
  return <>{Children.map(children, (child) => {
    const selected = child.props['data-id'] === value
    return cloneElement(child, { 'data-checked': selected }, <>
      <AnimatePresence initial={false}>
        {selected && <motion.span aria-hidden="true" className={className}
          layoutId={reduced ? undefined : `selection-${groupId}`}
          initial={false} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
          transition={reduced ? { duration: 0 } : { type: 'spring', stiffness: 480, damping: 38 }} />}
      </AnimatePresence>
      <span className="nav-item-content">{child.props.children}</span>
    </>)
  })}</>
}
