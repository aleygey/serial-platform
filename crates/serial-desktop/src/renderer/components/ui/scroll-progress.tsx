// Motion-Primitives, copyright (c) ibelick. See MOTION-PRIMITIVES-LICENSE.txt.
'use client';

import { motion, SpringOptions, useReducedMotion, useScroll, useSpring } from 'motion/react';
import { cn } from '../../lib/utils';
import { RefObject } from 'react';

export type ScrollProgressProps = {
  className?: string;
  springOptions?: SpringOptions;
  containerRef?: RefObject<HTMLDivElement | null>;
};

const DEFAULT_SPRING_OPTIONS: SpringOptions = {
  stiffness: 200,
  damping: 50,
  restDelta: 0.001,
};

export function ScrollProgress({
  className,
  springOptions,
  containerRef,
}: ScrollProgressProps) {
  const { scrollYProgress } = useScroll({
    container: containerRef,

  });

  const reduced = useReducedMotion();
  const scaleX = useSpring(scrollYProgress, {
    ...DEFAULT_SPRING_OPTIONS,
    ...(springOptions ?? {}),
  });

  return (
    <motion.div
      aria-hidden data-slot="scroll-progress"
      className={cn('inset-x-0 top-0 h-1 origin-left', className)}
      style={{
        scaleX: reduced ? scrollYProgress : scaleX,
      }}
    />
  );
}
