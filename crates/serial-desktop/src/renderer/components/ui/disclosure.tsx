// Motion-Primitives, copyright (c) ibelick. See MOTION-PRIMITIVES-LICENSE.txt.
'use client';
import * as React from 'react';
import {
  AnimatePresence,
  motion,
  MotionConfig,
  Transition,
  Variant,
  Variants,
} from 'motion/react';
import { createContext, useContext, useState, useId } from 'react';
import { cn } from '../../lib/utils';

export type DisclosureContextType = {
  open: boolean;
  toggle: () => void;
  variants?: { expanded: Variant; collapsed: Variant };
};

const DisclosureContext = createContext<DisclosureContextType | undefined>(
  undefined
);

export type DisclosureProviderProps = {
  children: React.ReactNode;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  variants?: { expanded: Variant; collapsed: Variant };
};

function DisclosureProvider({
  children,
  open: openProp,
  onOpenChange,
  variants,
}: DisclosureProviderProps) {
  const [internalOpenValue, setInternalOpenValue] = useState(false);
  const open = openProp ?? internalOpenValue;

  const toggle = () => {
    const newOpen = !open;
    if (openProp === undefined) setInternalOpenValue(newOpen);
    if (onOpenChange) {
      onOpenChange(newOpen);
    }
  };

  return (
    <DisclosureContext.Provider
      value={{
        open,
        toggle,
        variants,
      }}
    >
      {children}
    </DisclosureContext.Provider>
  );
}

function useDisclosure() {
  const context = useContext(DisclosureContext);
  if (!context) {
    throw new Error('useDisclosure must be used within a DisclosureProvider');
  }
  return context;
}

export type DisclosureProps = {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  children: React.ReactNode;
  className?: string;
  variants?: { expanded: Variant; collapsed: Variant };
  transition?: Transition;
};

export function Disclosure({
  open: openProp,
  onOpenChange,
  children,
  className,
  transition,
  variants,
}: DisclosureProps) {
  return (
    <MotionConfig transition={transition}>
      <div className={className}>
        <DisclosureProvider
          open={openProp}
          onOpenChange={onOpenChange}
          variants={variants}
        >
          {children}
        </DisclosureProvider>
      </div>
    </MotionConfig>
  );
}

export function DisclosureTrigger({ children, className, ...props }: React.ComponentProps<'button'>) {
  const { toggle, open } = useDisclosure();
  return <button {...props} type="button" className={className} aria-expanded={open} onClick={toggle}>{children}</button>;
}

export function DisclosureContent({
  children,
  className,
  id,
}: {
  id?: string;
  children: React.ReactNode;
  className?: string;
}) {
  const { open, variants } = useDisclosure();
  const uniqueId = useId();

  const BASE_VARIANTS: Variants = {
    expanded: {
      height: 'auto',
      opacity: 1,
    },
    collapsed: {
      height: 0,
      opacity: 0,
    },
  };

  const combinedVariants = {
    expanded: { ...BASE_VARIANTS.expanded, ...variants?.expanded },
    collapsed: { ...BASE_VARIANTS.collapsed, ...variants?.collapsed },
  };

  return (
    <div id={id} inert={!open} aria-hidden={!open} className={cn('overflow-hidden', className)}>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            id={uniqueId}
            initial='collapsed'
            animate='expanded'
            exit='collapsed'
            variants={combinedVariants}
          >
            {children}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default {
  Disclosure,
  DisclosureProvider,
  DisclosureTrigger,
  DisclosureContent,
};
