import { motion, useMotionValue, useReducedMotion, type HTMLMotionProps } from 'motion/react'
import { useCallback, useEffect, useRef, useState, type RefObject } from 'react'

type Point = { x: number; y: number }
interface Drag {
  port: string
  index: number | null
  weight: number
  keyboard: boolean
}

export function usePortDrag(deck: RefObject<HTMLDivElement | null>, onDrop: (port: string, index: number) => void) {
  const [drag, setDrag] = useState<Drag | null>(null)
  const [settling, setSettling] = useState(false)
  const settleTimer = useRef<number>(undefined)
  const live = useRef<Drag | null>(null)
  const centers = useRef<number[]>([])
  const pointer = useRef<Point | null>(null)
  const x = useMotionValue(0)
  const y = useMotionValue(0)
  const update = useCallback((next: Drag | null) => {
    live.current = next
    setDrag(next)
  }, [])
  const cancel = useCallback(() => {
    if (live.current?.index != null) {
      window.clearTimeout(settleTimer.current)
      setSettling(true)
      settleTimer.current = window.setTimeout(() => setSettling(false), 320)
    }
    pointer.current = null
    update(null)
  }, [update])
  const pointIndex = useCallback((point: Point): number | null => {
    const area = deck.current
    if (!area) return null
    const rect = area.getBoundingClientRect()
    const clientX = point.x - window.scrollX
    const clientY = point.y - window.scrollY
    x.set(clientX + 206 < window.innerWidth ? clientX + 12 : Math.max(6, clientX - 190))
    y.set(Math.max(5, Math.min(window.innerHeight - 100, clientY + 13)))
    if (clientX < rect.left || clientX > rect.right || clientY < rect.top || clientY > rect.bottom) return null
    const offset = clientX - rect.left + area.scrollLeft
    const index = centers.current.findIndex(center => offset < center)
    return index === -1 ? centers.current.length : index
  }, [deck, x, y])
  const begin = (port: string, point?: Point) => {
    const area = deck.current
    if (!area || document.querySelector('[aria-modal="true"]')) return
    window.clearTimeout(settleTimer.current)
    setSettling(false)
    pointer.current = point ?? null
    const left = area.getBoundingClientRect().left - area.scrollLeft
    const panels = Array.from(area.querySelectorAll<HTMLElement>('[data-panel]'))
    // Keep the hit regions stable while the actual panels animate out of the way.
    centers.current = panels.map(panel => {
      const box = panel.getBoundingClientRect()
      return box.left - left + box.width / 2
    })
    const weight = (node: HTMLElement) => Number.parseFloat(getComputedStyle(node).flexGrow) || 1
    const existing = panels.find(panel => panel.id === port)
    const share = existing ? weight(existing) : panels.reduce((sum, node) => sum + weight(node), 0) / (panels.length || 1)
    update({port, weight: share || 1, keyboard: !point, index: point ? pointIndex(point) : panels.length})
  }
  const move = (point: Point) => {
    const current = live.current
    if (!current) return
    pointer.current = point
    const index = pointIndex(point)
    if (current.index !== index) update({ ...current, index })
  }
  const finish = (point?: Point) => {
    const current = live.current
    if (!current) return
    const index = point ? pointIndex(point) : current.index
    if (index === null) return cancel()
    pointer.current = null
    update(null)
    onDrop(current.port, index)
  }
  const step = (direction: number) => {
    const current = live.current
    if (current?.keyboard) update({ ...current, index: Math.min(centers.current.length, Math.max(0, (current.index ?? 0) + direction)) })
  }
  const dragging = !!drag
  const pointerDragging = dragging && !drag.keyboard
  useEffect(() => {
    const area = deck.current
    if (!pointerDragging || !area || area.scrollWidth <= area.clientWidth) return
    let frame: number
    let previous = performance.now()
    const scrollNearEdge = (now: number) => {
      const current = live.current
      const point = pointer.current
      if (!current || !point) return
      const elapsed = Math.min(32, now - previous)
      previous = now
      if (current.index !== null) {
        const rect = area.getBoundingClientRect()
        const offset = point.x - window.scrollX - rect.left
        const edge = 32
        const speed = offset < edge ? (offset - edge) / edge : Math.max(0, (offset - rect.width + edge) / edge)
        if (speed) {
          area.scrollLeft += speed * elapsed * 0.65
          const index = pointIndex(point)
          if (current.index !== index) update({ ...current, index })
        }
      }
      frame = requestAnimationFrame(scrollNearEdge)
    }
    frame = requestAnimationFrame(scrollNearEdge)
    return () => cancelAnimationFrame(frame)
  }, [deck, pointerDragging, pointIndex, update])
  useEffect(() => () => window.clearTimeout(settleTimer.current), [])
  useEffect(() => {
    if (!dragging) return
    const key = (event: KeyboardEvent) => { if (event.key === 'Escape') { event.preventDefault(); cancel() } }
    window.addEventListener('keydown', key, true)
    window.addEventListener('pointercancel', cancel)
    window.addEventListener('blur', cancel)
    return () => {
      window.removeEventListener('keydown', key, true)
      window.removeEventListener('pointercancel', cancel)
      window.removeEventListener('blur', cancel)
    }
  }, [dragging, cancel])
  return { drag, settling, x, y, begin, move, finish, cancel, step }
}

export type PortDragController = ReturnType<typeof usePortDrag>
type HandleProps = Omit<HTMLMotionProps<'button'>, 'port' | 'onPan' | 'onPanStart' | 'onPanEnd'> & {
  port: string; controller?: PortDragController
}

export function PortDragHandle({ port, controller, onClick, onKeyDown, children, ...props }: HandleProps) {
  const panned = useRef(false)
  const keyboard = controller?.drag?.keyboard && controller.drag.port === port
  return <motion.button {...props} type="button" onPointerDown={() => { panned.current = false }}
    onPanStart={(_event, info) => { panned.current = true; controller?.begin(port, info.point) }}
    onPan={(_event, info) => controller?.move(info.point)}
    onPanEnd={(event, info) => { if (event.type === 'pointercancel') controller?.cancel(); else controller?.finish(info.point) }}
    onClick={event => {
      if (event.detail && panned.current) { panned.current = false; return }
      onClick?.(event)
    }}
    onKeyDown={event => {
      if (controller && event.key === ' ' && !event.repeat) {
        event.preventDefault()
        if (keyboard) controller.finish(); else controller.begin(port)
      } else if (keyboard && event.key === 'Enter') {
        event.preventDefault(); controller?.finish()
      } else if (keyboard && (event.key === 'ArrowLeft' || event.key === 'ArrowRight')) {
        event.preventDefault(); controller?.step(event.key === 'ArrowLeft' ? -1 : 1)
      } else if (keyboard && event.key === 'Tab') controller?.cancel()
      else onKeyDown?.(event)
    }}>{children}</motion.button>
}

export interface PortPreview { port: string; model?: string; lines: string[] }

export function DockPreview({ preview, floating = false }: { preview: PortPreview; floating?: boolean }) {
  const reduced = useReducedMotion()
  return <motion.div className={`dock-preview ${floating ? 'is-floating' : 'is-docked'}`} aria-hidden
    initial={reduced ? false : { scale: floating ? 0.84 : 0.90, opacity: 0.45, y: floating ? 3 : 8 }}
    animate={{scale:1, opacity:1, y:0}} transition={reduced ? {duration:0} : {type:'spring', stiffness:370, damping:26}}>
    <div className="dock-preview-heading">{preview.model && <strong>{preview.model}</strong>}<code>{preview.port}</code></div>
    {preview.lines.length > 0 && <pre>{preview.lines.join('\n')}</pre>}
  </motion.div>
}

export function InsertionGap({ active, weight, preview }: { active: boolean; weight: number; preview?: PortPreview }) {
  return <div className="insertion-gap" data-drop-preview={active || undefined} aria-hidden style={{flexGrow: active ? weight : 0}}>
    {active && preview && <DockPreview preview={preview} />}
  </div>
}
