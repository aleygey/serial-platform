// Rare UI. Copyright (c) 2026 Swami Malode. See RARE-UI-LICENSE.txt.
"use client"

import * as React from "react"
import {
  animate,
  motion,
  useMotionValue,
  useMotionValueEvent,
  useReducedMotion,
} from "motion/react"

import { cn } from "../../lib/utils"



const RAIL_X = 5
const PLANE = "M12 2 20.5 21 12 17.5 3.5 21z"
// Cut away the underlying dots and line beneath the pointer.
const PLANE_HOLE = 4

const useIsoLayoutEffect =
  typeof window !== "undefined" ? React.useLayoutEffect : React.useEffect

type RailNode = { id: string; x: number; y: number; belowCursor: number }
type Geometry = ReturnType<typeof measureRoute>

function measureRoute(list: HTMLElement, content: HTMLElement) {
  const nodes: RailNode[] = []
  const stops: number[] = []
  const segments: string[] = []
  const top = list.getBoundingClientRect().top
  let length = 0
  let lastX = 0
  let lastY = 0
  let width = 0

  function lineTo(x: number, y: number) {
    length += Math.hypot(x - lastX, y - lastY)
    segments.push(`L${x} ${y}`)
    lastX = x
    lastY = y
  }

  for (const row of content.querySelectorAll<HTMLElement>('[data-rail-node]')) {
    if (row.closest('[inert]') || !row.getClientRects().length) continue
    const rect = row.getBoundingClientRect()
    const x = RAIL_X + Number(row.dataset.depth ?? 0) * 10
    const inset = Math.min(rect.height / 2, 20)
    const y = Math.max(lastY, rect.top - top + inset)
    if (!nodes.length) {
      segments.push(`M${x} 0`)
      lastX = x
    } else if (x !== lastX) {
      const bend = (y - lastY) * 0.3
      lineTo(lastX, lastY + bend)
      lineTo(x, y - bend)
    }
    lineTo(x, y)
    nodes.push({ id: row.dataset.railNode!, x, y, belowCursor: rect.height - inset })
    stops.push(length)
    width = Math.max(width, x + 9)
  }
  return { nodes, stops, length, d: segments.join(" "), width, height: list.offsetHeight }
}

type RailTocProps = React.ComponentProps<"nav"> & {
  selectedId?: string
  paused?: boolean
  visible?: boolean
  onCursorMove?: (list: HTMLElement, y: number, belowCursor: number) => void
}

const RailToc = ({ selectedId, paused, visible = true, onCursorMove, children, className, ...props }: RailTocProps) => {
  const reduced = useReducedMotion()
  const reduceMotion = paused || reduced
  const maskId = `rail-toc-${React.useId().replace(/[^\w-]/g, "")}`
  const listRef = React.useRef<HTMLDivElement>(null)
  const body = React.useRef<HTMLDivElement>(null)
  const pathRef = React.useRef<SVGPathElement>(null)
  const holeRefs = React.useRef<(SVGCircleElement | null)[]>([])
  const [geometry, setGeometry] = React.useState<Geometry>()
  const [reached, setReached] = React.useState(0)
  const destination = React.useRef<{ id: string; index: number }>(undefined)
  // Travel in event order. An expanding row changes the path under the cursor,
  // while the cursor keeps its position between the same two events.
  const progress = useMotionValue(0)
  const x = useMotionValue(0)
  const y = useMotionValue(0)
  const heading = useMotionValue(180)
  const filled = useMotionValue(0)

  const pose = React.useCallback((step: number) => {
    const path = pathRef.current
    const list = listRef.current
    if (!visible || !geometry?.length || !path || !list) return
    const { stops, length } = geometry
    const at = Math.min(Math.max(0, step), stops.length - 1)
    const from = Math.floor(at)
    const to = Math.min(from + 1, stops.length - 1)
    const distance = stops[from] + (stops[to] - stops[from]) * (at - from)
    const point = path.getPointAtLength(distance)
    const behind = path.getPointAtLength(Math.max(0, distance - 1))
    const ahead = path.getPointAtLength(Math.min(length, distance + 1))
    x.set(point.x)
    y.set(point.y)
    heading.set((Math.atan2(ahead.y - behind.y, ahead.x - behind.x) * 180) / Math.PI + 90)
    filled.set(distance / length)
    setReached(Math.floor(at + 0.001) + 1)
    for (const hole of holeRefs.current) {
      hole?.setAttribute("cx", String(point.x))
      hole?.setAttribute("cy", String(point.y))
    }
    const fromInset = geometry.nodes[from].belowCursor
    const toInset = geometry.nodes[to].belowCursor
    onCursorMove?.(list, point.y, fromInset + (toInset - fromInset) * (at - from))
  }, [geometry, visible, x, y, heading, filled, onCursorMove])

  useMotionValueEvent(progress, "change", pose)

  useIsoLayoutEffect(() => {
    const list = listRef.current
    const content = body.current
    if (!visible || !list || !content) return
    let frame = 0
    const measure = () => {
      frame = 0
      const next = measureRoute(list, content)
      if (!next.nodes.length) { setGeometry(undefined); return }
      setGeometry(old => old?.d === next.d && old.height === next.height &&
        next.nodes.length === old.nodes.length &&
        next.nodes.every((node, i) => old.nodes[i].id === node.id && old.nodes[i].belowCursor === node.belowCursor) ? old : next)
    }
    const schedule = () => { if (!frame) frame = requestAnimationFrame(measure) }
    measure()
    const resize = new ResizeObserver(schedule)
    resize.observe(content)
    const observer = new MutationObserver(schedule)
    observer.observe(content, { childList: true, subtree: true, attributes: true, attributeFilter: ['inert', 'data-rail-node', 'data-depth'] })
    return () => {
      resize.disconnect()
      observer.disconnect()
      cancelAnimationFrame(frame)
      progress.stop()
      destination.current = undefined
    }
  }, [visible, progress])

  useIsoLayoutEffect(() => {
    if (!visible || !geometry) return
    const index = selectedId ? geometry.nodes.findIndex(node => node.id === selectedId) : 0
    // The event may arrive one frame before its row has been measured.
    if (index === -1) return
    const previous = destination.current
    const id = geometry.nodes[index].id
    if (!previous || reduceMotion || index < previous.index || geometry.nodes[previous.index]?.id !== previous.id) {
      progress.jump(index)
    } else if (previous.id !== id) {
      animate(progress, index, { duration: 0.34, ease: 'easeOut' })
    }
    destination.current = { id, index }
    pose(progress.get())
  }, [geometry, visible, selectedId, reduceMotion, progress, pose])

  return (
    <nav data-slot="rail-toc" data-active-node={selectedId} className={cn("run-outline", className)} {...props}>
      <div ref={listRef} data-slot="rail-toc-list" className="relative">
        {geometry && (
          <svg
            className="pointer-events-none absolute left-0 top-0 overflow-visible"
            width={geometry.width}
            height={geometry.height}
            aria-hidden
          >
            <linearGradient
              id={`${maskId}-fade`}
              gradientUnits="userSpaceOnUse"
              x1={0}
              y1={0}
              x2={0}
              y2={geometry.nodes[0].y - 4}
            >
              <stop offset="0" stopColor="black" />
              <stop offset="1" stopColor="white" />
            </linearGradient>
            <mask
              id={maskId}
              maskUnits="userSpaceOnUse"
              x={-8}
              y={-8}
              width={geometry.width + 16}
              height={geometry.height + 16}
            >
              <rect
                x={-8}
                y={-8}
                width={geometry.width + 16}
                height={geometry.height + 16}
                fill={`url(#${maskId}-fade)`}
              />
              {geometry.nodes.map((node, i) => (
                <circle key={i} cx={node.x} cy={node.y} r={4} fill="black" />
              ))}
              <circle
                ref={(el) => {
                  holeRefs.current[0] = el
                }}
                r={PLANE_HOLE}
                fill="black"
              />
            </mask>
            <mask
              id={`${maskId}-nodes`}
              maskUnits="userSpaceOnUse"
              x={-8}
              y={-8}
              width={geometry.width + 16}
              height={geometry.height + 16}
            >
              <rect
                x={-8}
                y={-8}
                width={geometry.width + 16}
                height={geometry.height + 16}
                fill="white"
              />
              <circle
                ref={(el) => {
                  holeRefs.current[1] = el
                }}
                r={PLANE_HOLE}
                fill="black"
              />
            </mask>
            <g mask={`url(#${maskId})`}>
              <path
                ref={pathRef}
                d={geometry.d}
                fill="none"
                strokeWidth="1.25"
                strokeDasharray="2 5"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="stroke-foreground/25"
              />
              <motion.path
                d={geometry.d}
                fill="none"
                strokeWidth="1.25"
                stroke="var(--rail)"
                strokeLinejoin="round"
                style={{ pathLength: filled }}
              />
            </g>
            <g mask={`url(#${maskId}-nodes)`}>
              {geometry.nodes.map((node, i) => {
                const covered = i < reached
                return (
                  <circle
                    key={i}
                    cx={node.x}
                    cy={node.y}
                    r={covered ? 3 : 3.25}
                    strokeWidth="1.25"
                    className={cn(
                      "transition-[fill,stroke] duration-200",
                      covered
                        ? "fill-[var(--rail)] stroke-[var(--rail)]"
                        : "fill-transparent stroke-foreground/30",
                    )}
                  />
                )
              })}
            </g>
          </svg>
        )}

        {geometry && (
          <motion.div
            data-slot="rail-toc-plane"
            className="pointer-events-none absolute left-0 top-0 -ml-2 -mt-2 h-4 w-4 text-foreground [filter:drop-shadow(0_0_2px_rgb(0_0_0/0.35))]"
            style={{ x, y, rotate: heading }}
            aria-hidden
          >
            <svg viewBox="0 0 24 24" className="h-4 w-4 overflow-visible">
              <path
                d={PLANE}
                fill="currentColor"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinejoin="round"
              />
            </svg>
          </motion.div>
        )}

        <div ref={body}>{children}</div>
      </div>
    </nav>
  )
}

export { RailToc }
