// Rare UI. Copyright (c) 2026 Swami Malode. See RARE-UI-LICENSE.txt.
import { Check, ChevronDown, ChevronLeft, ChevronRight, Search } from 'lucide-react'
import React, { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { cn } from '../../lib/utils'

export type PickerModel = { id: string; name: string; provider?: string; series?: boolean }

export type ModelPickerProps = Omit<React.ComponentProps<'div'>, 'onChange' | 'defaultValue' | 'children'> & {
    models: PickerModel[]
    value?: string
    defaultValue?: string
    onChange?: (id: string) => void
    label: string
    browseFamilies?: boolean
    searchable?: boolean
    searchPlaceholder?: string
    onSearchChange?: (query: string) => void
    disabled?: boolean
    compact?: boolean
    renderValue?: (model: PickerModel) => React.ReactNode
}

type Placement = { left: number; top?: number; bottom?: number; width: number; maxHeight: number }

export function ModelPicker({
    models, value, defaultValue, onChange, label, disabled, compact, renderValue, browseFamilies = false,
    searchable = false, searchPlaceholder = '搜索…', onSearchChange, className, ...props
}: ModelPickerProps) {
    const id = useId()
    const listId = id + '-list'
    const [inner, setInner] = useState(defaultValue ?? models[0]?.id)
    const selected = models.find(model => model.id === (value ?? inner)) ?? models[0]
    const [placement, setPlacement] = useState<Placement>()
    const [family, setFamily] = useState<string>()
    const [query, setQuery] = useState('')
    const [active, setActive] = useState(0)
    const trigger = useRef<HTMLButtonElement>(null)
    const panel = useRef<HTMLDivElement>(null)
    const list = useRef<HTMLDivElement>(null)
    const input = useRef<HTMLInputElement>(null)
    const open = !!placement && !disabled
    const choices = useMemo((): PickerModel[] => {
        if (!open) return []
        const q = query.trim().toLowerCase()
        if (q) return models.filter(model => (model.name + ' ' + (model.provider ?? '')).toLowerCase().includes(q))
        if (!browseFamilies) return models
        if (family) return models.filter(model => model.provider === family)
        const providers = new Set(models.map(model => model.provider).filter((name): name is string => !!name))
        return [...Array.from(providers, name => ({ id: 'series:' + name, name, series: true })), ...models.filter(model => !model.provider)]
    }, [open, query, models, browseFamilies, family])

    function show() {
        if (disabled || !trigger.current || !selected) return
        const rect = trigger.current.getBoundingClientRect()
        const anchor = compact ? trigger.current.closest('[data-serial-number]')?.getBoundingClientRect() ?? rect : rect
        const width = Math.min(window.innerWidth - 16, 390, Math.max(anchor.width, browseFamilies ? 300 : searchable ? 258 : 112))
        const below = window.innerHeight - rect.bottom - 12
        const above = rect.top - 12
        const down = below >= 320 || below >= above
        setPlacement({
            left: Math.max(8, Math.min(anchor.left, window.innerWidth - width - 8)),
            ...(down ? { top: rect.bottom + 4 } : { bottom: window.innerHeight - rect.top + 4 }),
            width, maxHeight: Math.max(36, Math.min(320, (down ? below : above) - (searchable ? 40 : 0) - (browseFamilies ? 38 : 0) - 2)),
        })
        setQuery('')
        onSearchChange?.('')
        setFamily(browseFamilies ? selected.provider : undefined)
        const siblings = browseFamilies ? models.filter(model => model.provider === selected.provider) : models
        setActive(browseFamilies && !selected.provider ? 0 : Math.max(0, siblings.indexOf(selected)))
    }
    function close(refocus = true) {
        setPlacement(undefined)
        if (refocus) trigger.current?.focus({ preventScroll: true })
    }
    function browse(next?: string) {
        setFamily(next)
        setActive(0)
        if (list.current) list.current.scrollTop = 0
        input.current?.focus({ preventScroll: true })
    }
    function choose(model: PickerModel) {
        if (model.series) return browse(model.name)
        close()
        if (value === undefined) setInner(model.id)
        if (model.id !== selected?.id) onChange?.(model.id)
    }
    useLayoutEffect(() => {
        if (!open) return
        ;(input.current ?? list.current)?.focus({ preventScroll: true })
        list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' })
    }, [open])
    useEffect(() => {
        if (!open) return
        const dismiss = () => setPlacement(undefined)
        const outside = (event: PointerEvent) => {
            const node = event.target as Node
            if (!trigger.current?.contains(node) && !panel.current?.contains(node)) dismiss()
        }
        const onScroll = (event: Event) => {
            if (trigger.current && event.target instanceof Node && event.target.contains(trigger.current)) dismiss()
        }
        document.addEventListener('pointerdown', outside)
        window.addEventListener('scroll', onScroll, true)
        window.addEventListener('resize', dismiss)
        return () => {
            document.removeEventListener('pointerdown', outside)
            window.removeEventListener('scroll', onScroll, true)
            window.removeEventListener('resize', dismiss)
        }
    }, [open])

    function onKey(event: React.KeyboardEvent) {
        if (event.nativeEvent.isComposing) return
        const count = choices.length
        let next = active
        switch (event.key) {
            case 'ArrowDown': next = (active + 1) % count; break
            case 'ArrowUp': next = (active - 1 + count) % count; break
            case 'Home': next = 0; break
            case 'End': next = count - 1; break
            case 'ArrowLeft':
                if (!browseFamilies || !family || query) return
                browse(); break
            case 'Enter':
                if (event.target instanceof HTMLButtonElement) return
                if (choices[active]) choose(choices[active])
                break
            case 'Escape': close(); break
            case 'Tab': close(); return
            default: return
        }
        event.preventDefault()
        event.stopPropagation()
        if (count && next !== active) {
            setActive(next)
            document.getElementById(id + '-option-' + next)?.scrollIntoView({ block: 'nearest' })
        }
    }

    if (!selected) return null
    const activeId = choices[active] ? id + '-option-' + active : undefined
    return <div data-slot="model-picker" data-compact={compact || undefined} className={cn('relative inline-flex', className)} {...props}>
        <button ref={trigger} type="button" aria-haspopup="listbox" aria-expanded={open} aria-controls={open ? listId : undefined}
            aria-label={label} aria-describedby={id + '-value'} title={label + (selected.name ? ' · ' + selected.name : '')}
            disabled={disabled} onClick={() => open ? close() : show()}
            onKeyDown={event => { if (event.key === 'ArrowDown' || event.key === 'ArrowUp') { event.preventDefault(); show() } }}>
            <span className="picker-value">{compact ? null : renderValue ? renderValue(selected) : selected.name}</span>
            <span aria-hidden><ChevronDown size={14} /></span>
        </button>
        <span className="sr-only" id={id + '-value'}>{selected.name}</span>
        {open && createPortal(<div className="picker-anchor" style={{ left: placement.left, top: placement.top, bottom: placement.bottom, width: placement.width }}>
            <div ref={panel} data-slot="model-picker-panel" onKeyDown={onKey}>
                {searchable && <div data-slot="picker-search" className="flex h-10 items-center gap-2">
                    <Search aria-hidden size={14}/><input ref={input} role="combobox" aria-label={'搜索' + label} aria-controls={listId}
                        aria-expanded={open} aria-activedescendant={activeId} placeholder={searchPlaceholder} value={query}
                        onChange={event => { setQuery(event.target.value); setActive(0); onSearchChange?.(event.target.value) }}
                        className="h-full min-w-0 flex-1 bg-transparent outline-none" /></div>}
                {browseFamilies && <div className="picker-level">{query.trim() ? <span>匹配的具体型号</span> : family ? <>
                    <button type="button" aria-label="全部机型系列" onClick={() => browse()}><ChevronLeft size={15}/>系列</button>
                    <strong>{family}</strong><span>具体型号</span></> : <strong>机型系列</strong>}</div>}
                <div ref={list} role="listbox" id={listId} aria-label={label} tabIndex={searchable ? undefined : -1}
                    aria-activedescendant={searchable ? undefined : activeId} className="overflow-y-auto p-1.5 outline-none" style={{ maxHeight: placement.maxHeight }}>
                    {choices.map((model, index) => <React.Fragment key={model.id}>
                        {model.provider && (!browseFamilies || !!query.trim()) && choices[index - 1]?.provider !== model.provider &&
                            <div className="picker-group-label">{browseFamilies && '系列 · '}{model.provider}</div>}
                        <div role="option" id={id + '-option-' + index} aria-selected={model.id === selected.id} data-active={active === index || undefined}
                            data-series={model.series || undefined} data-id={model.id} onPointerMove={() => setActive(index)}
                            onMouseDown={event => event.preventDefault()} onClick={() => choose(model)}
                            className="flex h-9 cursor-pointer items-center gap-2.5 rounded-[10px] px-2.5 text-[0.8125rem]">
                            <span className="min-w-0 flex-1 truncate">{model.name}</span>
                            {model.series && <ChevronRight aria-hidden size={14}/>}
                            {model.id === selected.id && <Check aria-hidden size={14}/>}
                        </div></React.Fragment>)}
                    {!choices.length && <div className="px-2.5 py-4 text-center">没有匹配项</div>}
                </div>
            </div>
        </div>, trigger.current?.closest('.app-frame')?.querySelector('.picker-layer') ?? document.body)}
    </div>
}
export default ModelPicker
