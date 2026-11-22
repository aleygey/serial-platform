import { ChevronDown } from 'lucide-react'
import { useId, type ReactNode } from 'react'
import { useReducedMotion } from '../lib/use-reduced-motion'
import type { ModelFamily } from '../../shared/contracts'
import { Disclosure, DisclosureContent, DisclosureTrigger } from './ui/disclosure'
import { ModelPicker } from './ui/model-picker'

export function MoreFields({ title, children }: { title: string; children: ReactNode }) {
  const id = useId()
  const reduced = useReducedMotion()
  return <Disclosure className="more-fields" transition={{ duration: reduced ? 0 : 0.2 }}>
    <DisclosureTrigger className="more-trigger" aria-controls={id}>{title}<ChevronDown size={15} aria-hidden /></DisclosureTrigger>
    <DisclosureContent id={id}><div className="more-body">{children}</div></DisclosureContent>
  </Disclosure>
}

export function OptionField<T extends string>({ label, value, options, onChange, searchable }: {
  label: string; value: T; options: { id: T; name: string }[]; onChange: (value: T) => void
  searchable?: boolean
}) {
  return <div className="form-field kit-field"><span>{label}</span>
    <ModelPicker label={label} models={options.length ? options : [{ id: '', name: '暂无选项' }]} value={value}
      disabled={!options.length} searchable={searchable ?? options.length > 7}
      onChange={id => { const option = options.find(item => item.id === id); if (option) onChange(option.id) }} />
  </div>
}

export function ModelIdentityPicker({ families, family, name, onChange, label = '机型', disabled }: {
  families: ModelFamily[]; family?: string | null; name?: string | null; label?: string; disabled?: boolean
  onChange: (family: string | null, name: string | null) => void
}) {
  const models = families.flatMap(group => group.model_names.map(model => ({
    id: JSON.stringify([group.name, model]), name: model, provider: group.name, family: group.name,
  })))
  const value = name ? JSON.stringify([family ?? null, name]) : ''
  if (name && !models.some(model => model.id === value)) models.unshift({ id: value, name, provider: family ?? '', family: family ?? '' })
  return <ModelPicker label={label} value={value} disabled={disabled} searchable searchPlaceholder="搜索机型…" models={[...models, { id: '', name: '未指定机型' }]}
    onChange={id => { const model = models.find(item => item.id === id); onChange(model?.family ?? null, model?.name ?? null) }} />
}
