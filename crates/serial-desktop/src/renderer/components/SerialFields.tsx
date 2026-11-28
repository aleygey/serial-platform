import { ChevronDown } from 'lucide-react'
import { useId, useMemo, useState, type ReactNode } from 'react'
import { useReducedMotion } from '../lib/use-reduced-motion'
import type { ModelFamily } from '../../shared/contracts'
import { Disclosure, DisclosureContent, DisclosureTrigger } from './ui/disclosure'
import { ModelPicker } from './ui/model-picker'
import { tcpPortAddress } from '../lib/tcp-endpoint'

export function MoreFields({ title, summary, defaultOpen = false, children }: { title: string; summary?: string; defaultOpen?: boolean; children: ReactNode }) {
  const id = useId()
  const [open, setOpen] = useState(defaultOpen)
  const reduced = useReducedMotion()
  return <Disclosure className="more-fields" open={open} onOpenChange={setOpen} transition={{ duration: reduced ? 0 : 0.2 }}>
    <DisclosureTrigger className="more-trigger" aria-controls={id}>{title}{summary && !open && <code className="disclosure-summary">{summary}</code>}<ChevronDown size={15} aria-hidden /></DisclosureTrigger>
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

export function PortEndpointField({ ports, port, onChange, label = '串口' }: {
  ports: { id: string; name: string; provider?: string }[]; port: string; onChange: (port: string) => void; label?: string
}) {
  const [adding, setAdding] = useState(false)
  const [address, setAddress] = useState('')
  const choices = ports.map(item => ({ ...item, id: 'port:' + item.id }))
  const edit = adding || !ports.length
  const changeAddress = (text: string) => {
    setAdding(true)
    setAddress(text)
    const canonical = tcpPortAddress(text)
    const existing = canonical ? ports.find(item => tcpPortAddress(item.id) === canonical) : undefined
    onChange(existing?.id ?? canonical ?? '')
  }
  return <div className="form-field kit-field"><span>{label}</span>
    <ModelPicker label={label} value={edit ? 'tcp' : 'port:' + port}
      models={[...choices, { id: 'tcp', name: '添加 TCP 串口服务器…' }]}
      searchable={choices.length > 7} onChange={id => {
        setAdding(id === 'tcp')
        if (id === 'tcp') changeAddress(address)
        else onChange(id.slice(5))
      }} />
    {edit && <label className="form-field"><span>TCP 服务器地址</span>
      <input aria-label="TCP 服务器地址" spellCheck={false} autoCapitalize="none" value={address}
        placeholder="192.168.1.252:10001" aria-invalid={!!address && !tcpPortAddress(address)}
        onChange={event => changeAddress(event.target.value)} />
      {address && !tcpPortAddress(address) && <small>请输入 IP 和端口（1–65535），IPv6 地址用方括号括起。</small>}
    </label>}
  </div>
}

export function ModelIdentityPicker({ families, family, name, onChange, label = '机型', disabled }: {
  families: ModelFamily[]; family?: string | null; name?: string | null; label?: string; disabled?: boolean
  onChange: (family: string | null, name: string | null) => void
}) {
  const value = name ? JSON.stringify([family ?? null, name]) : ''
  const models = useMemo(() => {
    const choices = families.flatMap(group => group.model_names.map(model => ({
      id: JSON.stringify([group.name, model]), name: model, provider: group.name,
    })))
    if (name && !choices.some(model => model.id === value)) choices.unshift({ id: value, name, provider: family ?? '' })
    return [...choices, { id: '', name: '未指定机型', provider: '' }]
  }, [families, name, value, family])
  return <ModelPicker label={label} value={value} disabled={disabled} searchable browseFamilies searchPlaceholder="搜索系列或具体型号…" models={models}
    onChange={id => { const model = id ? models.find(item => item.id === id) : undefined; onChange(model?.provider || null, model?.name ?? null) }} />
}
