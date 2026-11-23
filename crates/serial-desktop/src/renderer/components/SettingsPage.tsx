import { Plus, Save, Trash2 } from 'lucide-react'
import { useEffect, useId, useMemo, useRef, useState, type ReactNode, type KeyboardEvent } from 'react'
import { BaudRateField, SerialNumberField } from './BaudRateField'
import { BounceSidebar } from './ui/bounce-sidebar'
import { ModelIdentityPicker, MoreFields, OptionField } from './SerialFields'
import { ModelPicker } from './ui/model-picker'
import { TransitionPanel } from './ui/transition-panel'
import { WorkspaceSidebar } from './WorkspaceSidebar'
import { DEFAULT_THEMES, ThemeSelector } from './ui/theme-selector'
import { palettes, paletteId, terminalFontSize, terminalFontSizes } from '../../shared/appearance'
import { useAppearanceMode } from '../lib/appearance'
import type {
  DesktopPreferences,
  ModelFamily,
  ModelProfile,
  PortDescriptor,
  SerialConfigurationDraft,
  PortSnapshot,
  ThemePreference,
  TransportProfile
} from '../../shared/contracts'

interface Props {
  configuredPorts: PortSnapshot[]
  availablePorts: PortDescriptor[]
  transportProfiles: TransportProfile[]
  modelProfiles: ModelProfile[]
  modelFamilies: ModelFamily[]
  preferences: DesktopPreferences
  serviceControls?: ReactNode
  initialPort?: string
  onSaveSerial: (draft: SerialConfigurationDraft) => Promise<boolean>
  onSaveModels: (profiles: ModelProfile[]) => Promise<boolean>
  onSaveModelFamilies: (families: ModelFamily[]) => Promise<boolean>
  onSavePreferences: (preferences: DesktopPreferences) => Promise<void>
  onOpenSetup?: () => void
}

const sections = [
  { id: 'serial', label: '串口配置' },
  { id: 'model', label: '机型 Profile' },
  { id: 'identity', label: '机型名称' },
  { id: 'application', label: '应用' }
] as const

const themes = [
  { value: 'system', label: '跟随系统', colors: { ...DEFAULT_THEMES[1].colors, sidebar: '#29414c', strong: '#94acb6', muted: '#687e87' } },
  { ...DEFAULT_THEMES[1], label: '浅色' },
  { ...DEFAULT_THEMES[0], label: '深色' }
]

// A live status refresh sends new objects even when the saved configuration is identical.
function useConfigurationChange<T>(value: T, onChange: (next: T) => void): void {
  const previous = useRef(value)
  useEffect(() => {
    const last = previous.current
    previous.current = value
    if (last !== value && JSON.stringify(last) !== JSON.stringify(value)) onChange(value)
  }, [value, onChange])
}

export function SettingsPage(props: Props): React.JSX.Element {
  const [activeIndex, setActiveIndex] = useState(0)
  const [savedSection, setSavedSection] = useState<number>()
  const content = useRef<HTMLElement>(null)
  const groupId = useId()
  const tabIds = sections.map(({ id }) => `${groupId}-${id}`)

  function selectSection(index: number): void {
    if (activeIndex === index) return
    setActiveIndex(index)
    content.current?.scrollTo({ top: 0 })
  }

  function moveTab(event: KeyboardEvent<HTMLButtonElement>, index: number): void {
    let next: number
    switch (event.key) {
      case 'ArrowUp': next = (index + sections.length - 1) % sections.length; break
      case 'ArrowDown': next = (index + 1) % sections.length; break
      case 'Home': next = 0; break
      case 'End': next = sections.length - 1; break
      default: return
    }
    event.preventDefault()
    document.getElementById(tabIds[next])?.focus()
    selectSection(next)
  }

  const saveNotice = (): void => {
    setSavedSection(activeIndex)
    setTimeout(() => setSavedSection(undefined), 1_600)
  }

  return (
    <main className="settings-page" data-saved={savedSection === activeIndex || undefined}>
      <span className="sr-only" role="status">{savedSection === activeIndex ? '已保存' : ''}</span>
      <div className="workspace-shell settings-layout">
        <WorkspaceSidebar className="settings-nav" title="配置">
          <BounceSidebar className="settings-sections" items={sections.map(section => section.label)} value={activeIndex}
            role="tablist" aria-label="设备配置分类" aria-orientation="vertical" onChange={selectSection}
            itemProps={index => ({ role: 'tab', id: tabIds[index], 'data-id': sections[index].id,
              'aria-selected': activeIndex === index, 'aria-controls': tabIds[index] + '-panel', tabIndex: activeIndex === index ? 0 : -1,
              onKeyDown: event => moveTab(event, index) })} />
          <div className="compact-section-picker"><ModelPicker label="配置分类" models={sections.map(({ id, label }) => ({id, name: label}))}
            value={sections[activeIndex].id} onChange={id => selectSection(sections.findIndex(section => section.id === id))} /></div>
        </WorkspaceSidebar>
        <section className="settings-content" ref={content}>
          <TransitionPanel activeIndex={activeIndex} tabIds={tabIds}>
            <SerialEditor {...props} onSaved={saveNotice} />
            <ModelEditor profiles={props.modelProfiles} configuredPorts={props.configuredPorts} onSave={async (profiles) => {
              if (await props.onSaveModels(profiles)) saveNotice()
            }} />
            <ModelFamilyEditor families={props.modelFamilies} configuredPorts={props.configuredPorts} onSave={async (families) => {
              if (await props.onSaveModelFamilies(families)) saveNotice()
            }} />
            <div className="editor-shell"><div className="editor-heading"><h1>应用</h1></div>
              <ApplicationSettings preferences={props.preferences} onSave={props.onSavePreferences} serviceControls={props.serviceControls} onOpenSetup={props.onOpenSetup} />
            </div>
          </TransitionPanel>
        </section>
      </div>
    </main>
  )
}

function SerialEditor({
  configuredPorts,
  availablePorts,
  transportProfiles,
  modelProfiles,
  modelFamilies,
  initialPort,
  onSaveSerial,
  onSaved
}: Props & { onSaved: () => void }): React.JSX.Element {
  const allPorts = [...new Set([...configuredPorts.map((item) => item.config.port), ...availablePorts.map((port) => port.name)])]
  const [selectedPort, setSelectedPort] = useState(initialPort ?? allPorts[0] ?? '')
  const draft = useMemo(
    () => serialDraft(selectedPort, configuredPorts, transportProfiles),
    [selectedPort, configuredPorts, transportProfiles]
  )
  const [value, setValue] = useState(draft)
  useConfigurationChange(draft, setValue)
  const updateProfile = <K extends keyof TransportProfile>(key: K, next: TransportProfile[K]): void => {
    setValue((current) => ({ ...current, transportProfile: { ...current.transportProfile, [key]: next } }))
  }

  return (
    <div className="editor-shell">
      <div className="editor-heading">
        <div><h1>串口配置</h1></div>
        <button className="primary-button" type="button" disabled={!value.port || !value.transportProfile.baud_rate || Boolean(value.modelFamily) !== Boolean(value.modelName)} onClick={async () => {
          if (await onSaveSerial(value)) onSaved()
        }}><Save size={16} /> 保存串口配置</button>
      </div>
      <div className="config-form">
        <section className="config-section">
          <h2>设备</h2>
          <div className="field-grid two">
            <OptionField label="串口" value={selectedPort} options={allPorts.map(port => ({id:port, name:port}))} onChange={setSelectedPort} />
            <div className="form-field"><span>机型</span><ModelIdentityPicker families={modelFamilies} family={value.modelFamily} name={value.modelName}
              onChange={(modelFamily, modelName) => setValue(current => ({...current, modelFamily, modelName}))} /></div>
          </div>
        </section>
        <section className="config-section">
          <h2>通信参数</h2>
          <div className="field-grid three">
            <BaudRateField value={value.transportProfile.baud_rate} onChange={rate => updateProfile('baud_rate', rate)} />
            <SerialNumberField label="数据位" value={DATA_BITS.indexOf(value.transportProfile.data_bits) + 5} choices={[5,6,7,8]}
              onChange={n => updateProfile('data_bits', DATA_BITS[n - 5])} />
            <SerialNumberField label="停止位" value={value.transportProfile.stop_bits === 'one' ? 1 : 2} choices={[1,2]}
              onChange={n => updateProfile('stop_bits', n === 1 ? 'one' : 'two')} />
          </div>
          <MoreFields title="高级通信设置">
            <div className="field-grid two">
              <OptionField label="校验位" value={value.transportProfile.parity} options={[{id:'none',name:'无'},{id:'odd',name:'奇校验'},{id:'even',name:'偶校验'}]} onChange={id => updateProfile('parity', id)} />
              <OptionField label="流控" value={value.transportProfile.flow_control} options={[{id:'none',name:'无'},{id:'software',name:'软件'},{id:'hardware',name:'硬件'}]} onChange={id => updateProfile('flow_control', id)} />
              <OptionField label="交互 Profile" value={value.modelProfile ?? ''} options={[{id:'',name:'通用'}, ...modelProfiles.map(profile => ({id:profile.name,name:profile.name}))]}
                onChange={id => setValue(current => selectModelProfile(current, id || null, modelProfiles))} />
            </div>
            <div className="inline-switches">
              {(['dtr','rts','auto_open'] as const).map(key => <label key={key}><input type="checkbox" checked={value.transportProfile[key]} onChange={event => updateProfile(key,event.target.checked)} />{key === 'auto_open' ? '自动打开' : key.toUpperCase()}</label>)}
            </div>
          </MoreFields>
        </section>
          <label className="switch-row">
            <span><strong>保存后打开串口</strong></span>
            <input type="checkbox" checked={value.enabled} onChange={(event) => setValue((current) => ({ ...current, enabled: event.target.checked }))} />
            <span className="switch-control" />
          </label>

      </div>
    </div>
  )
}

interface ModelDraft {
  key: string
  profile: ModelProfile
  persisted: boolean
}

export interface ModelProfilePolicy {
  nameReadOnly: boolean
  deleteDisabled: boolean
}

export function resolveModelProfilePolicy(persisted: boolean, affectedPorts: string[]): ModelProfilePolicy {
  return {
    nameReadOnly: persisted,
    deleteDisabled: persisted && affectedPorts.length > 0
  }
}

export function ModelEditor({ profiles, configuredPorts, onSave }: { profiles: ModelProfile[]; configuredPorts: PortSnapshot[]; onSave: (profiles: ModelProfile[]) => Promise<void> }): React.JSX.Element {
  const [items, setItems] = useState<ModelDraft[]>(() => persistedModelDrafts(profiles))
  const [selected, setSelected] = useState(0)
  const nextDraftId = useRef(1)
  useConfigurationChange(profiles, (next) => {
    setItems(persistedModelDrafts(next))
    setSelected((current) => Math.min(current, Math.max(0, next.length - 1)))
  })
  const draft = items[selected]
  const profile = draft?.profile
  const affectedPorts = profile
    ? configuredPorts.filter((item) => item.config.model_profile === profile.name).map((item) => item.config.port)
    : []
  const policy = resolveModelProfilePolicy(draft?.persisted ?? false, affectedPorts)
  const catalog = items.map((item) => item.profile)
  const validationIssue = validateModelProfileCatalog(catalog, configuredPorts)
  const update = <K extends keyof ModelProfile>(key: K, value: ModelProfile[K]): void => {
    setItems((current) => current.map((item, index) => index === selected
      ? { ...item, profile: { ...item.profile, [key]: value } }
      : item))
  }

  return (
    <div className="editor-shell">
      <div className="editor-heading">
        <div><h1>机型 Profile</h1></div>
        <button className="primary-button" title={validationIssue} type="button" disabled={Boolean(validationIssue)} onClick={() => onSave(catalog)}><Save size={16} /> 保存机型配置</button>
      </div>
      <div className="config-form">
        <div className="catalog-pick">
          <OptionField label="编辑 Profile" value={draft?.key ?? ''} options={items.map(item => ({id:item.key,name:item.profile.name || '未命名'}))}
            onChange={key => setSelected(items.findIndex(item => item.key === key))} />
          <button className="add-profile" type="button" onClick={() => {
            const index = items.length
            const id = nextDraftId.current++
            const name = nextModelProfileName(items)
            setItems((current) => [...current, { key: `new:${id}`, profile: defaultModelProfile(name), persisted: false }])
            setSelected(index)
          }}><Plus size={16} /> 新建机型 Profile</button>
        </div>
        {profile ? (
          <div className="config-section">
            <div className="profile-form-header"><div className="form-section-title"><div><strong>Profile 标识</strong></div></div><button
              aria-label={`删除机型 Profile ${profile.name}`}
              className="danger-icon"
              disabled={policy.deleteDisabled}
              type="button"
              title={policy.deleteDisabled ? '请先在串口配置中改绑或解绑' : '删除 Profile'}
              onClick={() => { setItems((current) => current.filter((_, index) => index !== selected)); setSelected((current) => Math.max(0, current - 1)) }}
            ><Trash2 size={16} /></button></div>
            <Field label="Profile 名称"><input readOnly={policy.nameReadOnly} title={policy.nameReadOnly ? '已有 Profile 名称是稳定标识' : undefined} value={profile.name} onChange={(event) => update('name', event.target.value)} placeholder="例如 OpenWrt Shell" /></Field>
            {validationIssue && <div className="validation-notice" role="alert">{validationIssue}</div>}
            <div className="form-divider" />
            <div className="form-section-title"><div><strong>命令提示符</strong></div></div>
            <div className="field-grid two">
              <Field label="Shell 提示符"><input value={profile.shell_prompt ?? ''} onChange={(event) => update('shell_prompt', event.target.value || null)} placeholder="root@router:~# " /></Field>
              <Field label="U-Boot 提示符"><input value={profile.uboot_prompt ?? ''} onChange={(event) => update('uboot_prompt', event.target.value || null)} placeholder="=> " /></Field>
            </div>
            <div className="form-divider" />
            <MoreFields title="写入与回显">
            <div className="field-grid three">
              <OptionField label="发送换行" value={profile.write_eol ?? '\r'} options={[{id:'\r',name:'CR'},{id:'\n',name:'LF'},{id:'\r\n',name:'CRLF'}]} onChange={value => update('write_eol',value)} />
              <OptionField label="设备回显" value={profile.echo ?? 'auto'} options={[{id:'auto',name:'自动识别'},{id:'on',name:'开启'},{id:'off',name:'关闭'}]} onChange={value => update('echo',value)} />
              <Field label="每次写入字节"><input inputMode="numeric" value={profile.write_chunk_size ?? 1} onChange={(event) => update('write_chunk_size', Number(event.target.value))} /></Field>
              <Field label="写入间隔 (ms)"><input inputMode="numeric" value={profile.write_chunk_delay_ms ?? 0} onChange={(event) => update('write_chunk_delay_ms', Number(event.target.value))} /></Field>
            </div>
            </MoreFields>
          </div>
        ) : <div className="empty-profile"><strong>创建第一个机型 Profile</strong><span>编辑提示符和回显设置</span></div>}
      </div>
    </div>
  )
}

interface ModelFamilyDraft {
  key: string
  family: ModelFamily
  persisted: boolean
}

export function ModelFamilyEditor({ families, configuredPorts, onSave }: { families: ModelFamily[]; configuredPorts: PortSnapshot[]; onSave: (families: ModelFamily[]) => Promise<void> }): React.JSX.Element {
  const [items, setItems] = useState<ModelFamilyDraft[]>(() => persistedFamilyDrafts(families))
  const [selected, setSelected] = useState(0)
  const nextDraftId = useRef(1)
  useConfigurationChange(families, (next) => {
    setItems(persistedFamilyDrafts(next))
    setSelected((current) => Math.min(current, Math.max(0, next.length - 1)))
  })
  const draft = items[selected]
  const family = draft?.family
  const affectedPorts = family
    ? configuredPorts.filter((item) => item.config.model_family === family.name).map((item) => item.config.port)
    : []
  const policy = resolveModelProfilePolicy(draft?.persisted ?? false, affectedPorts)
  const catalog = items.map((item) => item.family)
  const validationIssue = validateModelFamilyCatalog(catalog, configuredPorts)
  const update = <K extends keyof ModelFamily>(key: K, value: ModelFamily[K]): void => {
    setItems((current) => current.map((item, index) => index === selected
      ? { ...item, family: { ...item.family, [key]: value } }
      : item))
  }

  return (
    <div className="editor-shell">
      <div className="editor-heading">
        <div><h1>机型名称</h1></div>
        <button className="primary-button" title={validationIssue} type="button" disabled={Boolean(validationIssue)} onClick={() => onSave(catalog)}><Save size={16} /> 保存机型名称</button>
      </div>
      <div className="config-form">
        <div className="catalog-pick">
          <OptionField label="机型系列" value={draft?.key ?? ''} options={items.map(item => ({id:item.key,name:item.family.name || '未命名'}))}
            onChange={key => setSelected(items.findIndex(item => item.key === key))} />
          <button className="add-profile" type="button" onClick={() => {
            const index = items.length
            const id = nextDraftId.current++
            const name = nextFamilyName(items)
            setItems((current) => [...current, { key: `new:${id}`, family: { name, model_names: [] }, persisted: false }])
            setSelected(index)
          }}><Plus size={16} /> 新建系列</button>
        </div>
        {family ? (
          <div className="config-section">
            <div className="profile-form-header"><div className="form-section-title"><strong>系列</strong></div><button
              aria-label={`删除机型系列 ${family.name}`}
              className="danger-icon"
              disabled={policy.deleteDisabled}
              type="button"
              title={policy.deleteDisabled ? '请先在串口配置中改绑机型' : '删除机型系列'}
              onClick={() => { setItems((current) => current.filter((_, index) => index !== selected)); setSelected((current) => Math.max(0, current - 1)) }}
            ><Trash2 size={16} /></button></div>
            <Field label="系列名称"><input readOnly={policy.nameReadOnly} value={family.name} onChange={(event) => update('name', event.target.value)} placeholder="例如 TL-AS7230" /></Field>
            <div className="model-name-editor">
              <div className="model-name-heading">
                <div><strong>{family.name ? family.name + ' 的具体型号' : '具体型号'}</strong></div>
                <button type="button" onClick={() => update('model_names', [...family.model_names, nextConcreteModelName(family.model_names)])}><Plus size={14} /> 添加具体型号</button>
              </div>
              {family.model_names.map((name, index) => {
                const boundPorts = configuredPorts.filter((item) => item.config.model_family === family.name && item.config.model_name === name).map((item) => item.config.port)
                const bound = boundPorts.length > 0
                return (
                  <div className="model-name-row" key={index}>
                    <input aria-label={`具体型号 ${index + 1}`} readOnly={bound} value={name} onChange={(event) => update('model_names', family.model_names.map((value, itemIndex) => itemIndex === index ? event.target.value : value))} />
                    <button aria-label={`删除具体型号 ${name}`} disabled={bound} type="button" onClick={() => update('model_names', family.model_names.filter((_, itemIndex) => itemIndex !== index))}><Trash2 size={14} /></button>
                  </div>
                )
              })}
              {family.model_names.length === 0 && <div className="empty-model-names">尚无具体型号</div>}
            </div>
            {validationIssue && <div className="validation-notice" role="alert">{validationIssue}</div>}
          </div>
        ) : <div className="empty-profile"><strong>创建第一个系列</strong><span>添加系列中的具体型号</span></div>}
      </div>
    </div>
  )
}

function ApplicationSettings({ preferences, onSave, serviceControls, onOpenSetup }: Pick<Props, 'preferences' | 'serviceControls' | 'onOpenSetup'> & { onSave: (preferences: DesktopPreferences) => Promise<void> }): React.JSX.Element {
  const [value, setValue] = useState(preferences)
  useConfigurationChange(preferences, setValue)
  const mode = useAppearanceMode(value.theme)
  const colors = Object.entries(palettes).map(([id, palette]) => {
    const swatch = palette[mode]
    return { value: id, label: palette.name, colors: { sidebar:swatch.rail, panel:swatch.terminal, border:swatch.accent,
      strong:swatch.accent, muted:`color-mix(in srgb, ${swatch.accent} 32%, ${swatch.panel})`, tile:swatch.panel } }
  })
  return (
    <div className="app-settings">
      <div className="appearance-setting"><h2>外观</h2>
        <ThemeSelector compact aria-label="明暗模式" themes={themes} value={value.theme} onChange={(theme) => {
          const next = { ...value, theme: theme as ThemePreference }
          setValue(next)
          void onSave(next)
        }} />
        <ThemeSelector aria-label="主题配色" themes={colors} value={paletteId(value.palette)} onChange={id => {
          const next = { ...value, palette:paletteId(id) }
          setValue(next)
          void onSave(next)
        }} />
        <SerialNumberField label="终端字号（px）" choices={terminalFontSizes} value={terminalFontSize(value.terminalFontSize)} onChange={size => {
          const next = { ...value, terminalFontSize: size }
          setValue(next)
          void onSave(next)
        }} />
      </div>
      <h2>后端连接</h2>
      {serviceControls}
      <label><span>后端地址</span><input value={value.endpoint} onChange={(event) => setValue((current) => ({ ...current, endpoint: event.target.value }))} onBlur={() => { if (value.endpoint !== preferences.endpoint) void onSave(value) }} /></label>
      <label className="auto-start-setting">
        <span><strong>自动启动本地后端</strong><small>连接不到配置地址时由 App 启动</small></span>
        <input
          aria-label="自动启动本地后端"
          checked={value.autoStartLocal}
          onChange={(event) => {
            const next = { ...value, autoStartLocal: event.target.checked }
            setValue(next)
            void onSave(next)
          }}
          type="checkbox"
        />
        <span className="switch-control" />
      </label>
      {onOpenSetup && <button className="setup-reopen" type="button" onClick={onOpenSetup}>打开首次配置向导</button>}

    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }): React.JSX.Element {
  return <label className="form-field"><span>{label}</span>{children}</label>
}

function serialDraft(port: string, configuredPorts: PortSnapshot[], profiles: TransportProfile[]): SerialConfigurationDraft {
  const configured = configuredPorts.find((item) => item.config.port === port)
  const profile = profiles.find((item) => item.name === configured?.config.transport_profile) ?? profiles[0] ?? defaultTransport()
  return {
    port,
    enabled: configured?.config.enabled ?? true,
    modelProfile: configured?.config.model_profile,
    modelFamily: configured?.config.model_family,
    modelName: configured?.config.model_name,
    transportProfile: { ...profile }
  }
}

function defaultTransport(): TransportProfile {
  return { name: '115200-8N1', baud_rate: 115200, data_bits: 'eight', parity: 'none', stop_bits: 'one', flow_control: 'none', dtr: false, rts: false, auto_open: true }
}

function persistedModelDrafts(profiles: ModelProfile[]): ModelDraft[] {
  return profiles.map((profile) => ({
    key: `persisted:${profile.name}`,
    profile: { ...profile },
    persisted: true
  }))
}

function nextModelProfileName(items: ModelDraft[]): string {
  const names = new Set(items.map((item) => item.profile.name))
  let suffix = 1
  while (names.has(`新 Profile ${suffix}`)) suffix += 1
  return `新 Profile ${suffix}`
}

function defaultModelProfile(name: string): ModelProfile {
  return { name, shell_prompt: null, uboot_prompt: '=> ', write_eol: '\r', echo: 'auto', write_chunk_size: 1, write_chunk_delay_ms: 0 }
}

export function selectModelProfile(
  draft: SerialConfigurationDraft,
  modelProfile: string | null,
  profiles: ModelProfile[]
): SerialConfigurationDraft {
  if (!modelProfile) return { ...draft, modelProfile: null }
  if (!profiles.some((profile) => profile.name === modelProfile)) return draft
  return { ...draft, modelProfile }
}

export function selectModelFamily(
  draft: SerialConfigurationDraft,
  modelFamily: string | null,
  families: ModelFamily[]
): SerialConfigurationDraft {
  if (!modelFamily) return { ...draft, modelFamily: null, modelName: null }
  const selected = families.find((family) => family.name === modelFamily)
  if (!selected) return draft
  const modelName = draft.modelFamily === modelFamily && draft.modelName && selected.model_names.includes(draft.modelName)
    ? draft.modelName
    : null
  return { ...draft, modelFamily, modelName }
}

export function validateModelProfileCatalog(profiles: ModelProfile[], configuredPorts: PortSnapshot[]): string | undefined {
  const profileNames = new Set<string>()
  for (const profile of profiles) {
    if (!profile.name.trim()) return 'Profile 名称不能为空。'
    if (profile.name !== profile.name.trim()) return `Profile “${profile.name}” 名称首尾不能有空格。`
    if (profileNames.has(profile.name)) return `Profile 名称“${profile.name}”重复。`
    profileNames.add(profile.name)
  }
  for (const item of configuredPorts) if (item.config.model_profile && !profileNames.has(item.config.model_profile)) return `端口 ${item.config.port} 正在使用 Profile“${item.config.model_profile}”；请先改绑。`
  return undefined
}

export function validateModelFamilyCatalog(families: ModelFamily[], configuredPorts: PortSnapshot[]): string | undefined {
  const familyNames = new Set<string>()
  for (const family of families) {
    if (!family.name.trim()) return '机型系列名称不能为空。'
    if (family.name !== family.name.trim()) return `机型系列“${family.name}”首尾不能有空格。`
    if (familyNames.has(family.name)) return `机型系列名称“${family.name}”重复。`
    familyNames.add(family.name)
    const modelNames = new Set<string>()
    for (const name of family.model_names) {
      if (!name.trim()) return `机型系列“${family.name}”包含空的具体型号。`
      if (name !== name.trim()) return `具体型号“${name}”首尾不能有空格。`
      if (modelNames.has(name)) return `机型系列“${family.name}”中的具体型号“${name}”重复。`
      modelNames.add(name)
    }
  }
  for (const item of configuredPorts) {
    const familyName = item.config.model_family
    const modelName = item.config.model_name
    if (!familyName && !modelName) continue
    const family = families.find((candidate) => candidate.name === familyName)
    if (!family || !modelName || !family.model_names.includes(modelName)) return `端口 ${item.config.port} 正在使用机型“${modelName ?? familyName}”；请先在串口配置中改绑。`
  }
  return undefined
}

function persistedFamilyDrafts(families: ModelFamily[]): ModelFamilyDraft[] {
  return families.map((family) => ({
    key: `persisted:${family.name}`,
    family: { ...family, model_names: [...family.model_names] },
    persisted: true
  }))
}

function nextFamilyName(items: ModelFamilyDraft[]): string {
  const names = new Set(items.map((item) => item.family.name))
  let suffix = 1
  while (names.has(`新系列 ${suffix}`)) suffix += 1
  return `新系列 ${suffix}`
}

function nextConcreteModelName(names: string[]): string {
  const existing = new Set(names)
  let suffix = 1
  while (existing.has(`新型号 ${suffix}`)) suffix += 1
  return `新型号 ${suffix}`
}

const DATA_BITS = ['five','six','seven','eight'] as const
