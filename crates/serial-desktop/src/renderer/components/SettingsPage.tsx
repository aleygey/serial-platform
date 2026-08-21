import { ArrowLeft, Cable, Check, Cpu, Moon, Plus, Save, Sun, Trash2 } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import type {
  DesktopPreferences,
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
  preferences: DesktopPreferences
  initialPort?: string
  onBack: () => void
  onSaveSerial: (draft: SerialConfigurationDraft) => Promise<void>
  onSaveModels: (profiles: ModelProfile[]) => Promise<void>
  onSavePreferences: (preferences: DesktopPreferences) => Promise<void>
}

type SettingsSection = 'serial' | 'model'

export function SettingsPage(props: Props): React.JSX.Element {
  const [section, setSection] = useState<SettingsSection>('serial')
  const [saved, setSaved] = useState(false)

  const saveNotice = (): void => {
    setSaved(true)
    setTimeout(() => setSaved(false), 1_600)
  }

  return (
    <main className="settings-page">
      <header className="settings-topbar">
        <button className="icon-button" type="button" onClick={props.onBack}><ArrowLeft size={18} /></button>
        <div>
          <strong>设备配置</strong>
          <small>串口连接与机型行为分别管理</small>
        </div>
        {saved && <span className="saved-badge"><Check size={14} /> 已保存</span>}
      </header>
      <div className="settings-layout">
        <nav className="settings-nav">
          <button className={section === 'serial' ? 'is-active' : ''} onClick={() => setSection('serial')} type="button">
            <span className="settings-nav-icon"><Cable size={18} /></span>
            <span><strong>串口配置</strong><small>端口与通信参数</small></span>
          </button>
          <button className={section === 'model' ? 'is-active' : ''} onClick={() => setSection('model')} type="button">
            <span className="settings-nav-icon"><Cpu size={18} /></span>
            <span><strong>机型 Profile</strong><small>提示符与交互行为</small></span>
          </button>
          <ApplicationSettings preferences={props.preferences} onSave={async (preferences) => {
            await props.onSavePreferences(preferences)
            saveNotice()
          }} />
        </nav>
        <section className="settings-content">
          {section === 'serial' ? (
            <SerialEditor {...props} onSaved={saveNotice} />
          ) : (
            <ModelEditor profiles={props.modelProfiles} configuredPorts={props.configuredPorts} onSave={async (profiles) => {
              await props.onSaveModels(profiles)
              saveNotice()
            }} />
          )}
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
  useEffect(() => setValue(draft), [draft])
  const updateProfile = <K extends keyof TransportProfile>(key: K, next: TransportProfile[K]): void => {
    setValue((current) => ({ ...current, transportProfile: { ...current.transportProfile, [key]: next } }))
  }

  return (
    <div className="editor-shell">
      <div className="editor-heading">
        <div><span className="eyebrow">SERIAL CONNECTION</span><h1>串口配置</h1><p>选择物理端口，并配置 UART 通信参数与关联机型。</p></div>
        <button className="primary-button" type="button" disabled={!value.port} onClick={async () => {
          await onSaveSerial(value)
          onSaved()
        }}><Save size={16} /> 保存串口配置</button>
      </div>
      <div className="serial-editor-grid">
        <div className="device-picker">
          {allPorts.map((port) => {
            const configured = configuredPorts.find((item) => item.config.port === port)
            const descriptor = availablePorts.find((item) => item.name === port)
            return (
              <button className={selectedPort === port ? 'is-active' : ''} key={port} onClick={() => setSelectedPort(port)} type="button">
                <span className={`status-dot ${configured?.session_state === 'online' ? 'is-open' : 'is-idle'}`} />
                <span><strong>{port}</strong><small>{descriptor?.product || configured?.config.model_profile || '可用串口'}</small></span>
              </button>
            )
          })}
          {!allPorts.length && <div className="empty-picker">没有检测到串口</div>}
        </div>
        <div className="form-card">
          <div className="form-section-title"><span>01</span><div><strong>端口绑定</strong><small>物理端口就是唯一设备位</small></div></div>
          <div className="field-grid two">
            <Field label="串口">
              <select value={value.port} onChange={(event) => { setSelectedPort(event.target.value); setValue((current) => ({ ...current, port: event.target.value })) }}>
                {allPorts.map((port) => <option key={port}>{port}</option>)}
              </select>
            </Field>
            <Field label="机型 Profile">
              <select value={value.modelProfile ?? ''} onChange={(event) => setValue((current) => ({ ...current, modelProfile: event.target.value || null }))}>
                <option value="">未关联机型</option>
                {modelProfiles.map((profile) => <option key={profile.name}>{profile.name}</option>)}
              </select>
            </Field>
          </div>
          <label className="switch-row">
            <span><strong>保存后打开串口</strong><small>后端启动时也会自动恢复连接</small></span>
            <input type="checkbox" checked={value.enabled} onChange={(event) => setValue((current) => ({ ...current, enabled: event.target.checked }))} />
            <span className="switch-control" />
          </label>
          <div className="form-divider" />
          <div className="form-section-title"><span>02</span><div><strong>UART 参数</strong><small>按端口和参数内容自动管理配置</small></div></div>
          <div className="field-grid three">
            <Field label="波特率"><input inputMode="numeric" value={value.transportProfile.baud_rate} onChange={(event) => updateProfile('baud_rate', Number(event.target.value))} /></Field>
            <Field label="数据位"><select value={value.transportProfile.data_bits} onChange={(event) => updateProfile('data_bits', event.target.value as TransportProfile['data_bits'])}>{['five', 'six', 'seven', 'eight'].map((option) => <option key={option} value={option}>{optionLabel(option)}</option>)}</select></Field>
            <Field label="校验位"><select value={value.transportProfile.parity} onChange={(event) => updateProfile('parity', event.target.value as TransportProfile['parity'])}>{['none', 'odd', 'even'].map((option) => <option key={option}>{option}</option>)}</select></Field>
            <Field label="停止位"><select value={value.transportProfile.stop_bits} onChange={(event) => updateProfile('stop_bits', event.target.value as TransportProfile['stop_bits'])}><option value="one">1</option><option value="two">2</option></select></Field>
            <Field label="流控"><select value={value.transportProfile.flow_control} onChange={(event) => updateProfile('flow_control', event.target.value as TransportProfile['flow_control'])}>{['none', 'software', 'hardware'].map((option) => <option key={option}>{option}</option>)}</select></Field>
          </div>
          <div className="inline-switches">
            {(['dtr', 'rts', 'auto_open'] as const).map((key) => (
              <label key={key}><input type="checkbox" checked={value.transportProfile[key]} onChange={(event) => updateProfile(key, event.target.checked)} /><span>{key === 'auto_open' ? '自动打开' : key.toUpperCase()}</span></label>
            ))}
          </div>
        </div>
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
  useEffect(() => { setItems(persistedModelDrafts(profiles)); setSelected((current) => Math.min(current, Math.max(0, profiles.length - 1))) }, [profiles])
  const draft = items[selected]
  const profile = draft?.profile
  const affectedPorts = profile
    ? configuredPorts.filter((item) => item.config.model_profile === profile.name).map((item) => item.config.port)
    : []
  const policy = resolveModelProfilePolicy(draft?.persisted ?? false, affectedPorts)
  const update = <K extends keyof ModelProfile>(key: K, value: ModelProfile[K]): void => {
    setItems((current) => current.map((item, index) => index === selected
      ? { ...item, profile: { ...item.profile, [key]: value } }
      : item))
  }

  return (
    <div className="editor-shell">
      <div className="editor-heading">
        <div><span className="eyebrow">DEVICE BEHAVIOR</span><h1>机型 Profile</h1><p>机型名、命令提示符和写入节奏只配置一次，可关联多个串口。</p></div>
        <button className="primary-button" type="button" disabled={!items.length || items.some((item) => !item.profile.name.trim())} onClick={() => onSave(items.map((item) => item.profile))}><Save size={16} /> 保存机型配置</button>
      </div>
      <div className="serial-editor-grid">
        <div className="device-picker profile-picker">
          {items.map((item, index) => <button className={selected === index ? 'is-active' : ''} key={item.key} onClick={() => setSelected(index)} type="button"><span className="profile-monogram">{item.profile.name.slice(0, 2).toUpperCase()}</span><span><strong>{item.profile.name || '未命名机型'}</strong><small>{item.profile.shell_prompt || '未配置 Shell 提示符'}</small></span></button>)}
          <button className="add-profile" type="button" onClick={() => {
            const index = items.length
            const id = nextDraftId.current++
            const name = nextModelName(items)
            setItems((current) => [...current, { key: `new:${id}`, profile: defaultModel(name), persisted: false }])
            setSelected(index)
          }}><Plus size={16} /> 新建机型 Profile</button>
        </div>
        {profile ? (
          <div className="form-card">
            <div className="profile-form-header"><div className="form-section-title"><span>01</span><div><strong>机型身份</strong><small>原样显示，不改写空格或大小写</small></div></div><button
              aria-label={`删除机型 Profile ${profile.name}`}
              className="danger-icon"
              disabled={policy.deleteDisabled}
              type="button"
              title={policy.deleteDisabled ? '请先在串口配置中改绑或解绑' : '删除 Profile'}
              onClick={() => { setItems((current) => current.filter((_, index) => index !== selected)); setSelected((current) => Math.max(0, current - 1)) }}
            ><Trash2 size={16} /></button></div>
            <Field label="机型名称"><input readOnly={policy.nameReadOnly} title={policy.nameReadOnly ? '已有 Profile 名称是稳定标识' : undefined} value={profile.name} onChange={(event) => update('name', event.target.value)} placeholder="例如 TL-AS7230 1.0" /></Field>
            {policy.nameReadOnly && <div className="field-note">已有 Profile 名称是稳定标识；如需使用新名称，请新建 Profile。</div>}
            {affectedPorts.length > 0 && (
              <div className="impact-notice">
                已关联端口：<strong>{affectedPorts.join('、')}</strong>。如需删除，请先在“串口配置”改绑或解绑。
              </div>
            )}
            <div className="form-divider" />
            <div className="form-section-title"><span>02</span><div><strong>命令提示符</strong><small>用于识别命令输出的完整边界</small></div></div>
            <div className="field-grid two">
              <Field label="Shell 提示符"><input value={profile.shell_prompt ?? ''} onChange={(event) => update('shell_prompt', event.target.value || null)} placeholder="root@router:~# " /></Field>
              <Field label="U-Boot 提示符"><input value={profile.uboot_prompt ?? ''} onChange={(event) => update('uboot_prompt', event.target.value || null)} placeholder="=> " /></Field>
            </div>
            <div className="form-divider" />
            <div className="form-section-title"><span>03</span><div><strong>交互行为</strong><small>换行、设备回显与慢速写入</small></div></div>
            <div className="field-grid three">
              <Field label="发送换行"><select value={profile.write_eol ?? '\r'} onChange={(event) => update('write_eol', event.target.value)}><option value="\r">CR</option><option value="\n">LF</option><option value="\r\n">CRLF</option></select></Field>
              <Field label="设备回显策略（用于 Agent 捕获解析）"><select value={profile.echo ?? 'auto'} onChange={(event) => update('echo', event.target.value as ModelProfile['echo'])}><option value="auto">自动识别</option><option value="on">设备回显开启</option><option value="off">设备回显关闭</option></select></Field>
              <Field label="每次写入字节"><input inputMode="numeric" value={profile.write_chunk_size ?? 1} onChange={(event) => update('write_chunk_size', Number(event.target.value))} /></Field>
              <Field label="写入间隔 (ms)"><input inputMode="numeric" value={profile.write_chunk_delay_ms ?? 0} onChange={(event) => update('write_chunk_delay_ms', Number(event.target.value))} /></Field>
            </div>
          </div>
        ) : <div className="empty-profile"><Cpu size={30} /><strong>创建第一个机型 Profile</strong><span>配置机型名和提示符后即可关联串口</span></div>}
      </div>
    </div>
  )
}

function ApplicationSettings({ preferences, onSave }: { preferences: DesktopPreferences; onSave: (preferences: DesktopPreferences) => Promise<void> }): React.JSX.Element {
  const [value, setValue] = useState(preferences)
  useEffect(() => setValue(preferences), [preferences])
  return (
    <div className="app-settings">
      <span className="app-settings-label">应用</span>
      <label><span>后端地址</span><input value={value.endpoint} onChange={(event) => setValue((current) => ({ ...current, endpoint: event.target.value }))} onBlur={() => void onSave(value)} /></label>
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
      <label className="theme-label"><span>外观</span><div className="theme-switch">{(['system', 'light', 'dark'] as ThemePreference[]).map((theme) => <button className={value.theme === theme ? 'is-active' : ''} key={theme} type="button" title={theme} onClick={() => { const next = { ...value, theme }; setValue(next); void onSave(next) }}>{theme === 'dark' ? <Moon size={13} /> : theme === 'light' ? <Sun size={13} /> : 'A'}</button>)}</div></label>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }): React.JSX.Element {
  return <label className="form-field"><span>{label}</span>{children}</label>
}

function serialDraft(port: string, configuredPorts: PortSnapshot[], profiles: TransportProfile[]): SerialConfigurationDraft {
  const configured = configuredPorts.find((item) => item.config.port === port)
  const profile = profiles.find((item) => item.name === configured?.config.transport_profile) ?? profiles[0] ?? defaultTransport()
  return { port, enabled: configured?.config.enabled ?? true, modelProfile: configured?.config.model_profile, transportProfile: { ...profile } }
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

function nextModelName(items: ModelDraft[]): string {
  const names = new Set(items.map((item) => item.profile.name))
  let suffix = 1
  while (names.has(`新机型 ${suffix}`)) suffix += 1
  return `新机型 ${suffix}`
}

function defaultModel(name: string): ModelProfile {
  return { name, shell_prompt: null, uboot_prompt: '=> ', write_eol: '\r', echo: 'auto', write_chunk_size: 1, write_chunk_delay_ms: 0 }
}

function optionLabel(value: string): string {
  return value === 'five' ? '5' : value === 'six' ? '6' : value === 'seven' ? '7' : '8'
}
