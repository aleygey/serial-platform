import { ChevronDown, Play, Plus, RefreshCw, Save, Search, Square } from 'lucide-react'
import { useEffect, useId, useRef, useState } from 'react'
import { Accordion, AccordionItem, AccordionContent, AccordionTrigger } from './ui/accordion'
import { GooeyNav } from './ui/gooey-nav'
import { Disclosure, DisclosureContent } from './ui/disclosure'
import { WorkspaceSidebar } from './WorkspaceSidebar'
import { BounceSidebar } from './ui/bounce-sidebar'
import { ModelPicker } from './ui/model-picker'
import { MoreFields, OptionField } from './SerialFields'
import { useReducedMotion } from '../lib/use-reduced-motion'
import { StepPlayer, type StepMark } from './ui/step-player'
import { macroExecutionLabel } from './AgentHistory'
import { displayCommand } from '../lib/history'
import type { TimelineEvent, MacroDefinition, MacroExecution, MacroListResponse, MacroParameter, MacroSaveRequest, ModelFamily, PortSnapshot } from '../../shared/contracts'

interface Editor {
  id: string
  name: string
  description: string
  parameters: string
  script: string
  shared: boolean
  modelFamily: string
  modelNames: string
  saved?: MacroDefinition
}

export interface MacroDraftStore { current?: Editor; drafts: Map<string, Editor> }

interface Props {
  events?: Record<string, TimelineEvent[]>
  ports: PortSnapshot[]
  modelFamilies: ModelFamily[]
  selectedPort?: string
  connected: boolean
  drafts: MacroDraftStore
  execution?: MacroExecution
  onExecution: (execution: MacroExecution) => void
}

const EMPTY: Editor = { id: '', name: '', description: '', parameters: '{}', script: 'cmd("version");\n', shared: false, modelFamily: '', modelNames: '' }
const UBOOT_SCRIPT = 'let boot = watch(prompt("uboot"));\ncmd("reboot", "send_only");\nwhile (!boot.matched) {\n    cmd("slp", "send_only");\n    wait(boot, args.interval_ms);\n}\n'

function fromDefinition(definition: MacroDefinition): Editor {
  return { id: definition.id, name: definition.name, description: definition.description, script: definition.script, parameters: JSON.stringify(definition.parameters, null, 2), shared: definition.shared, modelFamily: definition.applies_to?.model_family ?? '', modelNames: definition.applies_to?.model_names.join(', ') ?? '', saved: definition }
}

export function parseMacroParameters(text: string): Record<string, MacroParameter> {
  const value: unknown = JSON.parse(text)
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('参数定义必须是 JSON 对象')
  for (const [name, parameter] of Object.entries(value)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error(`参数名称无效：${name}`)
    if (!parameter || typeof parameter !== 'object' || !['string', 'integer', 'boolean'].includes(parameter.type)) throw new Error(`参数 ${name} 需要 type: string、integer 或 boolean`)
  }
  return value as Record<string, MacroParameter>
}

export function macroRunArguments(parameters: Record<string, MacroParameter>, values: Record<string, string>): Record<string, string | number | boolean> {
  const args: Record<string, string | number | boolean> = {}
  for (const [name, parameter] of Object.entries(parameters)) {
    const supplied = values[name]
    if (supplied === undefined) {
      if (parameter.default !== undefined) args[name] = parameter.default
      else throw new Error(`请填写参数 ${name}`)
      continue
    }
    if (parameter.type === 'string') args[name] = supplied
    else if (parameter.type === 'boolean') {
      if (!['true', 'false'].includes(supplied)) throw new Error(`${name} 必须是 true 或 false`)
      args[name] = supplied === 'true'
    } else {
      if (!/^-?\d+$/.test(supplied)) throw new Error(`${name} 必须是整数`)
      const integer = Number(supplied)
      if (!Number.isSafeInteger(integer) || (parameter.minimum !== undefined && integer < parameter.minimum) || (parameter.maximum !== undefined && integer > parameter.maximum)) throw new Error(`${name} 超出允许范围`)
      args[name] = integer
    }
  }
  return args
}

function requestFor(editor: Editor): MacroSaveRequest {
  return {
    id: editor.id, name: editor.name, description: editor.description, script: editor.script,
    parameters: parseMacroParameters(editor.parameters), shared: editor.shared,
    expected_revision: editor.saved?.revision,
    applies_to: editor.modelFamily ? { model_family: editor.modelFamily, model_names: editor.modelNames.split(',').map((name) => name.trim()).filter(Boolean) } : null
  }
}

export function MacroPage({ events, ports, modelFamilies, selectedPort, connected, drafts, execution, onExecution }: Props): React.JSX.Element {
  const [section, setSection] = useState<'编辑' | '指南'>('编辑')
  const [filterOpen, setFilterOpen] = useState(false)
  const filterId = useId()
  const scriptId = useId()
  const reduced = useReducedMotion()
  const [catalog, setCatalog] = useState<MacroListResponse>()
  const [query, setQuery] = useState('')
  const [includeDrafts, setIncludeDrafts] = useState(false)
  const [offset, setOffset] = useState(0)
  const [refresh, setRefresh] = useState(0)
  const [editor, setEditor] = useState<Editor>(() => drafts.current ?? { ...EMPTY })
  const editorRef = useRef(editor)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [starting, setStarting] = useState(false)
  const [target, setTarget] = useState(selectedPort ?? ports[0]?.config.port ?? '')
  const [args, setArgs] = useState<Record<string, string>>({})
  const [timeout, setTimeoutSeconds] = useState(30)
  const selectionRequest = useRef(0)
  const busy = starting || !!execution && ['running', 'stopping'].includes(execution.status) && !execution.outcome_uncertain

  const updateEditor = (value: Editor): void => {
    editorRef.current = value
    setEditor(value)
    drafts.current = value
    drafts.drafts.set(value.saved?.id ?? '__new__', value)
    setNotice('')
  }

  useEffect(() => {
    if (!connected) return
    let active = true
    setLoading(true)
    const timer = window.setTimeout(() => {
      void window.serial.listMacros({ query, include_drafts: includeDrafts, offset, limit: 50 }).then((result) => {
        if (active) { setCatalog(result); setError('') }
      }).catch((reason) => { if (active) setError(String(reason instanceof Error ? reason.message : reason)) }).finally(() => { if (active) setLoading(false) })
    }, 120)
    return () => { active = false; window.clearTimeout(timer) }
  }, [connected, query, includeDrafts, offset, refresh])

  const load = async (id: string): Promise<void> => {
    setSection('编辑')
    const request = ++selectionRequest.current
    const local = drafts.drafts.get(id)
    if (local) { updateEditor(local); setArgs({}); return }
    try {
      const result = await window.serial.listMacros({ id, include_drafts: true })
      if (request !== selectionRequest.current) return
      if (!result.definition) throw new Error('该宏不存在或已发生变化，请刷新目录')
      updateEditor(fromDefinition(result.definition))
      setArgs({}); setError('')
    } catch (reason) { if (request === selectionRequest.current) setError(String(reason instanceof Error ? reason.message : reason)) }
  }

  const save = async (): Promise<void> => {
    if (saving) return
    const submitted = editor
    const selection = selectionRequest.current
    setError(''); setSaving(true)
    try {
      const result = await window.serial.saveMacro(requestFor(submitted))
      const saved = fromDefinition(result.definition)
      drafts.drafts.set(saved.id, saved)
      if (selectionRequest.current === selection && editorRef.current.id === submitted.id) {
        // Keep edits typed while the server was validating an older draft.
        updateEditor(editorRef.current === submitted ? saved : { ...editorRef.current, saved: result.definition })
        setNotice(`已保存 v${result.definition.revision} · 语法校验通过${result.definition.shared ? ' · 已共享' : ' · 草稿未共享'}`)
      }
      setRefresh((value) => value + 1)
    } catch (reason) { if (selectionRequest.current === selection) setError(String(reason instanceof Error ? reason.message : reason)) }
    finally { setSaving(false) }
  }

  let dirty = true
  try { dirty = !editor.saved || JSON.stringify(requestFor(editor)) !== JSON.stringify(requestFor(fromDefinition(editor.saved))) } catch { /* invalid drafts remain editable */ }

  const run = async (): Promise<void> => {
    if (!editor.saved || dirty || busy || !target) return
    setError(''); setStarting(true)
    try {
      const result = await window.serial.runMacro(target, { macro_id: editor.saved.id, revision: editor.saved.revision, args: macroRunArguments(editor.saved.parameters, args), timeout_seconds: timeout })
      onExecution(result)
    } catch (reason) { setError(String(reason instanceof Error ? reason.message : reason)) }
    finally { setStarting(false) }
  }

  const newMacro = (template = false): void => {
    setSection('编辑')
    selectionRequest.current += 1
    updateEditor(template ? {
      ...EMPTY, id: 'enter_uboot', name: '进入 U-Boot', description: '发送 reboot，再重复 slp，直到匹配当前机型 U-Boot 提示符', script: UBOOT_SCRIPT,
      parameters: JSON.stringify({ interval_ms: { type: 'integer', default: 50, minimum: 20, maximum: 1000 } }, null, 2)
    } : drafts.drafts.get('__new__') ?? { ...EMPTY })
    setArgs({}); setError('')
  }

  const macros = catalog?.macros ?? []
  return <main className="workspace-shell macro-page">
    <WorkspaceSidebar title="宏目录" className="macro-catalog" actions={<>
      <button type="button" aria-label="筛选宏" title="筛选宏" aria-expanded={filterOpen} aria-controls={filterId} onClick={() => setFilterOpen(!filterOpen)}><Search size={15} /></button>
      <button type="button" className="macro-new" aria-label="新建宏" title="新建宏" onClick={() => newMacro()}><Plus size={16} /></button>
    </>}>
      <Disclosure className="catalog-filter" open={filterOpen} transition={{duration:reduced ? 0 : 0.18}}>
        <DisclosureContent id={filterId}><div className="catalog-filter-fields">
          <input className="macro-search" aria-label="查找宏" placeholder="搜索宏…" value={query} onChange={event => { setQuery(event.target.value); setOffset(0) }} />
          <label className="macro-drafts-toggle"><input type="checkbox" checked={includeDrafts} onChange={event => { setIncludeDrafts(event.target.checked); setOffset(0) }} /> 显示未共享草稿</label>
          <button type="button" aria-label="刷新宏目录" title="刷新宏目录" onClick={() => setRefresh(value => value + 1)}><RefreshCw size={14} /></button>
        </div></DisclosureContent>
      </Disclosure>
      <div className="macro-list" aria-label="已有宏">
        {macros.length > 0 && <BounceSidebar items={macros.map(macro => macro.name)} value={macros.findIndex(macro => macro.id === editor.saved?.id)}
          onChange={index => void load(macros[index].id)} itemProps={index => ({title: macros[index].description, 'aria-pressed': editor.saved?.id === macros[index].id})} />}
        {loading && !macros.length && <p role="status">正在读取目录…</p>}
        {!loading && !macros.length && <p>没有匹配的宏</p>}
      </div>
      <div className="macro-compact-picker"><ModelPicker label="选择宏" value={editor.saved?.id ?? ''}
        models={[{id: '', name: '选择宏'}, ...macros.map(macro => ({id: macro.id, name: macro.name}))]} searchable onSearchChange={value => { setQuery(value); setOffset(0) }} onChange={id => { if (id) void load(id) }} /></div>
      {(offset > 0 || catalog?.next_offset != null) && <div className="macro-pagination"><button type="button" disabled={!offset} onClick={() => setOffset(Math.max(0, offset - 50))}>上一页</button><span>{catalog?.total} 项</span><button type="button" disabled={catalog?.next_offset == null} onClick={() => setOffset(catalog?.next_offset ?? 0)}>下一页</button></div>}
    </WorkspaceSidebar>
    <section className="macro-editor-panel">
      <header className="macro-workbar"><div className="macro-workbar-inner">
        <div className="macro-title"><h1 title={section === '指南' ? '宏指南' : editor.saved?.name || '新建命令宏'}>{section === '指南' ? '宏指南' : editor.saved?.name || '新建命令宏'}</h1>
          {section === '编辑' && editor.saved && <small title={dirty ? '有未保存修改' : undefined}>v{editor.saved.revision}{dirty ? ' •' : ''}</small>}</div>
        <GooeyNav className="macro-tabs" aria-label="宏工作区" items={['编辑','指南']} value={section === '编辑' ? 0 : 1}
          onChange={index => setSection(index ? '指南' : '编辑')} size="xs" separation={5} radius={6} activeColor="var(--run-bg)" activeLabelColor="var(--run-text)" />
      </div></header>
      <div className="macro-scroll" key={section}><div className="macro-work">
      {section === '编辑' && <><form onSubmit={event => { event.preventDefault(); void save() }}>
        <div className="macro-fields"><label>名称<input required value={editor.name} onChange={event => updateEditor({ ...editor, name: event.target.value })} /></label>
          <label>宏 ID<input aria-label="宏 ID" required readOnly={!!editor.saved} pattern="[A-Za-z0-9_\-]+" value={editor.id} onChange={event => updateEditor({ ...editor, id: event.target.value })} /></label></div>
        <label>用途说明<input required value={editor.description} onChange={event => updateEditor({ ...editor, description: event.target.value })} /></label>
        <div className="macro-code-field"><div><label htmlFor={scriptId}>脚本</label>
          <button className="macro-template" type="button" onClick={() => newMacro(true)}>U-Boot 模板</button></div>
          <textarea id={scriptId} className="macro-script" spellCheck={false} value={editor.script} onChange={event => updateEditor({ ...editor, script: event.target.value })} /></div>
        <MoreFields title="参数与适用机型"><div className="macro-fields">
          <OptionField label="机型系列" value={editor.modelFamily} options={[{id: '', name: '通用'}, ...modelFamilies.map(({name}) => ({id: name, name}))]} onChange={modelFamily => updateEditor({...editor, modelFamily, modelNames: ''})} />
          <label>适用型号<input disabled={!editor.modelFamily} value={editor.modelNames} onChange={event => updateEditor({ ...editor, modelNames: event.target.value })} placeholder="多个型号用逗号分隔" /></label></div>
          <label>参数定义（JSON）<textarea spellCheck={false} value={editor.parameters} onChange={event => updateEditor({ ...editor, parameters: event.target.value })} /></label>
        </MoreFields>
        <div className="macro-save-row"><label><input type="checkbox" checked={editor.shared} onChange={event => updateEditor({ ...editor, shared: event.target.checked })} /> 共享到默认宏列表</label>
          <button className="primary-button" type="submit" disabled={!connected || saving}><Save size={15} /> {saving ? '校验保存中…' : '校验并保存'}</button></div>
      </form>
      <section className="macro-run-panel"><MoreFields title="试运行">
        <div className="macro-run-fields">
          <OptionField label="目标串口" value={target} options={ports.map(({config}) => ({id: config.port, name: [config.port, config.model_name].filter(Boolean).join(' · ')}))} onChange={setTarget} />
          <label>超时（秒）<input type="number" min={1} max={120} value={timeout} onChange={event => setTimeoutSeconds(Number(event.target.value))} /></label>
          {Object.entries(editor.saved?.parameters ?? {}).map(([name, parameter]) => parameter.type === 'boolean'
            ? <OptionField key={name} label={name} value={args[name] ?? String(parameter.default ?? '')} options={[{id:'',name:'请选择'},{id:'true',name:'true'},{id:'false',name:'false'}]} onChange={value => setArgs({...args, [name]: value})} />
            : <label key={name} title={parameter.description}>{name}<input type={parameter.type === 'integer' ? 'number' : 'text'} min={parameter.minimum} max={parameter.maximum}
              value={args[name] ?? String(parameter.default ?? '')} onChange={event => setArgs({...args, [name]: event.target.value})} /></label>)}
        </div>
        <div className="macro-run-actions"><button type="button" disabled={!connected || !editor.saved || dirty || busy || !target || !Number.isInteger(timeout) || timeout < 1 || timeout > 120} onClick={() => void run()}><Play size={15} /> {starting ? '启动中…' : '运行已保存版本'}</button>
          {execution && ['running', 'stopping'].includes(execution.status) && <button type="button" disabled={!connected || execution.status === 'stopping'} onClick={() => { void window.serial.cancelMacro(execution.port, execution.id).then(onExecution).catch(reason => setError(reason instanceof Error ? reason.message : String(reason))) }}><Square size={14} /> 停止</button>}
          {dirty && <small>修改保存后才能试运行</small>}</div>
        {execution && <MacroProgress execution={execution} events={events?.[execution.port] ?? []} />}
      </MoreFields></section></>}
      {section === '指南' && <MacroGuide />}
      {notice && <p role="status" className="macro-notice">{notice}</p>}
      {error && <pre role="alert" className="macro-error">{error}</pre>}
    </div></div></section>
  </main>
}

function MacroGuide() {
  const reduced = useReducedMotion()
  return <Accordion className="macro-guide" transition={{ duration: reduced ? 0 : 0.22 }}>
    {[
      ['宏的用途', <>把反复输入的一组命令保存为宏。人工执行和外部 Agent 调用可以复用同一个已保存版本。</>],
      ['适用场景与前提', <>读取设备版本、等待启动提示符或按条件重复发送命令，都可以写成宏。试运行前选择可用串口，并确认目标机型与宏的适用范围一致。</>],
      ['命令与参数规则', <><code>cmd("version")</code> 会加上换行并核对回显。<code>cmd("…", "send_only")</code> 只发送。用 <code>watch</code>、<code>wait</code>、<code>delay</code> 等待设备，用 <code>args.name</code> 读取字符串、整数或布尔参数。</>],
      ['从第一个宏开始', <>新建宏或选用 U-Boot 模板，填写名称和用途，编辑脚本后校验并保存。在编辑器下方展开「试运行」选择串口和参数。运行会实际向所选设备写入命令。</>],
    ].map(([title, body]) => <AccordionItem value={String(title)} key={String(title)}>
      <AccordionTrigger className="guide-trigger">{title}<ChevronDown size={16} aria-hidden /></AccordionTrigger>
      <AccordionContent className="guide-content"><p>{body}</p></AccordionContent>
    </AccordionItem>)}
  </Accordion>
}

function MacroProgress({ execution, events }: { execution: MacroExecution; events: TimelineEvent[] }) {
  const state = macroExecutionLabel(execution)
  const finished = !['running', 'stopping'].includes(execution.status)
  const marks: StepMark[] = [{ index: 0, tone: 'done', label: '已开始' }]
  if (execution.writes > 0) marks.push({ index: 1, tone: 'done', label: '已写入串口' })
  if (finished || execution.outcome_uncertain) marks.push({ index: 2, ...state })
  const commands = new Map<string | number, string>()
  for (const event of events) {
    if (event.kind !== 'tx' || event.daemon_epoch !== execution.daemon_epoch || event.generation !== execution.generation
      || event.metadata.macro_execution_id !== execution.id) continue
    const key = event.operation_id ?? event.seq
    commands.set(key, (commands.get(key) ?? '') + event.text)
  }
  return <div className={`macro-execution ${execution.outcome_uncertain ? 'is-uncertain' : ''}`}>
    <strong className="command-ink" data-tone={state.tone} title={state.label}>{execution.port} · {execution.description}</strong>
    <div className="macro-progress" role="status"><StepPlayer steps={marks} total={3} label={'宏执行阶段 · ' + state.label} /></div>
    <span>第 {execution.line} 行 · 已发送 {execution.writes} 条 · 输入已核对 {execution.input_verified_writes ?? 0} 条 · 只发送 {execution.send_only_writes ?? 0} 条</span>
    {!!commands.size && <ol className="macro-command-list">{[...commands].map(([key, text]) => <li key={key}><code className="command-ink" data-tone="idle" title="已发送">{displayCommand(text) || 'Enter'}</code></li>)}</ol>}
    {execution.message && <p>{execution.message}</p>}
    {execution.outcome_uncertain && <p>执行结果待核对，请查看串口时间线。</p>}
  </div>
}
