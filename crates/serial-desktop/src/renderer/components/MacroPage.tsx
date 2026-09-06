import { FileCode2, Play, Plus, RefreshCw, Save, Square } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import type { MacroDefinition, MacroExecution, MacroListResponse, MacroParameter, MacroSaveRequest, ModelFamily, PortSnapshot } from '../../shared/contracts'

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
  ports: PortSnapshot[]
  modelFamilies: ModelFamily[]
  selectedPort?: string
  connected: boolean
  drafts: MacroDraftStore
  execution?: MacroExecution
  onExecution: (execution: MacroExecution) => void
}

const EMPTY: Editor = { id: '', name: '', description: '', parameters: '{}', script: 'cmd("version");\n', shared: false, modelFamily: '', modelNames: '' }
const UBOOT_SCRIPT = 'let boot = watch(prompt("uboot"));\ncmd("reboot");\nwhile (!boot.matched) {\n    cmd("slp");\n    wait(boot, args.interval_ms);\n}\n'

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

export function MacroPage({ ports, modelFamilies, selectedPort, connected, drafts, execution, onExecution }: Props): React.JSX.Element {
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
    selectionRequest.current += 1
    updateEditor(template ? {
      ...EMPTY, id: 'enter_uboot', name: '进入 U-Boot', description: '发送 reboot，再重复 slp，直到匹配当前机型 U-Boot 提示符', script: UBOOT_SCRIPT,
      parameters: JSON.stringify({ interval_ms: { type: 'integer', default: 50, minimum: 20, maximum: 1000 } }, null, 2)
    } : drafts.drafts.get('__new__') ?? { ...EMPTY })
    setArgs({}); setError('')
  }

  return <main className="macro-page">
    <aside className="macro-catalog">
      <div className="macro-heading"><strong><FileCode2 size={17} /> 命令宏</strong><button type="button" title="刷新宏目录" onClick={() => setRefresh((value) => value + 1)}><RefreshCw size={15} /></button></div>
      <input aria-label="查找宏" placeholder="名称、用途、机型…" value={query} onChange={(event) => { setQuery(event.target.value); setOffset(0) }} />
      <label className="macro-drafts-toggle"><input type="checkbox" checked={includeDrafts} onChange={(event) => { setIncludeDrafts(event.target.checked); setOffset(0) }} /> 显示未共享草稿</label>
      <button className="macro-new" type="button" onClick={() => newMacro()}><Plus size={15} /> 新建宏</button>
      <div className="macro-list" aria-label="已有宏">{catalog?.macros.map((macro) => <button className={editor.saved?.id === macro.id ? 'is-selected' : ''} key={macro.id} type="button" onClick={() => void load(macro.id)}><strong>{macro.name}<small>v{macro.revision} · {macro.shared ? '共享' : '草稿'}</small></strong><span>{macro.description}</span><code>{macro.id}</code></button>)}</div>
      {loading && <p role="status">正在读取目录…</p>}
      {!loading && !catalog?.macros.length && <p>没有匹配的宏</p>}
      <div className="macro-pagination"><button type="button" disabled={!offset} onClick={() => setOffset(Math.max(0, offset - 50))}>上一页</button><span>{catalog?.total ?? 0} 项</span><button type="button" disabled={catalog?.next_offset == null} onClick={() => setOffset(catalog?.next_offset ?? 0)}>下一页</button></div>
    </aside>
    <section className="macro-editor-panel">
      <div className="macro-heading"><div><h2>{editor.saved ? editor.saved.name : '新建命令宏'}</h2><small>{editor.saved ? `v${editor.saved.revision}${dirty ? ' · 有未保存修改' : ''}` : '保存为草稿，通过试运行后可共享'} · 人和 Agent 使用同一份定义</small></div><button type="button" onClick={() => newMacro(true)}>U-Boot 模板</button></div>
      <form onSubmit={(event) => { event.preventDefault(); void save() }}>
        <div className="macro-fields"><label>ID<input aria-label="宏 ID" required readOnly={!!editor.saved} pattern="[A-Za-z0-9_-]+" value={editor.id} onChange={(event) => updateEditor({ ...editor, id: event.target.value })} /></label><label>名称<input required value={editor.name} onChange={(event) => updateEditor({ ...editor, name: event.target.value })} /></label></div>
        <label>用途说明<input required value={editor.description} onChange={(event) => updateEditor({ ...editor, description: event.target.value })} /></label>
        <div className="macro-fields"><label>适用一级机型<select value={editor.modelFamily} onChange={(event) => updateEditor({ ...editor, modelFamily: event.target.value, modelNames: '' })}><option value="">通用</option>{modelFamilies.map((family) => <option key={family.name}>{family.name}</option>)}</select></label><label>二级机型（可选，逗号分隔）<input disabled={!editor.modelFamily} value={editor.modelNames} onChange={(event) => updateEditor({ ...editor, modelNames: event.target.value })} placeholder="留空适用于该一级机型" /></label></div>
        <div className="macro-source-fields"><label>脚本<textarea className="macro-script" spellCheck={false} value={editor.script} onChange={(event) => updateEditor({ ...editor, script: event.target.value })} /></label><label>参数定义（JSON）<textarea spellCheck={false} value={editor.parameters} onChange={(event) => updateEditor({ ...editor, parameters: event.target.value })} /><small>脚本通过 args.name 读取参数。支持 string、integer、boolean。</small></label></div>
        <p className="macro-help"><code>cmd("命令")</code> 自动加换行；<code>watch / wait / delay</code> 等待设备；支持 <code>if / else / for / while</code>。保存仅校验语法，试运行会实际操作串口。</p>
        <div className="macro-save-row"><label><input type="checkbox" checked={editor.shared} onChange={(event) => updateEditor({ ...editor, shared: event.target.checked })} /> 共享到默认宏列表，供其他 Agent 查找复用</label><button type="submit" disabled={!connected || saving}><Save size={15} /> {saving ? '校验保存中…' : '校验并保存'}</button></div>
      </form>
      {notice && <p role="status" className="macro-notice">{notice}</p>}
      {error && <pre role="alert" className="macro-error">{error}</pre>}
      <section className="macro-run-panel"><h3>试运行{editor.saved ? ` · ${editor.saved.id} v${editor.saved.revision}` : ''}</h3><div className="macro-run-fields"><label>串口<select value={target} onChange={(event) => setTarget(event.target.value)}>{ports.map((port) => <option key={port.config.port} value={port.config.port}>{port.config.port}{port.config.model_name ? ` · ${port.config.model_name}` : ''}</option>)}</select></label><label>超时（秒）<input type="number" min={1} max={120} value={timeout} onChange={(event) => setTimeoutSeconds(Number(event.target.value))} /></label>{Object.entries(editor.saved?.parameters ?? {}).map(([name, parameter]) => <label key={name} title={parameter.description}>{name}{parameter.type === 'boolean' ? <select value={args[name] ?? String(parameter.default ?? '')} onChange={(event) => setArgs({ ...args, [name]: event.target.value })}><option value="">请选择</option><option>true</option><option>false</option></select> : <input type={parameter.type === 'integer' ? 'number' : 'text'} value={args[name] ?? String(parameter.default ?? '')} min={parameter.minimum} max={parameter.maximum} onChange={(event) => setArgs({ ...args, [name]: event.target.value })} />}</label>)}</div><div className="macro-run-actions"><button type="button" disabled={!connected || !editor.saved || dirty || busy || !target || !Number.isInteger(timeout) || timeout < 1 || timeout > 120} onClick={() => void run()}><Play size={15} /> {starting ? '启动中…' : '运行已保存版本'}</button>{execution && ['running', 'stopping'].includes(execution.status) && <button type="button" disabled={!connected || execution.status === 'stopping'} onClick={() => { void window.serial.cancelMacro(execution.port, execution.id).then(onExecution).catch((reason) => setError(String(reason instanceof Error ? reason.message : reason))) }}><Square size={14} /> 停止</button>}{dirty && <small>修改保存后才能试运行</small>}</div>
      {execution && <div className={`macro-execution ${execution.outcome_uncertain ? 'is-uncertain' : ''}`} role="status"><strong>{execution.port} · {execution.description}</strong><span>{statusLabel(execution.status)} · 第 {execution.line} 行 · 已发送 {execution.writes} 条</span>{execution.message && <p>{execution.message}</p>}{execution.outcome_uncertain && <p>执行结果不确定，请先查看串口时间线，勿直接重跑。</p>}</div>}</section>
    </section>
  </main>
}

function statusLabel(status: MacroExecution['status']): string {
  return { running: '运行中', stopping: '正在停止', succeeded: '已完成', timed_out: '已超时', cancelled: '已停止', interrupted_by_user: '用户已介入', failed: '执行失败' }[status]
}
