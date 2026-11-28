import { ArrowLeft, ArrowRight, Check, Copy, RefreshCw } from 'lucide-react'
import { useEffect, useId, useRef, useState } from 'react'
import type { DesktopSnapshot, MacroDefinition, MacroExecution } from '../../shared/contracts'
import type { SetupBrief, SetupRequest } from '../../shared/first-setup'
import { message } from '../lib/feedback'
import { tcpPortAddress } from '../lib/tcp-endpoint'
import { MoreFields, PortEndpointField } from './SerialFields'
import { SetupTranscript } from './SetupTranscript'

const stages = ['选择串口', '识别提示符', '设计检查', '接收方案']
const questions = ['你要配置哪个串口？', '设备准备好时，最后一行显示什么？', '怎样判断设备有响应？', '让外部 Agent 生成配置']
const checkLabels: Record<SetupBrief['check'],string> = { heartbeat:'回车心跳', status:'读取设备状态' }
const fallbackTransport = { name:'115200-8N1', baud_rate:115200, data_bits:'eight', stop_bits:'one', parity:'none', flow_control:'none', auto_open:true, dtr:false, rts:false } as const

export function FirstUseSetup({ snapshot, execution, onExecution, onLeave }: {
  snapshot: DesktopSnapshot; execution?: MacroExecution; onExecution: (value: MacroExecution) => void
  onLeave: (complete: boolean) => Promise<void>
}) {
  const { configuredPorts, availablePorts } = snapshot
  const ports = [...new Set([...configuredPorts.map(p => p.config.port), ...availablePorts.map(p => p.name)])]
  const saved = snapshot.preferences.setupTask?.server === snapshot.serverId ? snapshot.preferences.setupTask : undefined
  const [step, setStep] = useState(saved ? 3 : 0)
  const [chosenPort, setPort] = useState<string | undefined>(saved?.port)
  const port = chosenPort ?? configuredPorts.find(p => p.session_state === 'online' && !p.control)?.config.port ?? ports[0] ?? ''
  const [supplyPrompt, setSupplyPrompt] = useState(false)
  const [hint, setHint] = useState('')
  const [check, setCheck] = useState<SetupBrief['check']>('heartbeat')
  const [intent, setIntent] = useState('')
  const [id, setId] = useState(saved?.id)
  const [task, setTask] = useState<SetupRequest>()
  const [definition, setDefinition] = useState<MacroDefinition>()
  const [runId, setRunId] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [pollError, setPollError] = useState('')
  const [copied, setCopied] = useState(false)
  const [manualCopy, setManualCopy] = useState(false)
  const heading = useRef<HTMLHeadingElement>(null)
  const titleId = useId()
  const status = configuredPorts.find(p => p.config.port === port)
  const online = snapshot.connection === 'connected'
  const proposal = task?.plan
  const shownCheck = task?.brief.check ?? check
  const examplePrompt = proposal?.shell_prompt || (supplyPrompt && hint) || status?.effective_shell_prompt || 'board> '
  const running = execution?.id === runId ? execution : undefined
  const handoff = `请通过 Serial MCP 的 setup_proposal 工具读取配置任务 ${id}。根据我的回答和 ${port} 的回显生成 Shell 提示符与一次健康检查的方案，通过同一工具回传。保留提示符末尾的空格。识别不了时请问我。`

  useEffect(() => { heading.current?.focus({ preventScroll:true }) }, [step])
  useEffect(() => {
    if (step !== 3 || !id || proposal || !online) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout>
    async function poll() {
      try {
        const next = await window.serial.readSetup(id!)
        if (cancelled) return
        setTask(next); setPort(next.brief.port); setCheck(next.brief.check); setIntent(next.brief.intent)
        setSupplyPrompt(!!next.brief.prompt_hint); setHint(next.brief.prompt_hint); setPollError('')
        if (next.plan) return
      } catch (reason) {
        if (cancelled) return
        setPollError(message(reason))
      }
      if (!cancelled) timer = setTimeout(poll, 1000)
    }
    void poll()
    return () => { cancelled = true; clearTimeout(timer) }
  }, [id, step, !!proposal, online])

  async function action(work: () => Promise<void>) {
    if (busy) return
    setBusy(true); setError('')
    try { await work() } catch (reason) { setError(message(reason)) } finally { setBusy(false) }
  }
  async function next() {
    if (step === 0) {
      if (!port || !online) throw new Error('连接后端并选择一个串口。')
      if (!status) await window.serial.saveSerialConfiguration({ port, enabled:true, transportProfile:tcpPortAddress(port) ? fallbackTransport : snapshot.transportProfiles[0] ?? fallbackTransport }, snapshot.configRevision)
      else if (!status.config.enabled) await window.serial.setPortOpen(port, true)
      setStep(1)
    } else if (step === 1) {
      if (supplyPrompt && !hint.trim()) throw new Error('请输入终端最后一行的提示符。')
      setStep(2)
    } else if (step === 2) {
      const brief: SetupBrief = { port, prompt_hint: supplyPrompt ? hint : '', check, intent:check === 'heartbeat' ? '发送一次回车，等设备返回新的 Shell 提示符。' : intent.trim() || '读取固件版本，核对设备的回执。' }
      const nextTask = await window.serial.createSetup(brief)
      setId(nextTask.id); setTask(nextTask); setCopied(false); setManualCopy(false); setPollError(''); setStep(3)
      await window.serial.savePreferences({ setupTask:{ id:nextTask.id, server:snapshot.serverId ?? '', port } })
    } else if (id && proposal && !definition) {
      setDefinition(await window.serial.applySetup(id, snapshot.configRevision))
    } else if (definition) await onLeave(true)
  }
  async function copy() {
    try { await navigator.clipboard.writeText(handoff); setCopied(true) }
    catch { setManualCopy(true) }
  }
  const readyToCheck = online && status?.session_state === 'online' && !status.control
  return <main className="first-setup">
    <div className="setup-page">
      <header className="setup-head"><strong>首次配置</strong><button type="button" disabled={busy} onClick={() => void action(() => onLeave(!!definition))}>{definition ? '进入控制台' : '稍后配置'}</button></header>
      <ol className="setup-stages" aria-label="配置进度">{stages.map((name,i) => <li key={name} data-past={i<step} aria-current={step===i ? 'step' : undefined}><span aria-hidden>{i<step ? <Check size={12}/> : i+1}</span>{name}</li>)}</ol>
      <div className="setup-columns">
        <form aria-labelledby={titleId} onSubmit={event => { event.preventDefault(); void action(next) }}>
          <h1 id={titleId} ref={heading} tabIndex={-1}>{definition ? '配置已保存' : questions[step]}</h1>
          {step === 0 && <><p>先连接设备，向导会用它的回显来配置 Shell 和检查宏。</p>
            <div className="setup-port"><PortEndpointField label="选择要配置的串口" port={port}
              ports={ports.map(name => ({id:name, name, provider:configuredPorts.find(p=>p.config.port===name)?.config.model_name ?? undefined}))}
              onChange={setPort} /></div>
            {!online && <p role="status">{snapshot.connectionMessage}</p>}
            <button className="setup-link" type="button" onClick={() => void action(async()=>{await window.serial.refresh()})}><RefreshCw size={13}/>{online ? '刷新设备' : '重新连接后端'}</button>
          </>}
          {step === 1 && <><p>Shell 提示符出现时，上一条命令的输出已经结束。Agent 会用它来等待下一步。</p>
            <fieldset><legend className="sr-only">提示符来源</legend>
              <Choice name="prompt" selected={!supplyPrompt} onSelect={() => setSupplyPrompt(false)} title="让 Agent 从设备回显识别" />
              <Choice name="prompt" selected={supplyPrompt} onSelect={() => setSupplyPrompt(true)} title="使用我看到的最后一行" />
            </fieldset>
            {supplyPrompt && <label className="setup-input">Shell 提示符<input required autoCapitalize="none" spellCheck={false} maxLength={128} value={hint} placeholder={status?.effective_shell_prompt || 'board> '} onChange={e=>setHint(e.target.value)} /></label>}
          </>}
          {step === 2 && <><p>选一种检查方式，Agent 会把它配置成可重复执行的宏。</p>
            <fieldset><legend className="sr-only">健康检查方式</legend>
              <Choice name="check" title="回车心跳" detail="发送一次回车，等待新的 Shell 提示符。" selected={check==='heartbeat'} onSelect={()=>setCheck('heartbeat')}/>
              <Choice name="check" title="读取设备状态" detail="用自己的话描述要确认的信息。" selected={check==='status'} onSelect={()=>setCheck('status')}/>
            </fieldset>
            {check==='status' && <label className="setup-input">想确认什么？<textarea rows={2} maxLength={480} value={intent} placeholder="例如，读取固件版本并确认设备有响应" onChange={e=>setIntent(e.target.value)}/></label>}
          </>}
          {step === 3 && !proposal && <><p>把任务交给已接入 Serial MCP 的外部 Agent。方案提交后会自动出现在这里。</p>
            <div className="setup-await" role="status"><i />{online ? '等待外部 Agent 返回方案' : '等待后端重新连接'}</div>
            {id && <><button type="button" className="setup-secondary" onClick={()=>void copy()}><Copy size={14}/>{copied ? '已复制任务' : '复制给 Agent'}</button>
              <MoreFields title="查看交接内容"><textarea aria-label="给外部 Agent 的任务" readOnly value={handoff} />
                <p>首次接入时，在外部 Agent 的 MCP 设置中添加以下 stdio 命令。</p>
                <code>serial mcp --endpoint {snapshot.preferences.endpoint}</code>
              </MoreFields>
              {manualCopy && <textarea aria-label="复制任务文本" readOnly value={handoff} autoFocus onFocus={e=>e.target.select()}/>}</>}
          </>}
          {step===3 && proposal && <><p>{definition ? 'Shell Profile 已绑定到这个串口，检查宏已出现在宏目录。' : `${task?.agent || '外部 Agent'} 返回了方案。确认后会保存到这个串口。`}</p>
            <dl className="setup-plan"><div><dt>Shell 提示符</dt><dd><code>{proposal.shell_prompt}</code></dd></div><div><dt>{checkLabels[shownCheck]}</dt><dd><code>{proposal.command || '↵ 回车'}</code></dd></div><div><dt>通过条件</dt><dd>{proposal.expected_reply ? `收到「${proposal.expected_reply}」和新的提示符` : '收到新的 Shell 提示符'}</dd></div></dl>
            {definition && <><button className="setup-secondary" type="button" disabled={busy || !readyToCheck || running?.status === 'running'} onClick={() => void action(async()=>{
              const run = await window.serial.runMacro(port,{ macro_id:definition.id, revision:definition.revision, args:{}, timeout_seconds:8 }); setRunId(run.id); onExecution(run)
            })}>{running?.status==='running' ? '检查中…' : '运行一次检查'}</button>
              <span role="status" className="setup-check-state">{running ? ({succeeded:'收到新回执，串口已通',timed_out:'等待回执超时',running:'等待设备回执',cancelled:'检查已取消',failed:'检查失败',stopping:'正在停止',interrupted_by_user:'检查已中断'}[running.status]) : !readyToCheck ? '串口空闲并在线时可检查' : ''}</span></>}
          </>}
          {(error || pollError) && <p className="setup-error" role="alert">{error || pollError}</p>}
          <footer className="setup-actions">
            {step>0 && !definition && <button type="button" className="setup-link" disabled={busy} onClick={()=>void action(async()=>{ if (step===3) { setTask(undefined); setId(undefined); setPollError(''); await window.serial.savePreferences({setupTask:undefined}) } setStep(step-1) })}><ArrowLeft size={14}/>上一步</button>}
            {(step!==3 || proposal) && <button type="submit" className="setup-primary" disabled={busy || (step===0 && (!port || !online)) || (step===2 && !online)}>{busy ? '处理中…' : definition ? '完成配置' : ['连接并继续','继续','生成配置任务','确认并保存'][step]}<ArrowRight size={14}/></button>}
          </footer>
        </form>
        <SetupTranscript port={port} prompt={examplePrompt} command={step>=2 && shownCheck==='status' ? proposal?.command ?? 'version' : ''} reply={step>=2 && shownCheck==='status' ? proposal?.expected_reply ?? 'firmware 2.4.1' : ''}/>
      </div>
    </div>
  </main>
}

function Choice({name,title,detail,selected,onSelect}: {name:string; title:string; detail?:string; selected:boolean; onSelect:()=>void}) {
  return <label className="setup-choice"><input type="radio" name={name} checked={selected} onChange={onSelect}/><span><strong>{title}</strong>{detail && <small>{detail}</small>}</span></label>
}
