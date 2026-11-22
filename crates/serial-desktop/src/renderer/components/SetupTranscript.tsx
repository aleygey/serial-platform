import { RotateCcw } from 'lucide-react'
import { useState } from 'react'
import TypingText from '../../../components/animata/text/typing-text'
import { useReducedMotion } from '../lib/use-reduced-motion'

export function SetupTranscript({ port, prompt, command, reply }: { port: string; prompt: string; command: string; reply: string }) {
  const reduced = useReducedMotion()
  const [take, setTake] = useState(0)
  return <figure className="setup-transcript">
    <figcaption><span>回显示范</span><code>{port}</code><button type="button" aria-label="重播回显示范" onClick={() => setTake(take + 1)}><RotateCcw size={14} /></button></figcaption>
    <TranscriptLines key={JSON.stringify([take,prompt,command,reply])} prompt={prompt} command={command} reply={reply} reduced={reduced} />
  </figure>
}

function TranscriptLines({ prompt, command, reply, reduced }: { prompt:string; command:string; reply:string; reduced:boolean }) {
  const [line, setLine] = useState(0)
  const rows = [['TX', command || '↵'], ...(reply ? [['RX', reply]] : []), ['RX', prompt]]
  const done = reduced || line >= rows.length
  return <>
    <div className="setup-wire" aria-hidden>{rows.map(([direction, text], index) => <div className="setup-wire-line" key={index} data-prompt={index === rows.length - 1 || undefined} data-arrived={reduced || index <= line || undefined}>
      <small>{direction}</small><div>{reduced || index < line ? text : index === line ? <TypingText text={text} repeat={false} delay={45} grow cursor={null} onComplete={() => setLine(index + 1)} /> : null}</div>
    </div>)}</div>
    <div className="setup-trace-result" data-done={done}><i aria-hidden />{done ? '收到提示符，可以发下一条命令' : '等设备回显'}</div>
    <span className="sr-only">示例发送 {command || '回车'}，收到 {reply} 和 Shell 提示符 {prompt}。</span>
  </>
}
