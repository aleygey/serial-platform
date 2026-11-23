import { useSyncExternalStore } from 'react'
import { InputDiagnostics } from '../lib/input-diagnostics'
import { version } from '../../../package.json'

const elapsed = (start?: number, end?: number) => start === undefined || end === undefined ? '—' : Math.max(0, end - start).toFixed(0)

export function InputDiagnosticPanel({ recorder, port, historyCount }: { recorder: InputDiagnostics; port: string; historyCount: number }) {
  const rows = useSyncExternalStore(recorder.subscribe, recorder.snapshot, recorder.snapshot)
  return <section className="input-diagnostics" aria-label="输入诊断">
    <header><strong>输入诊断</strong><span>{port} · v{version} · 缓存 {historyCount} 条</span>
      <button type="button" aria-pressed={recorder.paused} onClick={() => recorder.togglePause()}>{recorder.paused ? '继续采样' : '定格'}</button></header>
    {rows.length > 0 ? <div className="input-timings"><table>
      <thead><tr>{['输入', '排队', '分发', '确认', '首包', '上屏', '总计', '提示符'].map(label => <th key={label} scope="col">{label}</th>)}</tr></thead>
      <tbody>{rows.map(row => <tr key={row.id}>
        <th scope="row">{row.index} {row.kind}{row.status === 'rejected' ? ' · 拒绝' : row.status === 'uncertain' ? ' · 未确认' : ''}</th>
        <td>{elapsed(row.keyAt, row.handledAt)}</td><td>{elapsed(row.handledAt, row.sentAt)}</td><td>{elapsed(row.sentAt, row.acceptedAt)}</td>
        <td>{elapsed(row.sentAt, row.receivedAt)}{row.sharedRx ? '*' : ''}</td><td>{elapsed(row.receivedAt, row.frameAt)}</td>
        <td>{elapsed(row.keyAt, row.frameAt)}</td><td>{elapsed(row.keyAt, row.promptAt)}</td>
      </tr>)}</tbody>
    </table></div> : <p>在下方输入框按回车，最近八次采样会出现在这里。</p>}
    <small>单位 ms。确认和首包均从发出时计时。首包可能是设备日志，* 表示多次输入共用一包。总计到首包界面帧，提示符计至匹配机型提示符的界面帧。</small>
  </section>
}
