import type { MacroSaveRequest, ModelProfile, PortConfig } from './contracts'

export interface SetupBrief {
  port: string
  prompt_hint: string
  check: 'heartbeat' | 'status'
  intent: string
}

export interface SetupPlan {
  shell_prompt: string
  command: string
  expected_reply: string
}

export interface SetupRequest {
  id: string
  brief: SetupBrief
  plan: SetupPlan | null
  agent: string | null
}

// Catalog limits count UTF-8 bytes. Keep names and descriptions on code-point boundaries.
function catalogLabel(value: string, maxBytes: number): string {
  let text = ''
  let bytes = 0
  const encoder = new TextEncoder()
  for (const character of value.replace(/[\s\p{Cc}]+/gu, ' ').trim()) {
    bytes += encoder.encode(character).length
    if (bytes > maxBytes) break
    text += character
  }
  return text.trimEnd()
}

export function setupChanges(task: SetupRequest, ports: PortConfig[], profiles: ModelProfile[]) {
  if (!task.plan) throw new Error('正在等待外部 Agent 提交方案。')
  const { brief, plan } = task
  const port = ports.find(item => item.port === brief.port)
  if (!port) throw new Error('目标串口已移除，请返回选择串口。')

  const base = profiles.find(item => item.name === port.model_profile)
  const shortPort = brief.port.split('/').filter(Boolean).at(-1) || brief.port
  const label = (catalogLabel(shortPort, 44) || '串口') + ' · Shell'
  const names = new Set(profiles.map(profile => profile.name))
  let name = label
  for (let suffix = 2; names.has(name); suffix++) name = label + ' (' + suffix + ')'
  const alreadyBound = base?.shell_prompt === plan.shell_prompt && base.write_eol !== ''
  const profile: ModelProfile = alreadyBound ? base : {
    ...base, name, shell_prompt: plan.shell_prompt, ...(base?.write_eol === '' && { write_eol: '\r' }),
  }

  // Install both watchers before TX so even a device's immediate reply is observed.
  const script = ['let ready = watch(prompt("shell"));']
  if (plan.expected_reply) script.push(`let reply = watch(${JSON.stringify(plan.expected_reply)});`)
  script.push(`cmd(${JSON.stringify(plan.command)}, "send_only");`)
  if (plan.expected_reply) script.push('expect(reply, 3000);')
  script.push('expect(ready, 3000);')

  const title = brief.check === 'heartbeat' ? '串口心跳' : '设备健康检查'
  const macro: MacroSaveRequest = {
    id: 'check_' + task.id.replaceAll('-', ''),
    name: catalogLabel(title + ' · ' + shortPort, 128),
    description: brief.check === 'heartbeat' ? '发送一次回车，等待设备返回 Shell 提示符。' : catalogLabel(brief.intent, 256) || '读取设备状态并等待新回执。',
    parameters: {}, script: script.join('\n'), shared: true,
    applies_to: port.model_family && port.model_name ? { model_family: port.model_family, model_names: [port.model_name] } : null,
  }
  return {
    alreadyBound, profile, macro,
    ports: ports.map(item => item === port ? { ...item, model_profile: profile.name } : item),
    profiles: alreadyBound ? profiles : [...profiles, profile],
  }
}
