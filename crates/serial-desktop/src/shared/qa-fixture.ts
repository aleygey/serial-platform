import type { DesktopPreferences, DesktopSnapshot, TimelineEvent } from './contracts'

export function createQaSnapshot(preferences: DesktopPreferences = {
  endpoint: 'http://127.0.0.1:3210', autoStartLocal: true, theme: 'system'
}): DesktopSnapshot {
  const now = Date.now() * 1_000_000
  const agent = { id: 'agent:codex', label: 'Codex', kind: 'agent' as const }
  const events: TimelineEvent[] = [
    rx(110, now - 24_000_000_000, 'U-Boot 2024.01\nDRAM: 512 MiB\nLoading Linux...\n'),
    rx(111, now - 20_000_000_000, 'TL-AS7230 login: '),
    tx(112, now - 19_000_000_000, 'admin\r', agent, '登录设备', 'login-seq', 0, 'Password: '),
    rx(113, now - 18_900_000_000, 'admin\nPassword: '),
    tx(114, now - 18_000_000_000, '••••••\r', agent, '登录设备', 'login-seq', 1, 'root@router:~# '),
    rx(115, now - 17_800_000_000, '\nWelcome to TL-AS7230\nroot@router:~# '),
    tx(116, now - 12_000_000_000, 'ip addr show br-lan\r', agent, '查看管理口地址', undefined, undefined, 'root@router:~# '),
    rx(117, now - 11_800_000_000, '3: br-lan: <BROADCAST,MULTICAST,UP> mtu 1500\n    inet 192.168.1.1/24 brd 192.168.1.255\n    link/ether 02:42:ac:11:00:02\nroot@router:~# '),
    tx(118, now - 6_000_000_000, 'cat /etc/version\r', agent, '确认固件版本', undefined, undefined, 'root@router:~# '),
    rx(119, now - 5_800_000_000, 'Serial Platform Firmware 0.8.0-debug\nBuild: 2026-08-22\nStatus: ready\nroot@router:~# ')
  ]
  const run = { id: 'qa-sensor-run', label: '读取传感器状态', owner: agent, start_seq: 2, status: 'active' as const }
  const sensorEvent = (seq: number, text: string, direction: TimelineEvent['direction']): TimelineEvent => ({
    port: 'COM7', daemon_epoch: 'qa-epoch', generation: 1, seq, wall_time_ns: now - (8 - seq) * 1_000_000_000,
    kind: direction, direction, text, metadata: {}, durable: true
  })
  const sensorEvents: TimelineEvent[] = [
    sensorEvent(1, 'Sensor Board / UART0\nFirmware 2.4.1\nI2C bus ready\n\nsensor> ', 'rx'),
    { ...sensorEvent(2, '', 'none'), kind: 'run_started', actor: agent, run_id: run.id, metadata: { run } },
    { ...sensorEvent(3, 'version\r', 'tx'), actor: agent, run_id: run.id, operation_id: 'qa-version',
      metadata: { command_description: '读取固件版本', command_capture_matchers: [{ kind: 'contains', value: 'sensor> ' }] } },
    sensorEvent(4, 'version\nboard     SHT31\nrevision  B2\nfirmware  2.4.1\nsensor> ', 'rx'),
    { ...sensorEvent(5, '', 'none'), kind: 'command_capture_completed', run_id: run.id, operation_id: 'qa-version',
      metadata: { capture: { daemon_epoch: 'qa-epoch', generation: 1, operation_id: 'qa-version', tx_event_seq: 3,
        evidence_from_seq: 3, evidence_through_seq: 4, completion: 'prompt', confidence: 'high' } } },
    { ...sensorEvent(6, 'sensor read\r', 'tx'), actor: agent, run_id: run.id, operation_id: 'qa-sample',
      metadata: { command_description: '读取实时采样', command_capture_matchers: [{ kind: 'contains', value: 'sensor> ' }] } },
    sensorEvent(7, 'sensor read\nSampling channel 0…\n', 'rx')
  ]
  return {
    connection: 'connected', connectionMessage: '实时连接已建立', serverId: 'qa-server',
    daemonEpoch: 'qa-epoch', configRevision: 8,
    humanHistory: { server_id: 'qa-server', revision: 4, entries: ['cat /etc/version', 'cat /proc/meminfo', 'echo 中🙂', 'version'].map((command, index) => ({ id: `human-${index}`, command, port: index % 2 ? 'COM7' : 'COM6', wall_time_ns: now, revision: 4 - index, uses: 1 })) },
    preferences: { ...preferences, selectedPort: preferences.selectedPort ?? 'COM6' },
    service: { owned: true, status: 'running', pid: 4208, program: 'seriald' },
    availablePorts: [
      { name: 'COM6', port_type: 'usb', manufacturer: 'FTDI', product: 'USB Serial Port' },
      { name: 'COM7', port_type: 'usb', manufacturer: 'Silicon Labs', product: 'CP2102N' },
      { name: '/dev/cu.usbserial-210', port_type: 'usb', manufacturer: 'FTDI', product: 'USB UART' }
    ],
    transportProfiles: [{
      name: '115200-8N1', baud_rate: 115200, data_bits: 'eight', parity: 'none',
      stop_bits: 'one', flow_control: 'none', dtr: false, rts: false, auto_open: true
    }],
    modelProfiles: [{
      name: 'TL-AS7230 Shell',
      shell_prompt: 'root@router:~# ', uboot_prompt: '=> ',
      write_eol: '\r', echo: 'auto', write_chunk_size: 1, write_chunk_delay_ms: 2
    }],
    modelFamilies: [
      { name: 'TL-AS7230', model_names: ['TL-AS7230-W 1.0', 'TL-AS7230-F4GE 2.0'] },
      { name: 'SHT3x', model_names: ['SHT31 Sensor Board'] }
    ],
    configuredPorts: [
      {
        config: {
          port: 'COM6', transport_profile: '115200-8N1', model_profile: 'TL-AS7230 Shell',
          model_family: 'TL-AS7230',
          model_name: 'TL-AS7230-W 1.0', enabled: true
        },
        daemon_epoch: 'qa-epoch', head_seq: 119, generation: 2, endpoint_present: true,
        session_state: 'online', effective_shell_prompt: 'root@router:~# ',
        effective_uboot_prompt: '=> ', effective_write_eol: '\r'
      },
      {
        config: { port: 'COM7', transport_profile: '115200-8N1', model_family: 'SHT3x', model_name: 'SHT31 Sensor Board', enabled: true },
        daemon_epoch: 'qa-epoch', head_seq: 7, generation: 1, endpoint_present: true, session_state: 'online',
        active_run: run, control: { id: 'qa-agent-lease', owner: agent, epoch: 'qa-epoch', generation: 1, fence: 1,
          issued_wall_time_ns: now, expires_wall_time_ns: now + 60_000_000_000 }
      }
    ],
    events: { COM6: events, COM7: sensorEvents }
  }
}

function rx(seq: number, wall_time_ns: number, text: string): TimelineEvent {
  return {
    port: 'COM6', daemon_epoch: 'qa-epoch', seq, generation: 2, wall_time_ns,
    kind: 'rx', direction: 'rx', text, metadata: {}, durable: true
  }
}

function tx(
  seq: number,
  wall_time_ns: number,
  text: string,
  actor: TimelineEvent['actor'],
  description: string,
  sequenceId?: string,
  stepIndex?: number,
  matcher?: string
): TimelineEvent {
  return {
    port: 'COM6', daemon_epoch: 'qa-epoch', seq, generation: 2, wall_time_ns,
    kind: 'tx', direction: 'tx', actor, operation_id: `op-${seq}`, text,
    metadata: {
      command_description: description,
      ...(sequenceId ? { command_sequence_id: sequenceId, command_sequence_description: description, command_sequence_step_count: 2 } : {}),
      ...(stepIndex === undefined ? {} : { command_sequence_step_index: stepIndex }),
      ...(matcher ? { command_capture_matchers: [{ kind: 'contains', value: matcher }] } : {})
    },
    durable: true
  }
}
