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
  return {
    connection: 'connected', connectionMessage: '实时连接已建立', serverId: 'qa-server',
    daemonEpoch: 'qa-epoch', configRevision: 8,
    preferences: { ...preferences, selectedPort: 'COM6' },
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
      name: 'TL-AS7230 Family', model_names: ['TL-AS7230-W 1.0', 'TL-AS7230-F4GE 2.0'],
      shell_prompt: 'root@router:~# ', uboot_prompt: '=> ',
      write_eol: '\r', echo: 'auto', write_chunk_size: 1, write_chunk_delay_ms: 2
    }],
    configuredPorts: [
      {
        config: {
          port: 'COM6', transport_profile: '115200-8N1', model_profile: 'TL-AS7230 Family',
          model_name: 'TL-AS7230-W 1.0', enabled: true
        },
        daemon_epoch: 'qa-epoch', head_seq: 119, generation: 2, endpoint_present: true,
        session_state: 'online', effective_shell_prompt: 'root@router:~# ',
        effective_uboot_prompt: '=> ', effective_write_eol: '\r'
      },
      {
        config: { port: 'COM7', transport_profile: '115200-8N1', model_profile: null, enabled: false },
        daemon_epoch: 'qa-epoch', head_seq: 0, generation: 0, endpoint_present: true,
        session_state: 'disabled'
      }
    ],
    events: { COM6: events, COM7: [] }
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
      ...(sequenceId ? { command_sequence_id: sequenceId, command_sequence_description: description } : {}),
      ...(stepIndex === undefined ? {} : { command_sequence_step_index: stepIndex }),
      ...(matcher ? { command_capture_matchers: [{ kind: 'contains', value: matcher }] } : {})
    },
    durable: true
  }
}
