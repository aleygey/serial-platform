# Serial Platform Protocol v9

本文是 `seriald` HTTP/WebSocket 和 `serial-mcp` transport 的当前线协议说明。Rust DTO 与编码实现位于 `serial-protocol`；Agent 工具参数见 [MCP_TOOLS.md](./MCP_TOOLS.md)。

## 端点

默认本地地址：

```text
seriald HTTP/WebSocket: http://127.0.0.1:3210
MCP Streamable HTTP:    http://127.0.0.1:3211/mcp
```

公开设备标识只有 `port`，值是操作系统串口名。HTTP path 中必须 percent-encode `/` 等保留字符，例如：

```text
/dev/cu.usbserial-210
→ /api/v1/ports/%2Fdev%2Fcu.usbserial-210/events
```

JSON field、WebSocket message、timeline event 与 MCP 参数都保留原始端口字符串。

## 本地活动端点

同一 resolved data root 中，`data/seriald.lock` 在 journal 打开前只允许一个 `seriald` 实例进入；监听成功后，该实例发布 `data/active-endpoint.json`：

```json
{
  "schema_version": 1,
  "endpoint": "http://127.0.0.1:3210",
  "address": "127.0.0.1:3210",
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "protocol_version": 9,
  "pid": 12345
}
```

记录只是发现入口。客户端必须调用其中 endpoint 的 `GET /api/v1/health`，并确认 `status=ok`、`server_id`、`daemon_epoch` 和 `protocol_version` 全部与记录一致后才能复用。`address` 来自实际 listener 地址：精确 bind 保留该 IP，IPv4/IPv6 通配 bind 分别发布本机可连接的 `127.0.0.1`/`::1`。失效记录不会阻止新实例取得 OS lock 并覆写；拥有进程正常退出时只清理与自身 `server_id`、`daemon_epoch` 一致的记录。

## HTTP v1

`/api/v1` 是 HTTP 路由命名空间，不是跨组件兼容代际。当前 HTTP DTO、WebSocket 握手和客户端兼容检查统一使用 `protocol_version=9`；路由仍保持 `/api/v1/...`。

### 路由

| Method | Path | 用途 |
|---|---|---|
| `GET` | `/api/v1/health` | 进程健康、server/epoch、uptime、protocol version |
| `GET` | `/api/v1/status` | 全局状态、config revision、所有配置端口 snapshot |
| `GET` | `/api/v1/ports` | 枚举主机可见的 OS 串口 |
| `PUT` | `/api/v1/config/ports` | 原子替换端口配置 |
| `GET` / `PUT` | `/api/v1/config/transport-profiles` | 读取/原子替换 Transport Profile catalog |
| `GET` / `PUT` | `/api/v1/config/model-profiles` | 读取/原子替换 Model Profile catalog |
| `GET` / `PUT` | `/api/v1/config/model-families` | 读取/原子替换两级 Model Family catalog |
| `GET` | `/api/v1/archives` | 枚举保留的端口/周期日志 |
| `GET` | `/api/v1/diagnostics` | 后端、连接、journal 与所有端口诊断 |
| `GET` | `/api/v1/diagnostics/storage` | journal 用量与 writer health |
| `GET` | `/api/v1/ports/{port}/diagnostics` | 一个端口的权威状态与 subscriber 指标 |
| `GET` | `/api/v1/ports/{port}/tail` | 从有界 replay ring 读取实时 tail/continuation |
| `GET` | `/api/v1/ports/{port}/recent-activity` | MCP 操作间的紧凑第三方活动 |
| `GET` | `/api/v1/ports/{port}/events` | 有界 journal 查询 |
| `GET` / `POST` | `/api/v1/monitors` | 列表/创建 Monitor |
| `GET` / `PUT` / `DELETE` | `/api/v1/monitors/{monitor_id}` | 读取/更新/停止 Monitor |
| `DELETE` | `/api/v1/monitors/{monitor_id}/history?expected_revision=N` | 显式删除已停止 Monitor 及 incident，拒绝运行中或旧 revision，不删除原始串口 journal |
| `GET` | `/api/v1/monitors/{monitor_id}/incidents` | 分页读取 incident |
| `POST` | `/api/v1/monitors/{monitor_id}/incidents/{incident_id}/ack` | 确认 incident |
| `GET` | `/api/v1/ws` | WebSocket protocol v9 |

### Health

```json
{
  "status": "ok",
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "uptime_ms": 1200,
  "protocol_version": 9
}
```

### Status 与 PortSnapshot

`GET /api/v1/status`：

```json
{
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "protocol_version": 9,
  "config_revision": 12,
  "sequence_write_precondition_supported": true,
  "serial_context_precondition_supported": true,
  "ports": []
}
```

每个 port snapshot 的主要字段：

```json
{
  "config": {
    "port": "COM4",
    "transport_profile": "uart-115200",
    "model_profile": "linux-shell",
    "model_family": "TL-AS7230",
    "model_name": "TL-AS7230-W 1.0",
    "enabled": true
  },
  "daemon_epoch": "uuid",
  "head_seq": 812,
  "ring_oldest_seq": 590,
  "generation": 3,
  "endpoint_present": true,
  "session_state": "online",
  "state_reason": null,
  "state_code": null,
  "target_activity": "active",
  "last_rx_wall_time_ns": 1700000000000000000,
  "rx_offset": 20480,
  "tx_offset": 311,
  "control": null,
  "pending_run_start": null,
  "active_run": null,
  "run_context": null,
  "active_trigger": null,
  "logging": "healthy",
  "effective_shell_prompt": "root@router:~# ",
  "effective_uboot_prompt": "=> ",
  "effective_write_eol": "\r",
  "effective_echo": "auto",
  "effective_transport": {
    "baud_rate": 115200,
    "data_bits": "eight",
    "parity": "none",
    "stop_bits": "one",
    "flow_control": "none",
    "dtr": false,
    "rts": false,
    "auto_open": true
  },
  "effective_write_pacing": {
    "chunk_size": 1,
    "chunk_delay_ms": 1
  }
}
```

`session_state`：`disabled`、`waiting_for_port`、`opening`、`online`、`backoff`、`stopping`。`pending_run_start` 是等待当前 Human Control holder 决策的完整 `PendingRunStartApproval`；`run_context` 是活动 Agent Run 的人工命令 revision/ack fence。两者在创建、决策、取消、过期、断连或代际变化时都会随 snapshot 广播更新。

存在人工干预的 Agent Run context 示例：

```json
{
  "run_id": "uuid",
  "revision": 2,
  "last_human_command_seq": 830,
  "acknowledged_revision": 1,
  "acknowledged_through_seq": 820
}
```

`revision > acknowledged_revision` 即表示 Agent physical-action gate 仍关闭。

### 配置 DTO

`PUT /api/v1/config/ports`：

```json
{
  "ports": [
    {
      "port": "COM4",
      "transport_profile": "uart-115200",
      "model_profile": "linux-shell",
      "model_family": "TL-AS7230",
      "model_name": "TL-AS7230-W 1.0",
      "enabled": true
    }
  ],
  "source": "human:desktop",
  "expected_revision": 12
}
```

响应返回更新后的 `ports` snapshots 与新 `config_revision`。`source` 是 1–128 字符的审计标签。`model_profile` 是独立的行为绑定；`model_family` 和 `model_name` 必须同时为字符串或同时为 null/省略，具体机型必须存在于对应 family 中。

`seriald.toml` 的当前持久配置是 `schema_version=3`。该号码与 `active-endpoint.json` 和 Monitor state 的 schema 无关。schema 2 有一条严格、单向的启动迁移：先验证完整旧配置并保留原始备份，再原子写入 schema 3；迁移保留 `server_id`、`config_revision`、串口配置、行为 Profile、机型身份和端口绑定。只读 discovery 可在内存中读取迁移结果但不改盘；持有数据目录运行时所有权的启动路径负责持久化。迁移失败或遇到其他 schema 时返回错误且不覆盖原文件。

Transport Profile：

```json
{
  "name": "uart-115200",
  "baud_rate": 115200,
  "data_bits": "eight",
  "parity": "none",
  "stop_bits": "one",
  "flow_control": "none",
  "dtr": false,
  "rts": false,
  "auto_open": true
}
```

Model Profile：

```json
{
  "name": "linux-shell",
  "shell_prompt": "root@router:~# ",
  "uboot_prompt": "=> ",
  "write_eol": "\r",
  "echo": "auto",
  "write_chunk_size": 1,
  "write_chunk_delay_ms": 1
}
```

Model Profile 只管理可复用的串口交互行为，不包含 `model_names`，也不与任何机型系列绑死。`GET /api/v1/config/model-profiles` 返回 `{profiles, config_revision}`；PUT body 是 `{profiles, expected_revision?}`，表示全量替换。

Model Family：

```json
{
  "name": "TL-AS7230",
  "model_names": [
    "TL-AS7230-W 1.0",
    "TL-AS7230-F4GE 1.0"
  ]
}
```

`GET /api/v1/config/model-families` 返回 `{families, config_revision}`；PUT body 是 `{families:[{name,model_names}], expected_revision?}`，响应为同形的 `{families, config_revision}`。这是独立的两级身份 catalog，第一级 `name` 是机型系列，第二级 `model_names` 是具体机型。全量替换不允许删除当前端口正在引用的 family 或 model name。所有 Profile 和机型身份名称都按原字符串比较和显示。

### TimelineEvent

历史 API 与 WebSocket timeline 使用相同事件模型：

```json
{
  "port": "COM4",
  "daemon_epoch": "uuid",
  "seq": 812,
  "generation": 3,
  "wall_time_ns": 1700000000000000000,
  "monotonic_time_ns": 4123000000,
  "kind": "rx",
  "direction": "rx",
  "actor": null,
  "run_id": null,
  "operation_id": null,
  "stream_offset_start": 20470,
  "stream_offset_end": 20480,
  "data": "base64",
  "metadata": {},
  "durable": true
}
```

`kind` 当前取值：

```text
rx tx
serial_opening serial_opened serial_open_failed serial_closed
port_reconfigured port_removed
control_granted control_released control_revoked control_expired
run_start_requested run_start_approved run_start_denied
run_start_timed_out run_start_cancelled
run_started run_ended run_aborted
trigger_started trigger_completed trigger_cancelled trigger_failed
command_capture_completed
break checkpoint logging_degraded gap
```

`direction` 是 `rx`、`tx` 或 `none`。`data` 在 JSON control/history 中使用 base64；WebSocket data frame 把 raw bytes 放在 payload 中。

Agent command TX 的 metadata 可以包含：

```json
{
  "command_description": "输入登录账号",
  "command_capture_matchers": [
    {"kind": "contains", "value": "Password:"}
  ],
  "command_sequence_id": "uuid",
  "command_sequence_description": "登录设备",
  "command_sequence_step_index": 0,
  "command_sequence_step_count": 2
}
```

matcher kind 为 `contains`、`regex`、`shell_prompt` 或 `uboot_prompt`。数组为空时省略。

`command_capture_matchers` 是随 TX 保存的兼容定位提示，不是 v7 的完成边界。命令完成后，Agent 通过 `record_command_capture` 提交证据范围；`seriald` 校验后写入 `command_capture_completed`。该事件的 `run_id`、`operation_id` 位于事件顶层，`metadata.capture` 保存完整、权威的 `CommandCaptureCompleted`：

```json
{
  "daemon_epoch": "uuid",
  "generation": 3,
  "run_id": "uuid",
  "operation_id": "uuid",
  "tx_event_seq": 812,
  "evidence_from_seq": 812,
  "evidence_through_seq": 826,
  "completion": "prompt",
  "completion_detail": "root@router:~# ",
  "confidence": "high",
  "record_event_seq": 827,
  "tx_stream_offset_start": 300,
  "tx_stream_offset_end": 311,
  "rx_stream_offset_start": 20480,
  "rx_stream_offset_end": 20742
}
```

`completion`：`literal`、`prompt`、`regex`、`quiet`、`signal`、`run_aborted`、`timeout` 或 `disconnected`。`confidence`：`high`、`medium`、`low`、`partial`、`interfered`、`incomplete` 或 `unreliable`。没有某一方向的证据时，对应 stream offset 可以省略。

Human 命令仍形成普通 `tx` 事件，但至少携带 `metadata.human_command=true` 和 `metadata.cooperative`。若它发生在活动 Agent Run 中，还携带该 Run 的 `interfered_run_id` 与递增后的 `context_revision`；这个 TX seq 是 read gate 必须覆盖的权威边界。

Run-start 审批事件使用稳定投影：`run_start_requested`、`run_start_approved`、`run_start_denied`、`run_start_timed_out`、`run_start_cancelled` 都在 `metadata.approval` 保存完整 `PendingRunStartApproval`，并在 `metadata.approval_id` 重复其 ID。批准事件还可在 `metadata.run` 保存创建后的 Run；系统取消会附带 `reason` 和 `cancelled_by`。

端口配置变化的 `port_reconfigured` metadata 包含 `source`，以及 `previous_` / `new_` 版本的 `model_profile`、`model_family` 和 `model_name`。

### Archive 与 events

`GET /api/v1/archives?port=COM4` 返回：

```json
{
  "archives": [
    {
      "port": "COM4",
      "epoch": "uuid",
      "first_seq": 1,
      "last_seq": 812,
      "first_segment_wall_time_ns": 0,
      "last_segment_wall_time_ns": 0,
      "segment_count": 2,
      "total_bytes": 1048576,
      "has_open_segment": true
    }
  ],
  "truncated": false
}
```

`GET /api/v1/ports/{port}/events` 接受以下 query 参数：

| 参数 | 含义 |
|---|---|
| `epoch` | 后端周期；省略时限定当前周期 |
| `after_seq` | 严格大于该序号 |
| `through_seq` | 包含式上界，形成 `(after_seq, through_seq]` |
| `before_wall_time_ns` / `after_wall_time_ns` | wall time 边界 |
| `direction` | `rx` / `tx` / `none` |
| `kind` | 一个 event kind |
| `actor_id` | actor 过滤 |
| `run_id` | Run 过滤 |
| `operation_id` | operation 过滤 |
| `contains` | 普通 UTF-8 文本 |
| `regex` | bounded UTF-8 regex，与 `contains` 互斥 |
| `limit_events` / `limit_bytes` | 返回边界 |

响应：

```json
{
  "events": [],
  "next_cursor": {"epoch": "uuid", "after_seq": 812},
  "truncated": false,
  "first_available_seq": 1,
  "gaps": []
}
```

每个 gap 是 `{epoch, first_seq, last_seq, reason}`。`reason`：`epoch_changed`、`ring_evicted`、`retention`、`corruption`、`logging_fault` 或 `sequence_discontinuity`。

### Live tail

`GET /api/v1/ports/{port}/tail` 从有界内存 ring 返回，接受：

- `tail_events`：1–2000，默认 200；
- continuation 必须同时提供 `epoch` 与 `after_seq`。

tail 使用 `EventQueryResponse` 结构。`truncated` 或 `gaps` 明确表示 ring 边界，不会静默跳过仍应读取的数据。

### Recent activity

`GET /api/v1/ports/{port}/recent-activity` 必须同时提供 `epoch`、`after_seq`、`through_seq`。它只从 ring 返回最多 32 条与协同上下文有关的 TX、Control、Run 中止、端口重配或移除事件，排除普通 RX。端口重配摘要同时携带行为 Model Profile、一级 `model_family` 和二级 `model_name` 的前后值。

### Diagnostics

全局 diagnostics 包含 uptime、WebSocket 连接数、journal metrics 和每端口 snapshot/subscriber lag。storage diagnostics 只返回 journal metrics。端口 diagnostics 返回 snapshot、subscriber count 和 lag events。

诊断读取不会主动探测目标设备，也不会写串口。

### Monitor HTTP

Monitor spec：

```json
{
  "port": "COM4",
  "matchers": [
    {"kind": "contains", "value": "watchdog"},
    {"kind": "regex", "value": "(?i)kernel panic|oops"}
  ],
  "start_cursor": {"epoch": "uuid", "after_seq": 100},
  "severity": "warning",
  "description": "观察复位",
  "debounce_ms": 250,
  "cooldown_ms": 30000,
  "duration_ms": 3600000
}
```

`matchers` 包含 1–16 个条件，每项是 `contains` 或 bounded `regex`。所有条件按 OR 计算；单项最多 4096 UTF-8 bytes，整组最多 16384 bytes。创建 body 是 `{request_id, spec}`，其中 `request_id` 同时作为幂等创建 ID。更新 body 是 `{spec, expected_revision}`；DELETE query 必须提供 `expected_revision`。

一个 incident 会记录 debounce window 内命中的去重条件和精确串口范围：

```json
{
  "id": "uuid",
  "incident_seq": 3,
  "monitor_id": "uuid",
  "port": "COM4",
  "daemon_epoch": "uuid",
  "seq_start": 820,
  "seq_end": 824,
  "wall_time_start_ns": 1770000000000000000,
  "wall_time_end_ns": 1770000000250000000,
  "severity": "warning",
  "matches": [
    {
      "index": 1,
      "matcher": {"kind": "regex", "value": "(?i)kernel panic|oops"}
    }
  ],
  "preview": "kernel panic ...",
  "evidence_cursor": {"epoch": "uuid", "after_seq": 819},
  "evidence_ref": "serial://server/ports/COM4/events?epoch=uuid&after_seq=819&through_seq=824",
  "created_wall_time_ns": 1770000000251000000
}
```

`matches[].index` 对应 `MonitorSpec.matchers` 的下标。HTTP 使用 `daemon_epoch`、`seq_start`、`seq_end` 三个原始字段；MCP `monitor_incidents` 将它们组合为 `serial_range`。

`serial_range` 也是 UI 定位证据的权威边界。当前 TUI 本地窗口不含完整范围时，客户端使用 incident 的 `daemon_epoch` 查询 `/api/v1/ports/{port}/events`，设置 `after_seq=seq_start-1` 和包含式 `through_seq=seq_end`。只有首尾与中间序号都完整连续、且没有重叠 gap 时才显示并高亮 RX 证据；这样旧后端周期和已从本地窗口淘汰的内容仍可从 journal 恢复，而 retention gap 不会被伪装成完整结果。

列表 query 可使用 `port` 和 `status`。incident query：`after_incident_seq`、`limit`、`include_acked`。incident 响应提供 `next_cursor`、`truncated`、`first_available_incident_seq` 和 `retention_gap`。

## WebSocket protocol v9

连接地址：`GET /api/v1/ws`。

### 二进制 envelope

每个 frame：

```text
[tag: u8][header_len: u32 big-endian][JSON header][raw payload]
```

| Tag | 方向 | 内容 |
|---|---|---|
| `0x01` | 双向 | JSON control message；raw payload 必须为空，client write bytes 位于 JSON 的 base64 `data` |
| `0x02` | server → client | RX `DataFrameHeader` + raw serial bytes |
| `0x03` | server → client | confirmed TX `DataFrameHeader` + raw serial bytes |

最大 JSON header 256 KiB，最大 payload 1 MiB；单次物理串口写入另有更小的后端边界。

### Hello 与 attach

客户端首先发送：

```json
{
  "type": "hello",
  "request_id": "uuid",
  "protocol_version": 9,
  "client_name": "serialctl",
  "actor_kind": "human"
}
```

`actor_kind` 为 `human`、`agent` 或 `script`；后端为连接生成 actor ID。成功时 server 发送 `welcome`：

```json
{
  "type": "welcome",
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "protocol_version": 9,
  "actor": {"id": "...", "label": "serialctl", "kind": "human"}
}
```

再发送 attach：

```json
{
  "type": "attach",
  "request_id": "uuid",
  "subscriptions": [
    {
      "port": "COM4",
      "cursor": {"epoch": "uuid", "after_seq": 800},
      "tail_events": 200
    }
  ]
}
```

每个端口依次收到 snapshot、可选 replay begin/timeline/gap、ready。之后实时 timeline 按序到达。detach 使用 `ports: ["COM4"]`。

### Client messages

control 消息使用 tagged JSON `type`：

```text
hello attach detach
acquire_control renew_control release_control cancel_acquire
request_run_start decide_run_start cancel_run_start
send_human_command write send_break
trigger_start trigger_status trigger_cancel
macro_start macro_status macro_cancel
start_run end_run checkpoint
acknowledge_run_context record_command_capture ping
```

端口相关消息全部包含 `port`。

`acquire_control` 的 `mode` 线形状仍为 `queue|takeover`，但 v7 不存在通用 Control waiter queue：端口被其他 actor 持有时，`queue` 立即返回 `conflict`，释放、过期和断连不会提升任何 waiter；`cancel_acquire` 只是兼容 no-op，返回 `acquire_cancelled {removed:false}`。Agent 的 `acquire_control` 和 Agent 的旧 `start_run` 均被拒绝；Agent 必须使用原子的 `request_run_start`。`takeover` 只允许 Human，并继续作为明确的强制接管操作。

### Agent Run-start 审批

Agent 请求的 `request_id` 同时是 approval ID 和幂等轮询 ID：

```json
{
  "type": "request_run_start",
  "request_id": "uuid",
  "port": "COM4",
  "label": "检查启动日志",
  "metadata": {},
  "ttl_ms": 60000
}
```

`ttl_ms` 是批准后 Agent Control lease 的 TTL，不是审批等待时长。审批寿命来自 daemon `[control].wait_timeout_ms`（默认 60 秒，硬上限 1 小时），权威截止时间是 DTO 的 `expires_wall_time_ns`。空闲端口在同一个 Slot turn 内原子授予 Agent Control 并创建 Run，返回 `run_start_granted {approval_id,lease,run}`；这里不会先暴露一个只有 Control、尚无 Run 的中间状态。若当前 holder 是 Human，则不写串口、不转移 Control，只返回：

```json
{
  "type": "run_start_pending",
  "approval": {
    "id": "uuid",
    "port": "COM4",
    "requester": {"id":"actor-agent","label":"agent","kind":"agent"},
    "required_approver": {"id":"actor-human","label":"serialctl","kind":"human"},
    "label": "检查启动日志",
    "metadata": {},
    "control_ttl_ms": 60000,
    "daemon_epoch": "uuid",
    "generation": 3,
    "expected_control_id": "uuid",
    "expected_fence": 7,
    "requested_wall_time_ns": 1700000000000000000,
    "expires_wall_time_ns": 1700000060000000000
  }
}
```

完全相同的 `request_run_start` 可用同一 `request_id` 幂等轮询；复用 ID 但更改 label、metadata 或 TTL 会被拒绝。只有 `required_approver` 指定的仍在线 Human holder 可以发送：

```json
{
  "type": "decide_run_start",
  "request_id": "new-human-rpc-uuid",
  "port": "COM4",
  "approval_id": "agent-request-uuid",
  "decision": "approve"
}
```

`decision` 为 `approve|deny`。批准时后端在同一个 Slot turn 重新核对 daemon epoch、generation、端口、活动 Run/Trigger 以及 Human Control ID/fence，然后原子撤销该 Human lease、授予 requester Agent lease并创建 Run。否决返回 `run_start_denied`；到期返回 `run_start_timed_out`；requester 可用 `cancel_run_start` 得到 `run_start_cancelled`。requester 或 approver 断连、holder/lease 改变、重配、串口断开/重开或 daemon shutdown 也会 fail closed 地取消：不创建 Run，不写任何字节。

其他 actor 已持有 Control、已有 Run/Trigger 或已有不同 pending approval 时立即冲突，不进入等待队列。

### Human command 与 read gate

Human 普通输入统一使用 `send_human_command`；它把授权判断和物理写入放在同一个 Slot turn，并且绝不排队：

```json
{
  "type": "send_human_command",
  "request_id": "uuid",
  "port": "COM4",
  "expected_generation": 3,
  "data": "dW5hbWUgLWEN",
  "operation_id": "uuid",
  "description": "人工查看系统版本"
}
```

- Control 空闲：原子授予该 Human Control 后写入，结果 mode 为 `owned` 并返回 `lease`；
- 当前 holder 正是该 Human：直接写入，结果 mode 为 `owned`；
- 当前 holder 是拥有活动 Run 的 Agent：不转移 lease，以 `cooperative` 模式写入并返回 `interfered_run_id`、`context_revision`；若 Trigger 正在运行，先在后端停止并收敛 Trigger，再接受 Human TX；
- 其他 holder、无匹配活动 Agent Run、端口/generation 不匹配或其他冲突：立即拒绝，绝不保存为延迟输入。

完整成功结果形状为：

```json
{
  "type": "human_command_accepted",
  "event_seq": 830,
  "mode": "cooperative",
  "interfered_run_id": "uuid",
  "context_revision": 2
}
```

`owned` 且本次从 idle 获得 Control 时还返回 `lease`；不适用的可选字段省略。

确认了至少一个 Human TX byte 后，活动 Agent Run 的 `run_context.revision` 才递增，`last_human_command_seq` 指向该 TX event。此时属于该 Run owner 的下一次 `write`、`send_break`、`macro_start` 或兼容 `trigger_start` 会在进入串口 writer 前返回 `user_read_required`，保证零字节写入。

只有该 Agent Run owner 可以确认最新 revision：

```json
{
  "type": "acknowledge_run_context",
  "request_id": "uuid",
  "port": "COM4",
  "run_id": "uuid",
  "revision": 2,
  "through_seq": 830
}
```

后端要求 `revision` 精确等于最新 revision，且 `through_seq >= last_human_command_seq` 且不超过当前 head，成功返回 `run_context_acknowledged {context}`。MCP adapter 只在 `read(scope=tail|continue)` 的实际响应包含同一 daemon epoch 下、带 `human_command=true` 的精确 TX event，并且返回 cursor 已覆盖该 seq 后发送这个 ACK。若 `wait` 已把普通 live cursor 推过该 TX，下一次 live read 会从 `last_human_command_seq-1` 临时恢复；若 ring 已淘汰该事件则返回明确 gap，仍不 ACK。`wait`、`search`、`read(scope=archive)`、只移动游标或读取了不含该 TX 的 live window 都不会清除 gate。

普通 physical write 的关键字段：

```json
{
  "type": "write",
  "request_id": "uuid",
  "port": "COM4",
  "control_id": "uuid",
  "fence": 7,
  "data": "dW5hbWUgLWEN",
  "operation_id": "uuid",
  "expected_run_id": "uuid",
  "pacing": {"chunk_size": 1, "chunk_delay_ms": 1},
  "description": "查看系统版本",
  "command_capture_matchers": [
    {"kind": "shell_prompt", "value": "root@router:~# "}
  ],
  "command_sequence": null,
  "sequence_precondition": {
    "cursor": {"epoch": "uuid", "after_seq": 811},
    "expected_generation": 3,
    "expected_tx_offset": 300
  }
}
```

`data` 是待写 bytes 的 base64 表示。后端在物理动作边界检查 Control/fence、Run、Human read gate、generation、sequence precondition 与 pacing budget。成功 result 是 `write_accepted` 并返回 TX event seq；确认后的同一批 bytes 再通过 `0x03` TX data frame 分发。v7 Human 客户端不借用 Agent fence，也不使用旧 `write.cooperative` 逃生路径，而是使用 `send_human_command`。

`send_break` 发送 UART line condition，不是字节；duration 为 1–5000 ms。

### 权威命令捕获

Agent 在 command capture 得到终态后发送：

```json
{
  "type": "record_command_capture",
  "request_id": "uuid",
  "port": "COM4",
  "report": {
    "daemon_epoch": "uuid",
    "generation": 3,
    "run_id": "uuid",
    "operation_id": "uuid",
    "tx_event_seq": 812,
    "evidence_from_seq": 812,
    "evidence_through_seq": 826,
    "completion": "prompt",
    "completion_detail": "root@router:~# ",
    "confidence": "high"
  }
}
```

`seriald` 不直接信任客户端的范围。它要求 epoch 与当前 daemon 一致、范围包含 `tx_event_seq` 且不超出 head，并从 replay ring 证明范围内没有 gap、没有 generation 边界；指定 TX 必须是同一 Agent actor、Run、operation 和 generation 的 confirmed `tx`。验证通过后，daemon 从事件本身派生 TX/RX stream offsets，持久化 `command_capture_completed`，并返回 `command_capture_recorded {capture}`。即使 Run 已结束或串口已断开，只要同一 daemon epoch 的证据仍在 ring 中且范围不跨 generation，仍可补记；证据已淘汰、不连续或身份不符时拒绝，不能伪造精确边界。

### Server messages

```text
welcome snapshot replay_begin ready timeline
result error gap lagged
```

`result` 通过 `request_id` 对应请求。v7 新增或关键结果为：

```text
run_start_pending run_start_granted run_start_denied
run_start_timed_out run_start_cancelled
human_command_accepted run_context_acknowledged
command_capture_recorded
```

其余结果包括 Control grant/renew/release、write accepted、Break sent、Trigger state、非 Agent Run state、checkpoint 和 pong。`control_queued` 仅保留在线 enum 中供反序列化兼容；v7 daemon 不产生该结果。

`error`：

```json
{
  "type": "error",
  "request_id": "uuid",
  "code": "sequence_boundary_changed",
  "message": "...",
  "retryable": true
}
```

稳定 error code：

```text
bad_request not_found conflict
control_required stale_fence port_offline cursor_ahead
sequence_boundary_changed user_read_required
resource_exhausted idempotency_expired
config_revision_mismatch profile_change_busy
port_not_found port_busy port_access_denied port_io
break_unsupported regex_invalid query_budget_exceeded
unavailable internal
```

`user_read_required` 是 daemon 的稳定线协议错误：最新 Human TX 尚未被 Agent Run owner 的 ACK 覆盖，检查发生在物理 action 前，保证本请求零字节。MCP 将它投影为 model-facing `user_command_used`，并要求先完成包含该 TX 的 live `read`。`retryable` 只说明错误类别可能在状态改变后恢复，不代表客户端应自动重放物理动作。连接丢失、timeout、partial write 或结果未确认时必须先观察时间线。

## 人工历史与 Macro（v8 引入，v9 增强）

新增 HTTP 目录接口（与串口物理写入分离）：

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/api/v1/history/commands` | 平台共用人工 LINE 历史，支持 prefix、contains、before_revision、limit；返回 server_id、revision、entries、next_before_revision、warning |
| GET | `/api/v1/macros` | 默认共享摘要；query/offset/limit 分页，include_drafts 显式包含草稿，id 返回完整 definition |
| POST | `/api/v1/macros` | 保存完整定义；新建 shared 默认 false，更新要求 expected_revision；版本冲突 409、无此宏 404、脚本/参数错误 400 |

人工 `send_human_command` 可额外提供 `"input":{"command":"status"}`。daemon 要求其 command + 有效 EOL 与实际 data 一致，且有 operation_id；仅确认完整发送后的非空 Human LINE 命令进入建议库。RAW/控制键省略 input；Agent/宏展开步骤不允许借此混入人工历史。建议库最多 10000 条不同命令，溢出或存储故障返回 warning；它不是原始 TX 审计的替代品。

Macro 控制消息：

```json
{
  "type": "macro_start",
  "request_id": "uuid",
  "port": "COM4",
  "control_id": "uuid",
  "fence": 3,
  "daemon_epoch": "uuid",
  "generation": 1,
  "operation_id": "稳定的执行 UUID",
  "expected_run_id": "Agent 当前 Run UUID；Human 为 null",
  "sequence_precondition": null,
  "spec": {
    "macro_id": "enter_uboot",
    "revision": 1,
    "args": {"interval_ms": 50},
    "timeout_seconds": 15
  }
}
```

也可使用 spec.script + description 代替 macro_id/revision/args。`macro_status` 只需要 request_id/port/execution_id；`macro_cancel` 另需 control_id/fence，且只能停止当前调用者拥有的执行。内部结果分别为 macro_started/macro_status/macro_cancelled，均包含 execution。

execution.id 等于 operation_id；状态为 running/stopping/succeeded/timed_out/cancelled/interrupted_by_user/failed。结果包含 epoch、generation、owner、Run、固定宏 revision、时间、源码行列、确认 TX 数量/字节数、first_seq/through_seq、message 和 outcome_uncertain。只有 Slot actor 提交完成并释放宏 guard 后，status 才对外暴露终态。取消先停止后续动作，再确认在途写；stopping 不是完成。

v9 将 `writes`（主机确认发送次数）、`input_verified_writes`（完整设备命令回显确认次数）、`send_only_writes`（显式不检查回显的次数）分开。`cmd(text)` 默认在继续前检查完整回显，最长 2 秒；`cmd(text, "send_only")` 仅确认 TX。两者均追加 Profile EOL，不判断命令执行结果。历史磁盘日志降级不会单独阻止宏，但实时 RX 缺口/订阅溢出/断连仍停止；晚到落盘确认允许日志状态恢复，并不抹去历史缺口。

宏编译、参数、版本、Profile、适用机型和审计记录大小在首次 TX 前校验；源码/参数/Profile 快照写入 started checkpoint，每个 TX 附 macro_execution_id/macro_id/macro_revision/macro_line/macro_column，结束写入 completed checkpoint。同一 daemon epoch 内复用 operation_id 不会重放物理动作；结果状态缓存有界，无法恢复时返回 NotFound/过期错误，不表示设备未执行。

单口宏与其他 Agent 物理写互斥，不隐式排队；原有 Control、Run、generation、fence 和 Human read gate 继续有效。Human Enter/RAW/Ctrl-D 停止后续宏步骤，保留原 Agent Run 的人工干预门槛。完整语言/参数规则见 [Macro Script v1](./MACRO_SCRIPT_V1.md)。

## Run、Control 与兼容 Trigger 语义

Control lease 绑定一个 actor、周期、generation 和 fence。续租不改变物理所有权；Human Takeover 产生新 fence 并使旧写入失效。不存在 Control queue，也不存在 Agent `AcquireControl` bypass。

Agent Run 只能由 `request_run_start` 原子创建：idle 直接 grant+Run；Human holder 则必须由该 holder 批准后原子 transfer+Run。Run 是审计与证据区间，不重置设备；结束或中止形成明确 timeline event。Human/script 的旧 `start_run` 仍要求自己已持有 Control，Agent 使用它会被拒绝。

Agent Run 的 `run_context` 是 daemon-authoritative revision gate。Human cooperative TX 不窃取 Agent lease，但会把所有后续 Agent physical action 锁在 `user_read_required`，直到精确 live evidence 被 ACK；结束/中止 Run 会删除该 context。

旧 Trigger wire 类型与历史解析保留兼容，但不再作为公开 MCP 工具。其 spec 包含 optional initial write、optional start literal、action bytes、interval、stop literals、timeout、max fires 和 optional pacing。所有旧 Trigger 写入仍走同一 Control/fence/Run/read-gate/confirmed TX 路径；与新 Macro 互斥。

## MCP Streamable HTTP

`serial-mcp --listen 127.0.0.1:3211`（默认单机）提供：

```text
GET  /health
POST /mcp
```

`GET /health` 只用于统一启动器确认 port 3211 上 adapter 的进程身份和它所连接的 `seriald`，不是 MCP host 的 session 或工具接口。当前响应示例：

```json
{
  "status": "ok",
  "service": "serial-mcp",
  "protocol_version": 9,
  "pid": 12345,
  "seriald_endpoint": "http://127.0.0.1:3210",
  "seriald_server_id": "uuid",
  "seriald_daemon_epoch": "uuid"
}
```

统一启动器只在 `service`、`protocol_version=9` 和完整 seriald endpoint/server/epoch 身份都与当前活动端点一致时复用该 adapter。启动器创建新 adapter 时还用 `pid` 区分并发启动中的 owner 与 loser。HTTP adapter 的 WebSocket session 固定为该启动身份；同一 endpoint 若返回不同 server/epoch，session 会拒绝跨 daemon 重连，adapter 重启后才会发布新的 `/health` 身份。

统一启动时，MCP listener 使用已验证 `active-endpoint.json.address` 的精确 IP，并固定使用 port 3211；只有 seriald 原本是 IPv4/IPv6 通配 bind 时，活动地址才先转换为 `127.0.0.1`/`::1`。因此 host-only 场景会得到例如 `192.168.56.109:3211`，不会悄悄改成 loopback，也不会绑定所有接口。

HTTP MCP 没有认证和 TLS。`--listen` 必须是一个精确的单播接口地址：拒绝 `0.0.0.0`、`::`、multicast 和 IPv4 broadcast；非 loopback 只适用于可信 host-only VM 网络，启动时会警告，并要求主机防火墙限制访问。不要把它暴露到 LAN、公共网络或不可信 bridge。

实现是 sessionless JSON-RPC：

- request 返回一个 JSON-RPC response；
- notification 和 cancellation notification 返回 HTTP 202；
- `GET /mcp` 返回 method not allowed；
- 支持 MCP protocol `2024-11-05`、`2025-03-26`、`2025-06-18`、`2025-11-25`；
- `MCP-Protocol-Version` 若存在必须是支持值；
- listener 必须是一个精确 loopback 或可信 host-only IP；
- `Origin` 省略时允许；存在时必须是无 credentials/path/query/fragment 的 `http://` origin，端口与 listener 相同；host 必须是 listener 的精确 numeric IP，只有 listener 为 loopback 时额外接受 `localhost`。

`Origin` 检查只降低浏览器跨站请求风险，不替代认证；非浏览器客户端可以不发送该 header。

initialize 响应声明 `tools.listChanged=false`。`tools/list` 返回固定 18 项：

```text
devices model_identity_set read command command_sequence signal macro_list macro_save macro_run wait
search monitor_start monitor_list monitor_status monitor_incidents monitor_stop
run_start run_end
```

`tools/call` 的成功与工具错误都放在 MCP tool result 中，结构化值位于 `structuredContent`，紧凑 JSON 文本位于 `content[0].text`。`devices` 是唯一发现工具，只公开 `port`、两级机型身份、Agent 所需状态和有效 Shell/U-Boot 提示符，不公开 Profile 名、Transport/UART、EOL/echo 或写入节奏。`run_start` 在 idle 时直接返回 Run，Human holder 存在时等待显式审批；否决、超时、取消或任一相关连接断开都不返回 Run。`run_end` 以可选 `outcome=completed|aborted` 区分正常完成与异常终止，默认 `completed`；正常完成后立即尝试释放 Control，异常终止只在 `ControlReleased` 权威确认后成功。

`command`/`command_sequence` 完成后必须取得 daemon 的 `command_capture_recorded`，才能把捕获范围作为权威历史交付。若 Human command gate 未确认，物理工具的结构化错误是 `user_command_used`、`no_bytes_written=true`；只有包含准确 Human TX 的 live `read(scope=tail|continue)` 会返回并完成 acknowledgement，`wait` 和 archive read 不会。

stdio transport 使用每行一个 JSON-RPC frame。stdout 只写 MCP frame，诊断写 stderr。并发 request 的响应次序可以与请求次序不同，但每个 frame 由一个 writer 完整写出。

## 幂等、取消与结果确定性

- `request_id` 标识协议请求；后端缓存近期已执行写入的结果。
- 相同请求重试可返回缓存；已执行但超出幂等缓存的 ID 被拒绝，避免再次写入。
- MCP cancellation 只中断 `devices`、`read`、`wait`、`search`、`monitor_list`、`monitor_status` 和 `monitor_incidents`。
- 物理 action、Run transition 和 Monitor mutation 会继续收敛到结果。
- 明确的前置条件拒绝会说明零字节写入；transport loss、timeout 和 partial write 不能据此自动重试。
- replay ring 淘汰、journal retention 与周期变化都通过 gap 显式表达。
