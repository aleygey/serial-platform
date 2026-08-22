# Serial Platform Protocol v5

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
  "protocol_version": 5,
  "pid": 12345
}
```

记录只是发现入口。客户端必须调用其中 endpoint 的 `GET /api/v1/health`，并确认 `status=ok`、`server_id`、`daemon_epoch` 和 `protocol_version` 全部与记录一致后才能复用。`address` 来自实际 listener 地址，通配 bind 会转换为本机可连接的 loopback。失效记录不会阻止新实例取得 OS lock 并覆写；拥有进程正常退出时只清理与自身 `server_id`、`daemon_epoch` 一致的记录。

## HTTP v1

`/api/v1` 是 HTTP 路由命名空间，不是跨组件兼容代际。当前 HTTP DTO、WebSocket 握手和客户端兼容检查统一使用 `protocol_version=5`；路由仍保持 `/api/v1/...`。

### 路由

| Method | Path | 用途 |
|---|---|---|
| `GET` | `/api/v1/health` | 进程健康、server/epoch、uptime、protocol version |
| `GET` | `/api/v1/status` | 全局状态、config revision、所有配置端口 snapshot |
| `GET` | `/api/v1/ports` | 枚举主机可见的 OS 串口 |
| `PUT` | `/api/v1/config/ports` | 原子替换端口配置 |
| `GET` / `PUT` | `/api/v1/config/transport-profiles` | 读取/原子替换 Transport Profile catalog |
| `GET` / `PUT` | `/api/v1/config/model-profiles` | 读取/原子替换 Model Profile catalog |
| `GET` | `/api/v1/archives` | 枚举保留的端口/周期日志 |
| `GET` | `/api/v1/diagnostics` | 后端、连接、journal 与所有端口诊断 |
| `GET` | `/api/v1/diagnostics/storage` | journal 用量与 writer health |
| `GET` | `/api/v1/ports/{port}/diagnostics` | 一个端口的权威状态与 subscriber 指标 |
| `GET` | `/api/v1/ports/{port}/tail` | 从有界 replay ring 读取实时 tail/continuation |
| `GET` | `/api/v1/ports/{port}/recent-activity` | MCP 操作间的紧凑第三方活动 |
| `GET` | `/api/v1/ports/{port}/events` | 有界 journal 查询 |
| `GET` / `POST` | `/api/v1/monitors` | 列表/创建 Monitor |
| `GET` / `PUT` / `DELETE` | `/api/v1/monitors/{monitor_id}` | 读取/更新/停止 Monitor |
| `GET` | `/api/v1/monitors/{monitor_id}/incidents` | 分页读取 incident |
| `POST` | `/api/v1/monitors/{monitor_id}/incidents/{incident_id}/ack` | 确认 incident |
| `GET` | `/api/v1/ws` | WebSocket protocol v5 |

### Health

```json
{
  "status": "ok",
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "uptime_ms": 1200,
  "protocol_version": 5
}
```

### Status 与 PortSnapshot

`GET /api/v1/status`：

```json
{
  "server_id": "uuid",
  "daemon_epoch": "uuid",
  "protocol_version": 5,
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
    "model_profile": "TL-AS7230",
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
  "active_run": null,
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

`session_state`：`disabled`、`waiting_for_port`、`opening`、`online`、`backoff`、`stopping`。

### 配置 DTO

`PUT /api/v1/config/ports`：

```json
{
  "ports": [
    {
      "port": "COM4",
      "transport_profile": "uart-115200",
      "model_profile": "TL-AS7230",
      "model_name": "TL-AS7230-W 1.0",
      "enabled": true
    }
  ],
  "source": "human:desktop",
  "expected_revision": 12
}
```

响应返回更新后的 `ports` snapshots 与新 `config_revision`。`source` 是 1–128 字符的审计标签。

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
  "name": "TL-AS7230",
  "model_names": [
    "TL-AS7230-W 1.0",
    "TL-AS7230-F4GE 1.0"
  ],
  "shell_prompt": "root@router:~# ",
  "uboot_prompt": "=> ",
  "write_eol": "\r",
  "echo": "auto",
  "write_chunk_size": 1,
  "write_chunk_delay_ms": 1
}
```

`ModelProfile.name` 是可复用的机型系列/交互 Profile 名称，`model_names` 是该系列允许绑定的具体机型名。端口的 `model_name` 必须来自当前 `model_profile.model_names`；未绑定机型 Profile 时不能保留具体机型名。两者都按原字符串比较和显示。

Profile catalog GET 响应是 `{profiles, config_revision}`；PUT body 是 `{profiles, expected_revision?}`，表示完整替换。

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
run_started run_ended run_aborted
trigger_started trigger_completed trigger_cancelled trigger_failed
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

端口机型变化的 `port_reconfigured` metadata 包含 `source`、`previous_model_profile`、`new_model_profile`、`previous_model_name` 和 `new_model_name`。

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

`GET /api/v1/ports/{port}/recent-activity` 必须同时提供 `epoch`、`after_seq`、`through_seq`。它只从 ring 返回最多 32 条与协同上下文有关的 TX、Control、Run 中止、端口重配或移除事件，排除普通 RX。端口重配摘要同时携带机型 Profile 和具体机型名的前后值。

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

## WebSocket protocol v5

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
  "protocol_version": 5,
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
  "protocol_version": 5,
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
write send_break
trigger_start trigger_status trigger_cancel
start_run end_run checkpoint ping
```

端口相关消息全部包含 `port`。

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
  },
  "cooperative": false
}
```

`data` 是待写 bytes 的 base64 表示。后端在物理动作边界检查 Control/fence、Run、generation、sequence precondition 与 pacing budget。成功 result 是 `write_accepted` 并返回 TX event seq；确认后的同一批 bytes 再通过 `0x03` TX data frame 分发。

`send_break` 发送 UART line condition，不是字节；duration 为 1–5000 ms。

### Server messages

```text
welcome snapshot replay_begin ready timeline
result error gap lagged
```

`result` 通过 `request_id` 对应请求。命令结果类型包括 Control grant/queue/renew/release、write accepted、Break sent、Trigger state、Run state、checkpoint 和 pong。

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
sequence_boundary_changed resource_exhausted idempotency_expired
config_revision_mismatch profile_change_busy
port_not_found port_busy port_access_denied port_io
break_unsupported regex_invalid query_budget_exceeded
unavailable internal
```

`retryable` 只说明错误类别可能在状态改变后恢复，不代表客户端应自动重放物理动作。连接丢失、timeout、partial write 或结果未确认时必须先观察时间线。

## Run、Control 与 Trigger 语义

Control lease 绑定一个 actor、周期、generation 和 fence。续租不改变物理所有权；Takeover 产生新 fence 并使旧写入失效。

Run 只能在当前 Control 上开始。Run 是审计与证据区间，不重置设备。结束或中止形成明确 timeline event。

Trigger spec 包含 optional initial write、optional start literal、action bytes、interval、stop literals、timeout、max fires 和 optional pacing。所有 Trigger 写入仍走同一 Control/fence/Run/confirmed TX 路径。

## MCP Streamable HTTP

`serial-mcp --listen 127.0.0.1:3211` 提供：

```text
GET  /health
POST /mcp
```

`GET /health` 只用于统一启动器确认固定 loopback 端口的进程身份和它所连接的 `seriald`，不是 MCP host 的 session 或工具接口。当前响应示例：

```json
{
  "status": "ok",
  "service": "serial-mcp",
  "protocol_version": 5,
  "pid": 12345,
  "seriald_endpoint": "http://127.0.0.1:3210",
  "seriald_server_id": "uuid",
  "seriald_daemon_epoch": "uuid"
}
```

统一启动器只在 `service`、`protocol_version=5` 和完整 seriald endpoint/server/epoch 身份都与当前活动端点一致时复用该 adapter。启动器创建新 adapter 时还用 `pid` 区分并发启动中的 owner 与 loser。HTTP adapter 的 WebSocket session 固定为该启动身份；同一 endpoint 若返回不同 server/epoch，session 会拒绝跨 daemon 重连，adapter 重启后才会发布新的 `/health` 身份。

实现是 sessionless JSON-RPC：

- request 返回一个 JSON-RPC response；
- notification 和 cancellation notification 返回 HTTP 202；
- `GET /mcp` 返回 method not allowed；
- 支持 MCP protocol `2024-11-05`、`2025-03-26`、`2025-06-18`、`2025-11-25`；
- `MCP-Protocol-Version` 若存在必须是支持值；
- 仅允许监听 `127.0.0.1`；
- `Origin` 若存在，只接受同端口的 `localhost` 或 `127.0.0.1`。

initialize 响应声明 `tools.listChanged=false`。`tools/list` 返回 19 项；`tools/call` 的成功与工具错误都放在 MCP tool result 中，结构化值位于 `structuredContent`，紧凑 JSON 文本位于 `content[0].text`。

stdio transport 使用每行一个 JSON-RPC frame。stdout 只写 MCP frame，诊断写 stderr。并发 request 的响应次序可以与请求次序不同，但每个 frame 由一个 writer 完整写出。

## 幂等、取消与结果确定性

- `request_id` 标识协议请求；后端缓存近期已执行写入的结果。
- 相同请求重试可返回缓存；已执行但超出幂等缓存的 ID 被拒绝，避免再次写入。
- MCP cancellation 只中断纯观察工具。
- 物理 action、Run transition 和 Monitor mutation 会继续收敛到结果。
- 明确的前置条件拒绝会说明零字节写入；transport loss、timeout 和 partial write 不能据此自动重试。
- replay ring 淘汰、journal retention 与周期变化都通过 gap 显式表达。
