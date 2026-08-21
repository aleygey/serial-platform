# Serial MCP Tools

`serial-mcp` 把 Serial Platform 收敛为 19 个 MCP 工具。完整机器可读 schema 由可执行文件直接生成：

```sh
serial mcp --dump-tools
# 或
serial-mcp --dump-tools
```

所有设备选择字段统一为 `port`，例如 `COM4` 或 `/dev/cu.usbserial-210`。工具不要求 Agent 传 request ID、Control ID、fence、generation、operation ID、pacing 或续租参数。

## Transport

### 统一启动的 HTTP MCP

运行：

```sh
serial
```

会在下面地址启动 sessionless Streamable HTTP MCP：

```text
http://127.0.0.1:3211/mcp
```

MCP host 对该 URL 发送 JSON-RPC `POST`。notification 返回 HTTP 202；没有持久 HTTP session 或 SSE GET channel。

### stdio MCP

MCP host 也可以直接启动：

```sh
serial mcp --actor-label codex:workstation
```

stdio 每行一个 JSON-RPC frame；stdout 只包含 MCP，运行信息输出到 stderr。`--endpoint` 或 `SERIALD_ENDPOINT` 可覆盖默认后端 `http://127.0.0.1:3210`。

支持 MCP protocol：`2024-11-05`、`2025-03-26`、`2025-06-18`、`2025-11-25`。

## 19 个工具

| Tool | Required | Optional | 作用 |
|---|---|---|---|
| `devices` | — | `port` | 读取端口、Profile、生效参数、连接、Control、Run、Trigger 和 cursor head |
| `model_profiles` | — | `port` | 读取 Model Profile catalog 与端口绑定 |
| `model_profile_set` | `port`, `profile` | — | 创建/更新一个完整 Model Profile 并绑定；`profile:null` 解绑 |
| `read` | `port` | `scope`, `epoch`, `after_seq`, `through_seq` | 从实时 ring 或指定历史周期读取有界文本 |
| `command` | `run_handle`, `command`, `description` | `expect`, `regex`, `timeout_seconds` | 追加有效 EOL、写入、捕获 RX 并保留任务说明 |
| `command_sequence` | `run_handle`, `description`, `steps` | 每步 matcher/timeout | 一次完成 1–8 步已知依赖交互 |
| `input` | `run_handle`, `text` | — | 不追加 EOL，写入精确 UTF-8 bytes |
| `signal` | `run_handle`, `signal` | Break 的 `duration_ms` | Ctrl-C/D/Z byte 或 UART Break |
| `trigger` | `run_handle`, `action` | `kickoff`, start/stop matcher 与硬上限 | 在后端执行一次有界低延迟反应 |
| `wait` | `run_handle` | `expect`, `regex`, `timeout_seconds` | 从 live cursor 等待 RX 边界 |
| `search` | `port`, `query` | `regex`, `scope`, `run_id`, `epoch`, `after_seq` | 搜索当前 Run、当前 cursor 或归档 |
| `monitor_start` | `port` + `contains`/`regex` 二选一 | `description`, `idempotency_key` | 创建持久 Monitor并立即返回 |
| `monitor_list` | — | `port` | 列出 Monitor |
| `monitor_status` | `monitor_id` | — | 读取一个 Monitor 的权威状态 |
| `monitor_incidents` | `monitor_id` | `after` | 读取 incident tail 或向前分页 |
| `monitor_stop` | `monitor_id` | — | 停止未来匹配，保留 incident |
| `run_start` | `port`, `label` | — | 排队获取 Control，开始 Run，返回 `run_handle` |
| `run_end` | `run_handle` | — | 正常结束 Run 并 best-effort 释放 Control |
| `release` | 见下文 | — | 释放无 Run Control，或显式中止当前 adapter 的 Run |

## 标准工作流

1. 调用 `devices`，明确选择 `port`，核对 `model_profile` 与实际设备。
2. 调用 `run_start`，在本次 Agent 工作流中保存返回的 `run_handle`。`run_id` 只用于审计和查询。
3. 普通 Shell/Bootloader 命令使用 `command`；已知的多轮依赖交互使用一次 `command_sequence`。
4. 需要补充观察时使用 `wait`、`read`、`search` 或 Monitor。
5. 在最终 Agent 回复前调用 `run_end`。只有明确把活动 Run 交给后续 Agent 工作流时才保持它打开。

Run 只界定证据，不复位设备，也不证明当前状态干净。Agent 应通过串口、其他设备界面或人工确认实际机型和状态。

## Run handle 与回收

`run_start` 的典型结果：

```json
{
  "port": "COM4",
  "run_id": "uuid",
  "run_handle": "22-character-handle",
  "cursor": {"epoch": "uuid", "after_seq": 120},
  "cleanup_required": "Call run_end ..."
}
```

`run_handle` 固定 22 个 URL-safe 字符，仅由当前 `serial-mcp` 进程解析。所有 Run-scoped 工具只需要这一个值，因此小模型不必在每次调用中同时复制端口、Run ID 和其他底层状态。

默认 `orphan_run_timeout_seconds=1800`。`0` 表示不限时，其他值至少 300 秒。该设置只处理最后一个 Run-scoped 调用后无人继续的情况；正常工作流仍调用 `run_end`。

设置来源优先级：

1. `--orphan-run-timeout-seconds`
2. `SERIAL_MCP_ORPHAN_RUN_TIMEOUT_SECONDS`
3. 共享 `serialctl.toml`
4. 默认 1800 秒

已运行的 adapter 不热加载该值。

## `devices`

Input：

```json
{}
```

或：

```json
{"port":"COM4"}
```

结果包含 `daemon_epoch`、`config_revision`、`ports` 和设备选择提示。每个端口项包含配置、session state、generation、head/ring cursor、有效 Transport/Model 参数、Control、active Run、active Trigger、logging 和 RX overflow。

指定未知 `port` 会失败；工具不会静默选择另一个端口。

## `model_profiles`

```json
{"port":"COM4"}
```

`port` 可省略。结果：

```json
{
  "config_revision": 12,
  "profiles": [],
  "bindings": [
    {"port":"COM4","model_profile":"TL-AS7230 1.0"}
  ],
  "port_filter": "COM4"
}
```

## `model_profile_set`

创建或替换一个完整 Profile 并绑定端口：

```json
{
  "port": "COM4",
  "profile": {
    "name": "TL-AS7230 1.0",
    "shell_prompt": "root@router:~# ",
    "uboot_prompt": "=> ",
    "write_eol": "\r",
    "echo": "auto",
    "write_chunk_size": 1,
    "write_chunk_delay_ms": 1
  }
}
```

Profile 字段：

- `name`：必填，不含首尾空白，1–64 UTF-8 bytes；
- `shell_prompt` / `uboot_prompt`：string 或 null，非空时最大 4096 UTF-8 bytes 且不能包含 NUL；
- `write_eol`：`""`、`"\r"`、`"\n"`、`"\r\n"` 或 null；
- `echo`：`on` / `off` / `auto` / null；
- `write_chunk_size`：正整数或 null；
- `write_chunk_delay_ms`：0–10000 或 null。

解绑：

```json
{"port":"COM4","profile":null}
```

结果返回 `previous_model_profile`、当前 `model_profile` 与新 `config_revision`。Profile 名称按输入原样保存。

## `read`

```json
{"port":"COM4","scope":"tail"}
```

scope：

- `tail`：默认；直接读取 replay ring 最近最多 200 events；
- `continue`：从 adapter 为该端口记住的 live cursor 继续，最多 1000 events；
- `archive`：必须给 `epoch`，可给 `after_seq` 和包含式 `through_seq`，最多 1000 events / 512 KiB。

`tail` 和 `continue` 不做 journal segment discovery，因此串口运行很久、journal 很大时，普通读取仍保持有界。ring 淘汰或后端重启以 gap/truncation 返回；需要旧内容时显式使用 `archive`。

结果主要字段：

```json
{
  "port": "COM4",
  "scope": "tail",
  "source": "live_ring",
  "text": "...",
  "cursor": {"epoch":"uuid","after_seq":812},
  "truncated": false,
  "gap": false
}
```

## `command`

```json
{
  "run_handle": "abcdefghijklmnopqrstuv",
  "command": "uname -a",
  "description": "查看内核版本",
  "expect": "root@router:~# ",
  "timeout_seconds": 10
}
```

- `command` 最大 4096 字符；空字符串表示只发送 EOL。
- `description` 必填，1–256 UTF-8 bytes，进入持久命令历史。
- `expect` 与 `regex` 互斥。
- timeout 是 1–120 秒，默认 10 秒。

完成边界优先级：

1. 显式 `regex`；
2. 显式 `expect`；
3. 当前 Model Profile 的 Shell/U-Boot prompt；
4. 没有 prompt 时，收到至少一个 post-TX RX 后等待 quiet boundary。

命令 TX 持久化实际使用的 `command_capture_matchers`：显式 matcher 一个，Profile fallback 0–2 个，quiet 不添加。TUI 与 App 用它定位 RX 区域，后来修改 Profile 不会改变旧命令的匹配定义。

典型结果：

```json
{
  "port": "COM4",
  "write": "confirmed",
  "capture": "prompt",
  "execution": "unknown",
  "confidence": "high",
  "text": "Linux ...\nroot@router:~# ",
  "description": "查看内核版本",
  "truncated": false,
  "gap": false,
  "interfered": false,
  "cursor": {"epoch":"uuid","after_seq":812},
  "run_handle": "abcdefghijklmnopqrstuv",
  "run_open": true
}
```

`execution` 保持 `unknown`：看到提示符只证明捕获边界出现，不证明 shell exit status。需要确定退出码时让命令输出唯一 sentinel。

effective `echo=on` 时，adapter 识别并移除设备自身的 command+EOL echo。缺少应有回显、gap、第三方 TX、timeout 或 truncation 会降低 `confidence` 并添加 `warnings`。不确定的物理写入不自动重试。

## `command_sequence`

用于 Agent 已经知道后续步骤、但每一步必须等待设备提示的交互。例如登录：

```json
{
  "run_handle": "abcdefghijklmnopqrstuv",
  "description": "登录设备控制台",
  "steps": [
    {
      "command": "admin",
      "description": "输入账号",
      "expect": "Password:",
      "timeout_seconds": 10
    },
    {
      "command": "admin123",
      "description": "输入密码",
      "expect": "root@router:~# ",
      "timeout_seconds": 10
    }
  ]
}
```

约束：

- 1–8 步；
- 每步必须有 `command` 和 `description`；
- 每个非最终步骤必须有且仅有 `expect` 或 `regex`；
- 最终步骤可以使用显式 matcher，也可以回落到 Profile prompt/quiet；
- 每步 timeout 1–120 秒，默认 10；有效 timeout 总和最多 300 秒；
- 每步含 EOL 后最多 4096 bytes；完整计划最多 32768 bytes。

adapter 在写第一步前验证完整计划，并为整个 sequence 持有该端口的 mutation lock。只有当前步骤到达 matcher 才发送下一步；timeout、disconnect、gap、Run/Control 丢失或上下文变化都会停止所有剩余写入。工具不分支、不循环、不重试。

每个已确认步骤保留独立 TX、description 和 matcher，整体由 `sequence_id` 与 sequence description 分组。结果包含 `requested_steps`、`completed_steps`、逐步结果、最终 cursor 和 Run 状态。

## `input`

```json
{"run_handle":"abcdefghijklmnopqrstuv","text":"exact bytes"}
```

写入 `text` 的 UTF-8 bytes，不追加 Model Profile EOL。1–4096 bytes。结果返回 `write=confirmed`、`kind=input`、bytes 和 cursor。

## `signal`

```json
{"run_handle":"abcdefghijklmnopqrstuv","signal":"ctrl_c"}
```

| `signal` | 动作 |
|---|---|
| `ctrl_c` | `0x03` |
| `ctrl_d` | `0x04` |
| `ctrl_z` | `0x1a` |
| `break` | UART Break line condition |

Break 默认 250 ms，可用 `duration_ms` 设置 1–5000 ms。其他 signal 不接受 duration。Break 不是 NUL 或任何编码字节。

## `trigger`

```json
{
  "run_handle": "abcdefghijklmnopqrstuv",
  "kickoff": {"text":"reboot","eol":"\r"},
  "action": {"text":" ","eol":""},
  "interval_ms": 20,
  "stop_contains": ["=> "],
  "timeout_ms": 5000,
  "max_fires": 250
}
```

- `kickoff` 可省略；
- `action` 必填；
- 普通调用省略 `start_contains`，kickoff 确认后立即允许 action；
- `start_contains` 只用于必须等待 live RX gate 的场景；
- `stop_contains` 最多 8 个 literal；
- interval 5–1000 ms；timeout 100–30000 ms；max fires 1–1000。

Trigger 在后端内调度，避免每个 action 都经过一次 Agent 往返。结果的 `matched=true` 只说明 stop literal 被观察到，不证明更大的业务流程成功。

## `wait`

```json
{
  "run_handle": "abcdefghijklmnopqrstuv",
  "regex": "ready|root@.*# ",
  "timeout_seconds": 30
}
```

`expect` 与 `regex` 互斥；均省略时使用 Profile prompt，仍没有 prompt 时使用 quiet boundary。wait 从 `run_start`、`command`、`command_sequence` 或上次 wait 保存的 live cursor 开始，避免两个调用之间的 RX 丢失窗口。

## `search`

```json
{
  "port": "COM4",
  "query": "kernel panic",
  "regex": false,
  "scope": "current_run"
}
```

scope：

- `current_run`：默认；只搜索当前 Run；
- `current_cursor`：从显式或 adapter 记住的 cursor 开始；
- `archive`：必须显式给 `epoch`。

`run_id` 可以进一步过滤。`regex=true` 使用 bounded server-side regex。结果 `truncated=true` 时必须按返回的 continuation 继续，直到 false，才能把空结果解释为“未找到”。

## Monitor tools

### `monitor_start`

```json
{
  "port": "COM4",
  "regex": "(?i)kernel panic|watchdog",
  "description": "观察间歇性设备崩溃"
}
```

`contains` 和 `regex` 二选一，最大 4096。可传 UUID `idempotency_key` 复用创建意图。调用立即返回；Monitor 在 `seriald` 中继续运行。

### `monitor_list` / `monitor_status`

`monitor_list` 可用 `port` 过滤。`monitor_status` 输入：

```json
{"monitor_id":"uuid"}
```

状态包含 matcher、cursor、incident/gap count 和 last error。

### `monitor_incidents`

```json
{"monitor_id":"uuid"}
```

省略 `after` 返回 recent tail；`after:"0"` 从最早保留 incident 开始；继续时原样传回十进制字符串 `next_after`。每个 incident 包含 preview、port、epoch、seq range、evidence cursor/ref 和 acknowledge 信息。

若需要完整证据，调用 `read(scope=archive)`，传 incident 的 epoch/after seq，并用 `through_seq=seq_end` 锁定包含式上界。

### `monitor_stop`

```json
{"monitor_id":"uuid"}
```

停止未来匹配，已有 incident 继续可读。

## `run_end`

```json
{"run_handle":"abcdefghijklmnopqrstuv"}
```

结束由 handle 指定的活动 Run，并 best-effort 释放 Control。结果中 `run_open=false`。这是正常 Agent 会话的标准结束动作。

## `release`

无活动 Run 时，按端口释放当前 adapter 的 Control：

```json
{"port":"COM4","abort_run":false}
```

显式中止当前 adapter 的活动 Run：

```json
{"run_handle":"abcdefghijklmnopqrstuv","abort_run":true}
```

中止模式不能同时传 `port`。正常完成应使用 `run_end`。adapter 没有持有本地 Control 时，release 是 no-op，不会影响其他连接的 Run。

## Recent context 与 physical action guard

adapter 记住每端口上次成功操作的 cursor。两个操作之间出现其他 actor 的 TX、用户 Takeover、Control/Run 中止、端口重配或机型切换时，相关工具结果才附加：

```json
{
  "recent_context": {
    "interference": true,
    "complete": true,
    "after_seq": 800,
    "through_seq": 812,
    "events": []
  }
}
```

没有第三方变化时省略该字段，减少 Agent context。

在 `command`、`command_sequence`、`input`、`signal`、`trigger` 前，adapter 把已观察 cursor、generation 和 TX offset 作为后端原子 precondition。若 ring 无法证明上下文连续，或有第三方 TX/重开/gap，工具在物理动作前返回：

```json
{
  "error": {
    "code": "context_changed",
    "no_bytes_written": true,
    "recent_context": {},
    "retry_hint": "Call read(scope=tail) or wait ..."
  }
}
```

Agent 应先读取并确认新状态，再决定是否重试。

## 结果、截断与取消

每个工具结果同时出现在：

- `structuredContent`：结构化 JSON；
- `content[0].text`：同一 JSON 的紧凑文本。

长串口内容受 event、byte 和 text budget 限制。`truncated`、`gap`、`omitted`、`warnings` 与 cursor 明确说明结果范围；不能把一次有界空结果当成全历史不存在。

可取消的纯观察工具：

```text
devices model_profiles read wait search
monitor_list monitor_status monitor_incidents
```

其他工具可能已经改变物理设备或后端状态，会继续完成并给出权威结果。MCP host 关闭 stdin 时也遵循同一原则。
