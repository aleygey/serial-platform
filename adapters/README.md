# Agent Adapter Setup

`serial-mcp` 是 Serial Platform 面向 Codex、OpenCode 和其他 MCP host 的 adapter。它通过 HTTP/WebSocket 访问 `seriald`，不会调用 `serialctl` shell 命令，也不直接打开物理串口。

## 开始之前

首次配置不需要运行后端：

```sh
serial setup
```

如果 TUI 和 Agent 在同一台机器，最简单的启动方式是：

```sh
serial
```

默认配置下，该命令一次启动：

```text
seriald       http://127.0.0.1:3210
serial-mcp    http://127.0.0.1:3211/mcp
serialctl     foreground TUI
```

`serial` 会在同一本地数据目录中自动发现并验证唯一 `seriald`，没有可用服务时才启动后端；默认和自定义 endpoint 使用相同复用规则。App→`serial` 和 `serial`→App 都会复用这个后端。只有裸 `serial` 会保证 HTTP MCP 可用；App 的 Local Service 只管理 `seriald`。

HTTP MCP listener 从所选后端的实际 `ActiveEndpoint` 继承精确 IP，并固定使用 3211 端口。默认后端因此仍是 `127.0.0.1:3211`；若活动后端是 `192.168.56.109:3210`，MCP 则是 `192.168.56.109:3211`。后端 wildcard bind 发布为 loopback `ActiveEndpoint`，不会让 MCP 跟随绑定到 wildcard。health、复用、启动等待和最终显示都使用同一目标地址；复用还会核对当前 Serial wire protocol v7、后端 endpoint、server ID 和 daemon epoch。

App 与 `serial` 只停止自己启动的进程。外部 owner 退出后，仍在运行的客户端不会自动 failover；重新启动后才重新发现或创建服务。

## 选择 MCP transport

### Streamable HTTP

支持 URL 型 MCP server 的 host 可直接配置默认地址：

```text
http://127.0.0.1:3211/mcp
```

这是 sessionless Streamable HTTP：host 对 `/mcp` 发送 JSON-RPC `POST`。`serial` 已经管理 adapter 进程，不需要 MCP host 再启动一个 stdio 进程。

如果 `seriald` 的实际 `ActiveEndpoint` 是非 loopback host-only IP，必须把 URL 的 host 换成同一个精确 IP。HTTP MCP 没有 token、Header 或角色认证，只允许绑定精确 loopback 或可信 host-only unicast 地址，并拒绝 wildcard、broadcast 和 multicast；非 loopback 时必须使用主机防火墙限制来源，不能暴露到普通局域网或公网。

浏览器型 host 若发送 `Origin`，它必须精确匹配 `http://<listener-IP>:3211`。仅 loopback listener 接受 `localhost`；其他主机名、不同 IP/端口、HTTPS、credentials、path、query 和 fragment 都会被拒绝。原生 host 可以省略 `Origin`。

### stdio

若 MCP host 只支持本地 command，配置它启动发行包内的统一入口：

```text
command = serial
args = ["mcp", "--actor-label", "codex:workstation"]
```

Windows 示例：

```toml
[mcp_servers.serial]
enabled = true
required = true
command = 'C:\Tools\serial-platform\serial.exe'
args = ["mcp", "--actor-label", "codex:workstation"]
startup_timeout_sec = 10.0
tool_timeout_sec = 130.0
```

Linux/macOS 示例：

```toml
[mcp_servers.serial]
enabled = true
required = true
command = "/usr/local/bin/serial"
args = ["mcp", "--actor-label", "codex:workstation"]
startup_timeout_sec = 10.0
tool_timeout_sec = 130.0
```

仓库中的 `codex/` 和 `opencode/` 目录提供可复制的路径示例。发行包组件应保持在同一目录；`serial mcp` 只解析同包的 sibling `serial-mcp`。

## Endpoint 与配置

stdio adapter 默认连接 `http://127.0.0.1:3210`。覆盖方式：

```sh
serial mcp --endpoint http://192.168.56.1:3210
```

或：

```text
SERIALD_ENDPOINT=http://192.168.56.1:3210
```

`--config` / `SERIALCTL_CONFIG` 可以指定共享 `serialctl.toml`。常用字段：

```toml
endpoint = "http://127.0.0.1:3210"
orphan_run_timeout_seconds = 1800
capture_max_events = 4096
capture_max_bytes = 1048576
```

`orphan_run_timeout_seconds=0` 表示不限时；其他值至少 300 秒。命令行 `--orphan-run-timeout-seconds` 或环境变量 `SERIAL_MCP_ORPHAN_RUN_TIMEOUT_SECONDS` 对新启动进程优先。正常 Agent 工作流仍在最终回复前调用 `run_end`。

未使用命令行 timeout override 时，运行中的 adapter 会监视共享配置。TUI 的 “serial MCP 设置” 保存后自动生效，不需要人工重启 MCP。

## 工具发现

adapter 暴露固定 16 项：

```text
devices              model_identity_set   read
command              command_sequence     signal
trigger              wait                 search
monitor_start        monitor_list         monitor_status
monitor_incidents    monitor_stop         run_start
run_end
```

`devices` 是唯一的设备发现工具。它提供 `port`、`model_family` / `model_name`、Agent 需要的连接与工作流状态，以及当前有效的 Shell/U-Boot 提示符；不暴露行为 Model Profile 名、Transport/UART 参数、EOL/echo 或写入节奏。`model_identity_set` 只绑定或解绑人通过 TUI、App 或 HTTP 预先配置的 family/name，不创建机型目录。

查看 host 实际应缓存的完整 schema：

```sh
serial mcp --dump-tools
```

更新 adapter 后，让 MCP host 重新执行 `tools/list`。所有设备参数统一使用 `port`。

OpenCode 会把 server 名作为工具前缀，例如 `serial_devices`、`serial_command_sequence` 和 `serial_model_identity_set`。Serial Platform 本身不提供 MCP 认证；MCP host 自己的工具确认策略不改变协议参数。非 loopback HTTP 部署必须遵守上面的 host-only 和防火墙边界。

## Agent 指令建议

MCP initialize 已提供服务器指令。若 host 支持附加 prompt，可以保持为以下短规则：

```text
先调用 devices，明确选择 port，并核对 model_family/model_name、有效 Shell/U-Boot 提示符与实际设备。
写入前调用 run_start；若 Human 持有端口，等待其在 TUI/App 明确审批。成功后在同一工作流中保存 run_handle。
普通命令用 command；账号/密码等已知依赖交互用一次 command_sequence。
每个 command/step 都填写简洁 description。
收到 user_command_used 时，用 live read(scope=tail/continue) 读到并确认 Human TX；wait/archive 不会解除门禁。
最终回复前调用 run_end；正常完成省略 outcome 或使用 completed，异常终止使用 aborted。
```

不要让模型传底层 Control、fence、generation、operation 或续租状态；adapter 会处理这些细节。

## Run 启动与 Human 审批

`run_start` 使用一个原子的后端请求。端口空闲时，`seriald` 同时授予 fenced Control 并创建 Run；不会出现“先取得 Control、稍后才创建 Run”的中间状态。

如果当前精确 Human Control holder 正在使用端口，`seriald` 创建有时限的 Run-start 审批，adapter 用相同 request ID 和相同请求内容轮询。调用会等待 Human 在 TUI/App 批准或拒绝；批准后才原子转移 Control、创建 Run，并返回 `approval_id`、`run_id` 和 `run_handle`。Human 拒绝、审批超时、后端取消、adapter 清理取消、调用方或 WebSocket 断连、端口 generation 变化等终态都不创建 Run，也不写任何串口 bytes。adapter 的本地等待上限为 120 秒，并在达到上限前尝试取消 pending 请求，因此 MCP host 的 tool timeout 建议至少 130 秒。

`run_start` 可能正在完成审批或清理，属于不可安全中断的状态变更工具。host 超时后不要假定成功，更不能绕过 `run_handle` 直接重试物理写入。

## 多步依赖交互

当下一条命令依赖上一条设备提示时，不需要让 Agent 发起多次 MCP round trip。使用 `command_sequence`：

```json
{
  "run_handle": "abcdefghijklmnopqrstuv",
  "description": "登录设备控制台",
  "steps": [
    {
      "command": "admin",
      "description": "输入账号",
      "expect": "Password:"
    },
    {
      "command": "admin123",
      "description": "输入密码",
      "expect": "root@router:~# "
    }
  ]
}
```

adapter 在发送下一步之前等待当前 matcher。任一步失败，所有剩余步骤不再写入。每步独立进入审计历史；TUI 先显示整个 sequence action，展开后可逐步选择、跳转并高亮各自的 RX 捕获区间。

## 权威命令捕获

`command` 和 `command_sequence` 的每个已执行步骤在捕获结束后，都会把权威 TX event、证据区间、completion 与 confidence 记录到 `seriald`。成功结果中的 `authoritative_capture` 是后端确认的 `CommandCaptureCompleted`，包含 record event 以及 TX/RX stream offset 边界；TUI/App 后续定位命令证据以它为准，不依赖后来修改的 Profile。

如果物理 TX 已确认，但持久化 capture 失败，工具会失败并明确警告不得盲目重发；当 `command_sequence` 已有可返回的逐步结果时，其结构化 `failure.phase` 为 `record_capture`。该命令可能已经作用于设备；应先检查权威 TX/RX timeline，再通过运维或后端恢复证据记录。sequence 在这种失败后不会发送剩余步骤。

## Monitor

`monitor_start` 在 `seriald` 中创建一个持久 Monitor，并立即返回。一个任务可带 1–16 个 literal/regex 条件，按 OR 匹配；stdio 进程退出不停止 Monitor。

```json
{
  "port": "COM4",
  "matchers": [
    {"kind":"contains","value":"watchdog"},
    {"kind":"regex","value":"(?i)kernel panic|oops"}
  ],
  "description": "观察设备异常复位"
}
```

后续用 `monitor_status` 或 `monitor_list` 查看状态；`monitor_incidents` 返回命中的条件与精确 `serial_range`，并将十进制 `next_after` 原样用于下一页或后续轮询。`monitor_stop` 停止未来匹配，保留已有 incident。

## 人工协作后的 Agent 上下文

人工在活动 Agent Run 中直接按 Enter 发送命令时，该命令立即作为 Human TX 写入和审计，但不会借用 Agent fence 或转移 Agent Control。后端同时更新 Run context；Agent 下一次 `command`、`command_sequence`、`signal` 或 `trigger` 会在发送前收到结构化 `user_command_used` tool error，并返回 `no_bytes_written=true`。

Agent 必须调用实时 `read(scope=tail)` 或 `read(scope=continue)`，直到返回范围确实包含最新 Human TX，并看到 `user_command_acknowledged=true`。即使 `wait` 已让普通 cursor 越过该 TX，下一次 live read 也会临时从该 TX 前一序号恢复读取。有界 live ring 已淘汰这条证据时会返回 `user_command_acknowledged=false` 和明确 gap/warning，门禁继续保持关闭，不能用 archive 结果冒充确认。`wait`、`search`、Monitor 和 `read(scope=archive)` 都不会确认或清除该门禁。确认只证明 Agent 已看过人工干预；是否重试原操作仍需根据新的串口状态重新判断。

## Cursor 与长时间运行

实时读取使用后端有界 replay ring：

- `read(scope=tail)` 最多读取最近 200 events；
- `read(scope=continue)` 从 adapter live cursor 继续，最多 1000 events；
- 二者不扫描持久 journal，不受历史段数量影响；
- ring 淘汰或后端重启以 gap/truncation 明确返回；
- 旧周期证据使用 `read(scope=archive, epoch=...)` 或 `search(scope=archive, epoch=...)`。

因此高流量端口运行很久后，普通 tail 不会因为 segment discovery 扫描超出 journal query budget。

## 并发与取消

stdio 可以并发处理独立请求，输出 frame 由单一 writer 完整写出。每个端口的物理 mutation 在 adapter 内串行化，`command_sequence` 整体持有该路径，bytes 不会和同一 adapter 的另一个命令交错。

MCP cancellation 可中断 `devices`、`read`、`wait`、`search`、`monitor_list`、`monitor_status` 和 `monitor_incidents`。其他工具可能已经跨过副作用边界，会继续收敛到权威结果，避免 host 因看不到结果而错误重试。

完整输入 schema、结果、capture 与 recent context 语义见 [MCP 工具目录](../docs/MCP_TOOLS.md)。
