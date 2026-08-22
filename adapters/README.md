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

该命令一次启动：

```text
seriald       http://127.0.0.1:3210
serial-mcp    http://127.0.0.1:3211/mcp
serialctl     foreground TUI
```

`serial` 会在同一本地数据目录中自动发现并验证唯一 `seriald`，没有可用服务时才启动后端；默认和自定义 endpoint 使用相同复用规则。App→`serial` 和 `serial`→App 都会复用这个后端。只有裸 `serial` 会保证 HTTP MCP `127.0.0.1:3211` 可用；App 的 Local Service 只管理 `seriald`。

App 与 `serial` 只停止自己启动的进程。外部 owner 退出后，仍在运行的客户端不会自动 failover；重新启动后才重新发现或创建服务。

## 选择 MCP transport

### Streamable HTTP

支持 URL 型 MCP server 的 host 可直接配置：

```text
http://127.0.0.1:3211/mcp
```

这是 sessionless Streamable HTTP：host 对 `/mcp` 发送 JSON-RPC `POST`。`serial` 已经管理 adapter 进程，不需要 MCP host 再启动一个 stdio 进程。

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

adapter 暴露固定 19 项：

```text
devices              model_profiles       model_profile_set
read                 command              command_sequence
input                signal               trigger
wait                 search
monitor_start        monitor_list          monitor_status
monitor_incidents    monitor_stop
run_start            run_end               release
```

查看 host 实际应缓存的完整 schema：

```sh
serial mcp --dump-tools
```

更新 adapter 后，让 MCP host 重新执行 `tools/list`。所有设备参数统一使用 `port`。

OpenCode 会把 server 名作为工具前缀，例如 `serial_devices`、`serial_command_sequence` 和 `serial_model_profile_set`。Serial Platform 本身不要求 token、Header 或角色配置；MCP host 自己的工具确认策略不改变协议参数。

## Agent 指令建议

MCP initialize 已提供服务器指令。若 host 支持附加 prompt，可以保持为以下短规则：

```text
先调用 devices 和 model_profiles，明确选择 port 并核对机型 Profile、具体 model_name 与实际设备。
写入前调用 run_start，并在同一工作流中保存 run_handle。
普通命令用 command；账号/密码等已知依赖交互用一次 command_sequence。
每个 command/step 都填写简洁 description。
最终回复前调用 run_end。
```

不要让模型传底层 Control、fence、generation、operation 或续租状态；adapter 会处理这些细节。

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

人工在活动 Agent Run 中使用 `Alt+Enter` 会产生 cooperative TX，但不会转移 Agent Control。Agent 下一次物理写会在发送前收到 `context_changed` tool error，结果包含 `no_bytes_written=true` 和 `recent_context`。先用 `read(scope=tail)` 或 `wait` 阅读并确认新的串口状态，再决定是否重试；连续操作间没有第三方变化时不会附加 `recent_context`。

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

MCP cancellation 只中断纯观察调用。物理写入、Run transition 和 Monitor mutation 可能已经跨过副作用边界，会继续收敛到权威结果，避免 host 因看不到结果而错误重试。

完整输入 schema、结果、capture 与 recent context 语义见 [MCP 工具目录](../docs/MCP_TOOLS.md)。
