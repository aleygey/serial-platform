# Serial Platform

Serial Platform 是一个面向人和 Agent 协同操作的通用串口平台。它让一个后端独占物理串口，同时把相同的实时数据、命令记录和持久历史提供给终端、桌面应用与 MCP Agent。

平台不内置某个芯片、Shell 或烧录流程。机型差异通过 Profile 描述，具体调试和自动化由人或 Agent 组合通用能力完成。

## 核心设计

- **端口就是设备位**：公开接口只使用操作系统串口名，例如 `COM4` 或 `/dev/cu.usbserial-210`。没有额外的设备位名称。
- **一个物理写入者**：`seriald` 独占串口句柄，通过带 fencing 的 Control 串行化人和 Agent 的写入。
- **同一份事实记录**：RX、确认后的 TX、Control、Run、Trigger、断连和重配事件进入同一条带序号时间线。
- **历史跨客户端重开保留**：日志由后端持久化；关闭再打开 TUI 或 App 不会清空已有串口记录。
- **人和 Agent 各自适合的界面**：人使用 TUI 或 Electron App，Agent 使用 19 个 MCP 工具；三者共享同一个后端状态。
- **配置分层清楚**：Transport Profile 管物理 UART 参数，Model Profile 管机型名、提示符、换行、设备回显解析和写入节奏。

## 快速开始

发行包解压后保留其中所有文件。首次配置不需要先启动后端：

```sh
serial setup
```

交互说明保持简短并同时显示中英文：

- 后端地址 / Endpoint：`seriald` 的监听 IP 和端口。
- 串口 Profile / Transport Profile：波特率、数据位、校验位、停止位、流控、DTR/RTS 和自动打开。
- 机型 Profile / Model Profile：机型原名、Shell/U-Boot 提示符、换行、设备回显解析和慢速写入。

配置完成后直接运行：

```sh
serial
```

不带子命令的 `serial` 会一次完成三件事：

1. 启动本地 `seriald` 后端；
2. 在 `http://127.0.0.1:3211/mcp` 启动 sessionless Streamable HTTP MCP；
3. 在前台打开 TUI。

若首次运行时还没有配置，`serial` 会先执行同样的简洁离线配置。退出前台 TUI 时，由本次启动的两个子进程也会结束。

常用独立命令：

```sh
serial console                         # 连接已有后端并打开 TUI
serial serve                           # 只运行 seriald
serial mcp                             # 以 stdio 运行 MCP adapter
serial mcp --dump-tools                # 输出完整 MCP tools/list JSON
serial status
serial doctor state --port COM4
serial logs --port COM4 --contains ready
serial logs --port COM4 --regex '(?i)panic|watchdog'
```

## 两类 Profile

端口配置只有四个字段：

```json
{
  "port": "COM4",
  "transport_profile": "uart-115200",
  "model_profile": "TL-AS7230 1.0",
  "enabled": true
}
```

Profile 名称按用户输入原样保存和显示，包括空格与大小写。

### Transport Profile

Transport Profile 可复用于多个端口，包含：

- `baud_rate`
- `data_bits`
- `parity`
- `stop_bits`
- `flow_control`
- `dtr` / `rts`
- `auto_open`

安全基线为 115200 8N1、无流控、DTR/RTS 低、自动打开。

### Model Profile

Model Profile 同时代表机型身份和串口交互行为，包含：

- `name`
- `shell_prompt`
- `uboot_prompt`
- `write_eol`
- `echo`
- `write_chunk_size`
- `write_chunk_delay_ms`

一个机型 Profile 可以绑定多个端口；保存共享 Profile 时，绑定端口会立即使用新的交互参数。物理 UART 参数变更可能重新打开串口，单纯修改机型行为不需要重新打开物理句柄。

CLI 管理示例：

```sh
serial profile transport create --interactive
serial profile model create --interactive
serial profile attach --port COM4 --transport uart-115200 --model 'TL-AS7230 1.0'
serial profile detach --port COM4 --model
```

两类 Profile 都支持 `list`、`show`、`create`、`update`、`clone`、`import`、`export` 和 `delete`。

## TUI 工作流

TUI 顶部只显示串口名和连接状态；串口输出标题只显示当前机型名。主输出区只渲染设备 RX，不再把本机发送的命令重复插入串口画面。

默认操作围绕键盘设计：

- 输入任意可打印字符会直接进入命令输入行；有内容时按 Enter 发送，没有内容时按 Enter 返回串口底部。
- `↑` / `↓` 选择 Agent 历史，`→` 展开，`←` 折叠。
- 滚轮和 `PgUp` / `PgDn` 浏览 Agent 历史或已展开的命令详情，不需要点击窗口切换焦点。
- `Ctrl-] PgUp` / `Ctrl-] PgDn` 专门滚动串口输出。
- `Ctrl-] /` 搜索持久串口历史，可切换普通文本/正则、大小写、RX/TX 和当前周期/保留周期/当前 Run。
- `Alt-1` 到 `Alt-9` 快速切换端口；`Ctrl-] ?` 打开完整帮助。

Agent 历史按从旧到新排列；未主动浏览旧记录时，新命令到达会自动跟随到底部。选择一条命令后，TUI 根据该 TX 事件持久化的 `command_capture_matchers` 在 RX 中定位并高亮命令与回显形成的区域；没有匹配时只临时显示命令文本，不污染串口历史。`command_sequence` 的每个步骤都可以单独选择和定位。

鼠标拖选和双击词语都会显示选中高亮并复制文本。串口着色使用词边界匹配错误、警告和成功关键词，不会把 `get_data_error_name` 一类标识符误判；IPv4、IPv6 和 MAC 地址使用独立颜色。

TUI 配置页将“串口配置”和“机型 Profile”分开显示，并可直接修改 `agent_history_rows`（3–20，默认 5）与 `orphan_run_timeout_seconds`（默认 1800 秒；`0` 表示不限时）。

## Electron App

每个平台发行包都包含现代桌面客户端：

- 左侧端口栏：串口名、机型名、连接状态和打开/关闭操作；
- 中间 RX 终端：持久历史、实时输出、文本搜索、地址/关键词着色和命令区域高亮；
- 右侧 Agent 历史：从旧到新展示 Run、普通命令和命令序列；
- 底部命令栏：面向当前端口发送人工命令；
- 独立配置页：分别编辑串口/Transport Profile 与 Model Profile；
- 系统、浅色和深色三种主题。

App 默认连接配置的本地后端；后端不存在且启用了自动启动时，App 会启动随包提供的服务，并只管理自己启动的进程。渲染进程只通过类型化 IPC 与主进程通信，不直接访问串口或后端网络。

快捷键：`Ctrl/Cmd+,` 打开配置，`Ctrl/Cmd+1` 返回控制台，`Ctrl/Cmd+F` 搜索终端，`Ctrl/Cmd+K` 聚焦命令输入，`Esc` 返回控制台。

## Agent 与 MCP

`serial-mcp` 暴露 19 个工具：

```text
devices              model_profiles       model_profile_set
read                 command              command_sequence
input                signal               trigger
wait                 search
monitor_start        monitor_list          monitor_status
monitor_incidents    monitor_stop
run_start            run_end               release
```

所有设备选择参数都叫 `port`。典型流程是：

1. `devices` 检查端口与机型；
2. `run_start(port, label)` 获取本次工作流的 `run_handle`；
3. 使用 `command`，或用一次 `command_sequence` 完成“账号 → 等待密码提示 → 密码”这类已知依赖交互；
4. 用 `read`、`wait`、`search` 或 Monitor 补充证据；
5. 在 Agent 最终回复前调用 `run_end`。

`run_handle` 是 MCP 进程内的工作流句柄；Agent 不需要传 Control ID、fence、generation、请求 UUID 或续租参数。默认孤立 Run 回收时间是 30 分钟，正常结束仍由 `run_end` 表达。

详见 [MCP 工具目录](./docs/MCP_TOOLS.md) 和 [adapter 配置](./adapters/README.md)。

## 持久记录与查询

每个端口的游标是 `(port, daemon_epoch, seq)`：

- `daemon_epoch` 每次后端进程启动都会改变；
- `seq` 在一个端口和一个后端周期内单调递增；
- 断开再打开物理串口会增加 `generation`，但不会清空当前周期的序号和 TUI 历史；
- 关闭再打开客户端会从持久 journal 恢复当前周期，再接上实时 WebSocket；
- 更早周期通过 `serial archives`、`serial logs --epoch ...`、TUI 搜索或 MCP archive 查询读取。

默认 journal 上限为 10 GiB，分段写入并带 CRC 与断尾恢复。实时 tail 从有界内存 ring 返回，避免串口运行很久后让普通 MCP 读取扫描全部历史；归档查询始终受事件数、字节数和时间预算限制。

## 接口与架构

```text
physical UART
    │
    ▼
seriald ── durable journal
    ├── HTTP v1 configuration / diagnostics / history
    ├── WebSocket protocol v4 realtime and control
    ├── serialctl TUI
    ├── Electron App
    └── serial-mcp ── stdio or Streamable HTTP ── Agent
```

- `seriald` 是唯一持有物理串口句柄的进程。
- `serialctl` 提供离线之外的配置、诊断、日志查询和 TUI。
- `serial-mcp` 把同一 HTTP/WebSocket 能力收敛为 Agent 友好的工具。
- Electron App 管理本地服务生命周期并复用 v4 接口。
- `serial` 是统一入口。

完整接口见 [架构文档](./DOCUMENTATION.md)、[protocol v4](./docs/PROTOCOL.md) 和 [roadmap](./ROADMAP.md)。

## 构建与发行

本地验证：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

cd crates/serial-desktop
npm ci --no-audit --no-fund
npm run build
```

Jenkins 是发布构建与 GitHub Release 发布入口。workspace 版本 tag 尚不存在时构建 Debug 包；当当前提交存在与 `Cargo.toml` 版本一致的 annotated tag（例如 `v0.8.0`）时，Jenkins 自动切换 Release、构建四个平台、生成校验和并发布 GitHub Release，不需要填写发布参数。若同名 tag 不是 annotated tag 或没有指向本次 commit，本次仅按 Debug 构建且不发布。

发布矩阵：

- Ubuntu x86_64
- Windows x86_64
- macOS arm64
- macOS x86_64

每个平台包都包含四个 Rust 程序 `serial`、`seriald`、`serialctl`、`serial-mcp`，以及对应的 Electron 应用：Linux AppImage、Windows portable EXE 或 macOS `.app`。

macOS 上，四个 Rust CLI 的 deployment target 是 11.0，Electron 43 `.app` 的最低系统版本是 12.0。当前 macOS App 没有 Developer ID 签名，也没有 Apple notarization。
