# Serial Platform

Serial Platform 是一个面向人和 Agent 协同操作的通用串口平台。它让一个后端独占物理串口，同时把相同的实时数据、命令记录和持久历史提供给终端、桌面应用与 MCP Agent。

平台不内置某个芯片、Shell 或烧录流程。机型身份由独立的两级目录管理，串口交互差异由 Model Profile 描述；具体调试和自动化由人或 Agent 组合通用能力完成。

## 核心设计

- **端口就是设备位**：公开接口只使用操作系统串口名，例如 `COM4` 或 `/dev/cu.usbserial-210`。没有额外的设备位名称。
- **一个物理写入者**：`seriald` 独占串口句柄，通过带 fencing 的 Control 串行化人和 Agent 的写入。
- **同一份事实记录**：RX、确认后的 TX、Control、Run、Trigger、断连和重配事件进入同一条带序号时间线。
- **历史跨客户端重开保留**：日志由后端持久化；关闭再打开 TUI 或 App 不会清空已有串口记录。
- **人和 Agent 各自适合的界面**：人使用 TUI 或 Electron App，Agent 使用 17 个 MCP 工具；三者共享同一个后端状态。
- **配置分层清楚**：Transport Profile 管物理 UART 参数；Model Profile 只管提示符、换行、设备回显解析和写入节奏；Model Family 目录独立管理“一级机型系列 → 二级具体机型”，与行为 Profile 可以独立切换。

## 快速开始

发行包解压后保留其中所有文件。首次配置不需要先启动后端：

```sh
serial setup
```

交互说明保持简短并同时显示中英文：

- 后端地址 / Endpoint：`seriald` 的监听 IP 和端口。
- 串口 Profile / Transport Profile：波特率、数据位、校验位、停止位、流控、DTR/RTS 和自动打开。
- 机型 Profile / Model Profile：可复用的 Shell/U-Boot 提示符、换行、设备回显解析和慢速写入行为。
- 机型名 / Model Family：独立维护一级机型系列与其下的二级具体机型，用于标记当前串口连接的设备。

配置完成后直接运行：

```sh
serial
```

不带子命令的 `serial` 会一次完成三件事：

1. 启动或复用本地 `seriald` 后端；
2. 在 `http://127.0.0.1:3211/mcp` 启动 sessionless Streamable HTTP MCP；
3. 在前台打开 TUI。

若首次运行时还没有配置，`serial` 会先执行同样的简洁离线配置。退出前台 TUI 时，只结束本次 `serial` 自己启动的后端和 MCP 子进程；复用的外部后端不受影响。

从 v0.8.0 升级到 v0.8.1 时，首次启动会把 `seriald.toml` 从 schema 2 自动、无损迁移到 schema 3：串口参数、行为 Profile、机型身份、端口绑定、安装身份和配置 revision 都会保留。原始文件会先保存为同目录下的 `seriald.toml.schema2.bak`（若文件名已占用则使用编号后缀），不需要删除配置或重新运行 setup。

同一本地数据目录只运行一个 `seriald`。`serial` 和 App 会自动发现并验证该后端；无论使用默认还是自定义地址、先启动 App 还是先运行 `serial`，后启动的一方都会复用同一个服务。

App 和 `serial` 只停止自己启动的进程。拥有后端的一方退出后，另一方不会自动接管或启动替代后端；重新启动后才重新发现或创建服务。

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

## Profile 与机型目录

端口配置由操作系统串口、Transport Profile、行为 Model Profile、成对的机型系列/具体机型身份和开关组成：

```json
{
  "port": "COM4",
  "transport_profile": "uart-115200",
  "model_profile": "linux-shell",
  "model_family": "TL-AS7230",
  "model_name": "TL-AS7230-W 1.0",
  "enabled": true
}
```

Profile、机型系列和具体机型名都按用户输入原样保存和显示，包括空格与大小写。

### Transport Profile

Transport Profile 可复用于多个端口，包含：

- `baud_rate`
- `data_bits`
- `parity`
- `stop_bits`
- `flow_control`
- `dtr` / `rts`
- `auto_open`

默认值为 115200 8N1、无流控、DTR/RTS 低、自动打开。

### Model Profile

Model Profile 代表一组可复用的串口交互行为，包含：

- `name`
- `shell_prompt`
- `uboot_prompt`
- `write_eol`
- `echo`
- `write_chunk_size`
- `write_chunk_delay_ms`

行为 Profile 不包含机型系列或具体机型名。一个 Profile 可以绑定多个端口；保存共享 Profile 时，这些端口会立即使用新的命令边界与写入行为。

### Model Family 目录

Model Family 是与行为 Profile 独立的两级身份目录：第一级 `name` 是机型系列，第二级 `model_names` 是该系列下可选的具体机型。端口的 `model_family` 和 `model_name` 必须同时设置或同时清空，且具体机型必须存在于对应系列中。

物理 UART 参数变更可能重新打开串口；单纯修改行为 Profile 或机型身份不需要重新打开物理句柄。

CLI 管理示例：

```sh
serial profile transport create --interactive
serial profile model create --interactive
serial profile attach --port COM4 --transport uart-115200 --model linux-shell \
  --model-family TL-AS7230 \
  --model-name 'TL-AS7230-W 1.0'
serial profile detach --port COM4 --model
serial profile detach --port COM4 --identity
```

两类 Profile 都支持 `list`、`show`、`create`、`update`、`clone`、`import`、`export` 和 `delete`。两级机型目录在 TUI 的“配置机型名”中维护，程序化管理使用 HTTP `/api/v1/config/model-families`。`--model` 只绑定行为 Profile，`--model-family` 与 `--model-name` 成对绑定身份；`detach --model` 只清除行为，`detach --identity` 只清除机型身份。

## TUI 工作流

TUI 顶部只显示串口名和连接状态；串口输出标题只显示当前机型名。主输出区只渲染设备 RX，不再把本机发送的命令重复插入串口画面。

默认操作围绕键盘设计：

- 输入任意可打印字符会直接进入命令输入行；按 Enter 总会发送并返回串口底部：有内容时发送“内容 + Profile EOL”，空输入时发送 Profile EOL；若 Profile 明确配置为无 EOL，空输入仍发送一个 `CR`。
- `↑` / `↓` 在当前层选择；`→` 从 Run 进入 action description，再进入具体命令；`←` 逐层返回。
- 展开的 Agent 详情超过面板高度时，用 `Shift+↑` / `Shift+↓` 滚动详情；普通方向键仍只控制历史树。
- 滚轮和 `PgUp` / `PgDn` 始终滚动串口输出，不会改变 Agent 历史选择或焦点；`Ctrl-] PgUp` / `Ctrl-] PgDn` 也执行同一操作。
- `Ctrl-] /` 搜索持久串口历史，可切换普通文本/正则、大小写、RX/TX 和当前周期/保留周期/当前 Run。
- `Alt-1` 到 `Alt-9` 快速切换端口；`Ctrl-] ?` 打开完整帮助。

任务与命令记录按从旧到新排列，采用 Run 标题 → action description → 具体命令的三层树。第二层缩进 4 列且不混入命令；第三层缩进 8 列，普通 `command` 显示一条，`command_sequence` 按 step 顺序显示多条。新的 Agent action 到达时，TUI 退回它所属的 Run；同一 sequence 的后续 step 或同一 TX 的分块只更新原 action。进入 action 或具体 step 时会定位并高亮设备回显、返回内容和完成边界。命令属于旧的后端周期或本地窗口已淘汰完整捕获区间时，TUI 会按原周期、命令序号和持久 matcher 从 journal 精确回取；只有从 TX 到完成边界完整连续时才显示 RX 高亮。retention gap、缺失、超限或查询失败会回到实时尾并明确提示，不把局部尾部伪装成完整结果。没有 matcher 时只临时显示命令文本，不污染串口历史。

Monitor 也显示在同一记录栏：按 `→` 进入查看 matcher，再按 `→` 进入 incident 层；用 `↑` / `↓` 选择 incident 后，根据它的 `serial_range` 跳转并高亮对应串口证据。如果 incident 属于旧的后端周期，或对应内容已从 TUI 本地窗口淘汰，TUI 会按周期和序号范围从 journal 读取完整连续证据；若持久历史已有 retention gap 或范围不完整，则明确提示，且不显示可能误导的局部结果。

鼠标拖选和双击词语都会显示选中高亮并复制文本。串口着色使用词边界匹配错误、警告和成功关键词，不会把 `get_data_error_name` 一类标识符误判；IPv4、IPv6 和 MAC 地址使用独立颜色。

TUI 菜单分成“修改当前串口配置”“创建配置”“设置”“帮助”：

- 当前串口配置中，按 `Tab` / `Shift+Tab` 直接切换正在编辑的串口；同一配置目录 revision 内，各串口的未保存草稿独立保留。任一保存、删除、创建或 Reload 使 revision 变化后，旧 revision 草稿会安全失效并在菜单中明确提示，避免覆盖新配置。按 `→` 展开端口/Profile/离散参数选项，`↑` / `↓` 选择，Enter 应用并折叠，`←` 折叠或返回；“机型 Profile”只选交互行为，独立的“机型名”按“一级机型系列 → 二级具体机型”选择。
- Shell/U-Boot 提示符和分段发送数值直接在当前行下输入；保存操作位于所有配置字段下方的独立操作区。
- “创建配置”还提供删除未绑定的串口/机型 Profile；删除前必须确认，并用配置 revision 防止旧页面覆盖并发更新。配置机型名时，按 `D` / `Delete` 可删除未绑定的一级系列或二级具体机型；删除一级系列会一并删除其下的具体机型名。
- “设置”下分别配置 `agent_history_rows`（3–20，默认 5）和 `orphan_run_timeout_seconds`（默认 1800 秒；`0` 表示不限时）。MCP 设置保存后自动生效，不需要手动重启。
- 菜单底部只显示一行按键指南；高亮配置项后按 `?` 查看该项说明。

## Electron App

每个平台发行包都包含现代桌面客户端：

- 左侧端口栏：串口名、机型名、连接状态和打开/关闭操作；
- 中间 RX 终端：持久历史、实时输出、文本搜索、地址/关键词着色和命令区域高亮；
- 右侧 Agent 历史：从旧到新展示 Run、普通命令和命令序列；
- 底部命令栏：面向当前端口发送人工命令；
- 独立配置页：分别编辑串口/Transport Profile、行为 Model Profile 与两级 Model Family 机型目录；
- 系统、浅色和深色三种主题。

App 默认连接配置的本地后端；后端不存在且启用了自动启动时，App 会启动随包提供的服务，并只管理自己启动的进程。渲染进程只通过类型化 IPC 与主进程通信，不直接访问串口或后端网络。

快捷键：`Ctrl/Cmd+,` 打开配置，`Ctrl/Cmd+1` 返回控制台，`Ctrl/Cmd+F` 搜索终端，`Ctrl/Cmd+K` 聚焦命令输入，`Esc` 返回控制台。

## Agent 与 MCP

`serial-mcp` 暴露 17 个工具：

```text
devices              model_identity_set   read
command              command_sequence     input
signal               trigger              wait
search               monitor_start        monitor_list
monitor_status       monitor_incidents    monitor_stop
run_start            run_end
```

所有设备选择参数都叫 `port`。典型流程是：

1. `devices` 检查端口、`model_family` / `model_name`、连接与工作流状态，以及当前有效的 Shell/U-Boot 提示符；
2. `run_start(port, label)` 获取本次工作流的 `run_handle`；
3. 使用 `command`，或用一次 `command_sequence` 完成“账号 → 等待密码提示 → 密码”这类已知依赖交互；
4. 用 `read`、`wait`、`search` 或 Monitor 补充证据；
5. 在 Agent 最终回复前调用 `run_end`；正常完成使用默认 `outcome=completed`，异常终止使用 `outcome=aborted`。

`run_handle` 是 MCP 进程内的工作流句柄；Agent 不需要传 Control ID、fence、generation、请求 UUID 或续租参数。`run_end` 的 `outcome` 可选 `completed` 或 `aborted`，默认正常完成；`completed` 关闭 Run 并立即尝试释放 Control，`aborted` 只有在 Control 已被权威释放后才成功。默认孤立 Run 回收时间是 30 分钟。

`devices` 是 Agent 唯一的设备发现工具。它不暴露行为 Model Profile 名、Transport/UART 参数、EOL/echo 或写入节奏。`model_identity_set` 只能绑定或解绑由人通过 TUI、App 或 HTTP 预先配置的两级机型名，不创建机型目录。

`monitor_start` 的 `matchers` 可同时配置 1–16 个文本或正则条件，按 OR 匹配；incident 返回实际命中的条件和精确 `serial_range`。如果人工在活动 Agent Run 中用 `Alt+Enter` 写入，Agent 下一次物理写会在发送前返回 `context_changed`、`no_bytes_written=true` 和 `recent_context`；调用 `read` 或 `wait` 确认新状态后再决定是否重试。

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
    ├── WebSocket protocol v6 realtime and control
    ├── serialctl TUI
    ├── Electron App
    └── serial-mcp ── stdio or Streamable HTTP ── Agent
```

- `seriald` 是唯一持有物理串口句柄的进程。
- `serialctl` 提供离线之外的配置、诊断、日志查询和 TUI。
- `serial-mcp` 把同一 HTTP/WebSocket 能力收敛为 Agent 友好的工具。
- Electron App 管理本地服务生命周期并复用 v6 接口。
- `serial` 是统一入口。

文档入口：

- [架构与交互设计](./DOCUMENTATION.md)：产品边界、配置模型、Control/Run、历史投影和启动所有权；
- [protocol v6](./docs/PROTOCOL.md)：HTTP v1、WebSocket v6、Timeline、Monitor 与 MCP transport 线协议；
- [MCP 工具契约](./docs/MCP_TOOLS.md)：17 个工具的输入、结果和 Agent 工作流；
- [Adapter 配置](./adapters/README.md)：stdio/Streamable HTTP host 接入；
- [Roadmap](./ROADMAP.md)：当前能力、发布质量门槛和非目标。

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

Jenkins 是发布构建与 GitHub Release 发布入口。Prepare 阶段只在 Rust 节点向 GitHub checkout 一次，固定并校验完整 commit 后生成带 SHA-256 的源码 bundle；Linux、macOS、归档和发布阶段均恢复并复验这一份源码，macOS 节点不再独立向 GitHub 拉取仓库。workspace 版本 tag 尚不存在时构建 Debug 包；当当前提交存在与 `Cargo.toml` 版本一致的 annotated `vX.Y.Z` tag 时，Jenkins 自动切换 Release、构建四个平台、生成校验和并发布 GitHub Release，不需要填写发布参数。若同名 tag 不是 annotated tag 或没有指向本次 commit，本次仅按 Debug 构建且不发布。

发布矩阵：

- Ubuntu x86_64
- Windows x86_64
- macOS arm64
- macOS x86_64

每个平台包都包含四个 Rust 程序 `serial`、`seriald`、`serialctl`、`serial-mcp`，以及对应的 Electron 应用：Linux AppImage、Windows portable EXE 或 macOS `.app`。

macOS 上，四个 Rust CLI 的 deployment target 是 11.0，Electron 43 `.app` 的最低系统版本是 12.0。当前 macOS App 没有 Developer ID 签名，也没有 Apple notarization。
