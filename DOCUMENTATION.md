# Serial Platform Architecture

本文描述 Serial Platform 的当前架构。具体 JSON 契约以 [protocol v5](./docs/PROTOCOL.md) 为准，Agent 工具以 [MCP 工具目录](./docs/MCP_TOOLS.md) 或 `serial mcp --dump-tools` 为准。

## 产品边界

Serial Platform 是通用的人/Agent 协同串口平台：后端独占物理 UART，把串口字节、控制状态和工作流边界形成可实时订阅、可恢复、可审计的统一时间线。

平台负责：

- 发现、打开、关闭和重连物理串口；
- 在多个观察者之间分发相同的数据；
- 串行化人和 Agent 的物理写入；
- 记录 RX、确认后的 TX 及状态变化；
- 提供有界实时读取、持久历史查询、Trigger 和 Monitor；
- 通过 TUI、Electron 和 MCP 呈现同一份状态。

平台不负责：

- 内置厂商烧录配方；
- 猜测某种 Linux、Bootloader 或芯片语义；
- 在 Run 开始时假装设备已经复位或处于干净状态；
- 根据静默自动判断设备失效；
- 把某个 Agent 运行时嵌入后端。

## 组件

| 组件 | 责任 |
|---|---|
| `serial` | 统一入口；离线 setup；一次启动后端、HTTP MCP 和前台 TUI；分发其他子命令 |
| `seriald` | 物理串口所有权、Control/Run/Trigger、journal、HTTP v1 和 WebSocket v5 |
| `serialctl` | 人工 setup、Profile 管理、诊断、历史查询和全屏 TUI |
| `serial-mcp` | 面向 Agent 的 19 工具；支持 stdio 与 sessionless Streamable HTTP |
| Electron App | 本地服务生命周期、三栏控制台、配置页和桌面快捷键 |
| `serial-protocol` | 跨组件 DTO、v5 WebSocket 消息和二进制帧 codec |

`seriald` 是唯一直接打开物理串口的组件。其余组件只通过 HTTP/WebSocket 访问后端，因此多个 TUI、桌面窗口和 Agent 可以观察同一条时间线，而不会争抢 OS 句柄。

## 公开身份模型

操作系统串口名是唯一的公开设备标识：

- Windows：`COM4`
- macOS：`/dev/cu.usbserial-210`
- Linux：`/dev/ttyUSB0`

配置、HTTP path、WebSocket 消息、时间线事件与 MCP 参数都使用 `port`。包含 `/` 的串口名在 HTTP path 中进行 percent-encoding。

其他身份有各自的生命周期：

| 字段 | 含义 | 变化时机 |
|---|---|---|
| `server_id` | 一套后端数据目录 | 创建全新配置时 |
| `daemon_epoch` | 一个 `seriald` 进程周期 | 每次后端重启 |
| `generation` | 一次物理串口会话 | 串口成功重新打开 |
| `seq` | 一个端口/周期内的逻辑事件序号 | 每个时间线事件 |
| RX/TX offset | 对应方向的确认字节偏移 | 字节到达或写入成功 |
| `model_profile` | 一类机型共用的交互 Profile | 人、CLI 或 Agent 修改端口绑定 |
| `model_name` | 当前串口连接的具体机型名 | 从绑定 Profile 的 `model_names` 中选择 |
| `run_id` | 一段 Agent 工作的审计身份 | 显式开始新 Run |
| `operation_id` | 一次物理操作的关联身份 | 客户端开始新操作 |

完整游标是 `(port, daemon_epoch, after_seq)`。不能用裸 `seq` 跨后端重启继续读取。物理会话变化会撤销旧 Control 并终止依赖该会话的 Run 或 Trigger。

## 配置模型

### 端口绑定

每项端口配置只有：

```json
{
  "port": "COM4",
  "transport_profile": "uart-115200",
  "model_profile": "TL-AS7230",
  "model_name": "TL-AS7230-W 1.0",
  "enabled": true
}
```

`transport_profile`、`model_profile` 与 `model_name` 均可省略。具体 `model_name` 必须属于绑定 Profile 的 `model_names`；解绑机型 Profile 会同时清除具体机型名。`enabled=false` 保留配置但不打开串口。

配置更新带 `config_revision` 乐观并发保护。后端先验证完整候选配置，再持久化并发布；物理 UART 变更通过暂停、关闭旧句柄、应用、提交的事务路径完成。失败不会留下部分生效的配置。

端口重配事件记录 `source`、机型 Profile 和具体机型名的前后值。MCP 的最近上下文因此可以告诉 Agent：人刚刚把哪个端口切换到了哪个系列和具体型号。

### Transport Profile

Transport Profile 描述主机 UART：波特率、数据位、校验位、停止位、流控、DTR、RTS 和自动打开。更新绑定中的 Transport Profile 可能触发串口重开。

通用基线：115200、8N1、无流控、DTR/RTS 低、自动打开。

### Model Profile

Model Profile 描述一个机型系列共用的交互行为，并列出该系列的具体型号：

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

`name` 是第一层机型系列/Profile 名称，`model_names` 是第二层具体机型名。名称原样保存，不替换空格、不改变大小写。提示符可以为空；后端不会猜测 Shell 或 U-Boot。Model Profile 更新会立即影响所有绑定端口的命令边界与写入行为，但不需要重开物理串口。

`echo` 只指导 Agent 捕获如何识别和去除设备自身回显；人类界面不会额外合成本机 TX 到 RX 终端。

### 写入节奏

有效写入节奏由 Model Profile 的可选覆盖决定，未设置时使用通用值。`write_chunk_size` 限制每次驱动写入的字节数，`write_chunk_delay_ms` 是相邻 chunk 之间的请求延时；`0` 选择全速路径。

后端在进入驱动前计算完整 pacing budget。单次物理写入受最大字节数和时间预算限制，无法完成的请求在触碰串口前失败。已进入驱动且结果不确定的写入不会自动重试。

## 物理所有权与协同写入

### Control

普通写入需要 Control lease。lease 包含 Control ID、`daemon_epoch`、`generation` 和单调 fence；旧连接或旧 fence 的写入会被拒绝。客户端负责续租，后端负责队列、公平授予、超时和撤销。

Agent 只能排队获取 Control，不能主动 Takeover。人可以显式 Takeover；这会撤销 Agent Control 并中止其 Run。人也可以在明确选择时向当前 Agent Run 注入一条 cooperative write，这条写入被独立审计，但不会转移 Agent Control。

### Run

Run 是证据边界，不是设备复位，也不保证设备状态干净。一个端口同时最多一个活动 Run。

MCP `run_start` 完成 Control 获取和 Run 创建，返回：

- `run_id`：时间线中的公开审计 ID；
- `run_handle`：22 字符、仅当前 adapter 进程解析的工作流句柄。

后续 Run-scoped 工具只传 `run_handle`。adapter 内部解析端口、Run 和 Control 状态，并在物理动作前再次检查。正常完成必须调用 `run_end`；默认 1800 秒的孤立 Run 回收只处理 Agent 中断或遗弃，`0` 表示不限时。该设置写在共享 `serialctl.toml`；未使用命令行 override 的 adapter 会在运行中自动加载修改。

### 串行上下文保护

Agent 物理动作带 daemon-enforced sequence precondition：上一游标、预期 generation 和预期 TX offset。新的 RX 不阻止写入，但 generation 变化、第三方 TX、显式 gap 或 replay ring 边界不足会在零字节写入时拒绝动作。

adapter 在结果中只在必要时附加 `recent_context`：例如用户 Takeover、其他 Agent 写入、Run 中止、机型 Profile 或具体机型名变化。如果连续两次操作之间没有第三方干扰，不增加该字段。

人工在 Agent Run 中使用 `Alt+Enter` 是绑定到该 Run 的 cooperative write，不转移 Control。它仍然改变了真实串口上下文，因此 Agent 下一次 `command`、`command_sequence`、`input`、`signal` 或 `trigger` 会在写前返回 `context_changed`，并明确给出 `no_bytes_written=true` 与 `recent_context`。Agent 调用 `read(scope=tail)` 或 `wait` 阅读并确认新状态后，才决定是否重试。

## 时间线与持久 journal

每条 `TimelineEvent` 包含：

- `port`、`daemon_epoch`、`seq`、`generation`
- wall-clock 与 monotonic 时间
- `kind`、`direction`
- 可选 actor、Run、operation
- 可选 RX/TX stream offset
- 原始 bytes 与 metadata
- `durable` 状态

RX 最多按 4 ms 或 4 KiB 合并。确认后的 TX 才形成 TX 事件；请求被拒绝时不会伪造“已发送”的历史。

journal 使用分段二进制记录、CRC 和断尾恢复。默认单段 64 MiB，整体上限 10 GiB，达到上限后按保留目标裁剪旧段。当前周期与历史周期分开，gap 以明确原因返回：周期变化、ring 淘汰、保留裁剪、损坏、写日志故障或序号不连续。

关闭 TUI/App 不影响 journal。客户端重新打开时先从当前周期的持久历史恢复，再从最终恢复游标附加实时 WebSocket，避免重复或丢失持久化尾部。

## 实时 ring 与有界查询

每个端口维护有界 replay ring。`/tail` 与 MCP `read(scope=tail|continue)` 只读取这个 ring，工作量与总 journal 大小无关，因此持续运行和大量串口输出不会让普通 tail 扫描历史段。

归档 `/events` 查询支持：周期、序号区间、时间区间、方向、事件类型、actor、Run、operation、普通文本、正则、事件数和字节数。查询具有扫描、编译、时间和并发预算；超过预算返回明确错误和 continuation，而不是占用后端至失去响应。

匹配可以跨相邻同方向事件，避免 OS read chunk 边界隐藏文本。`(after_seq, through_seq]` 的上界是包含式，适合精确读取 Monitor incident 证据。

## Agent 命令与输出定位

`command` 在确认 TX 前附加：

- `command_description`
- `command_capture_matchers`
- 可选 `command_sequence_*` 分组字段

`command_capture_matchers` 是数组，元素结构为：

```json
{"kind":"contains|regex|shell_prompt|uboot_prompt","value":"..."}
```

显式 `expect` 或 `regex` 产生一个 matcher；没有显式边界时，adapter 持久化当前 Model Profile 的 Shell/U-Boot matcher（0–2 个）；quiet completion 不添加 matcher。

TUI 和 App 从该命令 TX 后的 RX 开始寻找第一个匹配边界，并高亮 TX 对应的设备回显与完成边界之间的 RX 区域。因为 matcher 随 TX 一起持久化，后来修改 Model Profile 不会改变旧命令的定位语义。TUI 只在本地同周期窗口能够证明 TX 到匹配边界完整连续时直接高亮；本地记录淘汰或命令属于旧周期时，复用精确证据查询从 journal 回取。retention gap、序号缺失、预算超限或查询失败都不会留下局部高亮，而是返回实时尾并显示原因。找不到边界时只显示临时命令提示，不把 TX 注入 RX 历史。

`command_sequence` 在一个 MCP 调用中执行 1–8 个已知依赖步骤。每个非最终步骤必须配置 `expect` 或 `regex`；只有匹配后才发送下一条，任何失败都会停止剩余写入。每个步骤保留独立描述、命令 bytes、matcher 与执行状态，整体用 `sequence_id` 和总任务描述分组。

任务与命令记录使用两级动作模型：普通 `command` 是一个 action，进入后直接定位它的命令与完整 RX 捕获区间；`command_sequence` 是一个 action，进入后列出各 step，上下选择 step 后按该 step 的 TX 起点、下一 step 上界和 matcher 独立查询、定位并高亮。没有 matcher 时只显示临时命令文本；有 matcher 但本地证据不完整时必须等待 journal 的完整连续结果，不改写串口历史，也不降级展示局部尾部。

## Trigger 与 Monitor

### Trigger

Trigger 是后端内的有界低延迟反应：可先执行一次 kickoff，再按间隔发送 action，并由 RX literal、超时或最大发送次数结束。matcher 在 kickoff 前就已启用，避免短窗口跨 Agent/VM/network 往返。

Trigger 不包含设备厂商语义。每次写入仍经过 Control、Run、fence、generation、pacing 和确认审计。观察 gap、Control/Run 丢失或物理重开都会终止 Trigger。

### Monitor

Monitor 是后端持久运行的 RX 观察任务。一个 Monitor 包含 1–16 个 literal/regex matcher，条件按 OR 计算。它独立于一次 Agent 调用，按固定 debounce window 聚合 burst，并用 cooldown 限制重复 incident。

incident 保存命中的 matcher 索引与条件、短 preview、端口、周期、精确序号区间、evidence cursor 和 acknowledge 状态。详细 bytes 仍只保存在串口 journal 中。`monitor_stop` 停止未来匹配，但保留已有 incident。

## TUI 结构

TUI 从上到下由四部分组成：

1. 端口状态栏：仅串口名和连接状态；
2. RX 输出区：标题仅为绑定机型名；
3. Agent 任务与命令历史：两条 powerline 风格分隔栏之间；
4. 人工命令输入。

任务与命令记录按旧到新排列，最新在底部。新的 Agent command/command sequence action 到达时，TUI 自动退出旧 action 的子层级并回到底部；同一 action 的 TX 分块或后续 sequence step 只合并进原记录，不重复重置。Monitor 新 incident 只更新对应 Monitor，不强制改变当前选择。默认用 `↑` / `↓` 选择一次 command/command sequence/Monitor action，按 `→` 进入子层级，按 `←` 返回；滚轮和 PgUp/PgDn 浏览当前层级，带 `Ctrl-]` 前缀的 PgUp/PgDn 才滚动串口输出。

主终端只渲染 RX。普通 command 进入后定位完整捕获区间；command sequence 进入后逐步选择和定位；Monitor 进入后显示 matcher，继续进入可选择 incident，并按 `serial_range` 跳转到证据。命令捕获与 Monitor incident 共用同一条精确 journal 证据链：范围属于旧的后端周期或已从 TUI 本地窗口淘汰时，按持久周期和序号边界回取；只有区间完整连续时才显示并高亮 RX，retention gap、缺失、超限或查询失败会返回实时尾并明确提示。双击选词与拖选使用可见高亮，选择不会因为实时刷新立即消失。

持久历史搜索支持文本/正则、大小写、RX/TX、当前周期、保留周期和当前 Run。结果在独立视图中显示，不把旧周期内容混入当前实时终端。

配置菜单只有四个主入口：

1. “修改当前串口配置”：端口与离散参数用 `→` 展开选项、`↑` / `↓` 选择、Enter 应用并折叠；`←` 折叠或返回。串口 Profile 和机型 Profile 只显示已经创建的项。机型名按“机型系列 → 具体机型名”两级选择。提示符和分段发送数值在当前行下展开输入框。
2. “创建配置 Profile”：独立创建串口 Profile 或机型 Profile，直接填写/选择完整参数，不改变当前端口绑定。
3. “设置”：分为“终端界面显示设置”和“serial MCP 设置”，分别配置任务记录栏高度与孤立 Run 自动回收时间。
4. “帮助”：仅列当前工作流快捷键。

“保存并应用配置修改”位于串口和机型字段之后的独立操作区。菜单底部只显示一行按键提示；高亮任意配置项时按 `?` 才显示该项说明。MCP timeout 保存后由运行中的 adapter 自动加载，不需要人工重启。

## Electron 结构

Electron 主进程负责：

- 解析配置 endpoint，并发现同一 data root 已验证的活动 endpoint；
- 在需要时启动随包的本地 `seriald`；
- 连接 HTTP v1 与 WebSocket v5；
- 持久设置、服务退出和优雅清理；
- 向 renderer 暴露窄而类型化的 IPC。

renderer 是 React 视图，不直接访问后端。控制台为三栏布局：端口、RX 终端、Agent 历史；命令输入位于中间栏底部。配置页明确分成串口/Transport Profile、机型系列 Profile 与具体机型名。系统/浅色/深色主题使用相同设计变量。

端口历史在内存视图中有界，权威完整记录仍在后端 journal。桌面搜索和命令区域定位不会改变原始事件。

## MCP transport

`serial-mcp` 支持两种 transport：

- stdio：newline-delimited JSON-RPC，供 MCP host 直接启动；
- Streamable HTTP：`POST http://127.0.0.1:3211/mcp`，sessionless，仅监听 loopback。

两者共享 19 个工具和相同结构化结果。HTTP notification 返回 202；`GET /mcp` 不提供 SSE session。若请求带 `Origin`，只接受相同本地监听端口的 `localhost` 或 `127.0.0.1` origin。

HTTP adapter 还提供仅供统一启动器使用的本地 `GET /health`。启动器据此确认 3211 上确实是 protocol v5 `serial-mcp`，并且它连接的 `server_id`、`daemon_epoch` 和 endpoint 与当前选中的 `seriald` 完全一致；普通 TCP listener、旧协议 adapter 或连接到另一后端的 adapter 都不会被误复用。HTTP adapter 在启动时固定这组后端身份，若同一 endpoint 换成新的 daemon epoch，会拒绝重连，重启 adapter 后才会发布并使用新身份。

没有命令行 timeout override 时，adapter 监视共享 `serialctl.toml` 的 `orphan_run_timeout_seconds`；TUI 保存设置后运行中的 stdio/HTTP MCP 自动应用新值。

纯观察调用可以响应 MCP cancellation。物理写入、Run 变化、Monitor mutation 等调用可能已经跨过副作用边界，因此即使 host 取消，也会继续收敛到权威结果，避免隐藏写入结果后被错误重试。

## 启动拓扑

### 单机默认

```text
serial
  ├── seriald       127.0.0.1:3210
  ├── serial-mcp    127.0.0.1:3211/mcp
  └── serialctl     foreground TUI
```

每个 resolved data root 在打开 journal 前取得 `data/seriald.lock`，保证只有一个后端实例。后端监听成功后发布 `data/active-endpoint.json`，其中记录实际 endpoint、Socket 地址、`server_id`、`daemon_epoch`、protocol version 和 PID。发现端只有在 `/api/v1/health` 返回 `status=ok`，且身份、周期和 protocol v5 与记录完全一致时才接受该端点；失效记录不阻塞新实例取得 lock 并覆写。通配 bind 发布本机可连接的 loopback 地址。

活动端点是运行时事实，因此默认 endpoint 和自定义 endpoint 使用相同规则。App 先启动时，随后运行的 `serial` 复用 App 的后端；`serial` 先启动时，App 发现并复用该后端。App 对 preferred endpoint 也要求有效的 protocol v5 health 和服务身份，发现 marker 时还会逐项核对 health。两者并发首启时，首个进程取得 data-root lock，失败的一方等待并重新发现 winner；全新配置也只原子创建一次，所有启动器读取同一个 `server_id`。两者只管理自己启动的进程：拥有后端的一方退出后，仍在运行的外部客户端不会自动 failover，重新启动后才重新发现或创建服务。`serial` 在选定后端后再保证本地 HTTP MCP 可用，并只回收自己补齐的 MCP 进程。

### 分开运行

```sh
serial serve
serial console --endpoint http://127.0.0.1:3210
serial mcp --endpoint http://127.0.0.1:3210
```

`serial setup` 直接读写后端配置目录，不依赖一个正在运行的服务。TUI、App 和 Profile CLI 修改运行中配置时走 HTTP transaction。

## 发布结构

Jenkins 从一个确定 commit 构建：

- workspace 版本 tag 不存在：Debug 包，不发布 GitHub Release；
- 当前 commit 存在与 workspace 版本一致的 annotated `vX.Y.Z` tag：Release 包并自动发布。

没有人工发布参数。tag 是唯一发布信号，tag 必须 peel 到本次构建 commit；同名 lightweight tag 或指向其他 commit 时仅构建 Debug，不发布。

四个平台包均包含：

```text
serial
seriald
serialctl
serial-mcp
Electron application
BUILD-INFO.json
```

Electron 形态为 Linux AppImage、Windows portable EXE、macOS `.app`。最终 artifacts 生成统一 `SHA256SUMS`。

macOS Rust CLI 的 deployment target 是 11.0；Electron 43 App 的最低系统版本是 12.0。当前 macOS `.app` 没有 Developer ID 签名，也没有 Apple notarization。
