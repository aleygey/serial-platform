# Serial Platform Architecture

本文描述 Serial Platform 的当前架构。具体 JSON 契约以 [protocol v9](./docs/PROTOCOL.md) 为准，Agent 工具以 [MCP 工具目录](./docs/MCP_TOOLS.md) 或 `serial mcp --dump-tools` 为准。

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
| `seriald` | 物理串口所有权、Control/Run/Trigger、journal、HTTP v1 和 WebSocket v7 |
| `serialctl` | 人工 setup、Profile 管理、诊断、历史查询和全屏 TUI |
| `serial-mcp` | 面向 Agent 的 16 工具；支持 stdio 与 sessionless Streamable HTTP |
| Electron App | 本地服务生命周期、三栏控制台、配置页和桌面快捷键 |
| `serial-protocol` | 跨组件 DTO、v7 WebSocket 消息和二进制帧 codec |

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
| `model_profile` | 可复用的串口交互行为 Profile | 人通过 TUI、App、CLI 或 HTTP 修改端口绑定 |
| `model_family` | 当前设备的一级机型系列 | 与 `model_name` 成对设置或清空 |
| `model_name` | 当前设备的二级具体机型 | 从 `model_family.model_names` 中选择 |
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
  "model_profile": "linux-shell",
  "model_family": "TL-AS7230",
  "model_name": "TL-AS7230-W 1.0",
  "enabled": true
}
```

`transport_profile` 与 `model_profile` 可以各自省略。`model_family` 与 `model_name` 是独立于行为 Profile 的身份对：必须同时设置或同时省略，且具体名必须存在于对应 Model Family 的 `model_names` 中。解绑行为 Profile 不会清除机型身份；清除身份时两个字段一起清除。`enabled=false` 保留配置但不打开串口。

配置更新带 `config_revision` 乐观并发保护。后端先验证完整候选配置，再持久化并发布；物理 UART 变更通过暂停、关闭旧句柄、应用、提交的事务路径完成。失败不会留下部分生效的配置。当前 `seriald.toml` 持久配置是 `schema_version=3`。首次加载 schema 2 时，后端会严格解析并验证旧配置，在内存中确定性拆分行为 Profile 与两级机型身份，保留原 `server_id`、`config_revision` 和其他配置；取得数据目录所有权后先以 create-new 语义保留原始备份，再原子写入 schema 3。迁移失败不覆盖原文件；没有明确迁移规则的其他 schema 继续拒绝。

端口重配事件记录 `source`、行为 Profile、一级机型系列和二级具体机型的前后值。MCP 最近上下文只向 Agent 摘要端口被重配以及机型身份的前后值，不暴露行为 Profile 名；Agent 可重新调用 `devices` 获取当前已生效的提示符。

### Transport Profile

Transport Profile 描述主机 UART：波特率、数据位、校验位、停止位、流控、DTR、RTS 和自动打开。更新绑定中的 Transport Profile 可能触发串口重开。

通用基线：115200、8N1、无流控、DTR/RTS 低、自动打开。

### Model Family 目录

Model Family 目录只描述设备身份，不携带串口行为：

```json
{
  "name": "TL-AS7230",
  "model_names": [
    "TL-AS7230-W 1.0",
    "TL-AS7230-F4GE 1.0"
  ]
}
```

`name` 是第一级机型系列，`model_names` 是第二级具体机型。名称原样保存，不替换空格、不改变大小写。目录是全量替换契约；当前端口正在引用的系列或具体机型不能被删除。

### Model Profile

Model Profile 只描述可复用的串口交互行为：

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

Model Profile 不包含 `model_names`，也不规定设备必须属于哪个系列。提示符可以为空；后端不会猜测 Shell 或 U-Boot。Model Profile 更新会立即影响所有绑定端口的命令边界与写入行为，但不需要重开物理串口。

`echo` 只指导 Agent 捕获如何识别和去除设备自身回显；人类界面不会额外合成本机 TX 到 RX 终端。

### 写入节奏

有效写入节奏由 Model Profile 的可选覆盖决定，未设置时使用通用值。`write_chunk_size` 限制每次驱动写入的字节数，`write_chunk_delay_ms` 是相邻 chunk 之间的请求延时；`0` 选择全速路径。

后端在进入驱动前计算完整 pacing budget。单次物理写入受最大字节数和时间预算限制，无法完成的请求在触碰串口前失败。已进入驱动且结果不确定的写入不会自动重试。

## 物理所有权与协同写入

### Control

普通写入需要 Control lease。lease 包含 Control ID、`daemon_epoch`、`generation` 和单调 fence；旧连接或旧 fence 的写入会被拒绝。客户端负责续租，后端负责超时和撤销。

v7 没有通用 Control waiter queue。已有 holder 时，`AcquireControl(mode=queue)` 立即返回 busy；释放、到期或断连不会提升隐藏 waiter，`CancelAcquire` 只保留为返回 `removed=false` 的兼容 no-op。Agent 也不能调用 `AcquireControl` 绕过 Run 边界。Human 仍可以明确 Takeover；这会撤销旧 Control、停止相关 Trigger 并中止 Agent Run，而不是一次普通命令的默认路径。

Human 普通命令走 `SendHumanCommand`，授权和物理写入都在同一个端口 actor 中决定，绝不排队：

- Control 空闲时，先原子授予该 Human lease，再写入；
- 该 Human 已是 holder 时，按普通 owned write 写入；
- Agent 持有 Control 且同一 Agent 有活动 Run 时，按 cooperative write 写入，不借用 Agent fence，也不转移 lease；活动 Trigger 会先停止并收敛；
- 任何其他 owner、没有匹配的活动 Agent Run、端口或 generation 已变化时，立即冲突。

Human TX 是普通 confirmed `tx` timeline event，并带 `human_command=true`。若属于 cooperative write，还带 `interfered_run_id` 和新的 `context_revision`；部分写入只要确认了至少一个 byte，也会形成 TX、推进 revision 并按不确定物理结果处理。

### Run

Run 是证据边界，不是设备复位，也不保证设备状态干净。一个端口同时最多一个活动 Run。

MCP `run_start` 使用单个 v7 `RequestRunStart` 完成授权与 Run 创建，返回：

- `run_id`：时间线中的公开审计 ID；
- `run_handle`：22 字符、仅当前 adapter 进程解析的工作流句柄。

端口 idle 时，`seriald` 在同一个 Slot turn 中原子 grant Agent Control 并创建 Run，不暴露“有 Agent Control、没有 Run”的中间状态。若当前 holder 是 Human，daemon 创建仅由该精确 holder 决策的 `PendingRunStartApproval`；snapshot 的 `pending_run_start` 和 `run_start_requested` timeline event 立即向 UI 投影。批准时再次核对 daemon epoch、generation、Human Control ID/fence、Run 和 Trigger，然后在同一个 turn 原子 transfer+Run；否决、超时、请求方取消、任一相关连接断开、holder 改变、重配或物理重连全部 fail closed，不创建 Run，也不写串口。其他 owner、活动 Run/Trigger 或已有另一项审批都立即冲突，不进入 Control queue。

Agent 用同一 request ID 和完全相同参数幂等轮询 pending/terminal 结果。只有 requester Agent 能取消，只有 approval 指定的当前 Human holder 能 approve/deny。五类 timeline 投影 `run_start_requested`、`run_start_approved`、`run_start_denied`、`run_start_timed_out`、`run_start_cancelled` 都保留完整 approval，方便 TUI/App 在 snapshot 与实时事件之间无损收敛。

后续 Run-scoped 工具只传 `run_handle`。adapter 内部解析端口、Run 和 Control 状态，并在物理动作前再次检查。工作流收口统一调用 `run_end`：`outcome=completed` 表示正常完成且是默认值，记录 `RunEnded` 后立即尝试释放 Control；`outcome=aborted` 只有在收到权威的 `ControlReleased` 后才成功，并记录 `RunAborted`。默认 1800 秒的孤立 Run 回收只处理 Agent 中断或遗弃，`0` 表示不限时。该设置写在共享 `serialctl.toml`；未使用命令行 override 的 adapter 会在运行中自动加载修改。

### 串行上下文与 Human read gate

Agent 物理动作仍带 daemon-enforced sequence precondition：上一游标、预期 generation 和预期 TX offset。新的 RX 不阻止写入，但 generation 变化、第三方 TX、显式 gap 或 replay ring 边界不足会以 `sequence_boundary_changed` 在零字节写入时拒绝动作。adapter 还会在必要时附加有界 `recent_context`，例如 Takeover、其他 actor 写入、Run 中止或端口重配；摘要不暴露行为 Profile 名。

Human 在活动 Agent Run 中成功发送命令后，daemon 的 `run_context.revision` 递增，`last_human_command_seq` 指向最新 Human TX。此后该 Run owner 的 `Write`、UART Break 和 `TriggerStart` 都在触碰物理 writer 前返回 `UserReadRequired`，保证零字节。线协议稳定 error code 是 `user_read_required`，MCP 向模型暴露 `user_command_used`、`no_bytes_written=true`。

解除 gate 必须证明 Agent 实际读到了这条人工 TX：只有 `read(scope=tail|continue)` 的 live-ring 响应同时包含同一 daemon epoch、精确 seq、`direction=tx` 且 `human_command=true` 的事件，并且返回 cursor 覆盖该 seq，adapter 才发送 `AcknowledgeRunContext`。daemon 只接受当前 Agent Control/Run owner、精确最新 revision 且 `through_seq >= last_human_command_seq` 的 ACK。若 `wait` 已推进普通 live cursor 越过该 TX，下一次 live read 会临时从 `last_human_command_seq-1` 重新读取；若事件已被 ring 淘汰则明确返回 gap/warning，仍不会确认。`wait`、`search`、`read(scope=archive)`、不含该 TX 的 bounded live read，以及单纯移动 cursor 都不会清除 gate；Agent 必须先 live read，再决定下一条 command。

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

`command`/`command_sequence` 的每一步都先产生带 `run_id`、`operation_id` 和 description 的 confirmed TX。捕获完成后，adapter 向 daemon 发送 `RecordCommandCapture`，报告：

- daemon epoch 与 serial generation；
- Run、operation 和 confirmed TX event seq；
- 包含该 TX 的 `evidence_from_seq..=evidence_through_seq`；
- completion（literal/prompt/regex/quiet/signal/run-aborted/timeout/disconnected）、可选 detail 和 confidence。

`seriald` 不信任客户端自行声明的精确性：它从 replay ring 证明证据仍保留、范围没有 gap 且不跨 generation，并验证 TX 的 actor、Run、operation 与 generation 全部匹配。然后 daemon 从原始 timeline 派生 TX/RX stream offsets，持久化 `CommandCaptureCompleted` event，在 `metadata.capture` 保存完整 DTO，并以 `CommandCaptureRecorded` 返回同一权威记录。证据已淘汰、不连续、跨代或身份不符时拒绝记录；命令已经写入的情况下，adapter 会明确提示只恢复证据工作流，绝不盲目重发命令。

权威 capture 同时保留 sequence 边界和尽量精确的 byte offset：

```json
{
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

TUI 和 App 以这条 durable capture 作为命令历史定位的主索引：先按 operation 关联，再优先使用 epoch/sequence evidence 与 stream offsets；本地窗口不足时按精确范围从 journal 回取。只有区间连续完整且没有 gap 才显示和高亮 RX；retention、缺失、预算超限或查询失败都不能伪装成完整输出。

TX 上的 `command_capture_matchers` 仍随 description 和可选 `command_sequence_*` 分组字段持久化，但只用于读取旧历史或 capture event 缺失时的 legacy fallback。v7 新记录不能把 matcher 的再次扫描结果冒充 daemon-validated capture；后来修改 Model Profile 也不会改写任何已经持久化的 matcher 或 capture。

`command_sequence` 在一个 MCP 调用中执行 1–8 个已知依赖步骤。每个非最终步骤必须配置 `expect` 或 `regex`；只有匹配后才发送下一条，任何失败都会停止剩余写入。每个步骤保留独立描述、命令 bytes、matcher 与执行状态，整体用 `sequence_id` 和总任务描述分组。

任务与命令记录使用三层树模型：第一层是 Run 及其状态，第二层是 `command` / `command_sequence` action 的 description，第三层是实际发送的命令。普通 `command` 只有一个第三层子项；`command_sequence` 按 step 顺序列出多个子项，每个 step 按自己的 operation/capture 独立定位。还没有权威 capture 时只显示 pending/临时状态；本地证据不完整时必须等待 journal 的完整连续结果，不改写串口历史，也不降级展示局部尾部。

## Trigger 与 Monitor

### Trigger

Trigger 是后端内的有界低延迟反应：可先执行一次 kickoff，再按间隔发送 action，并由 RX literal、超时或最大发送次数结束。matcher 在 kickoff 前就已启用，避免短窗口跨 Agent/VM/network 往返。

Trigger 不包含设备厂商语义。每次写入仍经过 Control、Run、fence、Human read gate、generation、pacing 和确认审计。观察 gap、Control/Run 丢失、Human cooperative command 或物理重开都会终止 Trigger。

### Monitor

Monitor 是后端持久运行的 RX 观察任务。一个 Monitor 包含 1–16 个 literal/regex matcher，条件按 OR 计算。它独立于一次 Agent 调用，按固定 debounce window 聚合 burst，并用 cooldown 限制重复 incident。

incident 保存命中的 matcher 索引与条件、短 preview、端口、周期、精确序号区间、evidence cursor 和 acknowledge 状态。详细 bytes 仍只保存在串口 journal 中。`monitor_stop` 停止未来匹配，但保留已有 incident。

## TUI 结构

TUI 从上到下由四部分组成：

1. 端口状态栏：仅串口名和连接状态；
2. RX 输出区：标题仅为绑定机型名；
3. Agent 任务与命令历史：两条 powerline 风格分隔栏之间；
4. 人工命令输入。

任务与命令记录按旧到新排列，最新 Run 在底部。新的 Agent action 到达时，TUI 自动退回它所属的 Run；同一 action 的 TX 分块或后续 sequence step 只合并进原记录。Monitor 新 incident 只更新对应 Monitor，不强制改变当前选择。默认用 `↑` / `↓` 在当前层选择，按 `→` 按 Run → description → 具体命令逐层展开，按 `←` 逐层返回；展开的长详情用 `Shift+↑` / `Shift+↓` 滚动，滚轮和 PgUp/PgDn 始终滚动串口输出。

主终端只渲染 RX。普通 command 进入后定位完整捕获区间；command sequence 进入后逐步选择和定位；Monitor 进入后显示 matcher，继续进入可选择 incident，并按 `serial_range` 跳转到证据。命令捕获与 Monitor incident 共用同一条精确 journal 证据链：范围属于旧的后端周期或已从 TUI 本地窗口淘汰时，按持久周期和序号边界回取；只有区间完整连续时才显示并高亮 RX，retention gap、缺失、超限或查询失败会返回实时尾并明确提示。双击选词与拖选使用可见高亮，选择不会因为实时刷新立即消失。

TUI 从 snapshot 的 `pending_run_start` 与五类 Run-start timeline event 投影审批状态；只有 approval 中指定的当前 Human holder 能批准或否决。批准是 daemon 内原子 transfer+Run，UI 不自行先 release Control。人工命令直接调用 `SendHumanCommand`，不会进入 Control queue；Agent Run 中的 cooperative 结果会标明该 Run 与 context revision，供操作员理解为什么 Agent 必须先读到这条 TX。

`Ctrl-] /` 在串口输出右上角打开即时查找框。输入即在清洗后的显示行中匹配并把主输出定位到最新命中的上下文；Enter/F3 与 Shift+Enter/Shift+F3 循环切换结果，Alt 组合切换文本/正则、大小写、方向和范围。结果锚定 epoch/sequence/行内 offset，实时追加按 100 ms 合并刷新；当前本地窗口已淘汰更早内容时明确显示范围不完整，不把有界空结果冒充全 journal 未命中。完整持久历史仍由 `serial logs` 或 MCP archive 查询。

配置菜单只有四个主入口：

1. “修改当前串口配置”：端口与离散参数用 `→` 展开选项、`↑` / `↓` 选择、Enter 应用并折叠；`←` 折叠或返回。串口 Profile 和机型 Profile 只显示已经创建的项，其中“机型 Profile”只选交互行为。独立的“机型名”按“一级机型系列 → 二级具体机型”选择，二级 Enter 确认，空二级不能绑定。提示符和分段发送数值在当前行下展开输入框。
2. “创建配置”：包含“创建串口 Profile”“创建机型 Profile”和“配置机型名”。前两项创建可复用行为配置，不改变当前端口绑定。“配置机型名”首页显示“新增一级机型名”和已有机型系列；用 `↑` / `↓` 选择，`→` 或 Enter 进入系列，再通过“新增二级机型名”在当前行下输入具体机型；`←` 逐级返回。
3. “设置”：分为“终端界面显示设置”和“serial MCP 设置”，分别配置任务记录栏高度与孤立 Run 自动回收时间。
4. “帮助”：仅列当前工作流快捷键。

“保存并应用配置修改”位于串口和机型字段之后的独立操作区。菜单底部只显示一行按键提示；高亮任意配置项时按 `?` 才显示该项说明。MCP timeout 保存后由运行中的 adapter 自动加载，不需要人工重启。

## Electron 结构

Electron 主进程负责：

- 解析配置 endpoint，并发现同一 data root 已验证的活动 endpoint；
- 在需要时启动随包的本地 `seriald`；
- 连接 HTTP v1 与 WebSocket v7；
- 持久设置、服务退出和优雅清理；
- 向 renderer 暴露窄而类型化的 IPC。

renderer 是 React 视图，不直接访问后端。控制台为三栏布局：端口、RX 终端、Agent 历史；人工命令位于中间栏底部。终端右上角的 VS Code 风格 Find Widget 默认隐藏，Cmd/Ctrl+F 或 F3 显式打开，输入即定位，支持循环导航、跨 RX event 匹配和稳定 `n/total`；追加只扫描查询长度所需的尾部重叠区与新增文本，并仅重绘命中状态变化的 chunk。Run-start approval 由 snapshot/timeline 驱动，只有指定 Human holder 可以 approve/deny，终态按 `approval_id` 收敛。命令历史优先读取 `command_capture_completed.metadata.capture` 的 daemon-validated evidence，旧 matcher 只作兼容回退。配置页明确分成串口/Transport Profile、行为 Model Profile 与独立的两级 Model Family 机型目录。系统/浅色/深色主题使用相同设计变量。

端口历史在内存视图中有界，权威完整记录仍在后端 journal。桌面搜索和命令区域定位不会改变原始事件。

## MCP transport

`serial-mcp` 支持两种 transport：

- stdio：newline-delimited JSON-RPC，供 MCP host 直接启动；
- Streamable HTTP：sessionless `POST http://<exact-interface-ip>:3211/mcp`；默认单机是 `127.0.0.1`。

统一启动器从已验证的 `active-endpoint.json.address` 取 seriald 的精确 IP，再把 MCP 端口固定为 3211：seriald 绑定 `192.168.56.109:3210` 时 MCP 就绑定 `192.168.56.109:3211`；只有 seriald 的 IPv4/IPv6 bind 是通配地址时，活动 endpoint 才先转换为可连接的 `127.0.0.1`/`::1`。它不会把 host-only 配置悄悄改成 loopback，也不会让 MCP 监听所有接口。

Streamable HTTP 没有认证和 TLS。listener 必须是一个精确单播地址，拒绝 IPv4/IPv6 unspecified、multicast 和 IPv4 broadcast；非 loopback 只允许用于受信 host-only VM interface，进程会打印安全警告，并要求用主机防火墙限制访问。`Origin` 可以省略；存在时必须是同端口、无 credentials/path/query/fragment 的 `http://` origin，host 是 listener 的精确 numeric IP。仅当 listener 本身为 loopback 时额外允许 `localhost`。这项检查是浏览器跨站缓解，不是认证，不能据此把 endpoint 暴露到 LAN 或公共网络。

两种 transport 共享以下固定 18 个工具和相同结构化结果：

```text
devices model_identity_set read command command_sequence signal macro_list macro_save macro_run wait
search monitor_start monitor_list monitor_status monitor_incidents monitor_stop
run_start run_end
```

HTTP notification 返回 202；`GET /mcp` 不提供 SSE session。

MCP 公开面中，`devices` 是唯一的设备发现工具：它返回串口名、两级机型身份、Agent 需要的连接/Control/Run/Trigger/cursor、pending Run-start 与 Human read-gate 状态，以及当前有效的 Shell/U-Boot 提示符。它不返回行为 Model Profile 名、Transport/UART 参数、EOL/echo 或写入节奏。`model_identity_set` 只绑定或解绑人工预先配置的 family/name；Profile 和 Model Family 目录仍由 TUI、Electron 或 HTTP 配置。

HTTP adapter 还提供仅供统一启动器使用的 `GET /health`。启动器据此确认精确 IP:3211 上确实是 protocol v9 `serial-mcp`，并且它连接的 `server_id`、`daemon_epoch` 和 endpoint 与当前选中的 `seriald` 完全一致；普通 TCP listener、旧协议 adapter 或连接到另一后端的 adapter 都不会被误复用。HTTP adapter 在启动时固定这组后端身份，若同一 endpoint 换成新的 daemon epoch，会拒绝重连，重启 adapter 后才会发布并使用新身份。

`run_start` 在 idle 时得到原子 grant+Run，在 Human holder 存在时等待该 holder 的显式批准。`command` 和 `command_sequence` 只有在 daemon 持久化权威 capture 后才把完成范围交付给模型。Human command 触发 read gate 时，物理工具返回 `user_command_used`；live `read(scope=tail|continue)` 必须实际包含 Human TX 才会 ACK，`wait` 和 archive read 不会。

没有命令行 timeout override 时，adapter 监视共享 `serialctl.toml` 的 `orphan_run_timeout_seconds`；TUI 保存设置后运行中的 stdio/HTTP MCP 自动应用新值。

可取消的纯观察工具是 `devices`、`read`、`wait`、`search`、`monitor_list`、`monitor_status` 和 `monitor_incidents`。物理写入、Run 变化、机型身份修改、Monitor mutation 等调用可能已经跨过副作用边界，因此即使 host 取消，也会继续收敛到权威结果，避免隐藏变更结果后被错误重试。

## 启动拓扑

### 单机默认

```text
serial
  ├── seriald       127.0.0.1:3210
  ├── serial-mcp    127.0.0.1:3211/mcp
  └── serialctl     foreground TUI
```

每个 resolved data root 在打开 journal 前取得 `data/seriald.lock`，保证只有一个后端实例。后端监听成功后发布 `data/active-endpoint.json`，其中记录实际 endpoint、Socket 地址、`server_id`、`daemon_epoch`、protocol version 和 PID。发现端只有在 `/api/v1/health` 返回 `status=ok`，且身份、周期和 protocol v9 与记录完全一致时才接受该端点；失效记录不阻塞新实例取得 lock 并覆写。精确 bind 保留其 IP，通配 bind 发布本机可连接的 loopback 地址。

活动端点是运行时事实，因此默认 endpoint 和自定义 endpoint 使用相同规则。App 先启动时，随后运行的 `serial` 复用 App 的后端；`serial` 先启动时，App 发现并复用该后端。App 对 preferred endpoint 也要求有效的 protocol v9 health 和服务身份，发现 marker 时还会逐项核对 health。两者并发首启时，首个进程取得 data-root lock，失败的一方等待并重新发现 winner；全新配置也只原子创建一次，所有启动器读取同一个 `server_id`。两者只管理自己启动的进程：拥有后端的一方退出后，仍在运行的外部客户端不会自动 failover，重新启动后才重新发现或创建服务。`serial` 在选定后端后再保证该精确活动 IP 上的 HTTP MCP 可用，并只回收自己补齐的 MCP 进程。

### 分开运行

```sh
serial serve
serial console --endpoint http://127.0.0.1:3210
serial mcp --endpoint http://127.0.0.1:3210
```

`serial setup` 直接读写后端配置目录，不依赖一个正在运行的服务。TUI、App 和 Profile CLI 修改运行中配置时走 HTTP transaction。

## 发布结构

Jenkins 从一个确定 commit 构建。Prepare 阶段只在 Rust 节点向 GitHub checkout 一次，将本次 SCM commit 固定为完整 commit，完成 tag 判定后生成带 SHA-256 的 Git bundle。Linux、macOS、归档和发布阶段都从 Jenkins stash 恢复同一 bundle，并校验 bundle 校验和、HEAD commit 以及 Release tag 归属。macOS 节点不再单独向 GitHub 拉取仓库源码。

构建模式仍只由固定源码的版本 tag 决定：

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
