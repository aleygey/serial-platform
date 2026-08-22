# Serial Platform Roadmap

## 定义

Serial Platform 是一个基于人/Agent 协同交互的通用串口平台。

这个定义成立的前提是：

1. 物理串口由一个后端独占，避免多个工具争抢句柄；
2. 人和 Agent 看到同一份串口事实、控制状态与持久历史；
3. 写入有顺序、有确认、有来源，不把“请求发送”误当成“设备收到”；
4. 端口和机型配置足够直观，首次使用可以快速完成；
5. 通用平台只提供串口原语，不把厂商流程和 Shell 假设写进核心。

当前产品结构围绕这五点收敛。公开设备身份只有 OS 串口名 `port`；Transport Profile 描述主机 UART，Model Profile 描述一个机型系列共用的交互行为，端口的 `model_name` 标记当前连接的具体型号；TUI、Electron 与 19 个 MCP 工具共享 `seriald` 时间线。

## 当前能力

### seriald

- 独占 Windows、macOS 和 Linux 串口，自动打开、断连重试和显式开关。
- 多观察者订阅同一 RX、确认 TX 和状态事件。
- 带 fence 的 Control lease、排队、续租、人工 Takeover 和 cooperative write。
- 每个端口一个活动 Run，Run/operation 边界进入权威时间线。
- 端口重配 transaction 与 `config_revision` 并发保护。
- App 与 CLI 在同一本地数据目录中自动发现并验证唯一后端，默认和自定义 endpoint 使用相同复用规则。
- Transport Profile：baud、data/parity/stop、flow control、DTR/RTS、auto-open。
- Model Profile：机型系列、该系列的具体机型名列表、Shell/U-Boot prompt、EOL、echo 解析和 write pacing；端口单独绑定具体 `model_name`。
- 有界 replay ring；tail 查询的成本不随持久日志增长。
- 分段 journal、CRC、断尾恢复、gap ledger、保留上限和 bounded regex 查询。
- daemon-owned Trigger：kickoff、重复 action、RX stop literal 和硬上限。
- 持久 Monitor：1–16 个 OR literal/regex matcher、burst grouping、命中条件、精确串口范围、证据游标和 acknowledge。
- HTTP v1 与 WebSocket protocol v5；公开请求和事件统一使用 `port`。

### serialctl / TUI

- `serial setup` 在后端未运行时直接完成首次配置，并提供简短中英说明。
- 两类 Profile 的 list/show/create/update/clone/import/export/delete 与端口 attach/detach。
- connection、port、stream、storage、state 五层诊断和 JSON 输出。
- 持久日志的文本/正则、周期、Run、operation、方向、类型和游标查询。
- RX-only 主终端；关闭再打开后从当前周期 journal 恢复并接回实时流。
- 顶部使用串口名，输出标题使用原样机型名。
- 任务与命令记录从旧到新；新 Agent command/sequence action 强制回到底部，同一 action 的后续 step 不重复重置；方向键进入/返回层级。
- 普通 `command` 定位完整 RX 捕获；`command_sequence` 展开后逐 step 选择、跳转并高亮。
- Monitor action 展开 matcher 和 incident，按 `serial_range` 跳转串口证据；旧后端周期或本地窗口已淘汰时从 journal 回取完整连续范围后再高亮。
- 滚轮/PgUp/PgDn 浏览 Agent 历史，前缀组合滚动串口输出。
- 双击词语与拖选的可见高亮和复制。
- 串口历史搜索支持文本/正则、大小写、RX/TX 和不同周期范围。
- 配置菜单分成当前串口、创建 Profile、设置和帮助；选项行按 `→` 展开、`↑` / `↓` 选择、Enter 应用，文本/数值行内编辑，`?` 按需显示字段说明。
- 可配置 Agent 历史高度与 30 分钟默认孤立 Run 回收；`0` 为不限时，MCP 自动加载保存后的 timeout。
- 词边界关键词、IPv4、IPv6 和 MAC 地址着色。

### Electron App

- Electron 主进程管理本地后端生命周期并复用 HTTP/WebSocket v5。
- 左侧端口、中间 RX 终端、右侧 Agent 历史的三栏工作台。
- 串口开关、人工命令、持久历史、终端搜索和命令输出区域高亮。
- Agent 命令与序列按旧到新显示并自动跟随。
- 串口/Transport Profile、机型系列 Profile 与具体机型名分区配置。
- 机型系列和具体机型名原样显示；共享 Model Profile 影响端口明确提示。
- 系统、浅色、深色主题和常用快捷键。
- context-isolated preload 与类型化 IPC；renderer 不直接连接后端。
- Linux AppImage、Windows portable EXE、macOS arm64/x86_64 `.app`。

### serial-mcp

- stdio 与 loopback sessionless Streamable HTTP 两种 transport。
- 19 工具覆盖设备/机型系列与具体机型、Run、命令、原始输入、信号、Trigger、查询和 Monitor。
- `run_handle` 收敛 Run-scoped 参数；正常会话结束前由 Agent 调用 `run_end`。
- `command_sequence` 一次完成 1–8 步依赖交互；每个非最终步骤必须等到明确 RX 边界。
- 命令 TX 持久化 `command_capture_matchers`，供人类界面精确定位输出。
- process-local live cursor、bounded capture、明确 truncation/gap/interference。
- Agent 写入前的串口上下文保护；第三方变化时 fail-before-write，返回 `context_changed`、`no_bytes_written` 和紧凑 `recent_context`，由 `read`/`wait` 建立确认边界。
- 一个 Monitor 调用可提交多 matcher OR 条件；incident 返回命中条件与 `serial_range`。
- Monitor 在 adapter 退出后仍由后端运行。

### 交付

- 裸 `serial` 一次提供 `seriald`、HTTP MCP 和前台 TUI；App 与 `serial` 在默认或自定义 endpoint 下都复用同一 data root 的已验证活动后端，只管理自己启动的子进程，不对后来消失的外部后端自动 failover。
- 四个平台包都包含 `serial`、`seriald`、`serialctl`、`serial-mcp` 和 Electron App。
- Jenkins 只在 Prepare/Rust 节点 checkout GitHub 一次；后续 Linux、macOS、归档与发布节点消费并校验同一份 pinned source bundle，macOS 不再独立拉取仓库源码。workspace 版本 tag 不存在时构建 Debug；当前 commit 的 annotated version tag 自动触发 Release 与 GitHub 发布；tag 类型或 commit 不匹配时仅构建 Debug，不发布。
- GitHub Actions 负责 Rust 多平台检查和 Electron typecheck/test/build/原生打包验证，不承担发布。

## 发布前必须验证

这些项目是当前发布质量门槛：

- 两个端口持续 115200 baud 输出下，TUI、App 与 MCP tail 保持有界和可响应。
- Windows 不同 USB-UART 驱动上的拔插、访问占用、Break 与写超时分类。
- macOS arm64/x86_64 App、Windows portable、Linux AppImage 内的 sidecar 启动与退出。
- `serial` 首次运行、离线 `serial setup`、App→`serial` 与 `serial`→App 双向复用、默认/自定义 endpoint、外部 owner 退出和端口为空的启动路径。
- TUI 重开恢复、后端重启边界、journal retention gap 与损坏断尾恢复。
- command matcher 在普通命令、命令序列逐 step、无回显、提示符变化和无匹配时的定位。
- Monitor 多 matcher OR、debounce 聚合、incident 命中条件、串口证据跳转，以及旧周期/本地淘汰后的 journal 回取与 retention gap 提示。
- 人工 Takeover、cooperative write、其他 Agent 写入、机型系列/具体型号切换的 `recent_context` 与零字节拒写。
- TUI 选项展开/折叠、两级机型选择、行内输入、字段帮助和 MCP timeout 热加载。
- Streamable HTTP initialize、notification 202、cancellation、Origin 和 19 工具 schema。
- annotated tag 到 Jenkins、四平台 artifacts、SHA256SUMS 和 GitHub Release 的完整链路。

## 非目标

- 把 U-Boot、Linux、AT command 或厂商烧录流程编进 `seriald`。
- 为没有证据的设备状态提供“成功”结论。
- 自动重试结果不确定的物理写入。
- 在静默端口周期性发送猜测性探针。
- 为尚未出现的生产场景增加复杂配置和操作步骤。
