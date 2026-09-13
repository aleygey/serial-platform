# serialctl

`serialctl` 是 Serial Platform 的人工终端、运行时配置、诊断与历史查询客户端。通常通过统一入口使用：

```sh
serial                         # seriald + HTTP MCP + foreground TUI
serial console                 # 只连接已有后端并打开 TUI
serial status
serial doctor ...
serial archives
serial logs ...
serial profile ...
```

发行包中的 `serialctl` 也可以直接运行。它不打开物理 UART；所有实时数据与写入都经过 `seriald`。

裸 `serial` 会在同一本地数据目录中自动发现并验证唯一 `seriald`，没有可用服务时才启动后端。无论使用默认还是自定义 endpoint、先开 App 还是先运行 `serial`，后启动的一方都会复用同一个服务。

App 和 `serial` 只停止自己启动的进程。外部 owner 退出后，仍在运行的客户端不会自动 failover；重新启动后才重新发现或创建服务。

## 首次 setup

推荐：

```sh
serial setup
```

本地离线向导无需先启动 `seriald`：先自动扫描串口（只枚举、不打开或发送），支持刷新、多选、手动输入和稍后配置；再设置波特率/EOL、可选高级参数与机型，最后确认并选择保存或直接打开。`q` 取消不保存，未选中的已有端口和 Profile 保留。

若本地后端已在运行，会复用在线配置；`serial setup --endpoint http://HOST:3210` 或 `serialctl --endpoint http://HOST:3210 setup` 使用后端机器的扫描列表。在线向导支持 `r` 刷新、`m` 手动输入、`q` 取消，并在保存前确认。Profile 和机型列表直接显示供选择。

概念说明：

- 后端地址 / Endpoint：监听 IP 和端口；
- 串口 Profile / Transport Profile：波特率、数据位、校验位等 UART 参数；
- 机型 Profile / Model Profile：可复用的 Shell/U-Boot 提示符、换行、设备回显解析和写入节奏。
- 机型名：一级机型系列与其下的二级具体机型名，只用于标记当前串口连接的设备身份。

串口名是唯一端口标识，例如 `COM4`，没有额外 slot 名。机型 Profile（交互行为）和机型名（设备身份）彼此独立；端口同时保存 `model_family` 与 `model_name`，具体机型名必须属于所选一级系列。所有名称都按输入原样保存和显示。

## Profile CLI

Transport Profile 管理物理 UART：

```sh
serial profile transport list
serial profile transport show uart-115200
serial profile transport create --interactive
serial profile transport update uart-115200 --interactive
serial profile transport clone uart-115200 --name uart-921600 --baud-rate 921600
serial profile transport import profiles.toml
serial profile transport export uart-115200 --output uart-115200.toml
serial profile transport delete uart-115200 --yes
```

Model Profile 只管理可复用的设备交互行为：

```sh
serial profile model list
serial profile model show router-shell
serial profile model create --interactive
serial profile model update router-shell \
  --shell-prompt 'root@router:~# '
serial profile model clone router-shell --name router-shell-lab
serial profile model import models.json
serial profile model export router-shell --output model.json
serial profile model delete router-shell --yes
```

绑定和解绑：

```sh
serial profile attach --port COM4 --transport uart-115200 --model router-shell \
  --model-family TL-AS7230 \
  --model-name 'TL-AS7230-W 1.0'
serial profile detach --port COM4 --model
serial profile detach --port COM4 --identity
serial profile detach --port COM4 --transport
```

裸 `serial profile detach --port COM4` 只解绑机型行为 Profile，等价于 `--model`；设备身份只有显式传入 `--identity` 才会清除。`--model-family` 与 `--model-name` 必须成对提供。

`update` 只改变显式字段；`--interactive` 使用当前值作为默认。Model prompt 用 `--clear-shell-prompt` / `--clear-uboot-prompt` 清空；EOL、echo、chunk size/delay 可用对应 `--inherit-*` 恢复通用值。一级与二级机型名在 TUI 的“创建配置 → 配置机型名”中维护；按 `D` / `Delete` 可删除所选且未绑定的一级或二级机型名，不增加另一套 Profile CLI。

运行中 Profile mutation 带 `config_revision`，避免较旧页面覆盖新的配置。Transport 变化按需要重开串口；Model 行为更新在 snapshot 刷新后立即生效。

## TUI 页面

主页面从上到下：

1. 顶部状态栏：串口名和连接状态；
2. 串口输出：标题只显示绑定的机型名，正文只显示设备 RX；
3. Agent 任务与命令历史：由两条 powerline 风格分隔栏包围；
4. 人工命令输入。

本机 TX 仍进入权威 journal 和 Agent 命令历史，但不会重复合成到 RX 主终端。设备自身通过 UART 返回的 echo 属于 RX，正常显示一次。

## 默认键盘操作

全局行为：

- 输入任意可打印字符、Backspace、Delete、Tab 或 Enter，都会进入命令输入行。
- Enter 总会立即尝试写串口并返回当前输出底部：有内容时发送“内容 + 有效 Profile EOL”，空输入时发送有效 Profile EOL；若有效 EOL 明确配置为空，空输入固定发送一个 `CR`。端口正由 Agent Run 使用时，同一个 Enter 会作为 Human command 立即发送并建立 Agent 读取门禁，不进入等待队列；发送失败时保留原草稿，便于重试。Alt+Enter 没有独立语义。
- `↑` / `↓` 选择任务与命令 action；`→` 进入子层级；`←` 返回上一层。
- 展开详情后用 `Shift+↑` / `Shift+↓` 滚动长内容；普通方向键仍只控制历史树。
- `PgUp` / `PgDn` 和鼠标滚轮始终滚动串口输出，不受鼠标位置或 Agent 历史焦点影响。
- `Alt-1` … `Alt-9` 直接切换端口。
- `Ctrl-R` 在命令输入中搜索人工输入历史。

`Ctrl-]` 是串口操作前缀：先按 `Ctrl-]`，再按第二个键。

| 第二键 | 动作 |
|---|---|
| `1`…`9` | 切换端口 |
| `s` | 下一个端口 |
| `l` / `r` | LINE / RAW 模式 |
| `f` 或 `End` | 串口输出返回最新 |
| `PgUp` / `PgDn` | 滚动串口输出 |
| `/` | 在串口输出右上角打开即时查找 |
| `m` | 打开配置菜单 |
| `o` | 打开当前端口/Profile 配置 |
| `h` | 显示/隐藏 Agent 历史 |
| `t` | 人工 Takeover |
| `c` | 释放人工 Control |
| `p` | 确认粘贴 |
| `g` | 中英文切换 |
| `?` | 完整帮助 |
| `q` | 退出 |

在 LINE 与 RAW 模式中，`Ctrl-C` 都立即向设备发送 `0x03`，不会退出本地 TUI。RAW `Ctrl-D` / `Ctrl-Z` 分别发送 `0x04` / `0x1a`。

## 任务记录与输出高亮

任务记录按从旧到新显示，最新 Run 在底部。历史树是严格三层：顶层是带独立状态色的 Run 标题；第二层缩进 4 列，只显示每个 `command` / `command_sequence` action 的 description；第三层缩进 8 列，只显示具体命令。新的 Agent action 到达时，TUI 会退出正在浏览的旧子层级并回到它所属的 Run；同一 action 的 TX 分块或 sequence 后续 step 只合并进原记录，不重复重置。Monitor 新 incident 只更新对应 Monitor，不强制改变当前选择。

用 `↑` / `↓` 在当前层级选择，用 `→` 依次从 Run 进入 description、再进入具体命令，用 `←` 逐层返回。普通 `command` 的第三层只有一条具体命令；`command_sequence` 的第三层按 step 顺序列出多条命令，上下选择时同步定位各自的串口证据。Monitor 按 `→` 进入 matcher，再按 `→` 进入 incident；选择 incident 后按它的 `serial_range` 跳转串口证据。命令捕获和 incident 属于旧后端周期，或完整范围已从本地窗口淘汰时，TUI 会从 journal 回取原周期的完整连续区间并高亮 RX；retention gap、缺失、超限或查询失败会返回实时尾并明确提示，不显示可能误导的局部证据。

选择具体命令时，新记录优先使用 daemon 持久化的 `command_capture_completed` 权威范围，其中包含 TX、evidence 序号、RX stream offsets、完成方式和置信度。它直接限定真正属于该命令的输出，不会因为后续重复提示符而漂移。

只有尚未携带权威 capture 的旧记录，TUI 才读取 TX 事件保存的 `command_capture_matchers`：

```text
contains | regex | shell_prompt | uboot_prompt
```

旧记录推断不会拿面板中的命令字符串和某一行 RX 做全串相等比较：起点绑定 TX 的 daemon epoch、generation、sequence、operation/run 标识，且不能越过下一条外部 TX 或硬边界，终点才由上述 matcher 确认；界面会明确标为 inferred。echo=On 时 serial-mcp 另用预期 TX 字节确认设备回显；长命令越过目标 TTY 列宽时，跨 RX event 的 `CRLF` / `CRCRLF` 物理硬换行都按逻辑连续命令处理。明确的 `CRCRLF` 回显可安全剥离；普通 `CRLF` 与真实输出换行存在字节级歧义，因此只在达到合理终端列宽后参与匹配，同时保留原始 RX、把置信度降为 medium 并返回 warning，避免静默吞掉串口证据。

TUI 从命令后的 RX 开始匹配第一个完成边界，并将设备 echo、返回内容和完成边界组成的捕获区域定位到主终端、使用独立底色高亮。`command_sequence` 每个 step 使用自己的 TX 起点、下一 step 上界和 matcher 独立定位。本地同周期窗口只有在捕获区间完整可信时才直接高亮，否则异步读取 journal；缺口不会降级成局部高亮。没有 matcher 或持久记录也没有匹配时，仅临时展示命令文本，不修改持久 RX 画面。

默认 inline content 高度为 5 行，可在“设置 → 终端界面显示设置”中修改 `agent_history_rows` 为 3–20。小终端使用独立详情视图，展开状态不会因滚轮或实时输出自动折叠；长详情用 `Shift+↑` / `Shift+↓` 滚动。

## 文本选择

- 左键拖动选择串口文本，选择区域持续使用逆色高亮；
- 双击选择一整串词语；
- mouse-up 自动复制；右键可复制当前保留选择；
- 非 Windows 使用 OSC 52，把系统剪贴板交给终端模拟器；
- `mouse_capture=false` 可完全交回终端原生选择，同时停用 TUI 的鼠标滚动。

双击选择范围包括常见路径、IP、MAC 和命令字符，不限于一个单独字母数字 cell。

## 串口历史恢复与搜索

TUI 启动后，先从 `seriald` journal 恢复当前 `daemon_epoch` 的记录，再从实际恢复游标 attach WebSocket。关闭再打开 TUI 不会清空历史。

恢复受明确边界保护：最近最多 20,000 序号、每端口 8 MiB 处理预算、全部端口 10 秒启动预算。范围不足、retention gap 或读取失败会显示出来，不假装完整。

`Ctrl-] /` 在串口输出栏右上角打开默认隐藏的即时查找框。焦点直接进入搜索输入，输入每个字符都会在实际显示文本中重新匹配并把主输出定位到最新结果的上下文；多个结果循环导航，关闭后恢复原焦点。

| 键 | 选项 |
|---|---|
| 直接输入 / 粘贴 | 立即匹配并定位 |
| `Enter` / `F3` / `↓` | 下一个结果（到末尾后循环） |
| `Shift+Enter` / `Shift+F3` / `↑` | 上一个结果（到开头后循环） |
| `Alt+R` | 普通文本 / regex |
| `Alt+C` | 区分 / 忽略大小写 |
| `Alt+D` | RX + TX / 仅 RX / 仅 TX |
| `Alt+S` | 当前周期 / 本地保留输出 / 当前 Agent Run |
| `Esc` | 关闭查找并恢复原焦点 |

搜索针对经过终端控制序列清洗后的连续显示行，同一逻辑行可跨多个 timeline event；正则表达式每次查询只编译一次。结果使用 epoch、sequence、行内 offset 作为稳定锚点；追加输出以 100 ms 合并刷新，停留在最新命中时会跟随新结果，查看旧命中时不会跳走。搜索范围受 TUI 当前保留窗口约束；更早内容已经淘汰时查找框会明确显示不完整警告，不会声称零结果覆盖了全部 journal。完整持久历史请使用下列有界 CLI 查询。

Agent 请求在当前 Human Control 上启动 Run 时，TUI 会显示阻塞式审批框；只有当前精确持有者可以批准或拒绝。批准会由 daemon 原子转交 Control 并开始 Run，不存在“批准后 Agent 尚未接管”的写入窗口；审批期间普通输入不会穿透弹窗。

完整 CLI 查询：

```sh
serial archives --port COM4
serial logs --port COM4 --contains ready
serial logs --port COM4 --regex '(?i)panic|watchdog'
serial logs --port COM4 --epoch UUID --after-seq 100 --through-seq 200
serial logs --port COM4 --run UUID --direction rx
```

## 配置与帮助

菜单只有四个主入口：

1. “修改当前串口配置”：选择端口、已有串口 Profile、UART 离散参数、已有机型 Profile 和具体机型名；按 `Tab` / `Shift+Tab` 可在菜单内切换目标串口，同一配置目录 revision 内各串口草稿独立保留；revision 因保存、目录 mutation 或 Reload 改变时，旧草稿会失效并明确提示；
2. “创建配置”：可创建或删除未绑定的串口/机型 Profile，也可维护一级机型系列与二级具体机型名，不自动改变当前端口绑定；
3. “设置”：进入“终端界面显示设置”或“serial MCP 设置”；
4. “帮助”：按固定列显示“按键 + 简洁说明”。

在配置项上按 `→` 展开可选值，用 `↑` / `↓` 选择，Enter 应用并折叠，按 `←` 折叠或返回。当前串口的机型身份按“一级机型系列 → 二级具体机型名”两级选择；没有二级名称的系列不能绑定，一级列表首行“未绑定”可清除两级身份但保留机型行为 Profile。“配置机型名”页先新增一级系列，再进入该系列新增二级名称。名称、Shell/U-Boot 提示符和分段发送数值直接在当前行下输入，不打开独立弹窗。“保存并应用配置修改”位于所有串口与机型字段之后的独立操作区。

菜单底部只显示一行按键指南。高亮配置项后按 `?` 查看该字段说明。设置项包括：

- `agent_history_rows`：3–20，默认 5；
- `orphan_run_timeout_seconds`：默认 1800；`0` 表示不限时，有限值至少 300。

没有命令行 timeout override 时，运行中的 stdio/HTTP `serial-mcp` 会自动加载保存后的孤立 Run 时间，不需要人工重启。正常 Agent 工作流仍在最终回复前调用 `run_end`。

## 串口着色

关键词按 identifier 边界、大小写不敏感匹配。`error`、`(error)`、`[error]` 会着色；`get_data_error_name`、`errorCounter`、`information` 不会。错误、警告、成功/ready 使用不同颜色。

合法 IPv4、IPv6、冒号或连字符分隔的 MAC 地址使用地址色；格式不完整的相似文本保持普通前景色。

## 诊断

```sh
serial doctor
serial doctor port --port COM4
serial doctor stream --port COM4 --duration 10
serial doctor storage
serial doctor state --port COM4
```

stream 诊断使用独立 WebSocket 订阅，将 RX offsets 与 journal 对比。在线但静默的设备报告为 silent，不会被当成失效。所有诊断均支持 `--json`，并且不会向目标发送探针或命令。
