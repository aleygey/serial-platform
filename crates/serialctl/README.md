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

该流程直接读写本地后端配置，`seriald` 不需要先启动。交互只解释四项：

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

`update` 只改变显式字段；`--interactive` 使用当前值作为默认。Model prompt 用 `--clear-shell-prompt` / `--clear-uboot-prompt` 清空；EOL、echo、chunk size/delay 可用对应 `--inherit-*` 恢复通用值。一级与二级机型名在 TUI 的“创建配置 → 配置机型名”中维护，不增加另一套 Profile CLI。

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
- 输入有内容时 Enter 发送；输入为空时 Enter 返回当前串口底部，不发送空命令。
- `↑` / `↓` 选择任务与命令 action；`→` 进入子层级；`←` 返回上一层。
- `PgUp` / `PgDn` 浏览当前历史层级；展开详情时滚动详情内容。
- 鼠标滚轮与 `PgUp` / `PgDn` 行为相同，不需要点击不同 pane 切换焦点。
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
| `/` | 搜索持久串口历史 |
| `m` | 打开配置菜单 |
| `o` | 打开当前端口/Profile 配置 |
| `h` | 显示/隐藏 Agent 历史 |
| `t` | 人工 Takeover |
| `c` | 释放人工 Control |
| `u` | 查看排队的 LINE 命令 |
| `d` | 删除最新排队命令 |
| `e` | 将最新排队命令取回编辑 |
| `p` | 确认粘贴 |
| `g` | 中英文切换 |
| `?` | 完整帮助 |
| `q` | 退出 |

在 LINE 与 RAW 模式中，`Ctrl-C` 都立即向设备发送 `0x03`，不会退出本地 TUI。RAW `Ctrl-D` / `Ctrl-Z` 分别发送 `0x04` / `0x1a`。

## 任务记录与输出高亮

任务记录按从旧到新显示，最新 Run 在底部。历史树是严格三层：顶层是带独立状态色的 Run 标题；第二层缩进 4 列，只显示每个 `command` / `command_sequence` action 的 description；第三层缩进 8 列，只显示具体命令。新的 Agent action 到达时，TUI 会退出正在浏览的旧子层级并回到它所属的 Run；同一 action 的 TX 分块或 sequence 后续 step 只合并进原记录，不重复重置。Monitor 新 incident 只更新对应 Monitor，不强制改变当前选择。

用 `↑` / `↓` 在当前层级选择，用 `→` 依次从 Run 进入 description、再进入具体命令，用 `←` 逐层返回。普通 `command` 的第三层只有一条具体命令；`command_sequence` 的第三层按 step 顺序列出多条命令，上下选择时同步定位各自的串口证据。Monitor 按 `→` 进入 matcher，再按 `→` 进入 incident；选择 incident 后按它的 `serial_range` 跳转串口证据。命令捕获和 incident 属于旧后端周期，或完整范围已从本地窗口淘汰时，TUI 会从 journal 回取原周期的完整连续区间并高亮 RX；retention gap、缺失、超限或查询失败会返回实时尾并明确提示，不显示可能误导的局部证据。

选择具体命令时，TUI 读取该 TX 事件保存的 `command_capture_matchers`：

```text
contains | regex | shell_prompt | uboot_prompt
```

它从命令后的 RX 开始匹配第一个完成边界，并将设备 echo、返回内容和完成边界组成的捕获区域定位到主终端、使用独立底色高亮。`command_sequence` 每个 step 使用自己的 TX 起点、下一 step 上界和 matcher 独立定位。本地同周期窗口只有在捕获区间完整可信时才直接高亮，否则异步读取 journal；缺口不会降级成局部高亮。没有 matcher 或持久记录也没有匹配时，仅临时展示命令文本，不修改持久 RX 画面。

默认 inline content 高度为 5 行，可在“设置 → 终端界面显示设置”中修改 `agent_history_rows` 为 3–20。小终端使用独立详情视图，展开状态不会因滚轮或实时输出自动折叠。

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

`Ctrl-] /` 打开持久搜索：

| 键 | 选项 |
|---|---|
| `F2` / `Tab` | 普通文本 / regex |
| `F3` | 大小写敏感 |
| `F4` | RX / TX 方向 |
| `F5` | 当前周期 / 全部保留周期 / 当前 Agent Run |
| `↑` / `↓` | 选择结果 |
| `PgUp` / `PgDn` | 滚动结果详情 |

搜索结果放在独立视图，不把旧周期 bytes 混入实时终端。交互搜索最多显示 200 条、扫描最近四个 archive、每个 archive 最近 10,000 序号、八次 HTTP 请求、单响应 16 MiB、总时限 10 秒。partial、truncated 和 gap 明确显示。

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

1. “修改当前串口配置”：选择端口、已有串口 Profile、UART 离散参数、已有机型 Profile 和具体机型名；
2. “创建配置”：可创建串口 Profile、机型 Profile，或维护一级机型系列与二级具体机型名，不自动改变当前端口绑定；
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
