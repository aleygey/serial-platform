# Serial Platform Desktop

Serial Platform Desktop 是 Electron + React 桌面客户端。它与 TUI 使用相同的 `seriald` HTTP/WebSocket protocol v9，不直接打开物理串口。

## UI

左侧设备列表显示串口名、机型和连接状态。选择设备会切换当前栏，从设备列表把串口拖入工作区，现有栏会让出插入位置。松手时按指针所在的一侧插入；拖到工作区外取消。聚焦设备后也可以按空格拾取、左右键定位、回车放置。点击某一栏后，命令输入、查找和快捷键都作用于该栏。每栏右上角的 × 关闭视图，串口继续连接。

每个串口保留独立的草稿、发送回执和终端。输入命令按 Enter 或点击 Run 发送。输入栏上方展示外部 Agent 的接管、Run 启动和命令进度。展开历史可以查看每个 Run 的启动节点。串口归属栏显示外部接管申请、当前 Run 和控制权持有者。Agent 运行期间可以发送人工协作命令，按后端回执显示发送、回显匹配、超时、中止等状态。点击状态球可暂停该栏动效，系统减少动态效果偏好也会生效。

外部 Agent 通过 Serial MCP 取得串口控制权时，Matrix Orb 亮起。每栏输入框上方显示当前命令的描述，例如「Agent 正在读取固件版本」。任务结束时显示本轮任务名和结果，例如「读取传感器状态已完成」。首次收到 Run 时自动展开可滚动的历史，箭头沿动作和命令依次推进。已执行的步骤保持展开，手动收起的历史或动作会在切换页面后保留。选中历史命令可定位到原始串口输出。[Agent 接入说明](../../adapters/README.md)

运行 `npm run preview:browser` 可生成 `qa/browser-preview/index.html`。直接用浏览器打开即可操作模拟串口、观察示例 Run 的进度，并切换外观。Electron 应用使用相同的 React 页面和桌面通信接口。

浏览器示例的 COM7 输入框上方有「回放示例」，可重复观看接管、Run 启动、命令等待回显及归还控制权。桌面端从「配置 → 应用 → 观看 Agent 运行示例」或空的 Agent 历史打开独立模拟窗口。

首次使用桌面客户端时，向导会询问串口、Shell 提示符和健康检查需求。提示符示意使用 Animata 的 Typing Text。把向导生成的任务交给已接入 Serial MCP 的外部 Agent，查看返回的方案并确认保存，即可运行一次心跳或状态检查。已有工作区可在「配置 → 应用 → 打开首次配置向导」进入。[配置邀请契约](../../docs/MCP_TOOLS.md#首次配置邀请)

拖动分隔线可调整栏宽。拖动标题中的串口名可交换分栏位置，聚焦串口名后也可用左右方向键移动。点击标题中的机型可直接切换当前串口的型号。

Rare UI 的 Gooey Nav 和 Bounce Sidebar 用于导航，Prompt Box 负责单行发送，Matrix Orb 表示外部 Agent 占用。Rail TOC 和 Step Player 跟随串口事件，Model Picker 和 Animated Counter 用于通信参数，Theme Selector 用于切换外观。Motion-Primitives 的 Disclosure、Accordion、Scroll Progress、Transition Panel、Animated Background 和 Text Effect 分别用于展开详情、宏指南、滚动进度、切换、选中态和命令出现。组件通过各自的官方安装器加入本项目。[Rare UI](https://github.com/swamimalode07/rare-ui) · [Motion-Primitives](https://github.com/ibelick/motion-primitives)

配置页提供本地后端的启动和停止操作，也会标明当前连接的外部后端。配置页可以持久化“自动启动本地后端”开关，默认开启。App 和 `serial` 会在同一本地数据目录中自动发现并验证唯一后端；无论谁先启动、使用默认还是自定义 endpoint，后启动的一方都会复用同一个服务。

选择具体 Agent 命令时，终端优先按持久化的 `command_capture_completed.metadata.capture` 关联 `operation_id`，使用 daemon 校验过的 `evidence_from_seq`、`evidence_through_seq` 和 RX stream offsets 定位对应输出。这样 quiet completion、重复 prompt 和重连后的历史都不需要 UI 重新猜测结束位置。旧版记录没有权威 capture 时才在同一 daemon epoch、generation 和下一条 TX/硬边界之前重跑 `command_capture_matchers`，并明确标记为“旧记录：输出范围由匹配规则推断”。没有可用范围时只显示临时命令提示，不把本机 TX 插入 RX 历史。

终端支持 VS Code 风格的右上角 Find Widget、双击选词、错误/警告/成功词边界着色以及 IP/MAC 着色。Find Widget 默认隐藏，只能通过标题栏查找按钮、`Ctrl/Cmd+F` 或 `F3` 显式打开，普通字符不会自动抢占命令输入焦点。`Enter`/`F3` 和 `Shift+Enter`/`Shift+F3` 循环移动当前匹配，Widget 显示 `n/total`，并区分当前匹配和其他匹配；`Esc` 关闭后恢复原焦点，命令草稿与搜索草稿都保留。

搜索文档由终端实际显示的 RX 文本连续组成，因此匹配可以跨越多个 RX event。匹配锚点使用 daemon epoch、event seq 和 event-local offset；实时 append、滚动窗口 truncate 或 snapshot rebuild 后，仍存在的当前匹配不会因数组下标变化而跳走。首次查询扫描完整显示文档，之后 append 只扫描 `query.length - 1` 的尾部重叠区和新增文本；终端 chunk 使用稳定对象与 memoized rendering，避免每个 event 重编译搜索正则或全量重绘 DOM。

当 Agent 请求在 Human 已持有的串口上启动 Run 时，App 会从 snapshot 或实时 timeline 恢复 `pending_run_start`。只有当前 Desktop WebSocket actor 确实等于 `required_approver`，并且仍持有匹配 control id、fence、daemon epoch 和 generation 的 Human lease 时，才显示 RunStart 审批对话框。对话框展示串口、Agent label、Run label 和剩余时间；批准或拒绝通过 `DecideRunStart` 发送。approved、denied、timed out、cancelled、过期或断连都会清理对话框，提交锁和主进程的最新状态复验会阻止重复点击及旧请求误决策。

配置页明确分开：

- 串口配置：端口、enabled、Transport Profile、行为 Model Profile，以及独立的一级机型系列/二级具体机型身份；
- 机型 Profile：可复用的 Profile 名称、Shell/U-Boot prompt、EOL、echo 解析和 write pacing，不包含机型名；
- 机型名目录：独立编辑 Model Family，一级是机型系列，二级是该系列下可绑定的具体机型。

Model Profile 和 Model Family 是两份独立 catalog。端口的 `model_profile` 只决定串口交互行为；`model_family` 和 `model_name` 成对标记当前设备，具体机型必须来自所选系列的二级列表。

在「配置 → 应用」可选择海湾蓝、磷光绿、琥珀金、赤陶红或石墨灰。每种配色都有浅色和深色两套，也可以跟随系统。磷光绿的深色终端使用近黑底色和绿色输出。

快捷键：

| Shortcut | Action |
|---|---|
| `Ctrl/Cmd+,` | 打开配置 |
| `Ctrl/Cmd+1` | 返回控制台 |
| `Ctrl/Cmd+F` | 打开并聚焦 Find Widget |
| `F3` / `Shift+F3` | 打开 Find Widget，并移动到下一个/上一个匹配 |
| `Enter` / `Shift+Enter` | Find Widget 聚焦时移动到下一个/上一个匹配 |
| `Ctrl/Cmd+K` | 聚焦命令输入 |
| `Enter` / `Alt+Enter` | 命令输入聚焦时，以相同的 Human command 语义立即发送 |
| `Esc` | 关闭 Find Widget 并恢复焦点，或从配置返回控制台 |

## Process architecture

- `src/main`：本地服务生命周期、protocol v9 HTTP/WebSocket client、snapshot/timeline 协调和 IPC handler；
- `src/preload`：context-isolated、类型化 bridge；
- `src/renderer`：React UI 与纯展示状态；
- `src/shared`：DTO、preferences 和 QA fixture。

App 启动时先连接并验证当前可用的唯一后端；若没有活动服务且开启 `autoStartLocal`，主进程通过 `seriald serve --managed` 启动随包 sidecar。用户修改 endpoint 时，App 会先停止自己拥有的旧后端，再按新地址连接或启动。App 退出时先关闭自己受管子进程的 stdin，以 EOF 请求优雅退出，超时才强制终止；连接已有服务时不接管该进程，外部 owner 退出后也不会自动 failover。App Local Service 只管理 `seriald`，不启动 HTTP MCP。

renderer 不直接访问网络或子进程。命令输入的 `Enter` 和 `Alt+Enter` 都通过 preload/IPC 进入同一个 `SendHumanCommand` 请求，携带当前 `expected_generation` 并由 daemon 原子判定为 owned 或 cooperative；Desktop 不再先发 `AcquireControl mode=queue`，也不存在延迟控制队列。Human TX 仍进入后端审计历史，终端只渲染 RX。若命令 cooperative 地介入活动 Agent Run，App 会提示 Agent 必须先读取包含该 Human command 的最新串口状态，Desktop 不代替 Run owner 调用 `AcknowledgeRunContext`。

## Development

需要 Node.js 24.x：

```sh
npm ci --no-audit --no-fund
npm run dev
```

完整检查：

```sh
npm run typecheck
npm run test:run
npm run build
```

`npm run build` 会依次 typecheck、test 并生成 Electron main/preload/renderer bundle。

## Visual QA

使用固定 fixture 生成真实 Electron 深色/浅色截图：

```sh
npm run qa:screenshots
```

输出：

```text
qa/serial-platform-desktop-dark.png
qa/serial-platform-desktop-light.png
```

只捕获一种主题：

```sh
electron . --qa-screenshot --qa-theme=dark
electron . --qa-screenshot --qa-theme=light
```

renderer-only QA 必须显式使用 `?qa=1&theme=dark` 或 `light`；正常启动不会在 preload 缺失时回落到 fixture。

## Packaging

CI 先把同架构的 `serial`、`seriald` 和 `serial-mcp` 放入 `resources/bin`，再运行 electron-builder：

```sh
npm run package:mac
npm run package:linux
npm run package:win
```

产物形态：

- macOS `.app` directory；
- Linux AppImage；
- Windows portable EXE。

完整 Serial Platform 平台包还在 App 之外提供 `serial`、`seriald`、`serialctl` 和 `serial-mcp` 四个 Rust 程序。

macOS 边界：

- Rust CLI deployment target 是 macOS 11.0；
- Electron 43 App 的最低系统版本是 macOS 12.0；
- 当前 `.app` 没有 Developer ID 签名，也没有 Apple notarization。

Jenkins 分别构建 macOS arm64 和 x86_64 App。直接调用 electron-builder 时可使用：

```sh
npx electron-builder --mac dir --arm64 --publish never
npx electron-builder --mac dir --x64 --publish never
```
