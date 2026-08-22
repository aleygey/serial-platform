# Serial Platform Desktop

Serial Platform Desktop 是 Electron + React 桌面客户端。它与 TUI 使用相同的 `seriald` HTTP/WebSocket protocol v5，不直接打开物理串口。

## UI

控制台是三栏工作台：

- 左栏：OS 串口名、机型名、连接状态、打开/关闭串口和配置入口；
- 中栏：只显示设备 RX 的持久/实时终端，底部是人工命令输入；
- 右栏：从旧到新的 Agent Run、普通命令和 `command_sequence` 历史。

标题栏可启动 App 管理的本地后端，也只允许停止 App 自己启动的进程；连接外部后端时会明确显示为不可停止。配置页可以持久化“自动启动本地后端”开关，默认开启。App 和 `serial` 会在同一本地数据目录中自动发现并验证唯一后端；无论谁先启动、使用默认还是自定义 endpoint，后启动的一方都会复用同一个服务。

选择具体 Agent 命令时，终端读取 TX 事件持久化的 `command_capture_matchers`，定位并高亮对应 RX。没有匹配时只显示临时命令提示，不把本机 TX 插入 RX 历史。

终端支持普通文本搜索、双击选词、错误/警告/成功词边界着色以及 IP/MAC 着色。Agent 历史在 follow 状态下自动滚动到最新。

配置页明确分开：

- 串口配置：端口、enabled、Transport Profile、机型系列 Profile，以及从该系列中选择的具体机型名；
- 机型 Profile：系列/Profile 名称、该系列的具体机型名列表、Shell/U-Boot prompt、EOL、echo 解析和 write pacing。

Model Profile 的 `name` 是可复用的机型系列，`model_names` 列出系列下的具体型号；端口的 `model_name` 单独标记当前连接的设备，并且必须来自所选 Profile 的列表。

主题支持 system、light 和 dark。

快捷键：

| Shortcut | Action |
|---|---|
| `Ctrl/Cmd+,` | 打开配置 |
| `Ctrl/Cmd+1` | 返回控制台 |
| `Ctrl/Cmd+F` | 聚焦串口搜索 |
| `Ctrl/Cmd+K` | 聚焦命令输入 |
| `Esc` | 从配置返回控制台 |

## Process architecture

- `src/main`：本地服务生命周期、protocol v5 HTTP/WebSocket client、snapshot/timeline 协调和 IPC handler；
- `src/preload`：context-isolated、类型化 bridge；
- `src/renderer`：React UI 与纯展示状态；
- `src/shared`：DTO、preferences 和 QA fixture。

App 启动时先连接并验证当前可用的唯一后端；若没有活动服务且开启 `autoStartLocal`，主进程通过 `seriald serve --managed` 启动随包 sidecar。用户修改 endpoint 时，App 会先停止自己拥有的旧后端，再按新地址连接或启动。App 退出时先关闭自己受管子进程的 stdin，以 EOF 请求优雅退出，超时才强制终止；连接已有服务时不接管该进程，外部 owner 退出后也不会自动 failover。App Local Service 只管理 `seriald`，不启动 HTTP MCP。

renderer 不直接访问网络或子进程。人工命令由主进程获得 Human Control 后写入，TX 进入后端审计历史，终端仍只渲染 RX。

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

CI 先把同架构 `serial` 与 `seriald` 放入 `resources/bin`，再运行 electron-builder：

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
