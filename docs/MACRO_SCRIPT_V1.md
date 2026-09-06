# Macro Script v1

Macro 是在 seriald 内执行的有界串口脚本，不是宿主机 JavaScript、Shell 或 Python。人和 Agent 使用同一个目录、语言、参数规则与执行器。`command` 用于单条命令，`command_sequence` 保留用于 1–8 步线性依赖；需要循环、条件或重复使用的流程使用 Macro。公开 `trigger` 由 Macro 替代，旧 Trigger 时间线和底层 wire 类型仅用于兼容与审计。

## AI 如何使用

只提供三个公开 MCP 工具：

| 工具 | 用途 |
| --- | --- |
| `macro_list` | 默认共享摘要；支持 query、offset、limit；按 id 读取完整定义。include_drafts 显式包含未共享条目。 |
| `macro_save` | 校验并创建/更新完整定义；新建默认不共享，更新必须提供 expected_revision。保存绝不操作串口。 |
| `macro_run` | 在已有 run_handle 内执行指定 id+revision 或临时 script，等待最终结果。临时执行不入库。 |

典型流程：devices → run_start → macro_list 查找 → macro_run → read 核对状态 → run_end。没有合适宏时先 inline 执行；确有复用价值再参数化保存。匹配成功只证明观察到了指定文本，不自动证明设备进入了某个业务状态。

一次性执行：

```json
{
  "run_handle": "<run_start 返回的句柄>",
  "description": "重启后反复发送 slp，直到当前 Profile 的 U-Boot 提示符",
  "script": "let boot = watch(prompt(\"uboot\"));\ncmd(\"reboot\");\nwhile (!boot.matched) {\n  cmd(\"slp\");\n  wait(boot, 50);\n}\nexpect(boot, 0);",
  "timeout_seconds": 15
}
```

需要复用时保存：

```json
{
  "id": "enter_uboot",
  "name": "进入 U-Boot",
  "description": "重启并周期发送 slp，匹配当前 Profile 的 U-Boot 提示符",
  "parameters": {
    "interval_ms": {
      "type": "integer",
      "default": 50,
      "minimum": 20,
      "maximum": 1000
    }
  },
  "script": "let boot = watch(prompt(\"uboot\"));\ncmd(\"reboot\");\nwhile (!boot.matched) {\n  cmd(\"slp\");\n  wait(boot, args.interval_ms);\n}\nexpect(boot, 0);",
  "shared": false
}
```

保存返回 `definition.id` 和 `definition.revision`。执行时锁定版本：

```json
{
  "run_handle": "<当前 Run>",
  "macro_id": "enter_uboot",
  "revision": 1,
  "args": {"interval_ms": 50},
  "timeout_seconds": 15
}
```

编辑：先 `macro_list({"id":"enter_uboot"})`，再提交完整定义和 `expected_revision:1`。他人已更新时返回冲突，不覆盖、不随机创建重复宏；重新读取后由调用者决定合并。运行时固化源码、参数、Profile 和 revision，目录后续编辑不影响已开始的执行。

## 语言规则

语句以分号结束，控制块使用花括号，注释用 `//`。变量用 `let` 声明，有词法作用域，不允许改变类型。字符串用双引号，数字为有溢出检查的 64 位有符号整数，另有 true/false；无隐式类型转换。

支持赋值、`+= -= *= /= %=`、语句形式 `++ --`，整数运算、字符串拼接、比较、短路 `&& ||` 和 `!`；支持 `if/else`、`for(init; condition; update)`、`while`、`break`、`continue`。

```text
for (let i = 0; i < 3; i++) {
    let ready = watch(prompt("shell"));
    cmd("status");
    expect(ready, 10000);
    delay(100);
}
```

| 内建函数 | 行为 |
| --- | --- |
| `cmd(text)` | 自动追加有效 Profile EOL，等待本次 TX 确认，**不等待提示符**。 |
| `watch(literal)` | 安装字面值观察器；起点绑定下一次 cmd 的权威 TX，新 RX 才能命中。 |
| `prompt("shell")` / `prompt("uboot")` | 引用当前 Profile 的提示符，未配置则整个宏在 TX 前拒绝。 |
| `wait(w, ms)` | 命中返回 true；本次等待到时返回 false，观察器仍继续工作。 |
| `expect(w, ms)` | 必须在期限内有可信命中，否则整个宏失败，后续命令不发送。 |
| `delay(ms)` | 可中断延时，不忙轮询。 |

`w.matched` 命中后保持 true；下一独立命令响应阶段须新建 watcher。wait/expect 前必须先经过该 watcher 绑定的 cmd，不能借用旧日志。RX gap、流溢出、断线、人工介入不是普通 wait=false。

普通命令不提供 raw/no-EOL/input/send_keys；嵌入换行及控制字符的 cmd 会拒绝。未设置 EOL 使用平台默认值；显式空 EOL 的 Profile 拒绝执行宏。Ctrl-C/D 人工控制键与 MCP signal 保持独立，不属于宏语言。

参数支持 string/integer/boolean，由 `args.name` 读取，可声明 default，integer 可声明 minimum/maximum。未知参数、缺少必填参数、类型或范围错误在 TX 前拒绝。inline 首版不接受 args：直接提交脚本；保存的宏才带参数 schema。

不支持数组、对象、用户函数、import、eval、宿主文件、网络或系统命令。完整词法及编译器接口见 [核心语言说明](../crates/serial-macro/README.md)。

## 执行与失败边界

- 默认总时限 30 秒，允许 1–120 秒；源码最多 64 KiB，字符串最多 4096 字节，最多 256 个 watcher/变量、64 层嵌套、100000 条 VM 指令；每 256 条指令协作让出执行。
- 单条物理命令含 EOL 最多 4096 字节；整个宏含 EOL 的 TX 最多 1 MiB。实际写入仍受 pacing、原有写入时限和剩余 lease 约束。
- 一个端口同时只运行一个宏。使用同一个物理 writer，核对 epoch、generation、Run、Control fence、TX offset 和上下文连续性。并发 Agent 物理写不排队。
- 人工 Enter、RAW 或 Ctrl-D 停止后续宏步骤；已经提交的物理写先取得有界结果，再串行发送人工输入。不能撤回已到达设备的字节。Agent 下一次 command/macro 前必须实时 read 覆盖人工干预。
- UI 停止、MCP 取消、控制权丢失、断线、重配停止后续步骤。正常 run_end 在宏活动时报告忙，异常中止走控制权释放。断线不续跑、不自动重放。
- 同一 daemon epoch 内稳定 operation_id 防止启动响应丢失造成重复执行。重连只查询已知状态，不能用新 ID 猜测重跑。
- 结果包含状态、源码行列、已确认发送次数/字节数、证据序号范围与原因；不确定写入标记 outcome_uncertain。只有启动前校验失败能保证零 TX，运行失败可能已有部分命令生效。

首版匹配为大小写敏感字面值，支持跨 RX chunk、UTF-8 字节及 SGR/OSC 装饰。CR/CRLF 按已观察 RX 的输出边界处理：这是输出文本观察器，不是 TUI 最终屏幕状态查询。精确命令回显按 Profile 策略剔除，不猜任意前导文本为回显；裸响应恰好是命令前缀时宁可继续等待，不猜成功。CSI 光标重绘/退格暂不作为完成证据，遇到会明确失败。字面提示符不是设备状态证明；Profile 应配置准确且有区分度的提示符，并在真机验证重启命令、间隔与回显行为。

## 共享目录与人机审核

目录属于整个 seriald 平台，不按串口分裂。id 使用小写字母、数字、下划线和短横线；目录最多 512 项。名称、用途、源码、参数、适用机型和 revision 可由 TUI/App 与 Agent 共同查看编辑。

- inline 不进入目录。
- 新建默认 shared:false，默认 list 与 AI 初始化摘要不包含。
- 显式 shared:true 才进入共享列表；shared 不是权限或认证标记。
- 可选 applies_to 限定 model_family / model_names，执行前核对目标机型。
- 初始化及正常 devices/run_start 等结果提供共享摘要与目录版本。完整源码按需读取，不把全部脚本放进模型上下文。

TUI 使用 `Ctrl-] a` 打开宏目录；App 使用宏页面。保存执行的是同一份定义，执行前显示参数并锁定 revision。端口由其他人或 Agent 占用时，不暗中抢占。

宏 TX 使用专门来源元数据，不进入人工输入框的补全历史。日志保留执行 ID、宏 ID/revision、源码行列、已确认 TX 与最终 checkpoint。不要把密码写入共享脚本或参数默认值。
