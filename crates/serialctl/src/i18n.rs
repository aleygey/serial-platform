//! Minimal English/Chinese runtime localization for serialctl.
//!
//! User-visible strings live in one static table keyed by a stable dotted
//! name. [`tr`] resolves a key against the active language; [`trf`] formats
//! a translated template by substituting successive `{}` placeholders. The
//! active language is process-global and may be switched at runtime; every
//! render pass re-reads it, so the next repaint reflects a switch.

use std::sync::{OnceLock, RwLock};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lang {
    En,
    #[default]
    Zh,
}

impl Lang {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "en" | "en-us" | "en_us" | "en-gb" | "en_gb" => Some(Self::En),
            "zh" | "zh-cn" | "zh_cn" | "zh-hans" | "zh_hans" => Some(Self::Zh),
            _ => None,
        }
    }

    pub fn toggled(self) -> Self {
        match self {
            Self::En => Self::Zh,
            Self::Zh => Self::En,
        }
    }
}

static LANG: OnceLock<RwLock<Lang>> = OnceLock::new();

fn lang_cell() -> &'static RwLock<Lang> {
    LANG.get_or_init(|| RwLock::new(Lang::default()))
}

pub fn lang() -> Lang {
    *lang_cell().read().expect("language lock poisoned")
}

pub fn set_lang(lang: Lang) {
    *lang_cell().write().expect("language lock poisoned") = lang;
}

/// Serializes tests that depend on the process-global language and gives
/// legacy assertions a stable English baseline. Product code uses
/// `Lang::default()` (Chinese) when no preference is configured.
#[cfg(test)]
pub(crate) fn lang_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    set_lang(Lang::En);
    guard
}

/// (key, English, 简体中文)
static STRINGS: &[(&str, &str, &str)] = &[
    // ---- Subscription phase labels (tab bar) ----
    ("phase.off", "OFF", "离线"),
    ("phase.attach", "ATTACH", "接入中"),
    ("phase.replay", "REPLAY#{}-#{}", "补齐历史#{}-#{}"),
    ("phase.live", "LIVE#{}", "实时#{}"),
    ("phase.lagged", "LAGGED#{}-#{}", "历史缺失#{}-#{}"),
    // ---- Session state / target activity (tab bar) ----
    ("state.disabled", "DISABLED", "已禁用"),
    ("state.waiting", "WAITING", "等待串口"),
    ("state.opening", "OPENING", "打开中"),
    ("state.online", "ONLINE", "在线"),
    ("state.backoff", "BACKOFF", "等待重试"),
    ("state.stopping", "STOPPING", "停止中"),
    ("activity.active", "ACTIVE", "有数据"),
    ("activity.silent", "SILENT", "暂无数据"),
    ("activity.unknown", "UNKNOWN", "未知"),
    // ---- Connection summary (tab bar title) ----
    ("conn.reconnecting", "○ reconnecting", "○ 重连中"),
    ("conn.authenticating", "◐ authenticating", "◐ 认证中"),
    ("conn.live", "● live", "● 已连接"),
    ("conn.attaching", "◐ attaching", "◐ 接入中"),
    // ---- Status bar ----
    ("ui.control.none", "none", "无"),
    ("ui.prefix", " · PREFIX", " · 前缀"),
    (
        "ui.uncertain",
        " · {} WRITE OUTCOME(S) UNCERTAIN: inspect TX before retrying",
        " · 有 {} 次发送结果未确认；重试前请先检查发送记录",
    ),
    (
        "ui.queued",
        " · QUEUED #{} ({}s, {} chunk(s); d/e edits LINE, c cancels)",
        " · 控制权排队第 {} 位（已等待 {} 秒，{} 个待发送分段；d/e 编辑 LINE，c 取消）",
    ),
    (
        "ui.control.pending",
        " · CONTROL REQUEST PENDING (Ctrl-] c cancels)",
        " · 正在请求控制权（Ctrl-] c 取消）",
    ),
    (
        "ui.idle.release",
        " · idle release in {}s",
        " · 空闲 {} 秒后自动释放控制权",
    ),
    (
        "ui.trigger",
        " · trigger {} {} · {} fire(s)",
        " · 触发任务 {} {} · 已发送 {} 次",
    ),
    ("ui.status.control", "control:", "控制权："),
    ("ui.paused", " · PAUSED", " · 已暂停"),
    // ---- Input box ----
    (
        "ui.input.title.line",
        " command · Enter sends the interaction-profile line ending ",
        " 命令 · 回车发送并附加样机交互方案换行符 ",
    ),
    (
        "ui.input.title.line.queued",
        " command · QUEUED {} · {} · Ctrl-] d/e/c/u delete/edit/cancel/select ",
        " 命令 · 已排队 {} 条 · {} · Ctrl-] d/e/c/u 删除/编辑/取消/选择 ",
    ),
    (
        "ui.input.raw.text",
        "Keystrokes are sent directly. Ctrl-C sends ETX; Ctrl-] opens local commands.",
        "按键会直接发送。Ctrl-C 发送 ETX；Ctrl-] 打开本地命令。",
    ),
    (
        "ui.input.title.raw",
        " RAW direct transport ",
        " RAW 直接发送 ",
    ),
    (
        "ui.input.title.raw.queued",
        " RAW direct transport · QUEUED {} byte(s) · Ctrl-] c cancels ",
        " RAW 直接发送 · 已排队 {} 字节 · Ctrl-] c 取消 ",
    ),
    ("ui.input.queued.raw", "{} raw byte(s)", "{} 个原始字节"),
    (
        "ui.input.agent",
        "Agent is using this serial port · current task: <{}>",
        "Agent 正在使用当前串口通道 · 本轮任务：{}",
    ),
    (
        "ui.queue.title",
        " queued commands · Ctrl-] u then ↑/↓ cards, PgUp/PgDn text, d delete, e edit ",
        " 待发送命令 · Ctrl-] u 后用 ↑/↓ 选择命令，PgUp/PgDn 查看全文，d 删除，e 编辑 ",
    ),
    (
        "ui.queue.more",
        "… {} more visual row(s) · Ctrl-] u to inspect full commands",
        "… 还有 {} 行未显示 · Ctrl-] u 查看完整命令",
    ),
    (
        "ui.queue.page",
        "{} · text rows {}-{}/{} · PgUp/PgDn",
        "{} · 内容第 {}–{} 行，共 {} 行 · PgUp/PgDn",
    ),
    ("ui.queue.empty", "<empty command>", "<空命令>"),
    (
        "ui.queue.sending",
        " · SENDING (locked)",
        " · 发送中（已锁定）",
    ),
    // ---- Run / described Agent command history bar ----
    (
        "ui.run.title",
        " Agent task / command history ",
        " Agent 任务与命令记录 ",
    ),
    (
        "ui.run.title.limited",
        " Agent task / command history · recent ",
        " Agent 任务与命令记录 · 最近记录 ",
    ),
    (
        "ui.run.none",
        "No Agent task appears in the available records.",
        "当前可用记录中未发现 Agent 任务。",
    ),
    (
        "ui.run.history.limited",
        "Recent records only; use persistent logs for complete history (historical backfill is not available yet).",
        "这里只显示最近记录；完整历史请查看持久日志（历史回填功能尚未提供）。",
    ),
    ("ui.run.status.active", "running", "执行中"),
    ("ui.run.status.completed", "completed", "已完成"),
    ("ui.run.status.aborted", "aborted", "已中止"),
    ("ui.run.unknown", "unnamed Run", "未命名 Agent 任务"),
    ("ui.run.owner.unknown", "unknown owner", "未知执行者"),
    ("ui.run.header", "{} · {} · {} · {}", "{} · {} · {} · {}"),
    (
        "ui.run.no.described.commands",
        "No described Agent command yet",
        "暂无带用途说明的 Agent 命令",
    ),
    (
        "ui.run.description.missing",
        "purpose not provided",
        "未提供命令用途",
    ),
    (
        "ui.run.command.meta",
        "confirmed TX · sequence #{}-#{} · {}",
        "已确认发送 · 序号 #{}–#{} · {}",
    ),
    (
        "ui.run.command.meta.partial",
        "partially confirmed TX · sequence #{}-#{} · {}",
        "仅部分确认发送 · 序号 #{}–#{} · {}",
    ),
    ("ui.run.command.empty", "<empty TX>", "<空发送内容>"),
    (
        "ui.run.command.partial",
        "Only part of the bytes were confirmed; the content above is the confirmed prefix",
        "仅部分字节确认发送；上方内容是已确认的前缀",
    ),
    (
        "ui.run.command.truncated",
        "… command display truncated",
        "… 命令内容过长，已截断显示",
    ),
    ("ui.run.abort.reason", "abort reason: {}", "中止原因：{}"),
    // ---- Dynamic protocol state labels ----
    ("trigger.status.active", "active", "活动中"),
    ("trigger.status.armed", "armed", "已就绪"),
    (
        "trigger.status.waiting_for_start",
        "waiting_for_start",
        "等待开始条件",
    ),
    ("trigger.status.running", "running", "执行中"),
    ("trigger.status.stopping", "stopping", "正在停止"),
    ("trigger.status.matched", "matched", "已匹配停止条件"),
    ("trigger.status.timed_out", "timed_out", "已超时"),
    (
        "trigger.status.max_fires_reached",
        "max_fires_reached",
        "已达到最大发送次数",
    ),
    ("trigger.status.cancelled", "cancelled", "已取消"),
    (
        "trigger.status.control_lost",
        "control_lost",
        "控制权已失效",
    ),
    ("trigger.status.run_lost", "run_lost", "Agent 任务已结束"),
    (
        "trigger.status.generation_changed",
        "generation_changed",
        "串口会话已变更",
    ),
    ("trigger.status.port_closed", "port_closed", "串口已关闭"),
    ("trigger.status.write_failed", "write_failed", "发送失败"),
    ("trigger.status.rx_gap", "rx_gap", "接收历史缺失"),
    ("role.observer", "observer", "观察者"),
    ("role.operator", "operator", "操作员"),
    ("role.admin", "admin", "管理员"),
    (
        "gap.reason.epoch_changed",
        "epoch_changed",
        "服务实例已变更",
    ),
    ("gap.reason.ring_evicted", "ring_evicted", "内存历史已淘汰"),
    ("gap.reason.retention", "retention", "历史已超过保留期"),
    ("gap.reason.corruption", "corruption", "历史记录损坏"),
    ("gap.reason.logging_fault", "logging_fault", "日志写入故障"),
    (
        "gap.reason.sequence_discontinuity",
        "sequence_discontinuity",
        "历史序号不连续",
    ),
    ("error.bad_request", "bad_request", "请求无效"),
    ("error.unauthorized", "unauthorized", "未认证"),
    ("error.forbidden", "forbidden", "无权限"),
    ("error.not_found", "not_found", "未找到"),
    ("error.conflict", "conflict", "状态冲突"),
    ("error.control_required", "control_required", "需要控制权"),
    ("error.stale_fence", "stale_fence", "控制权凭据已失效"),
    ("error.port_offline", "port_offline", "串口离线"),
    ("error.cursor_ahead", "cursor_ahead", "历史游标超前"),
    ("error.resource_exhausted", "resource_exhausted", "资源不足"),
    (
        "error.idempotency_expired",
        "idempotency_expired",
        "幂等记录已过期",
    ),
    (
        "error.config_revision_mismatch",
        "config_revision_mismatch",
        "配置版本冲突",
    ),
    (
        "error.profile_change_busy",
        "profile_change_busy",
        "方案正在使用",
    ),
    ("error.port_not_found", "port_not_found", "未找到串口"),
    ("error.port_busy", "port_busy", "串口被占用"),
    (
        "error.port_access_denied",
        "port_access_denied",
        "无权访问串口",
    ),
    ("error.port_io", "port_io", "串口读写失败"),
    (
        "error.break_unsupported",
        "break_unsupported",
        "不支持 BREAK",
    ),
    ("error.regex_invalid", "regex_invalid", "正则表达式无效"),
    (
        "error.query_budget_exceeded",
        "query_budget_exceeded",
        "查询范围过大",
    ),
    ("error.unavailable", "unavailable", "服务暂不可用"),
    ("error.internal", "internal", "内部错误"),
    ("value.none", "none", "无"),
    (
        "state.removed",
        "removed from active configuration",
        "已从配置中移除",
    ),
    (
        "history.local.truncated",
        "Local display history was truncated; use `serialctl logs` for the complete history.",
        "本地显示历史已截断；请使用 `serialctl logs` 查询完整历史。",
    ),
    (
        "history.local.truncated.title",
        " · LOCAL HISTORY TRUNCATED",
        " · 本地历史已截断",
    ),
    // ---- Extensible configuration menu ----
    (
        "menu.title",
        "Serial console configuration",
        "串口控制台配置",
    ),
    ("menu.loading", "loading configuration…", "正在加载配置…"),
    (
        "menu.loaded",
        "configuration catalog loaded",
        "配置列表已加载",
    ),
    (
        "menu.busy",
        "a configuration request is still running",
        "配置请求仍在执行",
    ),
    (
        "menu.io.unavailable",
        "configuration worker is unavailable",
        "配置后台任务不可用",
    ),
    (
        "menu.io.full",
        "configuration request queue is full; retry shortly",
        "配置请求队列已满；请稍后重试",
    ),
    (
        "menu.io.failed",
        "configuration request failed: {}",
        "配置请求失败：{}",
    ),
    (
        "menu.catalog.unavailable",
        "catalog is not loaded; press r to retry",
        "配置列表尚未加载；按 r 重试",
    ),
    (
        "menu.current",
        "Current port {} · UART profile {} · DUT interaction profile {} · bound model {}",
        "当前串口通道：{} · 串口参数方案：{} · 样机交互方案：{} · 样机机型：{}",
    ),
    (
        "menu.value.generic",
        "Generic (no interaction profile)",
        "通用交互设置（不使用样机交互方案）",
    ),
    ("menu.value.unbound", "Unbound", "未绑定"),
    ("menu.value.enabled", "enabled", "启用"),
    ("menu.value.disabled", "disabled", "停用"),
    ("menu.value.on", "on", "开启"),
    ("menu.value.off", "off", "关闭"),
    (
        "menu.root.profile",
        "Configuration profiles",
        "配置方案（串口参数方案／样机交互方案）",
    ),
    (
        "menu.root.model",
        "DUT model bound to this port",
        "样机机型（当前串口绑定）",
    ),
    (
        "menu.root.serial",
        "Quickly change UART hardware parameters",
        "快速创建串口参数方案",
    ),
    ("menu.root.help", "Help", "帮助"),
    (
        "menu.root.detail",
        "Choose what the current serial port should use. Catalog reads run asynchronously; protected changes request temporary authorization only when the daemon requires it.",
        "选择当前串口通道使用的方案或机型。配置列表会在后台加载；需要授权时会临时提示输入管理员令牌。",
    ),
    ("menu.profile.title", "Configuration profiles", "配置方案"),
    (
        "menu.profile.detail",
        "Select an existing profile with Up/Down and Enter. New profiles are created by cloning the current effective settings or applying a preset; arbitrary field editing remains available in the CLI.",
        "用上下键选择已有方案，按 Enter 应用。新方案可复制当前生效设置或套用预设；如需逐项修改全部参数，请使用 CLI。",
    ),
    (
        "menu.profile.transport",
        "UART hardware profiles (baud/parity/flow)",
        "串口参数方案（波特率/校验/流控）",
    ),
    (
        "menu.profile.device",
        "DUT interaction profiles (prompt/EOL/echo/pacing)",
        "样机交互方案（提示符/换行/回显/分段发送）",
    ),
    (
        "menu.transport.title",
        "UART hardware profiles",
        "串口参数方案",
    ),
    (
        "menu.transport.new",
        "+ Create and bind safe 115200 8N1 profile",
        "+ 新建并绑定常用的 115200 8N1 方案",
    ),
    (
        "menu.transport.new.detail",
        "Creates a reusable 115200/8N1/no-flow UART hardware profile and binds it to this serial port.",
        "创建可复用的 115200/8N1/无流控串口参数方案，并绑定到当前串口通道。",
    ),
    (
        "menu.transport.bound",
        "UART hardware profile {} bound to the current port",
        "串口参数方案 {} 已绑定到当前串口通道",
    ),
    (
        "menu.transport.created",
        "UART hardware profile {} created and bound",
        "串口参数方案 {} 已创建并绑定",
    ),
    (
        "menu.transport.missing",
        "UART hardware profile {} no longer exists",
        "串口参数方案 {} 已不存在",
    ),
    (
        "menu.device.title",
        "DUT interaction profiles",
        "样机交互方案",
    ),
    (
        "menu.device.generic",
        "Generic settings (unbind profile)",
        "通用交互设置（解除样机交互方案绑定）",
    ),
    (
        "menu.device.new",
        "+ Clone current effective DUT interaction and bind",
        "+ 复制当前生效的样机交互设置并绑定",
    ),
    (
        "menu.device.clone.detail",
        "The new interaction profile clones current prompts, line ending, echo and write pacing; each preset changes only its named field.",
        "新样机交互方案会复制当前的提示符、换行符、回显和分段发送设置；每个预设只修改标出的项目。",
    ),
    (
        "menu.device.generic.detail",
        "Unbinds the DUT interaction profile and returns this port to generic compatibility settings.",
        "解除样机交互方案绑定，让当前串口通道使用通用交互设置。",
    ),
    (
        "menu.device.bound",
        "DUT interaction profile {} bound to the current port",
        "样机交互方案 {} 已绑定到当前串口",
    ),
    (
        "menu.device.generic.bound",
        "DUT interaction profile unbound; generic settings are active",
        "已解除样机交互方案；当前使用通用设置",
    ),
    (
        "menu.device.created",
        "DUT interaction profile {} created from effective settings and bound",
        "样机交互方案 {} 已复制当前生效设置并绑定",
    ),
    (
        "menu.device.missing",
        "DUT interaction profile {} no longer exists",
        "样机交互方案 {} 已不存在",
    ),
    (
        "menu.device.echo.on",
        "+ Clone with Echo On",
        "+ 复制当前设置并开启回显",
    ),
    (
        "menu.device.echo.off",
        "+ Clone with Echo Off",
        "+ 复制当前设置并关闭回显",
    ),
    (
        "menu.device.echo.auto",
        "+ Clone with Echo Auto (conservative)",
        "+ 复制当前设置并使用自动回显判断（保守）",
    ),
    (
        "menu.device.eol.cr",
        "+ Clone with EOL CR",
        "+ 复制当前设置并使用 CR 换行",
    ),
    (
        "menu.device.eol.lf",
        "+ Clone with EOL LF",
        "+ 复制当前设置并使用 LF 换行",
    ),
    (
        "menu.device.eol.crlf",
        "+ Clone with EOL CRLF",
        "+ 复制当前设置并使用 CRLF 换行",
    ),
    (
        "menu.device.eol.custom",
        "+ Clone with custom EOL",
        "+ 复制当前设置并使用自定义换行符",
    ),
    (
        "menu.profile.exists",
        "profile {} already exists; choose another name",
        "配置方案 {} 已存在；请选择其他名称",
    ),
    (
        "menu.model.title",
        "DUT model bound to this port",
        "样机机型（当前串口绑定）",
    ),
    (
        "menu.model.parent.title",
        "Choose parent model/family",
        "选择父级机型/系列",
    ),
    (
        "menu.model.add.root",
        "+ Add root model/family",
        "+ 新建一级机型/系列",
    ),
    (
        "menu.model.add.child",
        "+ Add derived child model",
        "+ 新建二级/衍生机型",
    ),
    (
        "menu.model.no.parent",
        "add a root model before adding a child",
        "请先新建一级机型，再添加子级",
    ),
    (
        "menu.model.verify",
        "Before binding, confirm the real DUT via serial identity output, Telnet, Web UI, or a Human. Enter expands parents and binds leaves; b binds any selected node.",
        "绑定前请通过串口身份信息、Telnet、Web 页面或人工确认真实样机。Enter 展开父级并绑定叶子；b 可绑定任意所选节点。",
    ),
    (
        "menu.model.confirm.note",
        "Selected in serialctl TUI after Human verification; reconfirm via serial/Telnet/Web/Human before Agent use",
        "由人工确认后在 serialctl TUI 选择；Agent 使用前应再经串口/Telnet/Web/人工确认",
    ),
    (
        "menu.model.bound",
        "model {} bound to the current serial port",
        "样机机型 {} 已绑定到当前串口通道",
    ),
    (
        "menu.model.created",
        "model {} created and bound to the current serial port",
        "样机机型 {} 已创建并绑定到当前串口通道",
    ),
    (
        "menu.serial.title",
        "Quickly change UART hardware parameters",
        "串口参数快捷设置",
    ),
    (
        "menu.serial.current",
        "Current UART hardware profile: {}. Applying a preset clones it as a new reusable profile.",
        "当前串口参数方案：{}。选择下方预设会基于它新建方案，并应用到当前串口通道。",
    ),
    (
        "menu.serial.baud",
        "Clone current profile · baud rate {}",
        "基于当前方案 · 波特率 {}",
    ),
    (
        "menu.serial.8n1",
        "Clone · 8 data / no parity / 1 stop",
        "基于当前方案 · 8 数据位 / 无校验 / 1 停止位",
    ),
    (
        "menu.serial.8e1",
        "Clone · 8 data / even parity / 1 stop",
        "基于当前方案 · 8 数据位 / 偶校验 / 1 停止位",
    ),
    (
        "menu.serial.8o1",
        "Clone · 8 data / odd parity / 1 stop",
        "基于当前方案 · 8 数据位 / 奇校验 / 1 停止位",
    ),
    (
        "menu.serial.8n2",
        "Clone · 8 data / no parity / 2 stop",
        "基于当前方案 · 8 数据位 / 无校验 / 2 停止位",
    ),
    (
        "menu.serial.flow.none",
        "Clone · no flow control",
        "基于当前方案 · 无流控",
    ),
    (
        "menu.serial.flow.hardware",
        "Clone · hardware flow control",
        "基于当前方案 · 硬件流控",
    ),
    (
        "menu.serial.dtr",
        "Clone · toggle DTR line",
        "基于当前方案 · 切换 DTR 控制线",
    ),
    (
        "menu.serial.rts",
        "Clone · toggle RTS line",
        "基于当前方案 · 切换 RTS 控制线",
    ),
    (
        "menu.serial.auto",
        "Clone · toggle automatic port opening",
        "基于当前方案 · 切换自动打开串口",
    ),
    ("menu.detail.baud", "baud {}", "波特率 {}"),
    ("menu.detail.data_bits", "{} data bits", "{} 数据位"),
    ("menu.detail.stop_bits", "{} stop bits", "{} 停止位"),
    ("menu.detail.parity.none", "no parity", "无校验"),
    ("menu.detail.parity.odd", "odd parity", "奇校验"),
    ("menu.detail.parity.even", "even parity", "偶校验"),
    ("menu.detail.flow.none", "no flow control", "无流控"),
    (
        "menu.detail.flow.software",
        "software flow control",
        "软件流控",
    ),
    (
        "menu.detail.flow.hardware",
        "hardware flow control",
        "硬件流控",
    ),
    (
        "menu.detail.transport",
        "{} · {} · {} · {} · {} · DTR {} · RTS {} · automatic open {}",
        "{} · {} · {} · {} · {} · DTR {} · RTS {} · 自动打开 {}",
    ),
    (
        "menu.detail.prompt.shell",
        "shell prompt {}",
        "Shell 提示符 {}",
    ),
    (
        "menu.detail.prompt.uboot",
        "U-Boot prompt {}",
        "U-Boot 提示符 {}",
    ),
    ("menu.detail.eol", "line ending {}", "换行 {}"),
    ("menu.detail.eol.none", "none", "无"),
    ("menu.detail.eol.custom", "custom", "自定义"),
    ("menu.detail.eol.inherit", "inherit", "继承"),
    ("menu.detail.echo.on", "echo on", "回显开启"),
    ("menu.detail.echo.off", "echo off", "回显关闭"),
    ("menu.detail.echo.auto", "echo auto", "回显自动"),
    (
        "menu.detail.pacing",
        "write pacing {} byte(s) / {} ms",
        "分段发送：每段 {} 字节，间隔 {} 毫秒",
    ),
    (
        "menu.detail.device",
        "{} · {} · {} · {} · {}",
        "{} · {} · {} · {} · {}",
    ),
    (
        "menu.help.title",
        "Terminal workflow help",
        "终端工作流帮助",
    ),
    (
        "menu.help.menu",
        "Ctrl-] m opens this extensible menu; Up/Down, Enter and Esc navigate it.",
        "Ctrl-] m 打开此可扩展菜单；使用上下、Enter 和 Esc 导航。",
    ),
    (
        "menu.help.queue",
        "Ordinary Enter queues a non-empty LINE operation without takeover; Ctrl-] u selects queued cards.",
        "直接按 Enter 会在不接管控制权的情况下排队非空 LINE 命令；Ctrl-] u 可选择待发送命令。",
    ),
    (
        "menu.help.enter",
        "While an Agent Run is active, empty Enter never queues bytes; it only returns output to the live tail.",
        "Agent 任务执行期间，空 Enter 不会进入队列，只会回到最新输出。",
    ),
    (
        "menu.help.cooperative",
        "Alt+Enter sends direct input bound to the exact matching Agent Run while that Agent keeps its lease.",
        "Alt+Enter 将输入直接发送到当前 Agent 任务，Agent 仍保留控制权。",
    ),
    (
        "menu.help.takeover",
        "Ctrl-] t is the separate explicit takeover path and may abort the Agent's current Run.",
        "Ctrl-] t 由人工接管控制权，并可能中止当前 Agent 任务。",
    ),
    (
        "menu.help.echo",
        "The dot is local TX projection. With device echo, echo=on plus merge_echo merges exact RX; auto conservatively suppresses nothing, so two copies may appear.",
        "圆点表示本地显示的发送内容。样机会回显命令时，设置 echo=on 并启用 merge_echo，可合并完全一致的回显；echo=auto 不主动去重，因此可能看到两份命令。",
    ),
    (
        "menu.help.model",
        "Confirm the connected model through serial, Telnet, Web, or a Human before Human or Agent operations.",
        "人工或 Agent 操作前，应通过串口、Telnet、Web 或向使用者确认当前连接的样机机型。",
    ),
    (
        "menu.help.token",
        "Administrator tokens are masked, passed only to the asynchronous request, never logged, and never saved.",
        "管理员令牌会被遮罩，仅传给异步请求，不记录日志，也不保存。",
    ),
    (
        "menu.footer",
        "↑/↓ select · Enter open/apply · Esc back · r reload",
        "↑/↓ 选择 · Enter 打开/应用 · Esc 返回 · r 重载",
    ),
    (
        "menu.footer.models",
        "Enter expand/bind leaf · ←/→ collapse/expand · b bind node · Esc back",
        "Enter 展开/绑定叶子 · ←/→ 收起/展开 · b 绑定节点 · Esc 返回",
    ),
    (
        "menu.footer.help",
        "PgUp/PgDn scroll · Esc returns to the menu",
        "PgUp/PgDn 滚动 · Esc 返回菜单",
    ),
    (
        "menu.prompt.admin",
        "One-time administrator token (masked)",
        "一次性管理员令牌（已遮罩）",
    ),
    (
        "menu.prompt.transport.name",
        "New UART hardware profile name",
        "新串口参数方案名称",
    ),
    (
        "menu.prompt.device.name",
        "New DUT interaction profile name",
        "新样机交互方案名称",
    ),
    (
        "menu.prompt.model.root",
        "New root model name",
        "新一级机型名称",
    ),
    (
        "menu.prompt.model.child",
        "New derived child model name",
        "新衍生子机型名称",
    ),
    ("menu.prompt.cancelled", "input cancelled", "已取消输入"),
    (
        "menu.admin.memory",
        "Token is masked and used only for this request; it is never saved.",
        "令牌已遮罩且仅用于本次请求；不会保存。",
    ),
    (
        "menu.admin.not.required",
        "Trusted local daemon: applying without an administrator token.",
        "受信任的本机守护进程：无需管理员令牌，正在应用。",
    ),
    (
        "menu.admin.required",
        "administrator token is required",
        "必须输入管理员令牌",
    ),
    (
        "menu.name.invalid",
        "name must be non-empty, trimmed, control-free, and at most 128 bytes",
        "名称必须非空、无首尾空白和控制字符，且不超过 128 字节",
    ),
    (
        "menu.slot.missing",
        "Slot {} no longer exists",
        "串口通道 {} 已不存在",
    ),
    (
        "ui.search.title",
        " history search · Enter accepts · Esc cancels ",
        " 历史搜索 · 回车接受 · Esc 取消 ",
    ),
    (
        "ui.search.query",
        "(reverse-i-search)`{}': {}",
        "（反向历史搜索）`{}'：{}",
    ),
    ("ui.output.baud", "{} baud", "波特率 {}"),
    // ---- Bottom help line ----
    (
        "ui.helpline",
        " Ctrl-] m menu · o profiles · h command purposes · ? help · {} · q quit ",
        " Ctrl-] m 菜单 · o 配置方案 · h 命令用途 · ? 帮助 · {} · q 退出 ",
    ),
    (
        "ui.scroll.prefix",
        "Ctrl-] PgUp/PgDn scroll",
        "Ctrl-] PgUp/PgDn 滚动",
    ),
    ("ui.scroll.plain", "PgUp/PgDn scroll", "PgUp/PgDn 滚动"),
    // ---- Help popup ----
    ("help.title", " serialctl help ", " serialctl 帮助 "),
    (
        "help.group.navigation",
        "Navigation and display",
        "导航与显示",
    ),
    (
        "help.group.control",
        "Control and Agent cooperation",
        "控制权与 Agent 协作",
    ),
    ("help.group.queue", "Queued input", "排队输入"),
    ("help.group.line", "LINE mode", "LINE 模式"),
    ("help.group.raw", "RAW mode", "RAW 模式"),
    ("help.group.safety", "Safety and history", "安全与历史"),
    (
        "help.switch",
        "  Alt-1..9 / Ctrl-] 1..9   switch Slot",
        "  Alt-1..9 / Ctrl-] 1..9   切换串口通道",
    ),
    (
        "help.next",
        "  Ctrl-] s                 next Slot",
        "  Ctrl-] s                 下一个串口通道",
    ),
    (
        "help.mode",
        "  Ctrl-] l / r             LINE / RAW mode",
        "  Ctrl-] l / r             LINE / RAW 模式",
    ),
    (
        "help.view",
        "  Ctrl-] v                 compact / detailed timeline",
        "  Ctrl-] v                 紧凑/详细时间线",
    ),
    (
        "help.lang",
        "  Ctrl-] g                 switch language (中文/EN)",
        "  Ctrl-] g                 切换语言（中文/EN）",
    ),
    (
        "help.scroll",
        "  Ctrl-] PgUp / PgDn       local scroll (especially in RAW)",
        "  Ctrl-] PgUp / PgDn       滚动本地历史（RAW 模式下尤其有用）",
    ),
    (
        "help.wheel",
        "  wheel / left drag / right click   scroll / select+copy / copy again",
        "  滚轮 / 左键拖动 / 右键       滚动 / 拖选后自动复制 / 再次复制",
    ),
    (
        "help.selection",
        "  mouse                    handled by terminal (serialctl wheel is off)",
        "  鼠标                     由终端处理（serialctl 滚轮已关闭）",
    ),
    (
        "help.mouse.paste",
        "  input right click / Ctrl-Shift-V   paste (right click is Windows-native)",
        "  输入框右键 / Ctrl-Shift-V      粘贴（右键使用 Windows 原生行为）",
    ),
    (
        "help.menu",
        "  Ctrl-] m                 profiles / bound model / quick UART settings",
        "  Ctrl-] m                 串口参数方案 / 样机交互方案 / 样机机型",
    ),
    (
        "help.profile",
        "  Ctrl-] o                 open profile selection directly",
        "  Ctrl-] o                 直接选择串口参数或样机交互方案",
    ),
    (
        "help.run.history",
        "  Ctrl-] h                 focus/show command-history bar; repeat while focused hides",
        "  Ctrl-] h                 聚焦/显示命令记录横栏；聚焦时再按可隐藏",
    ),
    (
        "help.run.keys",
        "  Up/Down · Enter/Right · Left   select command / expand confirmed TX / collapse",
        "  上下 · Enter/右 · 左       选择记录 / 展开已确认发送的命令原文 / 收起",
    ),
    (
        "help.takeover",
        "  Ctrl-] t                 force Human takeover; active Agent Run is aborted",
        "  Ctrl-] t                 强制人工接管控制权；当前 Agent 任务会被中止",
    ),
    (
        "help.cooperative",
        "  Alt-Enter                direct LINE bound to matching Agent Run; lease stays",
        "  Alt-Enter                向当前 Agent 任务直接发送；Agent 保留控制权",
    ),
    (
        "help.release",
        "  Ctrl-] c                 release control or cancel queued input",
        "  Ctrl-] c                 释放控制权或取消排队输入",
    ),
    (
        "help.queue.delete",
        "  Ctrl-] d                 delete newest queued LINE command",
        "  Ctrl-] d                 删除最新一条排队 LINE 命令",
    ),
    (
        "help.queue.edit",
        "  Ctrl-] e                 return newest LINE to editor; Enter requeues at tail",
        "  Ctrl-] e                 将最新 LINE 取回编辑；Enter 后重新排到队尾",
    ),
    (
        "help.queue.select",
        "  Ctrl-] u                 select command; ↑/↓ cards, PgUp/PgDn text, d/e",
        "  Ctrl-] u                 选择命令；↑/↓ 选择，PgUp/PgDn 查看全文，d/e",
    ),
    (
        "help.queue.behavior",
        "  ordinary Enter           queue each non-empty LINE; Agent Run empty Enter only follows",
        "  直接按 Enter             每条非空 LINE 独立排队；Agent 任务中空 Enter 只回到最新输出",
    ),
    (
        "help.follow",
        "  Ctrl-] f                 follow live output",
        "  Ctrl-] f                 跟随实时输出",
    ),
    (
        "help.echo",
        "  ● / ✓ marker             local TX / exact RX merged; echo=auto suppresses nothing",
        "  ● / ✓ 标记               本地 TX / 与精确 RX 合并；echo=auto 不抑制回显",
    ),
    (
        "help.paste",
        "  Ctrl-] p                 confirm blocked paste",
        "  Ctrl-] p                 确认待发送的多行或大段粘贴",
    ),
    (
        "help.byte",
        "  Ctrl-] Ctrl-]            send byte 0x1d",
        "  Ctrl-] Ctrl-]            发送字节 0x1d",
    ),
    (
        "help.interrupt",
        "  Ctrl-C                   send ETX (0x03); LINE draft is cleared",
        "  Ctrl-C                   发送 ETX (0x03)；LINE 草稿会清空",
    ),
    (
        "help.quit",
        "  Ctrl-] q                 quit",
        "  Ctrl-] q                 退出",
    ),
    (
        "help.line1",
        "  Enter                    queue command + interaction-profile line ending",
        "  Enter                    排队命令并附加样机交互方案的换行符",
    ),
    (
        "help.line2",
        "  Alt-Enter                cooperative direct input; does not take control",
        "  Alt-Enter                向当前 Agent 任务直接发送，不抢占控制权",
    ),
    (
        "help.line3",
        "  Up/Down · Ctrl-R · Tab   history / search / complete; empty Agent Enter follows",
        "  上/下 · Ctrl-R · Tab     历史 / 搜索 / 补全；Agent 任务中空回车只回到最新输出",
    ),
    (
        "help.raw1",
        "  keys / Ctrl-C            send bytes directly; Ctrl-C does not quit",
        "  按键 / Ctrl-C            直接发送字节；Ctrl-C 不会退出",
    ),
    (
        "help.raw2",
        "  PageUp/PageDown          sent to device; prefix them for local scroll",
        "  PageUp/PageDown          发往样机；加前缀才滚动本地历史",
    ),
    (
        "help.paste.note",
        "Large or multi-line paste is always held for explicit confirmation.",
        "大段或多行粘贴始终需要手动确认。",
    ),
    (
        "help.expire",
        "Queued input expires after {}s idle; cancel reconnects and releases this terminal's controls.",
        "排队输入在 {} 秒无操作后过期；取消时会重新连接，并释放本终端持有的控制权。",
    ),
    (
        "help.replay",
        "Disconnected input is never replayed after reconnect.",
        "断线期间的输入不会在重连后补发。",
    ),
    (
        "help.uncertain",
        "Sent writes without an acknowledgement are uncertain; inspect TX before retrying.",
        "发送后未收到确认的结果并不确定；重试前请先检查发送记录。",
    ),
    (
        "help.close",
        "PgUp/PgDn scroll · Home/End jump · Esc or ? closes help.",
        "PgUp/PgDn 滚动 · Home/End 跳转 · Esc 或 ? 关闭帮助。",
    ),
    (
        "help.position",
        "rows {}-{} / {} · PgUp/PgDn",
        "第 {}–{} 行，共 {} 行 · PgUp/PgDn",
    ),
    // ---- Status messages ----
    ("st.connecting", "connecting…", "连接中…"),
    ("st.viewing", "viewing {} ({})", "当前串口通道：{}（{}）"),
    (
        "st.transport",
        "transport connected; authenticating and attaching all Slots",
        "已连接服务器，正在认证并接入所有串口通道",
    ),
    (
        "st.disconnected",
        "disconnected: {}; reconnecting",
        "连接已断开：{}；正在重新连接",
    ),
    (
        "st.disconnected.uncertain",
        "disconnected: {}; {} sent write outcome(s) uncertain; inspect TX before retrying",
        "连接已断开：{}；有 {} 次发送结果未确认，重试前请先检查发送记录",
    ),
    (
        "st.welcome",
        "connected as {} (protocol v{})",
        "已连接，角色：{}（协议 v{}）",
    ),
    (
        "st.session.changed.unsent",
        "the serial session changed before queued input was sent",
        "串口会话已在排队输入发送前变更",
    ),
    (
        "st.session.changed.discarded",
        "the serial session changed; queued input was discarded",
        "串口会话已变更；排队输入已丢弃",
    ),
    (
        "st.invalidated",
        "{}: {} ({} write(s), {} request(s))",
        "{}：{}（{} 次发送，{} 个请求）",
    ),
    (
        "st.daemon.restarted",
        "daemon restarted; old control leases were invalidated",
        "守护进程已重启；之前的控制权租约已失效",
    ),
    (
        "st.epoch.changed",
        "daemon epoch changed; previous control leases and cursors are invalid",
        "服务实例已变更；之前的控制权租约和历史游标已失效",
    ),
    ("st.retryable", " (retryable)", "(可重试)"),
    (
        "st.discarded.chunks",
        "; {}: discarded {} queued chunk(s)",
        "；{}：已丢弃 {} 个待发送分段",
    ),
    (
        "st.history.gap",
        "history gap ({}); requested after {}, first available {}",
        "历史缺失（{}）；请求起点：{}，最早可用：{}",
    ),
    (
        "st.lagged",
        "slow client missed live events {}..={}; reconnecting for journal replay",
        "客户端处理较慢，漏掉了实时事件 {}..={}；正在重连补齐历史",
    ),
    (
        "st.replaying",
        "replaying {} #{}..=#{}",
        "正在补齐 {} 的历史记录 #{}..=#{}",
    ),
    (
        "st.live",
        "{} live at sequence {}",
        "{} 已进入实时模式，最新序号为 {}",
    ),
    (
        "st.granted",
        "write control granted for {}",
        "已获得 {} 的控制权",
    ),
    (
        "st.queued",
        "write control queued at position {}; input is held locally",
        "控制权请求排在第 {} 位；输入已暂存在本地",
    ),
    (
        "st.acquire.cancelled",
        "queued write control request cancelled for {}",
        "已取消 {} 的控制权排队请求",
    ),
    (
        "st.released",
        "write control released for {}",
        "已释放 {} 的控制权",
    ),
    (
        "st.write.confirmed",
        "{}: write confirmed at sequence {}",
        "{}：发送已确认，序号 {}",
    ),
    (
        "st.trigger.result",
        "Trigger {} is {} after {} confirmed fire(s)",
        "触发任务 {} 当前为 {}，已确认发送 {} 次",
    ),
    (
        "st.authenticated",
        "authenticated as {}",
        "已认证，角色：{}",
    ),
    (
        "st.watching",
        "watching {} Slot(s)",
        "正在监视 {} 个串口通道",
    ),
    (
        "st.detached",
        "detached {} Slot(s)",
        "已停止监视 {} 个串口通道",
    ),
    ("st.run.started", "run started: {}", "Agent 任务已开始：{}"),
    ("st.run.ended", "run ended: {}", "Agent 任务已结束：{}"),
    (
        "st.checkpoint",
        "checkpoint created at sequence {}",
        "已在序列 {} 创建检查点",
    ),
    (
        "st.not.auth.queued",
        "connection is not authenticated; input was not queued",
        "连接尚未认证；输入未加入队列",
    ),
    (
        "st.not.connected",
        "not connected; input was not queued",
        "尚未连接；输入未加入队列",
    ),
    (
        "st.too.many",
        "too many outstanding daemon requests; input was not sent",
        "待处理请求过多；输入未发送",
    ),
    (
        "st.outbound.full",
        "outbound queue is full; input was not sent",
        "发送队列已满；输入未发送",
    ),
    (
        "st.network.stopped",
        "network worker stopped",
        "网络工作线程已停止",
    ),
    (
        "st.not.auth2",
        "not authenticated; input was not queued",
        "尚未认证；输入未加入队列",
    ),
    (
        "st.not.live",
        "{} is not live yet; input was not queued",
        "{} 尚未进入实时模式；输入未加入队列",
    ),
    (
        "st.writeq.full",
        "local write queue is full; input was not queued",
        "本地发送队列已满；输入未加入队列",
    ),
    (
        "st.not.auth.live",
        "the selected Slot is not authenticated and live; control was not requested",
        "所选串口通道尚未认证并进入实时模式；未请求控制权",
    ),
    (
        "st.requesting.control",
        "requesting write control for {}…",
        "正在请求 {} 的控制权…",
    ),
    (
        "st.requesting.takeover",
        "requesting forced Human takeover of {}… the active Agent Run will be aborted",
        "正在请求人工接管 {}… 当前 Agent 任务将被中止",
    ),
    (
        "st.takeover.granted",
        "Human takeover of {} granted; the previous Agent Run was aborted",
        "已取得 {} 的人工控制权；之前的 Agent 任务已被中止",
    ),
    (
        "st.run.aborted",
        "Agent Run aborted: {} · reason: {}",
        "Agent 任务已中止：{} · 原因：{}",
    ),
    (
        "st.slot.not.live",
        "the selected Slot is not live; control was not released",
        "所选串口通道尚未进入实时模式；未释放控制权",
    ),
    (
        "st.cancel.reason",
        "operator cancelled queued input",
        "操作员取消了排队输入",
    ),
    (
        "st.no.control",
        "this Slot has no active write control",
        "当前串口通道没有人持有控制权",
    ),
    (
        "st.control.belongs",
        "write control belongs to {}",
        "控制权由 {} 持有",
    ),
    (
        "st.reconnect.reason",
        "{} for {}; reconnecting cancels this actor's queues and releases its controls on every Slot",
        "{}（{}）；重新连接会取消当前操作方的队列，并释放其在所有串口通道上的控制权",
    ),
    (
        "st.cancel.full",
        "cannot cancel queued control: outbound queue is full",
        "无法取消控制权排队请求：发送队列已满",
    ),
    (
        "st.cancel.stopped",
        "cannot cancel queued control: network worker stopped",
        "无法取消控制权排队请求：网络任务已停止",
    ),
    (
        "st.idle.release",
        "{}: releasing idle human control after {} seconds",
        "{}：人工控制权已空闲 {} 秒，正在释放",
    ),
    (
        "st.queue.expired",
        "queued human input expired after {} seconds of inactivity",
        "排队的人工输入在 {} 秒无活动后过期",
    ),
    (
        "st.prefix.hint",
        "command prefix: 1-9 serial port, m menu, o profiles, h command purposes, l/r mode, PgUp/PgDn scroll, t takeover, u queue, c release/cancel, ? help",
        "快捷键前缀：1-9 串口，m 菜单，o 配置方案，h 命令用途，l/r 模式，PgUp/PgDn 滚动，t 接管，u 队列，c 释放/取消，? 帮助",
    ),
    (
        "st.line.mode",
        "LINE mode: Enter sends the line plus the interaction-profile line ending",
        "LINE 模式：回车发送该行，并附加样机交互方案的换行符",
    ),
    (
        "st.raw.mode",
        "RAW mode: keystrokes are sent directly; Ctrl-] remains local",
        "RAW 模式：按键直接发送；Ctrl-] 仍用于本地命令",
    ),
    ("st.follow", "following live output", "正在跟随实时输出"),
    (
        "st.detailed",
        "detailed timeline: #seq and source columns shown",
        "详细时间线：显示序号和来源列",
    ),
    (
        "st.compact",
        "compact timeline: markers and inline highlighting",
        "紧凑时间线：显示标记和行内高亮",
    ),
    (
        "st.logs.hint",
        "use `serialctl logs --contains TEXT` for durable history search",
        "使用 `serialctl logs --contains TEXT` 进行持久历史搜索",
    ),
    (
        "st.unknown.prefix",
        "unknown prefix command; Ctrl-] ? opens help",
        "未知的快捷键前缀命令；按 Ctrl-] ? 查看帮助",
    ),
    (
        "st.queue.none",
        "no queued LINE command to change",
        "没有可修改的排队 LINE 命令",
    ),
    (
        "st.queue.already.sending",
        "the queued command is already being sent and can no longer be changed",
        "排队命令已经开始发送，不能再修改",
    ),
    (
        "st.queue.raw.only",
        "the newest queued input is RAW bytes; use Ctrl-] c to cancel the queue",
        "最新排队输入是 RAW 字节；请用 Ctrl-] c 取消队列",
    ),
    (
        "st.queue.deleted",
        "queued LINE command deleted",
        "已删除所选排队 LINE 命令",
    ),
    (
        "st.queue.restored",
        "queued LINE command returned to the editor; Enter requeues it at the tail",
        "已将所选排队 LINE 命令取回编辑框；Enter 后重新排到队尾",
    ),
    (
        "st.queue.select",
        "queued-command selection: ↑/↓ cards, PgUp/PgDn text, d deletes, e edits (Enter requeues at tail), Esc closes",
        "待发送命令选择：↑/↓ 选择命令，PgUp/PgDn 查看全文，d 删除，e 编辑（按 Enter 后排到队尾），Esc 关闭",
    ),
    (
        "st.queue.select.closed",
        "queued-command selection closed",
        "已关闭排队命令选择",
    ),
    (
        "st.agent.enter.follow",
        "Agent Run is active; empty Enter only resumed live output",
        "Agent 任务正在执行；空回车只会回到最新输出",
    ),
    (
        "st.cooperative.unavailable",
        "cooperative input requires a matching active Agent lease and Run; draft kept",
        "直接发送要求当前 Agent 的控制权租约与 Agent 任务匹配；命令草稿已保留",
    ),
    (
        "st.cooperative.sent",
        "cooperative input sent without takeover",
        "输入已直接发送，未接管控制权",
    ),
    (
        "st.menu.open",
        "configuration menu opened",
        "已打开配置菜单",
    ),
    (
        "st.menu.profile.open",
        "profile selection opened",
        "已打开配置方案选择",
    ),
    (
        "st.menu.closed",
        "configuration menu closed",
        "已关闭配置菜单",
    ),
    (
        "st.clipboard.copied",
        "copied {} character(s) from serial output",
        "已从串口输出复制 {} 个字符",
    ),
    (
        "st.selection.copied",
        "selected and copied {} character(s); right-click repeats the copy",
        "已自动复制 {} 个字符；在输出区右键可再次复制",
    ),
    (
        "st.run.panel.focused",
        "command-history bar focused; Up/Down selects, Enter expands, Ctrl-] h hides",
        "已聚焦命令记录横栏；上下选择，Enter 展开，Ctrl-] h 隐藏",
    ),
    (
        "st.run.panel.hidden",
        "command-history bar hidden",
        "已隐藏命令记录横栏",
    ),
    (
        "st.run.panel.left",
        "left command-history bar",
        "已返回命令输入",
    ),
    (
        "st.clipboard.copy.failed",
        "cannot copy selection: {}",
        "无法复制所选文本：{}",
    ),
    (
        "st.clipboard.paste.shortcut",
        "right-click paste is unavailable on this platform; use Ctrl-Shift-V",
        "此平台不支持应用内右键粘贴；请使用 Ctrl-Shift-V",
    ),
    (
        "st.clipboard.paste.failed",
        "cannot read clipboard: {}",
        "无法读取剪贴板：{}",
    ),
    (
        "st.paste.rejected",
        "paste rejected: {} bytes exceeds the {} byte interactive safety limit",
        "无法粘贴：{} 字节超过 {} 字节的交互安全上限",
    ),
    (
        "st.paste.blocked",
        "multi-line/large paste blocked; Ctrl-] p confirms for the original Slot",
        "多行或大段粘贴正在等待确认；按 Ctrl-] p 发送到原串口通道",
    ),
    (
        "st.paste.none",
        "no blocked paste to confirm",
        "没有待确认的粘贴",
    ),
    (
        "st.paste.gone",
        "the paste target Slot no longer exists",
        "粘贴目标串口通道已不存在",
    ),
    (
        "st.paste.queued",
        "confirmed paste queued for {}",
        "已将确认后的粘贴加入 {} 的发送队列",
    ),
    (
        "st.no.slot",
        "no Slot is configured; run `serialctl init`",
        "尚未配置串口通道；请运行 `serialctl init`",
    ),
    ("st.language", "language: {}", "语言：{}"),
    (
        "st.write.disappeared",
        "write control disappeared before send",
        "发送前控制权已失效",
    ),
    (
        "st.break.confirmed",
        "BREAK confirmed at sequence {}",
        "串口 BREAK 已确认，序号 {}",
    ),
    // ---- display.rs labels ----
    ("d.dev", "DEV", "样机"),
    ("d.tx", "TX>", "发送>"),
    ("d.system", "SYSTEM", "系统"),
    ("d.gap", "GAP", "历史缺失"),
    ("d.kind.human", "HUMAN", "人工"),
    ("d.kind.agent", "AGENT", "Agent"),
    ("d.kind.script", "SCRIPT", "脚本"),
    ("d.kind.system", "SYSTEM", "系统"),
    ("d.ev.rx", "rx", "接收"),
    ("d.ev.tx", "tx", "发送"),
    ("d.ev.serial_opening", "serial_opening", "串口打开中"),
    ("d.ev.serial_opened", "serial_opened", "串口已打开"),
    (
        "d.ev.serial_open_failed",
        "serial_open_failed",
        "串口打开失败",
    ),
    ("d.ev.serial_closed", "serial_closed", "串口已关闭"),
    (
        "d.ev.slot_reconfigured",
        "slot_reconfigured",
        "串口通道配置已更新",
    ),
    ("d.ev.slot_removed", "slot_removed", "串口通道已移除"),
    ("d.ev.control_granted", "control_granted", "控制权已授予"),
    ("d.ev.control_released", "control_released", "控制权已释放"),
    ("d.ev.control_revoked", "control_revoked", "控制权已撤销"),
    ("d.ev.control_expired", "control_expired", "控制权已过期"),
    ("d.ev.run_started", "run_started", "Agent 任务开始"),
    ("d.ev.run_ended", "run_ended", "Agent 任务结束"),
    ("d.ev.run_aborted", "run_aborted", "Agent 任务中止"),
    ("d.run.start", "RUN START", "Agent 任务开始"),
    ("d.run.end", "RUN END", "Agent 任务结束"),
    ("d.run.abort", "RUN ABORTED", "Agent 任务中止"),
    ("d.ev.trigger_started", "trigger_started", "触发任务已启动"),
    (
        "d.ev.trigger_completed",
        "trigger_completed",
        "触发任务已完成",
    ),
    (
        "d.ev.trigger_cancelled",
        "trigger_cancelled",
        "触发任务已取消",
    ),
    ("d.ev.trigger_failed", "trigger_failed", "触发任务失败"),
    ("d.ev.break", "break", "串口 BREAK"),
    ("d.break.duration", "BREAK · {} ms", "串口 BREAK · {} 毫秒"),
    ("d.ev.checkpoint", "checkpoint", "检查点"),
    ("d.ev.logging_degraded", "logging_degraded", "日志降级"),
    ("d.ev.gap", "gap", "历史缺失"),
    ("d.event.detail", "{}: {}", "{}：{}"),
    ("d.run.abort.reason", "reason: {}", "原因：{}"),
    // ---- main.rs runtime output ----
    (
        "m.terminal.required",
        "interactive mode requires a terminal; use `serialctl status --json` or `serialctl logs --json`",
        "交互模式需要终端；请使用 `serialctl status --json` 或 `serialctl logs --json`",
    ),
    (
        "m.scope.error",
        "--initial-slot applies only to the interactive `serialctl` console",
        "--initial-slot 仅适用于交互式 `serialctl` 控制台",
    ),
    (
        "m.status.header",
        "seriald {}  epoch {}  {} Slot(s)",
        "seriald {}  服务实例 {}  {} 个串口通道",
    ),
    ("m.status.control", "control: {}", "控制权：{}"),
    ("m.status.reason", "  reason: {}", "  原因：{}"),
    (
        "m.status.trigger",
        "  trigger: {}  status: {}  fires: {}",
        "  触发任务：{}  状态：{}  已发送：{} 次",
    ),
    ("m.doctor.config", "config", "配置文件"),
    ("m.doctor.endpoint", "endpoint", "服务地址"),
    ("m.doctor.token", "token", "令牌"),
    ("m.doctor.daemon", "daemon", "守护进程"),
    ("m.doctor.server", "server", "服务器"),
    ("m.doctor.epoch", "epoch", "服务实例"),
    ("m.doctor.protocol", "protocol", "协议版本"),
    ("m.doctor.protocol.compatible", "compatible", "兼容"),
    (
        "m.doctor.protocol.mismatch",
        "version mismatch",
        "版本不匹配",
    ),
    ("m.doctor.uptime", "uptime", "运行时长"),
    ("m.doctor.slots", "slots", "串口通道"),
    ("m.token.configured", "configured", "已配置"),
    ("m.token.missing", "not configured", "未配置"),
    (
        "m.doctor.slots.value",
        "{} total, {} online",
        "共 {} 个，{} 个在线",
    ),
    // ---- doctor.rs human-readable diagnostics ----
    ("doctor.field.source", "Source", "数据来源"),
    ("doctor.field.slot", "Slot", "串口通道"),
    ("doctor.field.port", "Port", "串口设备"),
    ("doctor.field.discovery", "Discovery", "串口发现"),
    ("doctor.field.session", "Session", "会话状态"),
    ("doctor.field.assessment", "Assessment", "诊断结论"),
    ("doctor.field.state_code", "State code", "状态代码"),
    ("doctor.field.reason", "Reason", "原因"),
    ("doctor.field.counters", "Counters", "数据计数"),
    ("doctor.field.consumers", "Consumers", "订阅客户端"),
    ("doctor.field.history", "History", "历史记录"),
    ("doctor.field.usage", "Usage", "存储用量"),
    ("doctor.field.retention", "Retention", "保留策略"),
    ("doctor.field.archives", "Archives", "归档数量"),
    ("doctor.field.writer", "Writer queue", "日志写入队列"),
    ("doctor.field.logging", "Logging", "日志状态"),
    ("doctor.field.quota", "Quota", "存储配额"),
    (
        "doctor.field.degraded_slots",
        "Degraded Slots",
        "日志降级通道",
    ),
    ("doctor.field.catalog", "Archive catalog", "归档目录"),
    ("doctor.field.note", "Note", "提示"),
    ("doctor.field.stream", "Stream", "数据流"),
    ("doctor.field.control", "Control", "控制权"),
    ("doctor.field.run", "Agent Run", "Agent 任务"),
    ("doctor.field.trigger", "Trigger", "触发任务"),
    ("doctor.field.profiles", "Profiles", "配置方案"),
    ("doctor.field.transport", "Effective UART", "生效串口参数"),
    ("doctor.field.pacing", "Write pacing", "分段发送"),
    ("doctor.field.eol", "Write EOL", "发送换行符"),
    ("doctor.field.echo", "Echo", "回显设置"),
    ("doctor.field.prompts", "DUT prompts", "样机提示符"),
    ("doctor.field.duration", "Duration", "观察时长"),
    ("doctor.field.offsets", "Offsets", "偏移变化"),
    ("doctor.field.websocket", "Live stream", "实时订阅"),
    ("doctor.field.journal", "Journal", "持久日志"),
    ("doctor.field.overflow", "RX overflow", "接收溢出"),
    (
        "doctor.heading.port_lifecycle",
        "Recent serial-port lifecycle:",
        "最近的串口生命周期记录：",
    ),
    ("doctor.value.yes", "yes", "是"),
    ("doctor.value.no", "no", "否"),
    ("doctor.value.present", "present", "已发现"),
    ("doctor.value.missing", "missing", "未发现"),
    ("doctor.value.unavailable", "unavailable", "不可用"),
    ("doctor.value.discovery", "{} ({})", "{}（{}）"),
    (
        "doctor.value.session",
        "{} (generation {})",
        "{}（会话代数 {}）",
    ),
    (
        "doctor.value.session_activity",
        "{} / {}, generation {}",
        "{} / {}，会话代数 {}",
    ),
    (
        "doctor.value.counters",
        "rx={} tx={} overflow={} bytes",
        "接收={}，发送={}，溢出={} 字节",
    ),
    (
        "doctor.value.consumers",
        "{} attached, {} lagged event(s)",
        "已连接 {} 个，累计漏接 {} 个事件",
    ),
    (
        "doctor.value.history_unavailable",
        "unavailable ({})",
        "不可用（{}）",
    ),
    ("doctor.value.usage", "{} / {} bytes", "{} / {} 字节"),
    (
        "doctor.value.usage_at_least",
        "at least {} bytes",
        "至少 {} 字节",
    ),
    (
        "doctor.value.retention",
        "{} bytes ({} bytes per segment)",
        "{} 字节（每段 {} 字节）",
    ),
    (
        "doctor.value.writer",
        "{} / {} queue entries free",
        "剩余 {} / 容量 {} 个队列项",
    ),
    (
        "doctor.value.quota_unavailable",
        "unavailable on this seriald",
        "当前 seriald 不提供配额信息",
    ),
    (
        "doctor.value.catalog_truncated",
        "incomplete (bounded scan was truncated)",
        "不完整（受限扫描已截断）",
    ),
    ("doctor.value.slot", "{} ({})", "{}（{}）"),
    (
        "doctor.value.stream",
        "head={} rx={} tx={} overflow={} bytes",
        "最新序号={}，接收={}，发送={}，溢出={} 字节",
    ),
    ("doctor.value.run", "{} · {} · {}", "{} · {} · {}"),
    ("doctor.value.trigger", "{} · {}", "{} · {}"),
    (
        "doctor.value.profiles",
        "UART={} · DUT interaction={}",
        "串口参数方案={} · 样机交互方案={}",
    ),
    (
        "doctor.value.transport",
        "{} baud · {} data bits · {} · {} stop bits · {} · DTR {} · RTS {} · auto-open {}",
        "波特率 {} · {} 数据位 · {} · {} 停止位 · {} · DTR {} · RTS {} · 自动打开 {}",
    ),
    (
        "doctor.value.pacing",
        "{} byte(s) per chunk · {} ms between chunks",
        "每段 {} 字节 · 段间隔 {} 毫秒",
    ),
    ("doctor.value.eol.none", "none", "无"),
    ("doctor.value.eol.custom", "custom ({})", "自定义（{}）"),
    (
        "doctor.value.prompts",
        "Shell={} · U-Boot={}",
        "Shell={} · U-Boot={}",
    ),
    ("doctor.value.duration", "{} s", "{} 秒"),
    (
        "doctor.value.offsets",
        "rx {} -> {} (+{}) · head {} -> {}",
        "接收 {} → {}（+{}）· 最新序号 {} → {}",
    ),
    (
        "doctor.value.websocket",
        "ready={} · rx {} frame(s)/{} bytes · tx {} frame(s)/{} bytes",
        "就绪={} · 接收 {} 帧/{} 字节 · 发送 {} 帧/{} 字节",
    ),
    (
        "doctor.value.journal",
        "{} RX event(s)/{} bytes · gaps={} · truncated={}",
        "{} 个接收事件/{} 字节 · 历史缺失={} · 已截断={}",
    ),
    ("doctor.value.overflow", "+{} bytes", "+{} 字节"),
    (
        "doctor.source.port.enumeration",
        "daemon port enumeration",
        "守护进程串口枚举",
    ),
    (
        "doctor.source.slot.snapshot",
        "authoritative Slot snapshot",
        "串口通道权威快照",
    ),
    (
        "doctor.source.storage.diagnostics",
        "authoritative daemon diagnostics",
        "守护进程权威诊断数据",
    ),
    (
        "doctor.source.archive.fallback",
        "archive-catalog fallback",
        "归档目录兼容数据",
    ),
    (
        "doctor.source.slot.diagnostics",
        "authoritative Slot diagnostics",
        "串口通道权威诊断数据",
    ),
    (
        "doctor.source.status.fallback",
        "status fallback",
        "状态快照兼容数据",
    ),
    ("doctor.logging.healthy", "healthy", "正常"),
    ("doctor.logging.degraded", "degraded", "已降级"),
    (
        "doctor.note.upgrade_storage",
        "upgrade seriald for authoritative quota and writer-queue metrics",
        "升级 seriald 后可查看权威配额和日志写入队列指标",
    ),
    (
        "doctor.assessment.slot_disabled",
        "the Slot is disabled",
        "串口通道已禁用",
    ),
    (
        "doctor.assessment.port_not_present",
        "the configured serial port is not present",
        "未发现已配置的串口设备",
    ),
    (
        "doctor.assessment.online",
        "the serial session is online",
        "串口会话在线，未发现异常",
    ),
    (
        "doctor.assessment.opening",
        "the serial port is opening",
        "正在打开串口",
    ),
    (
        "doctor.assessment.open_failed_backoff",
        "opening failed; waiting to retry",
        "串口打开失败，正在等待重试",
    ),
    (
        "doctor.assessment.waiting_for_port",
        "waiting for the configured serial port",
        "正在等待已配置的串口设备出现",
    ),
    (
        "doctor.assessment.stopping",
        "the serial session is stopping",
        "串口会话正在停止",
    ),
    (
        "doctor.assessment.inconclusive_session_changed",
        "inconclusive: the daemon or serial session changed during observation",
        "无法判断：观察期间服务实例或串口会话发生了变化",
    ),
    (
        "doctor.assessment.live_subscription_not_ready",
        "the live subscription did not become ready",
        "实时订阅未进入就绪状态",
    ),
    (
        "doctor.assessment.subscriber_lagged",
        "the live subscriber fell behind and missed events",
        "实时订阅处理过慢，漏接了事件",
    ),
    (
        "doctor.assessment.stream_gap_detected",
        "a gap was detected in live or persistent history",
        "实时记录或持久日志中存在历史缺失",
    ),
    (
        "doctor.assessment.target_silent_during_window",
        "the DUT produced no data during the observation window",
        "观察期间样机没有输出数据",
    ),
    (
        "doctor.assessment.healthy",
        "live delivery is healthy",
        "实时接收正常",
    ),
    (
        "doctor.assessment.live_delivery_fault",
        "persistent RX exists, but live delivery received no data",
        "持久日志有接收记录，但实时订阅未收到数据",
    ),
    (
        "doctor.assessment.journal_degraded",
        "the journal is degraded and no RX was observed",
        "持久日志已降级，且未观察到样机数据",
    ),
    (
        "doctor.assessment.ingestion_visibility_fault",
        "the RX offset changed, but neither live delivery nor the journal exposed RX events",
        "接收偏移已变化，但实时订阅和持久日志均未显示接收事件",
    ),
    (
        "doctor.assessment.unknown",
        "unknown assessment ({})",
        "未知诊断结果（{}）",
    ),
    (
        "doctor.error.ws_url",
        "invalid seriald WebSocket URL",
        "seriald WebSocket 地址无效",
    ),
    (
        "doctor.error.token_header",
        "token contains invalid HTTP header characters",
        "令牌包含 HTTP 请求头不允许的字符",
    ),
    (
        "doctor.error.ws_timeout",
        "independent WebSocket connection timed out",
        "独立 WebSocket 连接超时",
    ),
    (
        "doctor.error.ws_connect",
        "independent WebSocket connection failed",
        "独立 WebSocket 连接失败",
    ),
    (
        "doctor.error.subscription_rejected",
        "seriald rejected the diagnostic subscription: {}",
        "seriald 拒绝诊断订阅：{}",
    ),
    (
        "doctor.error.ws_text",
        "seriald sent unsupported text on the binary protocol",
        "seriald 在二进制协议连接中发送了不受支持的文本消息",
    ),
    (
        "doctor.error.unknown_slot",
        "unknown Slot `{}`",
        "串口通道 `{}` 不存在",
    ),
    ("m.uptime.ms", "{} ms", "{} 毫秒"),
    (
        "m.archives.none",
        "No retained serial archives found.",
        "未找到保留的串口归档。",
    ),
    (
        "m.archives.line",
        "{} {}  segment-open {} .. {}  seq {}..={}  {}  {} segment(s){}",
        "{} {}  段窗口 {} .. {}  序列 {}..={}  {}  {} 个段{}",
    ),
    ("m.archives.open", "  [open]", "  [打开]"),
    (
        "m.archives.truncated",
        "archive catalog is incomplete because its bounded scan skipped unreadable entries or reached the response limit",
        "归档目录不完整：受限扫描跳过了不可读条目，或已达到响应上限",
    ),
    (
        "m.logs.span.warn",
        "warning: this query spans the entire selected daemon epoch and may include older test cycles; --contains only filters that global range, so narrow it with --run, --operation, --after-seq, or --after-time/--before-time",
        "警告：此查询覆盖所选服务实例的全部历史，可能包含较早的测试周期；--contains 只过滤该范围，请用 --run、--operation、--after-seq 或 --after-time/--before-time 缩小范围",
    ),
    (
        "m.logs.truncated",
        "results truncated; repeat the same filters with --epoch {} --after-seq {}",
        "结果已截断；使用相同过滤条件并附加 --epoch {} --after-seq {} 继续查询",
    ),
    (
        "m.logs.truncated.nocursor",
        "results truncated without a continuation cursor",
        "结果已截断，且没有续传游标",
    ),
    (
        "m.logs.gap",
        "gap {}..={} ({}, epoch {})",
        "历史缺失 {}..={}（{}，服务实例 {}）",
    ),
    (
        "m.logs.time.order",
        "--after-time must be earlier than --before-time",
        "--after-time 必须早于 --before-time",
    ),
    (
        "m.logs.seq.order",
        "--after-seq must not exceed --through-seq",
        "--after-seq 不能大于 --through-seq",
    ),
    (
        "m.limit.int",
        "limit must be a positive integer",
        "limit 必须是正整数",
    ),
    (
        "m.limit.range",
        "limit must be between 1 and 10000",
        "limit 必须在 1 到 10000 之间",
    ),
    (
        "m.time.invalid",
        "invalid RFC3339 timestamp `{}`: {}; include a timezone, for example 2026-07-19T12:30:00+08:00",
        "无效的 RFC3339 时间戳 `{}`：{}；请包含时区，例如 2026-07-19T12:30:00+08:00",
    ),
    (
        "m.time.range",
        "RFC3339 timestamp `{}` is outside the nanosecond range",
        "RFC3339 时间戳 `{}` 超出纳秒范围",
    ),
    (
        "m.direction.unknown",
        "unknown direction `{}`; use rx, tx, or none",
        "未知方向 `{}`；请使用 rx、tx 或 none",
    ),
    (
        "m.kind.unknown",
        "unknown event kind `{}`; use rx, tx, serial-opened, serial-closed, run-started, trigger-started, checkpoint, or another protocol event kind",
        "未知事件类型 `{}`；请使用 rx、tx、serial-opened、serial-closed、run-started、trigger-started、checkpoint 或其他协议事件类型",
    ),
    // ---- init wizard ----
    ("i.endpoint", "seriald endpoint", "seriald 服务地址"),
    (
        "i.token.notice",
        "The saved token is treated as the daily operator token; setup still requires a separate admin token.",
        "已保存的令牌将作为日常操作员令牌；初始配置仍需单独的管理员令牌。",
    ),
    (
        "i.admin.prompt",
        "seriald admin bearer token (required for setup; never saved): ",
        "seriald 管理员令牌（配置必需，不会保存）：",
    ),
    (
        "i.admin.required",
        "an admin bearer token is required; seriald v1 does not support disabled authentication",
        "必须提供管理员令牌；seriald v1 不支持关闭认证",
    ),
    (
        "i.unreachable",
        "cannot reach seriald; start seriald on Windows and verify the host-only endpoint",
        "无法连接 seriald；请在 Windows 上启动 seriald，并检查仅本机可访问的服务地址",
    ),
    (
        "i.status.fail",
        "cannot read existing Slot configuration; verify the admin token",
        "无法读取现有串口通道配置；请检查管理员令牌",
    ),
    (
        "i.connected",
        "Connected to seriald {} (epoch {}).",
        "已连接 seriald {}（服务实例 {}）。",
    ),
    (
        "i.no.ports",
        "seriald found no serial ports on its host",
        "seriald 在其主机上未发现串口",
    ),
    (
        "i.ports.header",
        "\nSerial ports discovered on the seriald host:",
        "\n在 seriald 所在主机上发现以下串口：",
    ),
    (
        "i.select.ports",
        "Select ports for the complete Slot set (comma-separated numbers)",
        "选择要配置为串口通道的端口（使用逗号分隔编号）",
    ),
    (
        "i.profile.note",
        "\nNew ports use: 115200 8N1, no flow control, DTR/RTS low, TX EOL \\r, echo on, no guessed device prompt, probe disabled, auto-open.",
        "\n新串口通道默认使用：115200 8N1、无流控、DTR/RTS 低电平、TX 换行符 \\r、开启回显、不猜测样机提示符、关闭探测、自动打开。",
    ),
    (
        "i.existing.keep",
        "Previously configured ports keep their Profile and serial settings.",
        "此前配置过的端口会保留原串口参数方案和样机交互方案。",
    ),
    ("i.slot.name", "Slot name for {}", "{} 的串口通道名称"),
    ("i.slot.id", "Slot ID for {}", "{} 的串口通道 ID"),
    (
        "i.omitted.header",
        "\nExisting Slots not selected in this scan:",
        "\n本次扫描未选择的已有串口通道：",
    ),
    (
        "i.omitted.note",
        "  {} → {} (kept by default, including when the COM port is temporarily absent)",
        "  {} → {}（默认保留，即使 COM 口暂时不可用）",
    ),
    (
        "i.omitted.delete",
        "Explicitly delete these omitted Slots from seriald configuration?",
        "是否从 seriald 配置中删除这些未选择的串口通道？",
    ),
    (
        "i.omitted.deleting",
        "Deleting {} explicitly omitted Slot(s).",
        "正在删除 {} 个未选择的串口通道。",
    ),
    (
        "i.omitted.keeping",
        "Keeping {} existing Slot(s).",
        "保留 {} 个已有串口通道。",
    ),
    (
        "i.configured",
        "\nConfigured {} Slot(s):",
        "\n已配置 {} 个串口通道：",
    ),
    (
        "i.operator.keep",
        "seriald operator bearer token for daily use (leave empty to keep the saved token): ",
        "seriald 日常操作员令牌（留空可保留已保存的令牌）：",
    ),
    (
        "i.operator.required.prompt",
        "seriald operator bearer token for daily use (required; saved locally): ",
        "seriald 日常操作员令牌（必需；保存在本机）：",
    ),
    (
        "i.operator.required",
        "an operator bearer token is required for the daily console; the admin token is not saved",
        "日常控制台需要操作员令牌；管理员令牌不会保存",
    ),
    (
        "i.operator.fail",
        "the operator token could not read daemon status; the token file was not changed",
        "操作员令牌无法读取守护进程状态；令牌文件未更改",
    ),
    (
        "i.role.fail",
        "the daily token role could not be verified; the token file was not changed",
        "无法验证日常令牌的角色；令牌文件未更改",
    ),
    (
        "i.role.wrong",
        "the daily token has role {}; an operator token is required and the token file was not changed",
        "日常令牌的角色为 {}；必须使用操作员令牌，令牌文件未更改",
    ),
    (
        "i.saved",
        "Saved serialctl configuration to {}.",
        "serialctl 配置已保存到 {}。",
    ),
    (
        "i.open.console",
        "Run `serialctl` to open the multi-Slot console.",
        "运行 `serialctl` 打开多串口通道控制台。",
    ),
    (
        "i.interactive",
        "this command requires an interactive terminal",
        "此命令需要交互式终端",
    ),
    (
        "i.invalid.selection",
        "invalid port selection `{}`",
        "无效的端口选择 `{}`",
    ),
    (
        "i.selection.range",
        "port selection {} is outside 1..={}",
        "端口选择 {} 超出 1..={} 范围",
    ),
    (
        "i.selection.empty",
        "select at least one serial port",
        "请至少选择一个串口",
    ),
    (
        "i.delete.confirm",
        "enter `y` to delete the omitted Slots or `n` to keep them",
        "输入 `y` 删除未选择的串口通道，输入 `n` 保留",
    ),
];

/// Resolves `key` in the active language. Unknown keys return the key itself
/// so a missing entry is visible during development instead of panicking.
pub fn tr(key: &'static str) -> &'static str {
    let entry = STRINGS.iter().find(|(name, ..)| *name == key);
    let Some((_, en, zh)) = entry else {
        return key;
    };
    match lang() {
        Lang::En => en,
        Lang::Zh => zh,
    }
}

/// Formats the translated template for `key`, replacing each successive `{}`
/// placeholder with the next argument. Extra placeholders are left as-is and
/// extra arguments are ignored.
pub fn trf(key: &'static str, args: &[&str]) -> String {
    let template = tr(key);
    let mut output = String::with_capacity(template.len() + 16);
    let mut rest = template;
    for arg in args {
        let Some(index) = rest.find("{}") else {
            break;
        };
        output.push_str(&rest[..index]);
        output.push_str(arg);
        rest = &rest[index + 2..];
    }
    output.push_str(rest);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_language_defaults_to_chinese() {
        assert_eq!(Lang::default(), Lang::Zh);
    }

    #[test]
    fn language_switch_picks_the_matching_column() {
        let _guard = lang_test_lock();
        assert_eq!(tr("ui.paused"), " · PAUSED");
        set_lang(Lang::Zh);
        assert_eq!(tr("ui.paused"), " · 已暂停");
        set_lang(Lang::En);
    }

    #[test]
    fn unknown_keys_fall_back_to_the_key_name() {
        let _guard = lang_test_lock();
        assert_eq!(tr("no.such.key"), "no.such.key");
    }

    #[test]
    fn formatting_substitutes_placeholders_in_order() {
        let _guard = lang_test_lock();
        set_lang(Lang::Zh);
        assert_eq!(
            trf("st.live", &["slot-1", "42"]),
            "slot-1 已进入实时模式，最新序号为 42"
        );
        set_lang(Lang::En);
        assert_eq!(
            trf("st.live", &["slot-1", "42"]),
            "slot-1 live at sequence 42"
        );
        assert_eq!(trf("st.live", &[]), "{} live at sequence {}");
    }

    #[test]
    fn every_zh_entry_is_present_and_nonempty() {
        for (key, en, zh) in STRINGS {
            assert!(!en.is_empty(), "empty English text for {key}");
            assert!(!zh.is_empty(), "empty Chinese text for {key}");
            assert_eq!(
                en.matches("{}").count(),
                zh.matches("{}").count(),
                "placeholder count mismatch for {key}"
            );
        }
    }

    #[test]
    fn main_menu_chinese_names_explain_the_binding_scope() {
        let _guard = lang_test_lock();
        set_lang(Lang::Zh);
        assert_eq!(
            tr("menu.root.profile"),
            "配置方案（串口参数方案／样机交互方案）"
        );
        assert_eq!(tr("menu.root.model"), "样机机型（当前串口绑定）");
        assert_eq!(tr("menu.root.serial"), "快速创建串口参数方案");
        assert!(tr("menu.current").contains("当前串口"));
        assert!(!tr("menu.current").contains("Transport"));
    }

    #[test]
    fn lang_parses_common_spellings() {
        assert_eq!(Lang::parse("en"), Some(Lang::En));
        assert_eq!(Lang::parse("ZH-CN"), Some(Lang::Zh));
        assert_eq!(Lang::parse("fr"), None);
        assert_eq!(Lang::En.toggled(), Lang::Zh);
        assert_eq!(Lang::Zh.toggled(), Lang::En);
    }
}
