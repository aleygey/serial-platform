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
/// assertions a stable English baseline. Product code uses
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
    ("conn.handshaking", "◐ handshaking", "◐ 建立会话"),
    ("conn.live", "● live", "● 已连接"),
    ("conn.attaching", "◐ attaching", "◐ 接入中"),
    // ---- Input box ----
    ("ui.input.title.line", " command ", " 命令 "),
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
        " queued commands · Ctrl-] u then ↑/↓, d delete, e edit ",
        " 待发送命令 · Ctrl-] u 后用 ↑/↓ 选择，d 删除，e 编辑 ",
    ),
    (
        "ui.queue.more",
        "… {} more visual row(s) · Ctrl-] u to inspect full commands",
        "… 还有 {} 行未显示 · Ctrl-] u 查看完整命令",
    ),
    ("ui.queue.empty", "<empty command>", "<空命令>"),
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
    ("ui.run.status.active", "running", "执行中"),
    ("ui.run.status.completed", "completed", "已完成"),
    ("ui.run.status.aborted", "aborted", "已中止"),
    ("ui.run.unknown", "unnamed Run", "未命名 Agent 任务"),
    ("ui.run.header", "{} · {}", "{} · {}"),
    (
        "ui.run.description.missing",
        "purpose not provided",
        "未提供命令用途",
    ),
    ("ui.run.command.empty", "<empty TX>", "<空发送内容>"),
    ("ui.monitor.status.running", "monitoring", "监控中"),
    ("ui.monitor.status.completed", "completed", "已完成"),
    ("ui.monitor.status.stopped", "stopped", "已停止"),
    ("ui.monitor.status.failed", "failed", "失败"),
    ("ui.monitor.unnamed", "Monitor", "Monitor 任务"),
    (
        "ui.monitor.header",
        "{} {} {} · {} · {} incident(s)",
        "{} {} {} · {} · {} 个事件",
    ),
    ("ui.monitor.matcher.contains", "contains {}", "包含 {}"),
    ("ui.monitor.matcher.regex", "regex {}", "正则 {}"),
    (
        "st.monitor.jump",
        "Monitor Incident #{}-#{} selected",
        "已定位 Monitor 事件 #{}–#{}",
    ),
    (
        "st.monitor.jump.loading",
        "Loading Monitor Incident evidence #{}-#{} from the journal",
        "正在从日志加载 Monitor 事件证据 #{}–#{}",
    ),
    (
        "st.monitor.jump.journal",
        "Monitor Incident evidence #{}-#{} loaded from the journal",
        "已从日志定位 Monitor 事件证据 #{}–#{}",
    ),
    (
        "st.monitor.jump.gap",
        "Monitor Incident #{}-#{} is unavailable: journal gap #{}-#{} ({})",
        "无法定位 Monitor 事件 #{}–#{}：日志缺失 #{}–#{}（{}）",
    ),
    (
        "st.monitor.jump.incomplete",
        "Monitor Incident evidence #{}-#{} is no longer fully retained",
        "Monitor 事件证据 #{}–#{} 已无法完整读取",
    ),
    (
        "st.monitor.jump.limit",
        "Monitor Incident evidence #{}-#{} exceeds the bounded display query",
        "Monitor 事件证据 #{}–#{} 超出单次显示查询上限",
    ),
    (
        "st.monitor.jump.query.failed",
        "Monitor Incident evidence query failed: {}",
        "Monitor 事件证据查询失败：{}",
    ),
    (
        "st.monitor.jump.query.busy",
        "Another Monitor evidence query is still queued",
        "已有 Monitor 证据查询正在排队",
    ),
    (
        "st.monitor.jump.query.unavailable",
        "Monitor evidence query is unavailable",
        "Monitor 证据查询当前不可用",
    ),
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
        "Profile 正在使用",
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
        "history.startup.failed",
        "Persistent history recovery was incomplete: {}",
        "持久历史回填不完整：{}",
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
        "Port {} · Serial Profile {} · Model Profile {} · Model {}",
        "串口：{} · 串口 Profile：{} · 机型 Profile：{} · 机型名：{}",
    ),
    (
        "menu.value.generic",
        "Generic (no interaction profile)",
        "通用交互设置（不使用机型 Profile）",
    ),
    ("menu.value.unbound", "Unbound", "未绑定"),
    ("menu.value.enabled", "enabled", "启用"),
    ("menu.value.disabled", "disabled", "停用"),
    ("menu.value.on", "on", "开启"),
    ("menu.value.off", "off", "关闭"),
    (
        "menu.root.profile",
        "Edit current port configuration",
        "修改当前串口配置",
    ),
    (
        "menu.root.create",
        "Create configuration Profile",
        "创建配置 Profile",
    ),
    ("menu.root.settings", "Settings", "设置"),
    (
        "menu.root.display",
        "Terminal display settings",
        "终端界面显示设置",
    ),
    ("menu.root.mcp", "serial MCP settings", "serial MCP 设置"),
    ("menu.root.help", "Help", "帮助"),
    ("menu.settings.title", "Settings", "设置"),
    (
        "menu.create.title",
        "Create configuration Profile",
        "创建配置 Profile",
    ),
    (
        "menu.create.transport",
        "Create Serial Profile",
        "创建串口 Profile",
    ),
    (
        "menu.create.model",
        "Create Model Profile",
        "创建机型 Profile",
    ),
    (
        "menu.create.transport.title",
        "Create Serial Profile",
        "创建串口 Profile",
    ),
    (
        "menu.create.model.title",
        "Create Model Profile",
        "创建机型 Profile",
    ),
    (
        "menu.create.row.name",
        "Profile name: {}",
        "Profile 名称：{}",
    ),
    (
        "menu.create.row.model.names",
        "Concrete model names: {}",
        "具体机型名：{}",
    ),
    ("menu.create.row.save", "Create Profile", "创建 Profile"),
    (
        "menu.model.family.title",
        "Choose model family",
        "选择机型系列",
    ),
    (
        "menu.model.name.title",
        "Choose concrete model",
        "选择具体机型名",
    ),
    ("menu.current.section.actions", "Apply", "应用配置"),
    ("menu.choice.open", "options expanded", "选项已展开"),
    ("menu.choice.closed", "options collapsed", "选项已折叠"),
    (
        "menu.choice.empty",
        "no configured option is available",
        "暂无可用的已配置选项",
    ),
    (
        "menu.profile.title",
        "Edit current port configuration",
        "修改当前串口配置",
    ),
    (
        "menu.current.section.serial",
        "Serial port configuration",
        "串口配置",
    ),
    (
        "menu.current.section.model",
        "Device model profile",
        "机型 Profile 配置",
    ),
    ("menu.current.row.port", "Port: {}", "端口：{}"),
    (
        "menu.current.row.transport",
        "Serial Profile: {}",
        "串口 Profile：{}",
    ),
    ("menu.current.row.baud", "Baud rate: {}", "波特率：{}"),
    ("menu.current.row.data", "Data bits: {}", "数据位：{}"),
    ("menu.current.row.parity", "Parity: {}", "校验位：{}"),
    ("menu.current.row.stop", "Stop bits: {}", "停止位：{}"),
    ("menu.current.row.flow", "Flow control: {}", "流控：{}"),
    ("menu.current.row.dtr", "DTR: {}", "DTR：{}"),
    ("menu.current.row.rts", "RTS: {}", "RTS：{}"),
    ("menu.current.row.auto", "Auto-open: {}", "自动打开串口：{}"),
    (
        "menu.current.row.device",
        "Model Profile: {}",
        "机型 Profile：{}",
    ),
    (
        "menu.current.row.model.name",
        "Model name: {}",
        "机型名：{}",
    ),
    (
        "menu.current.row.eol",
        "Write line ending: {}",
        "发送换行符：{}",
    ),
    (
        "menu.current.row.echo",
        "Device echo policy (Agent capture parsing only): {}",
        "设备回显策略（仅影响 Agent 捕获解析）：{}",
    ),
    (
        "menu.current.row.shell",
        "Shell prompt: {}",
        "Shell 提示符：{}",
    ),
    (
        "menu.current.row.uboot",
        "U-Boot prompt: {}",
        "U-Boot 提示符：{}",
    ),
    (
        "menu.current.row.chunk",
        "Write chunk size: {} bytes",
        "分段发送大小：{} 字节",
    ),
    (
        "menu.current.row.delay",
        "Inter-chunk delay: {} ms",
        "分段发送间隔：{} 毫秒",
    ),
    (
        "menu.current.row.apply.changed",
        "Save and apply configuration changes *",
        "保存并应用配置修改 *",
    ),
    (
        "menu.current.row.apply.clean",
        "Save and apply configuration changes",
        "保存并应用配置修改",
    ),
    (
        "menu.current.modified",
        "Draft changed; select Save and apply to submit it",
        "草稿已修改；请选择“保存并应用配置修改”提交",
    ),
    (
        "menu.current.no.changes",
        "There are no configuration changes to save",
        "当前没有需要保存的配置修改",
    ),
    (
        "menu.profile.shared.title",
        "Confirm shared profile update",
        "确认修改共享 Profile",
    ),
    (
        "menu.profile.shared.warning",
        "Warning: this changes every port listed below, not only the currently selected serial port.",
        "注意：本次修改会影响下方列出的每个串口通道，不只影响当前选中的串口。",
    ),
    (
        "menu.profile.shared.transport",
        "Shared Serial Profile “{}” — {} affected port(s):",
        "共享串口 Profile“{}”——影响 {} 个串口：",
    ),
    (
        "menu.profile.shared.device",
        "Shared Model Profile “{}” — {} affected port(s):",
        "共享机型 Profile“{}”——影响 {} 个串口：",
    ),
    ("menu.profile.shared.port", "  • {} — {}", "  • {} — {}"),
    (
        "menu.profile.shared.revision",
        "A binding or catalog change before submission is rejected by the revision guard; reload and review the new impact list.",
        "若提交前 Profile 绑定或目录版本发生变化，版本保护会拒绝写入；请重新加载并核对新的影响列表。",
    ),
    (
        "menu.profile.shared.pending",
        "Review every affected port, then explicitly confirm or cancel",
        "请逐一核对所有受影响通道，再明确确认或取消",
    ),
    (
        "menu.profile.shared.footer",
        "Enter/Y confirm · Esc/N cancel · ↑/↓/PgUp/PgDn scroll",
        "Enter/Y 确认 · Esc/N 取消 · ↑/↓/PgUp/PgDn 滚动",
    ),
    (
        "menu.profile.shared.cancelled",
        "Shared profile update cancelled; the draft remains on this page",
        "已取消共享 Profile 修改；草稿仍保留在本页",
    ),
    (
        "menu.current.transport.missing",
        "The bound UART profile is missing from the catalog; bind an existing profile first",
        "当前串口 Profile 已不在列表中；请先绑定一个现有 Profile",
    ),
    (
        "menu.current.device.unbound",
        "Bind a Model Profile before editing its fields",
        "请先绑定机型 Profile，再修改其字段",
    ),
    (
        "menu.current.value.invalid",
        "Invalid value; port must be non-empty, baud/chunk size must be positive integers, and delay must be a non-negative integer",
        "输入无效：端口不能为空，波特率和分段大小必须为正整数，分段间隔必须为非负整数",
    ),
    (
        "menu.current.prompt.shell",
        "Shell prompt (empty clears)",
        "Shell 提示符（留空即清除）",
    ),
    (
        "menu.current.prompt.uboot",
        "U-Boot prompt (empty clears)",
        "U-Boot 提示符（留空即清除）",
    ),
    (
        "menu.current.prompt.chunk",
        "Write chunk bytes (empty inherits)",
        "分段发送字节数（留空即继承）",
    ),
    (
        "menu.current.prompt.delay",
        "Inter-chunk delay ms (empty inherits)",
        "分段发送间隔毫秒（留空即继承）",
    ),
    (
        "menu.profile.updated",
        "Current Profiles saved and applied",
        "当前 Profile 已保存并应用",
    ),
    (
        "menu.profile.revision.conflict",
        "configuration changed while this form was open; reload and review before saving",
        "本页打开后配置已发生变化；请重新加载并核对后再保存",
    ),
    (
        "menu.profile.binding.changed",
        "the port profile binding changed while this form was open; reload before saving",
        "本页打开后串口的 Profile 绑定已变化；请重新加载后再保存",
    ),
    (
        "menu.transport.created",
        "Serial Profile {} created",
        "串口 Profile {} 已创建",
    ),
    (
        "menu.transport.missing",
        "Serial Profile {} no longer exists",
        "串口 Profile {} 已不存在",
    ),
    (
        "menu.device.created",
        "Model Profile {} created",
        "机型 Profile {} 已创建",
    ),
    (
        "menu.device.missing",
        "Model Profile {} no longer exists",
        "机型 Profile {} 已不存在",
    ),
    (
        "menu.profile.exists",
        "profile {} already exists; choose another name",
        "Profile {} 已存在；请选择其他名称",
    ),
    (
        "menu.display.title",
        "Terminal display settings",
        "终端界面显示设置",
    ),
    (
        "menu.display.history.rows",
        "Agent task/command-history height: {} rows",
        "Agent 任务与命令记录栏高度：{} 行",
    ),
    (
        "menu.display.history.prompt",
        "History content rows ({}–{})",
        "命令记录栏内容行数（{}–{}）",
    ),
    (
        "menu.display.history.invalid",
        "Enter a whole number from {} through {}",
        "请输入 {} 到 {} 之间的整数",
    ),
    (
        "menu.display.saved",
        "Agent task/command-history height saved as {} rows",
        "Agent 任务与命令记录栏高度已保存为 {} 行",
    ),
    (
        "menu.display.saved.session",
        "Agent task/command-history height set to {} rows for this session",
        "Agent 任务与命令记录栏高度已在本次会话中设为 {} 行",
    ),
    (
        "menu.display.save.failed",
        "height applied for this session, but saving failed: {}",
        "高度已在本次会话中生效，但保存失败：{}",
    ),
    ("menu.mcp.title", "serial MCP settings", "serial MCP 设置"),
    (
        "menu.run.timeout.row",
        "Orphan Run cleanup: {}",
        "无人继续使用的任务回收：{}",
    ),
    ("menu.run.timeout.seconds", "{} seconds", "{} 秒"),
    ("menu.run.timeout.unlimited", "unlimited", "无限"),
    (
        "menu.run.timeout.prompt",
        "Orphan Run timeout: 0 = unlimited, otherwise >= {} seconds",
        "无人继续使用的任务回收时间：0 表示无限，否则不少于 {} 秒",
    ),
    (
        "menu.run.timeout.invalid",
        "enter 0 for unlimited or an integer of at least {} seconds",
        "请输入 0（无限），或不少于 {} 的整数秒数",
    ),
    (
        "menu.run.timeout.saved",
        "Orphan Run cleanup saved as {} and automatically applied",
        "Agent 任务回收设置已保存为 {}，并已自动应用",
    ),
    (
        "menu.run.timeout.saved.session",
        "Orphan Run cleanup set to {} for this console session only",
        "Agent 任务回收设置仅在本次终端会话中设为 {}",
    ),
    (
        "menu.run.timeout.save.failed",
        "timeout was selected for this console session, but saving failed: {}",
        "本次终端会话已选择该回收时间，但保存失败：{}",
    ),
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
    ("menu.detail.eol.none", "none", "无"),
    ("menu.detail.eol.inherit", "inherit", "继承"),
    ("menu.detail.echo.on", "device echoes", "设备会回显"),
    ("menu.detail.echo.off", "device does not echo", "设备不回显"),
    ("menu.detail.echo.auto", "auto-detect", "自动判断"),
    (
        "menu.help.title",
        "Terminal workflow help",
        "终端工作流帮助",
    ),
    (
        "menu.footer",
        "↑/↓ select · → expand · ← collapse/back · Enter confirm · ? help",
        "↑/↓ 选择 · → 展开 · ← 折叠/返回 · Enter 确认 · ? 说明",
    ),
    (
        "menu.footer.help",
        "PgUp/PgDn scroll · Esc returns to the menu",
        "PgUp/PgDn 滚动 · Esc 返回菜单",
    ),
    (
        "menu.prompt.transport.name",
        "New Serial Profile name",
        "新串口 Profile 名称",
    ),
    (
        "menu.prompt.device.name",
        "New Model Profile name",
        "新机型 Profile 名称",
    ),
    (
        "menu.prompt.model.names",
        "Concrete model names, separated by commas",
        "具体机型名（使用逗号分隔）",
    ),
    ("menu.field.help.title", "Field description", "配置项说明"),
    ("menu.field.help.close", "any key closes", "按任意键关闭"),
    (
        "menu.help.field.port",
        "Select a detected or already configured operating-system serial port.",
        "选择系统检测到或已经配置的串口。",
    ),
    (
        "menu.help.field.transport",
        "Select an existing Serial Profile. New Profiles are created from the separate Create configuration Profile menu.",
        "选择已经创建的串口 Profile；新增 Profile 请使用独立的“创建配置 Profile”。",
    ),
    (
        "menu.help.field.model.profile",
        "A Model Profile describes one model family's prompts, line ending and write pacing.",
        "机型 Profile 描述一类机型共用的提示符、换行和分段发送参数。",
    ),
    (
        "menu.help.field.model.name",
        "Choose the family first, then the concrete model name attached to this port.",
        "先选择机型系列，再选择当前串口连接的具体机型名。",
    ),
    (
        "menu.help.field.shell",
        "The Shell prompt marks the end of a completed Shell command response.",
        "Shell 提示符用于识别 Shell 命令返回结束位置。",
    ),
    (
        "menu.help.field.uboot",
        "The U-Boot prompt marks the end of a completed U-Boot command response.",
        "U-Boot 提示符用于识别 U-Boot 命令返回结束位置。",
    ),
    (
        "menu.help.field.pacing",
        "Write pacing splits one command into bounded chunks with a delay between chunks.",
        "分段发送会把命令按指定字节数拆分，并在相邻分段之间等待。",
    ),
    (
        "menu.help.field.apply",
        "Save the draft and apply it to the selected serial port.",
        "保存当前草稿并应用到所选串口。",
    ),
    (
        "menu.help.field.serial",
        "Press Right or Enter to expand this field's available values.",
        "按右方向键或 Enter 展开当前配置项的可选值。",
    ),
    (
        "menu.help.field.create",
        "Create a reusable Serial Profile or Model Profile without changing the current port binding.",
        "创建可复用的串口 Profile 或机型 Profile，不会自动修改当前串口绑定。",
    ),
    (
        "menu.help.field.display",
        "Sets the number of content rows in the Agent task and command history pane.",
        "设置 Agent 任务与命令记录栏显示的内容行数。",
    ),
    (
        "menu.help.field.mcp",
        "Sets how long an abandoned Agent Run remains active. Zero means unlimited.",
        "设置无人继续使用的 Agent Run 自动回收时间；0 表示无限。",
    ),
    (
        "menu.help.field.navigation",
        "Use Up and Down to select, Right to enter, and Left to return.",
        "按 ↑/↓ 选择、→ 进入、← 返回。",
    ),
    ("menu.prompt.cancelled", "input cancelled", "已取消输入"),
    (
        "menu.name.invalid",
        "name must be non-empty, trimmed, control-free, and at most 128 bytes",
        "名称必须非空、无首尾空白和控制字符，且不超过 128 字节",
    ),
    (
        "menu.port.missing",
        "Port {} no longer exists",
        "串口 {} 已不存在",
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
    // ---- Durable serial-output search ----
    (
        "ui.output.search.title",
        " persistent serial history search ",
        " 持久串口历史搜索 ",
    ),
    ("ui.output.search.query", " Search text ", " 搜索内容 "),
    ("ui.output.search.matcher.literal", "text", "普通文本"),
    ("ui.output.search.matcher.regex", "regex", "正则表达式"),
    (
        "ui.output.search.case.sensitive",
        "case-sensitive",
        "区分大小写",
    ),
    (
        "ui.output.search.case.insensitive",
        "ignore case",
        "忽略大小写",
    ),
    ("ui.output.search.direction.both", "RX + TX", "接收 + 发送"),
    ("ui.output.search.direction.rx", "RX only", "仅接收"),
    ("ui.output.search.direction.tx", "TX only", "仅发送"),
    (
        "ui.output.search.scope.epoch",
        "current seriald run",
        "当前 seriald 运行期",
    ),
    (
        "ui.output.search.scope.retained",
        "all retained history",
        "全部保留历史",
    ),
    (
        "ui.output.search.scope.run",
        "current Agent task",
        "当前 Agent 任务",
    ),
    (
        "ui.output.search.filters",
        "Match: {} · Case: {} · Direction: {} · Scope: {}",
        "匹配：{} · 大小写：{} · 方向：{} · 范围：{}",
    ),
    (
        "ui.output.search.target.epoch",
        "Query target: epoch {} through #{} (refreshed on submit)",
        "查询目标：epoch {} 至 #{}（提交时刷新）",
    ),
    (
        "ui.output.search.target.run",
        "Query target: epoch {} through #{} · Run {} (refreshed on submit)",
        "查询目标：epoch {} 至 #{} · Run {}（提交时刷新）",
    ),
    (
        "ui.output.search.target.retained",
        "Query target: retained archive catalog is refreshed on submit",
        "查询目标：提交时重新读取保留归档目录",
    ),
    (
        "ui.output.search.filter.keys",
        "F2/Tab match · F3 case · F4 direction · F5 scope",
        "F2/Tab 匹配方式 · F3 大小写 · F4 方向 · F5 范围",
    ),
    (
        "ui.output.search.loading",
        "Searching the durable journal… The terminal remains live.",
        "正在查询持久日志……主串口连接仍保持运行。",
    ),
    (
        "ui.output.search.boundary.note",
        "Results stay separate from live output. A match may span adjacent journal events, so the main pane never shows a misleading exact highlight.",
        "结果会在独立列表中显示，不会混入实时串口画面。匹配可能跨越相邻日志事件，因此主画面不会显示容易误解的精确高亮。",
    ),
    (
        "ui.output.search.edit.footer",
        "Enter searches · Esc closes",
        "Enter 开始搜索 · Esc 关闭",
    ),
    ("ui.output.search.result.query", "Search: {}", "搜索：{}"),
    (
        "ui.output.search.row",
        "{} · {} · #{} · {} · {}",
        "{} · {} · #{} · {} · {}",
    ),
    (
        "ui.output.search.empty.event",
        "<empty event payload>",
        "<事件内容为空>",
    ),
    (
        "ui.output.search.none",
        "No matching RX/TX event was found in this bounded scope.",
        "在本次有界查询范围内没有找到匹配的收发记录。",
    ),
    (
        "ui.output.search.no.detail",
        "No result selected.",
        "没有可查看的结果。",
    ),
    ("ui.output.search.detail", " selected record ", " 所选记录 "),
    (
        "ui.output.search.integrity.complete",
        "Bounded query complete",
        "本次有界查询完整",
    ),
    (
        "ui.output.search.integrity.partial",
        "⚠ PARTIAL",
        "⚠ 结果不完整",
    ),
    (
        "ui.output.search.position",
        "{}/{} · {} archives · ↑↓/nN select · PgUp/PgDn detail · / edit",
        "第 {}/{} 条 · {} 个归档 · ↑↓/nN 选择 · PgUp/PgDn 详情 · / 修改",
    ),
    (
        "ui.output.search.partial",
        " · PARTIAL: newest-first only within scanned data (max 4 archives, a 10,000-sequence window/archive, 8 queries, 200 results)",
        " · 结果不完整：仅在已扫描部分内按新到旧排列（最多 4 个归档、每个归档最近 10,000 个序号范围、8 次查询、显示 200 条）",
    ),
    (
        "ui.output.search.gaps",
        " · {} journal gap(s)",
        " · {} 处日志缺口",
    ),
    (
        "ui.output.search.completion.block",
        "A cross-block match points to the block that completed it; this row may not contain the whole expression.",
        "如果匹配跨越多个串口块，列表显示完成匹配的那个块；这一行不一定包含完整表达式。",
    ),
    (
        "ui.output.search.empty",
        "Enter a non-empty search expression.",
        "请输入搜索内容。",
    ),
    (
        "ui.output.search.too.long",
        "The encoded matcher exceeds the {}-byte journal limit.",
        "编码后的匹配表达式超过持久日志的 {} 字节上限。",
    ),
    (
        "ui.output.search.no.run",
        "This port has no active Agent task to search.",
        "当前串口没有可搜索的 Agent 任务。",
    ),
    (
        "ui.output.search.port.missing",
        "This serial channel no longer exists.",
        "当前串口通道已不存在。",
    ),
    (
        "ui.output.search.unavailable",
        "The history-search worker is unavailable.",
        "串口历史搜索服务当前不可用。",
    ),
    (
        "ui.output.search.busy",
        "Another history query is still queued; wait and retry.",
        "已有历史查询正在排队，请稍后重试。",
    ),
    (
        "ui.output.search.failed",
        "History search failed: {}",
        "串口历史搜索失败：{}",
    ),
    (
        "ui.output.search.timeout",
        "history search exceeded the {}-second total deadline",
        "串口历史搜索超过 {} 秒总时限",
    ),
    (
        "ui.output.model.unconfigured",
        "device model not configured",
        "未配置样机机型",
    ),
    (
        "ui.separator.agent",
        "Agent task and commands",
        "Agent 任务与命令记录",
    ),
    (
        "ui.separator.agent.recent",
        "Agent task and commands · recent records",
        "Agent 任务与命令记录 · 最近记录",
    ),
    ("ui.separator.input", "Human input", "用户输入"),
    // ---- Bottom help line ----
    (
        "ui.helpline",
        " Ctrl-] m menu · o profiles · h command purposes · ? help · {} · q quit ",
        " Ctrl-] m 菜单 · o Profile · h 命令用途 · ? 帮助 · {} · q 退出 ",
    ),
    (
        "ui.scroll.plain",
        "wheel/PgUp/PgDn browse Agent history",
        "滚轮/PgUp/PgDn 查看 Agent 历史",
    ),
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
    ("help.group.line", "LINE mode", "LINE 模式"),
    (
        "help.key.switch",
        "Alt-1..9 / Ctrl-] 1..9",
        "Alt-1..9 / Ctrl-] 1..9",
    ),
    ("help.desc.switch", "Switch serial port", "切换串口"),
    ("help.key.history.select", "Up / Down", "上 / 下"),
    (
        "help.desc.history.select",
        "Select an Agent action or child command",
        "选择 Agent 动作或子命令",
    ),
    ("help.key.history.expand", "Right / Left", "右 / 左"),
    (
        "help.desc.history.expand",
        "Expand or return one history level",
        "展开或返回上一层记录",
    ),
    ("help.key.history.panel", "Ctrl-] h", "Ctrl-] h"),
    (
        "help.desc.history.panel",
        "Show or hide Agent history",
        "显示或隐藏 Agent 历史",
    ),
    (
        "help.key.scroll",
        "Wheel / PgUp / PgDn",
        "滚轮 / PgUp / PgDn",
    ),
    (
        "help.desc.scroll",
        "Browse the current Agent-history level",
        "浏览当前 Agent 历史层级",
    ),
    ("help.key.follow", "Ctrl-] f", "Ctrl-] f"),
    (
        "help.desc.follow",
        "Return to live serial output",
        "回到最新串口输出",
    ),
    ("help.key.menu", "Ctrl-] m", "Ctrl-] m"),
    ("help.desc.menu", "Open configuration", "打开配置"),
    ("help.key.profile", "Ctrl-] o", "Ctrl-] o"),
    (
        "help.desc.profile",
        "Edit current serial configuration",
        "修改当前串口配置",
    ),
    ("help.key.search.output", "Ctrl-] /", "Ctrl-] /"),
    (
        "help.desc.search.output",
        "Search persistent serial history",
        "搜索持久串口历史",
    ),
    ("help.key.enter", "Enter", "Enter"),
    (
        "help.desc.enter",
        "Send non-empty input; empty input follows the live tail",
        "发送非空命令；空输入回到串口底部",
    ),
    ("help.key.alt.enter", "Alt-Enter", "Alt-Enter"),
    (
        "help.desc.alt.enter",
        "Send cooperative input during an Agent Run",
        "Agent 任务中协同发送输入",
    ),
    ("help.key.input.search", "Ctrl-R", "Ctrl-R"),
    (
        "help.desc.input.search",
        "Search input history",
        "搜索输入历史",
    ),
    ("help.key.complete", "Tab", "Tab"),
    (
        "help.desc.complete",
        "Complete from input history",
        "从输入历史补全",
    ),
    ("help.key.paste", "Ctrl-Shift-V", "Ctrl-Shift-V"),
    (
        "help.desc.paste",
        "Paste into the command input",
        "粘贴到命令输入栏",
    ),
    ("help.key.takeover", "Ctrl-] t", "Ctrl-] t"),
    (
        "help.desc.takeover",
        "Take over the current serial port",
        "接管当前串口",
    ),
    ("help.key.release", "Ctrl-] c", "Ctrl-] c"),
    (
        "help.desc.release",
        "Release control or queued input",
        "释放控制权或排队输入",
    ),
    ("help.key.mode", "Ctrl-] l / r", "Ctrl-] l / r"),
    (
        "help.desc.mode",
        "Switch LINE or RAW input",
        "切换 LINE 或 RAW 输入",
    ),
    ("help.key.interrupt", "Ctrl-C", "Ctrl-C"),
    ("help.desc.interrupt", "Send ETX (0x03)", "发送 ETX（0x03）"),
    ("help.key.lang", "Ctrl-] g", "Ctrl-] g"),
    (
        "help.desc.lang",
        "Switch Chinese or English",
        "切换中文或英文",
    ),
    ("help.key.quit", "Ctrl-] q", "Ctrl-] q"),
    ("help.desc.quit", "Quit serialctl", "退出 serialctl"),
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
        "transport connected; establishing the session and attaching all ports",
        "已连接服务器，正在建立会话并接入所有串口",
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
        "connected (protocol v{})",
        "已连接（协议 v{}）",
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
    ("st.session.ready", "connection ready", "连接就绪"),
    ("st.watching", "watching {} port(s)", "正在监视 {} 个串口"),
    ("st.detached", "detached {} port(s)", "已停止监视 {} 个串口"),
    ("st.run.started", "run started: {}", "Agent 任务已开始：{}"),
    ("st.run.ended", "run ended: {}", "Agent 任务已结束：{}"),
    (
        "st.checkpoint",
        "checkpoint created at sequence {}",
        "已在序列 {} 创建检查点",
    ),
    (
        "st.not.ready.queued",
        "connection handshake is incomplete; input was not queued",
        "连接握手尚未完成；输入未加入队列",
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
        "st.not.ready",
        "connection handshake is incomplete; input was not queued",
        "连接握手尚未完成；输入未加入队列",
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
        "st.not.ready.live",
        "the selected port is not attached and live; control was not requested",
        "所选串口尚未接入实时会话；未请求控制权",
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
        "st.port.not.live",
        "the selected port is not live; control was not released",
        "所选串口尚未进入实时模式；未释放控制权",
    ),
    (
        "st.cancel.reason",
        "operator cancelled queued input",
        "操作员取消了排队输入",
    ),
    (
        "st.no.control",
        "this port has no active write control",
        "当前串口没有人持有控制权",
    ),
    (
        "st.control.belongs",
        "write control belongs to {}",
        "控制权由 {} 持有",
    ),
    (
        "st.reconnect.reason",
        "{} for {}; reconnecting cancels this actor's queues and releases its controls on every port",
        "{}（{}）；重新连接会取消当前操作方的队列，并释放其在所有串口上的控制权",
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
        "快捷键前缀：1-9 串口，m 菜单，o Profile，h 命令用途，l/r 模式，PgUp/PgDn 滚动，t 接管，u 队列，c 释放/取消，? 帮助",
    ),
    (
        "st.line.mode",
        "LINE mode: Enter sends the line plus the interaction-profile line ending",
        "LINE 模式：回车发送该行，并附加机型 Profile 的换行符",
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
        "st.output.search.open",
        "persistent serial history search opened",
        "已打开持久串口历史搜索",
    ),
    (
        "st.output.search.closed",
        "serial history search closed",
        "已关闭串口历史搜索",
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
        "已打开 Profile 选择",
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
        "command-history bar focused; Up/Down selects, Enter expands and locates output, Ctrl-] h hides",
        "已聚焦命令记录横栏；上下选择，Enter 展开并定位串口输出，Ctrl-] h 隐藏",
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
        "st.run.jump",
        "serial output positioned at command sequence #{}",
        "串口输出已定位到命令序号 #{}",
    ),
    (
        "st.run.jump.overlay",
        "command sequence #{} has no verifiable output boundary; showing the command only",
        "命令序号 #{} 没有可验证的输出边界，仅显示命令本身",
    ),
    (
        "st.run.jump.loading",
        "loading exact command evidence from the journal at sequence #{}",
        "正在从日志加载命令序号 #{} 的完整证据",
    ),
    (
        "st.run.jump.journal",
        "exact command evidence loaded from the journal at sequence #{}",
        "已从日志定位命令序号 #{} 的完整证据",
    ),
    (
        "st.run.jump.gap",
        "command sequence #{} is unavailable: journal gap #{}-#{} ({})",
        "无法定位命令序号 #{}：日志缺失 #{}–#{}（{}）",
    ),
    (
        "st.run.jump.incomplete",
        "command evidence #{}-#{} is no longer fully retained or has no verified boundary",
        "命令证据 #{}–#{} 已无法完整读取或没有可验证的结束边界",
    ),
    (
        "st.run.jump.limit",
        "command evidence at sequence #{} exceeds the bounded display query",
        "命令序号 #{} 的证据超出单次显示查询上限",
    ),
    (
        "st.run.jump.query.failed",
        "command evidence query failed: {}",
        "命令证据查询失败：{}",
    ),
    (
        "st.run.jump.query.busy",
        "another exact evidence query is still queued",
        "已有完整证据查询正在排队",
    ),
    (
        "st.run.jump.query.unavailable",
        "exact command evidence query is unavailable",
        "命令完整证据查询当前不可用",
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
        "multi-line/large paste blocked; Ctrl-] p confirms for the original port",
        "多行或大段粘贴正在等待确认；按 Ctrl-] p 发送到原串口",
    ),
    (
        "st.paste.none",
        "no blocked paste to confirm",
        "没有待确认的粘贴",
    ),
    (
        "st.paste.gone",
        "the paste target port no longer exists",
        "粘贴目标串口已不存在",
    ),
    (
        "st.paste.queued",
        "confirmed paste queued for {}",
        "已将确认后的粘贴加入 {} 的发送队列",
    ),
    (
        "st.no.port",
        "no port is configured; run `serialctl setup`",
        "尚未配置串口；请运行 `serialctl setup`",
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
        "d.ev.port_reconfigured",
        "port_reconfigured",
        "串口配置已更新",
    ),
    ("d.ev.port_removed", "port_removed", "串口已移除"),
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
        "--initial-port applies only to the interactive `serialctl` console",
        "--initial-port 仅适用于交互式 `serialctl` 控制台",
    ),
    (
        "m.status.header",
        "seriald {}  epoch {}  {} port(s)",
        "seriald {}  服务实例 {}  {} 个串口",
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
    ("m.doctor.ports", "ports", "串口"),
    (
        "m.doctor.ports.value",
        "{} total, {} online",
        "共 {} 个，{} 个在线",
    ),
    // ---- doctor.rs human-readable diagnostics ----
    ("doctor.field.source", "Source", "数据来源"),
    ("doctor.field.configured_port", "Port", "串口"),
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
        "doctor.field.degraded_ports",
        "Degraded ports",
        "日志降级串口",
    ),
    ("doctor.field.catalog", "Archive catalog", "归档目录"),
    ("doctor.field.note", "Note", "提示"),
    ("doctor.field.stream", "Stream", "数据流"),
    ("doctor.field.control", "Control", "控制权"),
    ("doctor.field.run", "Agent Run", "Agent 任务"),
    ("doctor.field.trigger", "Trigger", "触发任务"),
    ("doctor.field.profiles", "Profiles", "Profile"),
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
    ("doctor.value.port", "{} ({})", "{}（{}）"),
    (
        "doctor.value.stream",
        "head={} rx={} tx={} overflow={} bytes",
        "最新序号={}，接收={}，发送={}，溢出={} 字节",
    ),
    ("doctor.value.run", "{} · {} · {}", "{} · {} · {}"),
    ("doctor.value.trigger", "{} · {}", "{} · {}"),
    (
        "doctor.value.profiles",
        "Serial Profile={} · Model Profile={}",
        "串口 Profile={} · 机型 Profile={}",
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
        "doctor.source.port.snapshot",
        "authoritative port snapshot",
        "串口权威快照",
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
        "doctor.source.port.diagnostics",
        "authoritative port diagnostics",
        "串口权威诊断数据",
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
        "doctor.assessment.port_disabled",
        "the port is disabled",
        "串口已禁用",
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
        "doctor.error.unknown_port",
        "unknown port `{}`",
        "串口 `{}` 不存在",
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
        "i.unreachable",
        "cannot reach seriald; start seriald on Windows and verify the host-only endpoint",
        "无法连接 seriald；请在 Windows 上启动 seriald，并检查仅本机可访问的服务地址",
    ),
    (
        "i.status.fail",
        "cannot read the existing port configuration",
        "无法读取现有串口配置",
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
        "Select the serial ports to configure (comma-separated numbers)",
        "选择要配置的串口（使用逗号分隔编号）",
    ),
    (
        "i.omitted.header",
        "\nExisting ports not selected in this scan:",
        "\n本次扫描未选择的已有串口：",
    ),
    (
        "i.omitted.delete",
        "Explicitly delete these omitted ports from seriald configuration?",
        "是否从 seriald 配置中删除这些未选择的串口？",
    ),
    (
        "i.configured",
        "\nConfigured {} port(s):",
        "\n已配置 {} 个串口：",
    ),
    (
        "i.saved",
        "Saved serialctl configuration to {}.",
        "serialctl 配置已保存到 {}。",
    ),
    (
        "i.open.console",
        "Run `serialctl` to open the multi-port console.",
        "运行 `serialctl` 打开多串口控制台。",
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
        "enter `y` to delete the omitted ports or `n` to keep them",
        "输入 `y` 删除未选择的串口，输入 `n` 保留",
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
        assert_eq!(tr("state.online"), "ONLINE");
        set_lang(Lang::Zh);
        assert_eq!(tr("state.online"), "在线");
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
            trf("st.live", &["COM4", "42"]),
            "COM4 已进入实时模式，最新序号为 42"
        );
        set_lang(Lang::En);
        assert_eq!(trf("st.live", &["COM4", "42"]), "COM4 live at sequence 42");
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
    fn main_menu_chinese_names_match_the_current_configuration_flow() {
        let _guard = lang_test_lock();
        set_lang(Lang::Zh);
        assert_eq!(tr("menu.root.profile"), "修改当前串口配置");
        assert_eq!(tr("menu.root.create"), "创建配置 Profile");
        assert_eq!(tr("menu.root.settings"), "设置");
        assert_eq!(tr("menu.root.help"), "帮助");
        assert!(tr("menu.current").contains("串口："));
        assert!(tr("menu.current").contains("串口 Profile"));
        assert!(tr("menu.current").contains("机型 Profile"));
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
