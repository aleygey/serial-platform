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
    #[default]
    En,
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
    LANG.get_or_init(|| RwLock::new(Lang::En))
}

pub fn lang() -> Lang {
    *lang_cell().read().expect("language lock poisoned")
}

pub fn set_lang(lang: Lang) {
    *lang_cell().write().expect("language lock poisoned") = lang;
}

/// Serializes tests that depend on the process-global language and resets
/// the language to English for the duration of the guard.
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
    ("phase.attach", "ATTACH", "附着"),
    ("phase.replay", "REPLAY#{}-#{}", "回放#{}-#{}"),
    ("phase.live", "LIVE#{}", "实时#{}"),
    ("phase.lagged", "LAGGED#{}-#{}", "滞后#{}-#{}"),
    // ---- Session state / target activity (tab bar) ----
    ("state.disabled", "DISABLED", "已禁用"),
    ("state.waiting", "WAITING", "等待端口"),
    ("state.opening", "OPENING", "打开中"),
    ("state.online", "ONLINE", "在线"),
    ("state.backoff", "BACKOFF", "退避"),
    ("state.stopping", "STOPPING", "停止中"),
    ("activity.active", "ACTIVE", "活跃"),
    ("activity.silent", "SILENT", "静默"),
    ("activity.unknown", "UNKNOWN", "未知"),
    // ---- Connection summary (tab bar title) ----
    ("conn.reconnecting", "○ reconnecting", "○ 重连中"),
    ("conn.authenticating", "◐ authenticating", "◐ 认证中"),
    ("conn.live", "● live", "● 实时"),
    ("conn.attaching", "◐ attaching", "◐ 附着中"),
    // ---- Status bar ----
    ("ui.control.none", "none", "无"),
    ("ui.prefix", " · PREFIX", " · 前缀"),
    (
        "ui.uncertain",
        " · {} WRITE OUTCOME(S) UNCERTAIN: inspect TX before retrying",
        " · {} 个写入结果不确定: 重试前请检查 TX",
    ),
    (
        "ui.queued",
        " · QUEUED #{} ({}s, {} chunk(s); d/e edits LINE, c cancels)",
        " · 排队中 #{}({} 秒, {} 块; d/e 修改 LINE, c 取消)",
    ),
    (
        "ui.control.pending",
        " · CONTROL REQUEST PENDING (Ctrl-] c cancels)",
        " · 控制请求待处理(Ctrl-] c 取消)",
    ),
    (
        "ui.idle.release",
        " · idle release in {}s",
        " · {} 秒后空闲释放",
    ),
    (
        "ui.trigger",
        " · trigger {} {} · {} fire(s)",
        " · 触发任务 {} {} · 已发送 {} 次",
    ),
    ("ui.status.control", "control:", "控制:"),
    ("ui.paused", " · PAUSED", " · 已暂停"),
    // ---- Input box ----
    (
        "ui.input.title.line",
        " command · Enter sends Profile EOL ",
        " 命令 · 回车发送并附加 Profile EOL ",
    ),
    (
        "ui.input.title.line.queued",
        " command · QUEUED {} · {} · Ctrl-] d/e/c/u delete/edit/cancel/select ",
        " 命令 · 已排队 {} 条 · {} · Ctrl-] d/e/c/u 删除/编辑/取消/选择 ",
    ),
    (
        "ui.input.raw.text",
        "Keystrokes are sent directly. Ctrl-C sends ETX; Ctrl-] opens local commands.",
        "按键直接发送。Ctrl-C 发送 ETX;Ctrl-] 打开本地命令。",
    ),
    ("ui.input.title.raw", " RAW direct transport ", " RAW 直传 "),
    (
        "ui.input.title.raw.queued",
        " RAW direct transport · QUEUED {} byte(s) · Ctrl-] c cancels ",
        " RAW 直传 · 已排队 {} 字节 · Ctrl-] c 取消 ",
    ),
    ("ui.input.queued.raw", "{} raw byte(s)", "{} 个原始字节"),
    (
        "ui.input.agent",
        "Agent is using this serial port · current task: <{}>",
        "Agent 正在使用，当前正在执行 <{}>",
    ),
    (
        "ui.queue.title",
        " queued commands · Ctrl-] u then ↑/↓ cards, PgUp/PgDn text, d delete, e edit ",
        " 排队命令 · Ctrl-] u 后用 ↑/↓ 选卡片，PgUp/PgDn 看全文，d 删除，e 编辑 ",
    ),
    (
        "ui.queue.more",
        "… {} more visual row(s) · Ctrl-] u to inspect full commands",
        "… 另有 {} 个视觉行 · Ctrl-] u 查看命令全文",
    ),
    (
        "ui.queue.page",
        "{} · text rows {}-{}/{} · PgUp/PgDn",
        "{} · 正文行 {}-{}/{} · PgUp/PgDn",
    ),
    ("ui.queue.empty", "<empty command>", "<空命令>"),
    (
        "ui.queue.sending",
        " · SENDING (locked)",
        " · 发送中（已锁定）",
    ),
    // ---- Extensible configuration menu ----
    ("menu.title", "Serial settings menu", "串口配置总菜单"),
    ("menu.loading", "loading configuration…", "正在加载配置…"),
    (
        "menu.loaded",
        "configuration catalog loaded",
        "配置目录已加载",
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
        "目录尚未加载；按 r 重试",
    ),
    (
        "menu.current",
        "Slot {} · Transport {} · Device {} · Model {}",
        "槽位 {} · 传输 {} · 设备 {} · 机型 {}",
    ),
    ("menu.root.profile", "Profile", "Profile 配置"),
    ("menu.root.model", "DUT model", "样机机型"),
    ("menu.root.serial", "Serial port settings", "串口设置"),
    ("menu.root.help", "Help", "帮助"),
    (
        "menu.root.detail",
        "Enter opens a submenu. Reads are asynchronous; every administrator mutation asks for a one-time in-memory token.",
        "Enter 进入子菜单。读取异步执行；每次管理员写入都会索取仅驻留内存的一次性令牌。",
    ),
    ("menu.profile.title", "Profiles", "Profile"),
    (
        "menu.profile.transport",
        "Transport Profiles (physical UART)",
        "Transport Profile（物理 UART）",
    ),
    (
        "menu.profile.device",
        "Device Profiles (prompt/EOL/echo/pacing)",
        "Device Profile（提示符/EOL/回显/节奏）",
    ),
    (
        "menu.transport.title",
        "Transport Profiles",
        "Transport Profile",
    ),
    (
        "menu.transport.new",
        "+ New and bind safe 115200 8N1 Profile",
        "+ 新建并绑定安全 115200 8N1 Profile",
    ),
    (
        "menu.transport.new.detail",
        "Creates one reusable 115200/8N1/no-flow Transport Profile and binds this Slot.",
        "创建可复用的 115200/8N1/无流控 Transport Profile，并绑定当前槽位。",
    ),
    (
        "menu.transport.bound",
        "Transport Profile {} bound to the current Slot",
        "Transport Profile {} 已绑定到当前槽位",
    ),
    (
        "menu.transport.created",
        "Transport Profile {} created and bound",
        "Transport Profile {} 已创建并绑定",
    ),
    (
        "menu.transport.missing",
        "Transport Profile {} no longer exists",
        "Transport Profile {} 已不存在",
    ),
    ("menu.device.title", "Device Profiles", "Device Profile"),
    (
        "menu.device.generic",
        "Generic (unbound)",
        "Generic（不绑定）",
    ),
    (
        "menu.device.new",
        "+ Clone current effective device settings and bind",
        "+ 克隆当前生效设备设置并绑定",
    ),
    (
        "menu.device.clone.detail",
        "The new Profile clones the current effective prompts, EOL, echo and write pacing; presets change only the named field.",
        "新 Profile 会克隆当前生效的提示符、EOL、回显和写入节奏；预设仅修改所示字段。",
    ),
    (
        "menu.device.generic.detail",
        "Unbinds the Device Profile and returns to the Slot's generic compatibility settings.",
        "解除 Device Profile 绑定，恢复槽位的通用兼容设置。",
    ),
    (
        "menu.device.bound",
        "Device Profile {} bound to the current Slot",
        "Device Profile {} 已绑定到当前槽位",
    ),
    (
        "menu.device.generic.bound",
        "Device Profile unbound; Generic behavior is active",
        "已解除 Device Profile；当前使用 Generic 行为",
    ),
    (
        "menu.device.created",
        "Device Profile {} created from effective settings and bound",
        "Device Profile {} 已从生效设置克隆并绑定",
    ),
    (
        "menu.device.missing",
        "Device Profile {} no longer exists",
        "Device Profile {} 已不存在",
    ),
    (
        "menu.device.echo.on",
        "+ Clone with Echo On",
        "+ 克隆并设 Echo On",
    ),
    (
        "menu.device.echo.off",
        "+ Clone with Echo Off",
        "+ 克隆并设 Echo Off",
    ),
    (
        "menu.device.echo.auto",
        "+ Clone with Echo Auto (conservative)",
        "+ 克隆并设 Echo Auto（保守）",
    ),
    (
        "menu.device.eol.cr",
        "+ Clone with EOL CR",
        "+ 克隆并设 EOL CR",
    ),
    (
        "menu.device.eol.lf",
        "+ Clone with EOL LF",
        "+ 克隆并设 EOL LF",
    ),
    (
        "menu.device.eol.crlf",
        "+ Clone with EOL CRLF",
        "+ 克隆并设 EOL CRLF",
    ),
    (
        "menu.device.eol.custom",
        "+ Clone with custom EOL",
        "+ 克隆并设自定义 EOL",
    ),
    (
        "menu.profile.exists",
        "Profile {} already exists; choose another name",
        "Profile {} 已存在；请选择其他名称",
    ),
    ("menu.model.title", "DUT model catalog", "样机机型目录"),
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
        "model {} bound to the current Slot",
        "机型 {} 已绑定到当前槽位",
    ),
    (
        "menu.model.created",
        "model {} created and bound to the current Slot",
        "机型 {} 已创建并绑定到当前槽位",
    ),
    ("menu.serial.title", "Serial port presets", "串口设置预设"),
    (
        "menu.serial.current",
        "Current authoritative Transport Profile: {}",
        "当前权威 Transport Profile：{}",
    ),
    (
        "menu.serial.baud",
        "Clone current Profile · baud {}",
        "克隆当前 Profile · 波特率 {}",
    ),
    ("menu.serial.8n1", "Clone · 8N1", "克隆 · 8N1"),
    ("menu.serial.8e1", "Clone · 8E1", "克隆 · 8E1"),
    ("menu.serial.8o1", "Clone · 8O1", "克隆 · 8O1"),
    ("menu.serial.8n2", "Clone · 8N2", "克隆 · 8N2"),
    (
        "menu.serial.flow.none",
        "Clone · flow control None",
        "克隆 · 无流控",
    ),
    (
        "menu.serial.flow.hardware",
        "Clone · hardware flow control",
        "克隆 · 硬件流控",
    ),
    ("menu.serial.dtr", "Clone · toggle DTR", "克隆 · 切换 DTR"),
    ("menu.serial.rts", "Clone · toggle RTS", "克隆 · 切换 RTS"),
    (
        "menu.serial.auto",
        "Clone · toggle auto-open",
        "克隆 · 切换自动打开",
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
        "普通 Enter 会在不接管的情况下排队非空 LINE 操作；Ctrl-] u 选择排队卡片。",
    ),
    (
        "menu.help.enter",
        "While an Agent Run is active, empty Enter never queues bytes; it only returns output to the live tail.",
        "Agent Run 活动时，空 Enter 不会排队任何字节，只会让输出回到实时尾部。",
    ),
    (
        "menu.help.cooperative",
        "Alt+Enter sends direct input bound to the exact matching Agent Run while that Agent keeps its lease.",
        "Alt+Enter 发送绑定到精确匹配 Agent Run 的协作直写；该 Agent 继续持有租约。",
    ),
    (
        "menu.help.takeover",
        "Ctrl-] t is the separate explicit takeover path and may abort the Agent's current Run.",
        "Ctrl-] t 是独立的显式接管路径，可能中止 Agent 当前 Run。",
    ),
    (
        "menu.help.echo",
        "The dot is local TX projection. With device echo, echo=on plus merge_echo merges exact RX; auto conservatively suppresses nothing, so two copies may appear.",
        "圆点是本地 TX 投影。设备会回显时，echo=on 配合 merge_echo 才合并精确 RX；auto 保守地不抑制，因此可能显示两份。",
    ),
    (
        "menu.help.model",
        "Confirm the connected model through serial, Telnet, Web, or a Human before Human or Agent operations.",
        "人或 Agent 操作前，应通过串口、Telnet、Web 或人工确认当前连接机型。",
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
        "Esc returns to the menu",
        "Esc 返回菜单",
    ),
    (
        "menu.prompt.admin",
        "One-time administrator token (masked)",
        "一次性管理员令牌（已遮罩）",
    ),
    (
        "menu.prompt.transport.name",
        "New Transport Profile name",
        "新 Transport Profile 名称",
    ),
    (
        "menu.prompt.device.name",
        "New Device Profile name",
        "新 Device Profile 名称",
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
        "槽位 {} 已不存在",
    ),
    (
        "ui.search.title",
        " history search · Enter accepts · Esc cancels ",
        " 历史搜索 · 回车接受 · Esc 取消 ",
    ),
    // ---- Bottom help line ----
    (
        "ui.helpline",
        " Ctrl-] m menu · Ctrl-] ? help · Alt-1/2 switch · {} · Ctrl-] q quit ",
        " Ctrl-] m 菜单 · Ctrl-] ? 帮助 · Alt-1/2 切换 · {} · Ctrl-] q 退出 ",
    ),
    (
        "ui.scroll.prefix",
        "Ctrl-] PgUp/PgDn scroll",
        "Ctrl-] PgUp/PgDn 滚动",
    ),
    ("ui.scroll.plain", "PgUp/PgDn scroll", "PgUp/PgDn 滚动"),
    // ---- Help popup ----
    ("help.title", " serialctl help ", " serialctl 帮助 "),
    ("help.all.modes", "All modes", "所有模式"),
    (
        "help.switch",
        "  Alt-1..9 / Ctrl-] 1..9   switch Slot",
        "  Alt-1..9 / Ctrl-] 1..9   切换 Slot",
    ),
    (
        "help.next",
        "  Ctrl-] s                 next Slot",
        "  Ctrl-] s                 下一个 Slot",
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
        "  Ctrl-] g                 切换语言 (中文/EN)",
    ),
    (
        "help.scroll",
        "  Ctrl-] PgUp / PgDn       local scroll (especially in RAW)",
        "  Ctrl-] PgUp / PgDn       本地滚动(RAW 下尤其有用)",
    ),
    (
        "help.wheel",
        "  wheel / left drag / right click   scroll / select / copy output",
        "  滚轮 / 左键拖动 / 右键       滚动 / 选择 / 复制串口输出",
    ),
    (
        "help.selection",
        "  mouse                    handled by terminal (serialctl wheel is off)",
        "  鼠标                     由终端处理(serialctl 滚轮已关闭)",
    ),
    (
        "help.mouse.paste",
        "  input right click / Ctrl-Shift-V   paste (right click is Windows-native)",
        "  输入框右键 / Ctrl-Shift-V      粘贴(右键为 Windows 原生支持)",
    ),
    (
        "help.menu",
        "  Ctrl-] m                 open Profile / model / serial settings menu",
        "  Ctrl-] m                 打开 Profile / 机型 / 串口设置菜单",
    ),
    (
        "help.takeover",
        "  Ctrl-] t                 explicit human takeover",
        "  Ctrl-] t                 显式人工接管",
    ),
    (
        "help.cooperative",
        "  Alt-Enter                direct LINE bound to matching Agent Run; lease stays",
        "  Alt-Enter                绑定到匹配 Agent Run 的协作直写；租约不变",
    ),
    (
        "help.release",
        "  Ctrl-] c                 release control or cancel queued input",
        "  Ctrl-] c                 释放控制或取消排队输入",
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
        "  Ctrl-] u                 选择命令；↑/↓ 选卡片，PgUp/PgDn 看全文，d/e",
    ),
    (
        "help.queue.behavior",
        "  ordinary Enter           queue each non-empty LINE; Agent Run empty Enter only follows",
        "  普通 Enter               每条非空 LINE 独立排队；Agent Run 时空 Enter 仅跟随到底部",
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
        "  Ctrl-] p                 确认被阻止的粘贴",
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
        "LINE: Enter queues the line plus Profile EOL without takeover; Alt-Enter",
        "LINE: Enter 不接管并排队该行及 Profile EOL；Alt-Enter",
    ),
    (
        "help.line2",
        "is cooperative direct input. Both return to the live tail. Up/Down browse",
        "是协作直写。两者都回到实时尾部。上/下浏览历史；",
    ),
    (
        "help.line3",
        "history; Ctrl-R searches and Tab completes. Agent Run empty Enter only follows.",
        "Ctrl-R 搜索、Tab 补全；Agent Run 时空 Enter 仅跟随到底部。",
    ),
    (
        "help.raw1",
        "RAW: keys are bytes; Ctrl-C is sent to the device and does not quit.",
        "RAW: 按键即字节;Ctrl-C 发送到设备,不会退出。",
    ),
    (
        "help.raw2",
        "RAW PageUp/PageDown go to the device; use the prefix for local scroll.",
        "RAW 下 PageUp/PageDown 发往设备;本地滚动请用前缀。",
    ),
    (
        "help.paste.note",
        "Large or multi-line paste is always held for explicit confirmation.",
        "大段或多行粘贴总是需要显式确认。",
    ),
    (
        "help.expire",
        "Queued input expires after {}s idle; cancel reconnects and releases this terminal's controls.",
        "排队输入空闲 {} 秒后过期;取消会重连并释放本终端的控制。",
    ),
    (
        "help.replay",
        "Disconnected input is never replayed after reconnect.",
        "断连期间的输入在重连后不会重放。",
    ),
    (
        "help.uncertain",
        "Sent writes without an acknowledgement are uncertain; inspect TX before retrying.",
        "未确认的已发送写入结果不确定;重试前请检查 TX。",
    ),
    (
        "help.close",
        "Press any key to close help.",
        "按任意键关闭帮助。",
    ),
    // ---- Status messages ----
    ("st.connecting", "connecting…", "连接中…"),
    ("st.viewing", "viewing {} ({})", "正在查看 {}({})"),
    (
        "st.transport",
        "transport connected; authenticating and attaching all Slots",
        "传输已连接;正在认证并附着所有 Slot",
    ),
    (
        "st.disconnected",
        "disconnected: {}; reconnecting",
        "已断开: {};正在重连",
    ),
    (
        "st.disconnected.uncertain",
        "disconnected: {}; {} sent write outcome(s) uncertain; inspect TX before retrying",
        "已断开: {};{} 个已发送写入结果不确定;重试前请检查 TX",
    ),
    (
        "st.welcome",
        "connected as {:?} (protocol v{})",
        "已连接,角色 {:?}(协议 v{})",
    ),
    (
        "st.session.changed.unsent",
        "the serial session changed before queued input was sent",
        "串口会话已在排队输入发送前变更",
    ),
    (
        "st.session.changed.discarded",
        "the serial session changed; queued input was discarded",
        "串口会话已变更;排队输入已丢弃",
    ),
    (
        "st.invalidated",
        "{}: {} ({} write(s), {} request(s))",
        "{}: {}({} 个写入, {} 个请求)",
    ),
    (
        "st.daemon.restarted",
        "daemon restarted; old control leases were invalidated",
        "守护进程已重启;旧的控制租约已失效",
    ),
    (
        "st.epoch.changed",
        "daemon epoch changed; previous control leases and cursors are invalid",
        "守护进程 epoch 已变更;之前的控制租约与游标已失效",
    ),
    ("st.retryable", " (retryable)", "(可重试)"),
    (
        "st.discarded.chunks",
        "; {}: discarded {} queued chunk(s)",
        "; {}: 已丢弃 {} 个排队块",
    ),
    (
        "st.history.gap",
        "history gap ({:?}); requested after {:?}, first available {:?}",
        "历史空洞 ({:?});请求起点 {:?},最早可用 {:?}",
    ),
    (
        "st.lagged",
        "slow client missed live events {}..={}; reconnecting for journal replay",
        "慢客户端错过实时事件 {}..={};正在重连以回放日志",
    ),
    (
        "st.replaying",
        "replaying {} #{}..=#{}",
        "正在回放 {} #{}..=#{}",
    ),
    ("st.live", "{} live at sequence {}", "{} 已上线,序列 {}"),
    (
        "st.granted",
        "write control granted for {}",
        "已获得 {} 的写入控制",
    ),
    (
        "st.queued",
        "write control queued at position {}; input is held locally",
        "写入控制排队第 {} 位;输入已本地保留",
    ),
    (
        "st.acquire.cancelled",
        "queued write control request cancelled for {}",
        "已取消 {} 的排队写入控制请求",
    ),
    (
        "st.released",
        "write control released for {}",
        "已释放 {} 的写入控制",
    ),
    (
        "st.write.confirmed",
        "{}: write confirmed at sequence {}",
        "{}: 写入已在序列 {} 确认",
    ),
    (
        "st.trigger.result",
        "Trigger {} is {} after {} confirmed fire(s)",
        "触发任务 {} 当前为 {}，已确认发送 {} 次",
    ),
    ("st.authenticated", "authenticated as {:?}", "已认证为 {:?}"),
    ("st.watching", "watching {} Slot(s)", "正在监视 {} 个 Slot"),
    (
        "st.detached",
        "detached {} Slot(s)",
        "已断开 {} 个 Slot 的监视",
    ),
    ("st.run.started", "run started: {}", "运行已开始: {}"),
    ("st.run.ended", "run ended: {}", "运行已结束: {}"),
    (
        "st.checkpoint",
        "checkpoint created at sequence {}",
        "已在序列 {} 创建检查点",
    ),
    (
        "st.not.auth.queued",
        "connection is not authenticated; input was not queued",
        "连接未认证;输入未入队",
    ),
    (
        "st.not.connected",
        "not connected; input was not queued",
        "未连接;输入未入队",
    ),
    (
        "st.too.many",
        "too many outstanding daemon requests; input was not sent",
        "待处理守护请求过多;输入未发送",
    ),
    (
        "st.outbound.full",
        "outbound queue is full; input was not sent",
        "出站队列已满;输入未发送",
    ),
    (
        "st.network.stopped",
        "network worker stopped",
        "网络工作线程已停止",
    ),
    (
        "st.not.auth2",
        "not authenticated; input was not queued",
        "未认证;输入未入队",
    ),
    (
        "st.not.live",
        "{} is not live yet; input was not queued",
        "{} 尚未上线;输入未入队",
    ),
    (
        "st.writeq.full",
        "local write queue is full; input was not queued",
        "本地写队列已满;输入未入队",
    ),
    (
        "st.not.auth.live",
        "the selected Slot is not authenticated and live; control was not requested",
        "所选 Slot 未认证上线;未请求控制",
    ),
    (
        "st.requesting.control",
        "requesting write control for {}…",
        "正在请求 {} 的写入控制…",
    ),
    (
        "st.requesting.takeover",
        "requesting explicit takeover of {}…",
        "正在请求显式接管 {}…",
    ),
    (
        "st.slot.not.live",
        "the selected Slot is not live; control was not released",
        "所选 Slot 未上线;未释放控制",
    ),
    (
        "st.cancel.reason",
        "operator cancelled queued input",
        "操作员取消了排队输入",
    ),
    (
        "st.no.control",
        "this Slot has no active write control",
        "此 Slot 没有活动的写入控制",
    ),
    (
        "st.control.belongs",
        "write control belongs to {}",
        "写入控制属于 {}",
    ),
    (
        "st.reconnect.reason",
        "{} for {}; reconnecting cancels this actor's queues and releases its controls on every Slot",
        "{} ({});重连将取消此 actor 的队列并释放其在所有 Slot 上的控制",
    ),
    (
        "st.cancel.full",
        "cannot cancel queued control: outbound queue is full",
        "无法取消排队控制: 出站队列已满",
    ),
    (
        "st.cancel.stopped",
        "cannot cancel queued control: network worker stopped",
        "无法取消排队控制: 网络工作线程已停止",
    ),
    (
        "st.idle.release",
        "{}: releasing idle human control after {} seconds",
        "{}: 人工控制空闲 {} 秒,正在释放",
    ),
    (
        "st.queue.expired",
        "queued human input expired after {} seconds of inactivity",
        "排队的人工输入在 {} 秒无活动后过期",
    ),
    (
        "st.prefix.hint",
        "command prefix: 1-9 Slot, m menu, l/r mode, PgUp/PgDn scroll, t takeover, u queue list, d/e newest queue item, c release/cancel, ? help",
        "命令前缀: 1-9 Slot, m 菜单, l/r 模式, PgUp/PgDn 滚动, t 接管, u 队列列表, d/e 最新排队项, c 释放/取消, ? 帮助",
    ),
    (
        "st.line.mode",
        "LINE mode: Enter sends the line plus Profile EOL",
        "LINE 模式: 回车发送该行并附加 Profile EOL",
    ),
    (
        "st.raw.mode",
        "RAW mode: keystrokes are sent directly; Ctrl-] remains local",
        "RAW 模式: 按键直接发送;Ctrl-] 仍为本地命令",
    ),
    ("st.follow", "following live output", "正在跟随实时输出"),
    (
        "st.detailed",
        "detailed timeline: #seq and source columns shown",
        "详细时间线: 显示 #seq 与来源列",
    ),
    (
        "st.compact",
        "compact timeline: markers and inline highlighting",
        "紧凑时间线: 标记与行内高亮",
    ),
    (
        "st.logs.hint",
        "use `serialctl logs --contains TEXT` for durable history search",
        "使用 `serialctl logs --contains TEXT` 进行持久历史搜索",
    ),
    (
        "st.unknown.prefix",
        "unknown prefix command; Ctrl-] ? opens help",
        "未知前缀命令;Ctrl-] ? 打开帮助",
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
        "排队命令选择：↑/↓ 选卡片，PgUp/PgDn 看全文，d 删除，e 编辑（Enter 后排到队尾），Esc 关闭",
    ),
    (
        "st.queue.select.closed",
        "queued-command selection closed",
        "已关闭排队命令选择",
    ),
    (
        "st.agent.enter.follow",
        "Agent Run is active; empty Enter only resumed live output",
        "Agent Run 正在执行；空回车仅恢复到底部实时输出",
    ),
    (
        "st.cooperative.unavailable",
        "cooperative input requires a matching active Agent lease and Run; draft kept",
        "协作输入要求当前 Agent 租约与活动 Run 匹配；命令草稿已保留",
    ),
    (
        "st.cooperative.sent",
        "cooperative input sent without takeover",
        "协作输入已发送，未接管串口",
    ),
    (
        "st.menu.open",
        "configuration menu opened",
        "已打开配置菜单",
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
        "st.selection.ready",
        "selected {} character(s); live output resumed, right-click output to copy",
        "已选择 {} 个字符；实时输出已恢复，右键输出区即可复制",
    ),
    (
        "st.clipboard.copy.failed",
        "cannot copy selection: {}",
        "无法复制所选文本: {}",
    ),
    (
        "st.clipboard.paste.shortcut",
        "right-click paste is unavailable on this platform; use Ctrl-Shift-V",
        "此平台不支持应用内右键粘贴;请使用 Ctrl-Shift-V",
    ),
    (
        "st.clipboard.paste.failed",
        "cannot read clipboard: {}",
        "无法读取剪贴板: {}",
    ),
    (
        "st.paste.rejected",
        "paste rejected: {} bytes exceeds the {} byte interactive safety limit",
        "粘贴被拒绝: {} 字节超过 {} 字节的交互安全上限",
    ),
    (
        "st.paste.blocked",
        "multi-line/large paste blocked; Ctrl-] p confirms for the original Slot",
        "多行/大段粘贴已阻止;Ctrl-] p 确认发送到原 Slot",
    ),
    (
        "st.paste.none",
        "no blocked paste to confirm",
        "没有待确认的粘贴",
    ),
    (
        "st.paste.gone",
        "the paste target Slot no longer exists",
        "粘贴目标 Slot 已不存在",
    ),
    (
        "st.paste.queued",
        "confirmed paste queued for {}",
        "已确认的粘贴已入队 {}",
    ),
    (
        "st.no.slot",
        "no Slot is configured; run `serialctl init`",
        "未配置 Slot;请运行 `serialctl init`",
    ),
    ("st.language", "language: {}", "语言: {}"),
    (
        "st.write.disappeared",
        "write control disappeared before send",
        "写入控制在发送前消失",
    ),
    (
        "st.break.confirmed",
        "BREAK confirmed at sequence {}",
        "串口 BREAK 已确认，序号 {}",
    ),
    // ---- display.rs labels ----
    ("d.dev", "DEV", "设备"),
    ("d.tx", "TX>", "发送>"),
    ("d.system", "SYSTEM", "系统"),
    ("d.gap", "GAP", "缺口"),
    ("d.kind.human", "HUMAN", "人工"),
    ("d.kind.agent", "AGENT", "智能体"),
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
    ("d.ev.slot_reconfigured", "slot_reconfigured", "槽位已重配"),
    ("d.ev.slot_removed", "slot_removed", "槽位已移除"),
    ("d.ev.control_granted", "control_granted", "控制已授予"),
    ("d.ev.control_released", "control_released", "控制已释放"),
    ("d.ev.control_revoked", "control_revoked", "控制被撤销"),
    ("d.ev.control_expired", "control_expired", "控制已过期"),
    ("d.ev.run_started", "run_started", "运行开始"),
    ("d.ev.run_ended", "run_ended", "运行结束"),
    ("d.ev.run_aborted", "run_aborted", "运行中止"),
    ("d.run.start", "RUN START", "运行开始"),
    ("d.run.end", "RUN END", "运行结束"),
    ("d.run.abort", "RUN ABORTED", "运行中止"),
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
    ("d.ev.gap", "gap", "空洞"),
    // ---- main.rs runtime output ----
    (
        "m.terminal.required",
        "interactive mode requires a terminal; use `serialctl status --json` or `serialctl logs --json`",
        "交互模式需要终端;请使用 `serialctl status --json` 或 `serialctl logs --json`",
    ),
    (
        "m.scope.error",
        "--initial-slot applies only to the interactive `serialctl` console",
        "--initial-slot 仅适用于交互式 `serialctl` 控制台",
    ),
    (
        "m.status.header",
        "seriald {}  epoch {}  {} Slot(s)",
        "seriald {}  epoch {}  {} 个 Slot",
    ),
    ("m.status.control", "control: {}", "控制: {}"),
    ("m.status.reason", "  reason: {}", "  原因: {}"),
    (
        "m.status.trigger",
        "  trigger: {}  status: {}  fires: {}",
        "  触发任务: {}  状态: {}  已发送: {} 次",
    ),
    ("m.doctor.config", "config", "配置文件"),
    ("m.doctor.endpoint", "endpoint", "端点"),
    ("m.doctor.token", "token", "令牌"),
    ("m.doctor.daemon", "daemon", "守护进程"),
    ("m.doctor.server", "server", "服务器"),
    ("m.doctor.epoch", "epoch", "epoch"),
    ("m.doctor.protocol", "protocol", "协议"),
    ("m.doctor.protocol.compatible", "compatible", "兼容"),
    (
        "m.doctor.protocol.mismatch",
        "version mismatch",
        "版本不匹配",
    ),
    ("m.doctor.uptime", "uptime", "运行时长"),
    ("m.doctor.slots", "slots", "槽位"),
    ("m.token.configured", "configured", "已配置"),
    ("m.token.missing", "not configured", "未配置"),
    (
        "m.doctor.slots.value",
        "{} total, {} online",
        "共 {} 个,{} 个在线",
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
        "归档目录不完整: 受限扫描跳过了不可读条目或达到响应上限",
    ),
    (
        "m.logs.span.warn",
        "warning: this query spans the entire selected daemon epoch and may include older test cycles; --contains only filters that global range, so narrow it with --run, --operation, --after-seq, or --after-time/--before-time",
        "警告: 此查询覆盖整个所选守护 epoch,可能包含较旧的测试周期;--contains 只过滤该全局范围,请用 --run、--operation、--after-seq 或 --after-time/--before-time 缩小范围",
    ),
    (
        "m.logs.truncated",
        "results truncated; repeat the same filters with --epoch {} --after-seq {}",
        "结果已截断;使用相同过滤条件并附加 --epoch {} --after-seq {} 继续",
    ),
    (
        "m.logs.truncated.nocursor",
        "results truncated without a continuation cursor",
        "结果已截断,且无续传游标",
    ),
    (
        "m.logs.gap",
        "gap {}..={} ({:?}, epoch {})",
        "空洞 {}..={}({:?},epoch {})",
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
        "无效的 RFC3339 时间戳 `{}`: {};请包含时区,例如 2026-07-19T12:30:00+08:00",
    ),
    (
        "m.time.range",
        "RFC3339 timestamp `{}` is outside the nanosecond range",
        "RFC3339 时间戳 `{}` 超出纳秒范围",
    ),
    (
        "m.direction.unknown",
        "unknown direction `{}`; use rx, tx, or none",
        "未知方向 `{}`;请使用 rx、tx 或 none",
    ),
    (
        "m.kind.unknown",
        "unknown event kind `{}`; use rx, tx, serial-opened, serial-closed, run-started, trigger-started, checkpoint, or another protocol event kind",
        "未知事件类型 `{}`;请使用 rx、tx、serial-opened、serial-closed、run-started、trigger-started、checkpoint 或其他协议事件类型",
    ),
    // ---- init wizard ----
    ("i.endpoint", "seriald endpoint", "seriald 端点"),
    (
        "i.token.notice",
        "The saved token is treated as the daily operator token; setup still requires a separate admin token.",
        "已保存的令牌将作为日常操作员令牌;初始配置仍需单独的管理员令牌。",
    ),
    (
        "i.admin.prompt",
        "seriald admin bearer token (required for setup; never saved): ",
        "seriald 管理员令牌(配置必需,不会保存): ",
    ),
    (
        "i.admin.required",
        "an admin bearer token is required; seriald v1 does not support disabled authentication",
        "必须提供管理员令牌;seriald v1 不支持关闭认证",
    ),
    (
        "i.unreachable",
        "cannot reach seriald; start seriald on Windows and verify the host-only endpoint",
        "无法连接 seriald;请在 Windows 上启动 seriald 并确认仅本机的端点",
    ),
    (
        "i.status.fail",
        "cannot read existing Slot configuration; verify the admin token",
        "无法读取现有 Slot 配置;请检查管理员令牌",
    ),
    (
        "i.connected",
        "Connected to seriald {} (epoch {}).",
        "已连接 seriald {}(epoch {})。",
    ),
    (
        "i.no.ports",
        "seriald found no serial ports on its host",
        "seriald 在其主机上未发现串口",
    ),
    (
        "i.ports.header",
        "\nSerial ports discovered on the seriald host:",
        "\nseriald 主机上发现的串口:",
    ),
    (
        "i.select.ports",
        "Select ports for the complete Slot set (comma-separated numbers)",
        "选择完整 Slot 集合包含的端口(逗号分隔的编号)",
    ),
    (
        "i.profile.note",
        "\nNew ports use: 115200 8N1, no flow control, DTR/RTS low, TX EOL \\r, echo on, no guessed device prompt, probe disabled, auto-open.",
        "\n新端口使用: 115200 8N1、无流控、DTR/RTS 低电平、TX EOL \\r、回显开、不猜测设备提示符、探测禁用、自动打开。",
    ),
    (
        "i.existing.keep",
        "Previously configured ports keep their Profile and serial settings.",
        "此前配置过的端口保留其 Profile 与串口参数。",
    ),
    ("i.slot.name", "Slot name for {}", "{} 的 Slot 名称"),
    ("i.slot.id", "Slot ID for {}", "{} 的 Slot ID"),
    (
        "i.omitted.header",
        "\nExisting Slots not selected in this scan:",
        "\n本次扫描未选择的已有 Slot:",
    ),
    (
        "i.omitted.note",
        "  {} → {} (kept by default, including when the COM port is temporarily absent)",
        "  {} → {}(默认保留,即使 COM 口暂时缺失)",
    ),
    (
        "i.omitted.delete",
        "Explicitly delete these omitted Slots from seriald configuration?",
        "是否从 seriald 配置中显式删除这些未选择的 Slot?",
    ),
    (
        "i.omitted.deleting",
        "Deleting {} explicitly omitted Slot(s).",
        "正在删除 {} 个显式未选择的 Slot。",
    ),
    (
        "i.omitted.keeping",
        "Keeping {} existing Slot(s).",
        "保留 {} 个已有 Slot。",
    ),
    (
        "i.configured",
        "\nConfigured {} Slot(s):",
        "\n已配置 {} 个 Slot:",
    ),
    (
        "i.operator.keep",
        "seriald operator bearer token for daily use (leave empty to keep the saved token): ",
        "seriald 日常操作员令牌(留空保留已保存令牌): ",
    ),
    (
        "i.operator.required.prompt",
        "seriald operator bearer token for daily use (required; saved locally): ",
        "seriald 日常操作员令牌(必需;本地保存): ",
    ),
    (
        "i.operator.required",
        "an operator bearer token is required for the daily console; the admin token is not saved",
        "日常控制台需要操作员令牌;管理员令牌不会保存",
    ),
    (
        "i.operator.fail",
        "the operator token could not read daemon status; the token file was not changed",
        "操作员令牌无法读取守护状态;令牌文件未更改",
    ),
    (
        "i.role.fail",
        "the daily token role could not be verified; the token file was not changed",
        "无法验证日常令牌角色;令牌文件未更改",
    ),
    (
        "i.role.wrong",
        "the daily token has role {:?}; an operator token is required and the token file was not changed",
        "日常令牌角色为 {:?};需要操作员令牌,令牌文件未更改",
    ),
    (
        "i.saved",
        "Saved serialctl configuration to {}.",
        "serialctl 配置已保存到 {}。",
    ),
    (
        "i.open.console",
        "Run `serialctl` to open the multi-Slot console.",
        "运行 `serialctl` 打开多 Slot 控制台。",
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
        "输入 `y` 删除未选择的 Slot,输入 `n` 保留",
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
        assert_eq!(trf("st.live", &["slot-1", "42"]), "slot-1 已上线,序列 42");
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
    fn lang_parses_common_spellings() {
        assert_eq!(Lang::parse("en"), Some(Lang::En));
        assert_eq!(Lang::parse("ZH-CN"), Some(Lang::Zh));
        assert_eq!(Lang::parse("fr"), None);
        assert_eq!(Lang::En.toggled(), Lang::Zh);
        assert_eq!(Lang::Zh.toggled(), Lang::En);
    }
}
