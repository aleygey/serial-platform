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
    let guard = LOCK.lock().expect("language test lock poisoned");
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
        " · QUEUED #{} ({}s, {} chunk(s); Ctrl-] c cancels)",
        " · 排队中 #{}({} 秒, {} 块; Ctrl-] c 取消)",
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
    ("ui.status.control", "control:", "控制:"),
    ("ui.paused", " · PAUSED", " · 已暂停"),
    // ---- Input box ----
    (
        "ui.input.title.line",
        " command · Enter sends Profile EOL ",
        " 命令 · 回车发送并附加 Profile EOL ",
    ),
    (
        "ui.input.raw.text",
        "Keystrokes are sent directly. Ctrl-C sends ETX; Ctrl-] opens local commands.",
        "按键直接发送。Ctrl-C 发送 ETX;Ctrl-] 打开本地命令。",
    ),
    ("ui.input.title.raw", " RAW direct transport ", " RAW 直传 "),
    (
        "ui.search.title",
        " history search · Enter accepts · Esc cancels ",
        " 历史搜索 · 回车接受 · Esc 取消 ",
    ),
    // ---- Bottom help line ----
    (
        "ui.helpline",
        " Ctrl-] ? help · Alt-1/2 switch · {} · Ctrl-] q quit ",
        " Ctrl-] ? 帮助 · Alt-1/2 切换 · {} · Ctrl-] q 退出 ",
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
        "  mouse wheel              scroll 3 lines (bottom resumes follow)",
        "  鼠标滚轮                 滚动 3 行(回到底部恢复跟随)",
    ),
    (
        "help.takeover",
        "  Ctrl-] t                 explicit human takeover",
        "  Ctrl-] t                 显式人工接管",
    ),
    (
        "help.release",
        "  Ctrl-] c                 release control or cancel queued input",
        "  Ctrl-] c                 释放控制或取消排队输入",
    ),
    (
        "help.follow",
        "  Ctrl-] f                 follow live output",
        "  Ctrl-] f                 跟随实时输出",
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
        "help.quit",
        "  Ctrl-] q                 quit",
        "  Ctrl-] q                 退出",
    ),
    (
        "help.line1",
        "LINE: Enter sends the line plus the Profile EOL (default CR) and",
        "LINE: 回车发送该行并附加 Profile EOL(默认 CR),",
    ),
    (
        "help.line2",
        "returns to the live tail. Up/Down browse history; Ctrl-R starts an",
        "并回到实时尾部。上/下浏览历史;Ctrl-R 开始",
    ),
    (
        "help.line3",
        "incremental history search; Tab completes from history.",
        "增量历史搜索;Tab 从历史补全。",
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
        "command prefix: 1-9 Slot, l LINE, r RAW, PgUp/PgDn scroll, v detail, t takeover, c release/cancel, ? help",
        "命令前缀: 1-9 Slot, l LINE, r RAW, PgUp/PgDn 滚动, v 详情, t 接管, c 释放/取消, ? 帮助",
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
    ("st.input.cleared", "input cleared", "输入已清空"),
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
    ("m.doctor.config", "config", "配置文件"),
    ("m.doctor.endpoint", "endpoint", "端点"),
    ("m.doctor.token", "token", "令牌"),
    ("m.doctor.daemon", "daemon", "守护进程"),
    ("m.doctor.server", "server", "服务器"),
    ("m.doctor.epoch", "epoch", "epoch"),
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
        "unknown event kind `{}`; use rx, tx, serial-opened, serial-closed, run-started, checkpoint, or another protocol event kind",
        "未知事件类型 `{}`;请使用 rx、tx、serial-opened、serial-closed、run-started、checkpoint 或其他协议事件类型",
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
        "\nNew ports use: 115200 8N1, no flow control, DTR/RTS low, TX EOL \\r, echo on, U-Boot prompt `SigmaStar #`, probe disabled, auto-open.",
        "\n新端口使用: 115200 8N1、无流控、DTR/RTS 低电平、TX EOL \\r、回显开、U-Boot 提示符 `SigmaStar #`、探测禁用、自动打开。",
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
