use std::collections::{BTreeMap, VecDeque};

use chrono::{Local, TimeZone as _};
use serial_protocol::{ActorKind, Direction, EventKind, SlotSnapshot, TimelineEvent};
use uuid::Uuid;

const MAX_CONSOLE_ROWS: usize = 5_000;
const MAX_CONSOLE_BYTES: usize = 2 * 1024 * 1024;
const MAX_AGENT_RECORDS: usize = 256;
const MAX_AGENT_COMMAND_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct ConsoleRow {
    pub seq: u64,
    pub direction: Direction,
    pub time: String,
    pub source: String,
    pub text: String,
    pub replay: bool,
}

#[derive(Debug, Clone)]
pub struct AgentRecord {
    pub seq: u64,
    pub run_id: Option<Uuid>,
    pub status: Option<&'static str>,
    pub description: String,
    pub sequence_id: Option<Uuid>,
    pub commands: Vec<AgentCommand>,
}

#[derive(Debug, Clone)]
pub struct AgentCommand {
    pub first_seq: u64,
    pub step_index: Option<usize>,
    operation_id: Option<Uuid>,
    data: Vec<u8>,
    truncated: bool,
}

impl AgentCommand {
    fn from_event(event: &TimelineEvent) -> Self {
        let mut data = event.data.clone();
        let truncated = data.len() > MAX_AGENT_COMMAND_BYTES;
        data.truncate(MAX_AGENT_COMMAND_BYTES);
        Self {
            first_seq: event.seq,
            step_index: command_sequence_step_index(event),
            operation_id: event.operation_id,
            data,
            truncated,
        }
    }

    fn matches_event(&self, event: &TimelineEvent) -> bool {
        event.operation_id.is_some() && self.operation_id == event.operation_id
            || self.step_index.is_some() && self.step_index == command_sequence_step_index(event)
    }

    fn append_event(&mut self, event: &TimelineEvent) {
        let remaining = MAX_AGENT_COMMAND_BYTES.saturating_sub(self.data.len());
        let appended = remaining.min(event.data.len());
        self.data.extend_from_slice(&event.data[..appended]);
        self.truncated |= appended < event.data.len();
    }

    pub fn text(&self) -> String {
        let mut text = display_bytes(&self.data);
        if self.truncated {
            text.push('…');
        }
        text
    }
}

#[derive(Debug, Default)]
pub struct SlotViewModel {
    pub snapshot: Option<SlotSnapshot>,
    pub rows: VecDeque<ConsoleRow>,
    pub agent_records: VecDeque<AgentRecord>,
    pub follow_output: bool,
    pub unseen: usize,
    epoch: Option<Uuid>,
    last_seq: u64,
    retained_bytes: usize,
}

impl SlotViewModel {
    pub fn set_snapshot(&mut self, snapshot: SlotSnapshot) {
        if self
            .epoch
            .is_some_and(|epoch| epoch != snapshot.daemon_epoch)
        {
            self.clear_history();
        }
        self.epoch = Some(snapshot.daemon_epoch);
        self.snapshot = Some(snapshot);
    }

    pub fn push_event(&mut self, event: TimelineEvent, replay: bool) {
        if self.epoch.is_some_and(|epoch| epoch != event.daemon_epoch) {
            self.clear_history();
        }
        if self.epoch == Some(event.daemon_epoch) && event.seq <= self.last_seq {
            return;
        }
        self.epoch = Some(event.daemon_epoch);
        self.last_seq = event.seq;
        self.observe_agent(&event);

        let row = match event.kind {
            EventKind::Rx | EventKind::Tx => Some(ConsoleRow {
                seq: event.seq,
                direction: event.direction,
                time: format_time(event.wall_time_ns),
                source: source_label(&event),
                text: display_bytes(&event.data),
                replay,
            }),
            EventKind::SerialOpened => Some(system_row(&event, "串口已打开", replay)),
            EventKind::SerialClosed => Some(system_row(&event, "串口已关闭", replay)),
            EventKind::SerialOpenFailed => Some(system_row(&event, "串口打开失败", replay)),
            EventKind::Gap => Some(system_row(&event, "历史存在缺口", replay)),
            EventKind::RunStarted => Some(system_row(&event, "Agent 任务开始", replay)),
            EventKind::RunEnded => Some(system_row(&event, "Agent 任务完成", replay)),
            EventKind::RunAborted => Some(system_row(&event, "Agent 任务中止", replay)),
            _ => None,
        };
        let Some(row) = row else {
            return;
        };
        self.retained_bytes = self
            .retained_bytes
            .saturating_add(row.text.len().saturating_add(row.source.len()));
        self.rows.push_back(row);
        while self.rows.len() > MAX_CONSOLE_ROWS || self.retained_bytes > MAX_CONSOLE_BYTES {
            let Some(removed) = self.rows.pop_front() else {
                break;
            };
            self.retained_bytes = self
                .retained_bytes
                .saturating_sub(removed.text.len().saturating_add(removed.source.len()));
        }
        if !self.follow_output {
            self.unseen = self.unseen.saturating_add(1);
        }
    }

    pub fn console_text(&self) -> String {
        let mut text = String::new();
        for row in &self.rows {
            let marker = match row.direction {
                Direction::Rx => "←",
                Direction::Tx => "→",
                Direction::None => "•",
            };
            text.push_str(&format!(
                "{} {} {:<12} {}\n",
                row.time, marker, row.source, row.text
            ));
        }
        text
    }

    pub fn clear_history(&mut self) {
        self.rows.clear();
        self.agent_records.clear();
        self.last_seq = 0;
        self.retained_bytes = 0;
        self.unseen = 0;
    }

    fn observe_agent(&mut self, event: &TimelineEvent) {
        let record = match event.kind {
            EventKind::RunStarted | EventKind::RunEnded | EventKind::RunAborted => {
                let actor_is_agent = event
                    .actor
                    .as_ref()
                    .is_some_and(|actor| actor.kind == ActorKind::Agent);
                if !actor_is_agent && event.run_id.is_none() {
                    return;
                }
                let run = event.metadata.get("run");
                let label = run
                    .and_then(|value| value.get("label"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("未命名 Agent 任务");
                let status = match event.kind {
                    EventKind::RunStarted => "开始",
                    EventKind::RunEnded => "完成",
                    EventKind::RunAborted => "中止",
                    _ => unreachable!(),
                };
                Some(AgentRecord {
                    seq: event.seq,
                    run_id: event.run_id,
                    status: Some(status),
                    description: sanitize_inline(label),
                    sequence_id: None,
                    commands: Vec::new(),
                })
            }
            EventKind::Tx
                if event
                    .actor
                    .as_ref()
                    .is_some_and(|actor| actor.kind == ActorKind::Agent) =>
            {
                self.observe_agent_tx(event);
                None
            }
            _ => None,
        };
        if let Some(record) = record {
            self.agent_records.push_back(record);
            while self.agent_records.len() > MAX_AGENT_RECORDS {
                self.agent_records.pop_front();
            }
        }
    }

    fn observe_agent_tx(&mut self, event: &TimelineEvent) {
        let sequence_id = command_sequence_id(event);
        let description = if sequence_id.is_some() {
            event
                .metadata
                .get("command_sequence_description")
                .or_else(|| event.metadata.get("command_description"))
        } else {
            event.metadata.get("command_description")
        }
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
        let Some(description) = description else {
            return;
        };

        let existing = if let Some(sequence_id) = sequence_id {
            self.agent_records.iter_mut().find(|record| {
                record.status.is_none()
                    && record.run_id == event.run_id
                    && record.sequence_id == Some(sequence_id)
            })
        } else if event.operation_id.is_some() {
            self.agent_records.iter_mut().find(|record| {
                record.status.is_none()
                    && record.run_id == event.run_id
                    && record.sequence_id.is_none()
                    && record
                        .commands
                        .iter()
                        .any(|command| command.operation_id == event.operation_id)
            })
        } else {
            None
        };

        if let Some(record) = existing {
            record.seq = record.seq.min(event.seq);
            if sequence_id.is_some() {
                record.description = sanitize_inline(description);
            }
            if let Some(command) = record
                .commands
                .iter_mut()
                .find(|command| command.matches_event(event))
            {
                command.append_event(event);
            } else {
                record.commands.push(AgentCommand::from_event(event));
            }
            sort_agent_commands(&mut record.commands);
            return;
        }

        self.agent_records.push_back(AgentRecord {
            seq: event.seq,
            run_id: event.run_id,
            status: None,
            description: sanitize_inline(description),
            sequence_id,
            commands: vec![AgentCommand::from_event(event)],
        });
        while self.agent_records.len() > MAX_AGENT_RECORDS {
            self.agent_records.pop_front();
        }
    }

    /// Adds a local diagnostic marker without advancing the authoritative
    /// daemon cursor. The next real event can therefore still be deduplicated
    /// exclusively by `(epoch, seq)`.
    pub fn push_notice(&mut self, text: impl Into<String>) {
        let text = text.into();
        self.retained_bytes = self.retained_bytes.saturating_add(text.len() + 6);
        self.rows.push_back(ConsoleRow {
            seq: self.last_seq,
            direction: Direction::None,
            time: "--:--:--.---".into(),
            source: "SYSTEM".into(),
            text,
            replay: true,
        });
        while self.rows.len() > MAX_CONSOLE_ROWS || self.retained_bytes > MAX_CONSOLE_BYTES {
            let Some(removed) = self.rows.pop_front() else {
                break;
            };
            self.retained_bytes = self
                .retained_bytes
                .saturating_sub(removed.text.len().saturating_add(removed.source.len()));
        }
    }
}

pub fn ensure_slot<'a>(
    slots: &'a mut BTreeMap<String, SlotViewModel>,
    slot_id: &str,
) -> &'a mut SlotViewModel {
    slots
        .entry(slot_id.to_string())
        .or_insert_with(|| SlotViewModel {
            follow_output: true,
            ..SlotViewModel::default()
        })
}

fn system_row(event: &TimelineEvent, text: &str, replay: bool) -> ConsoleRow {
    ConsoleRow {
        seq: event.seq,
        direction: Direction::None,
        time: format_time(event.wall_time_ns),
        source: "SYSTEM".into(),
        text: text.into(),
        replay,
    }
}

fn source_label(event: &TimelineEvent) -> String {
    match event.direction {
        Direction::Rx => "DUT".into(),
        Direction::Tx => event
            .actor
            .as_ref()
            .map_or_else(|| "TX".into(), |actor| sanitize_inline(&actor.label)),
        Direction::None => "SYSTEM".into(),
    }
}

fn command_sequence_id(event: &TimelineEvent) -> Option<Uuid> {
    event.metadata.get("command_sequence_id").and_then(|value| {
        value
            .as_str()
            .and_then(|value| Uuid::parse_str(value).ok())
            .or_else(|| serde_json::from_value(value.clone()).ok())
    })
}

fn command_sequence_step_index(event: &TimelineEvent) -> Option<usize> {
    event
        .metadata
        .get("command_sequence_step_index")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn sort_agent_commands(commands: &mut [AgentCommand]) {
    commands.sort_by_key(|command| (command.step_index.unwrap_or(usize::MAX), command.first_seq));
}

fn display_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .filter_map(|character| match character {
            '\r' => None,
            '\n' | '\t' => Some(character),
            character if character.is_control() => Some('�'),
            character => Some(character),
        })
        .collect()
}

fn sanitize_inline(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .trim()
        .to_string()
}

fn format_time(wall_time_ns: i64) -> String {
    let seconds = wall_time_ns.div_euclid(1_000_000_000);
    let nanos = wall_time_ns.rem_euclid(1_000_000_000) as u32;
    Local.timestamp_opt(seconds, nanos).single().map_or_else(
        || "--:--:--.---".into(),
        |value| value.format("%H:%M:%S%.3f").to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serial_protocol::{Actor, ActorKind};

    use super::*;

    fn event(seq: u64, kind: EventKind, direction: Direction, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: Uuid::nil(),
            seq,
            generation: 1,
            wall_time_ns: 0,
            monotonic_time_ns: 0,
            kind,
            direction,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: data.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    #[test]
    fn replay_and_live_boundary_is_deduplicated_by_epoch_and_sequence() {
        let mut slot = SlotViewModel {
            follow_output: true,
            ..SlotViewModel::default()
        };
        slot.push_event(event(1, EventKind::Rx, Direction::Rx, b"boot\r\n"), true);
        slot.push_event(event(1, EventKind::Rx, Direction::Rx, b"boot\r\n"), false);
        slot.push_event(event(2, EventKind::Rx, Direction::Rx, b"ready\r\n"), false);

        assert_eq!(slot.rows.len(), 2);
        assert!(slot.console_text().contains("boot"));
        assert!(slot.console_text().contains("ready"));
    }

    #[test]
    fn described_agent_tx_enters_the_agent_history() {
        let mut slot = SlotViewModel::default();
        let mut command = event(7, EventKind::Tx, Direction::Tx, b"cat /proc/meminfo\r");
        command.actor = Some(Actor {
            id: "agent:test".into(),
            label: "Test Agent".into(),
            kind: ActorKind::Agent,
        });
        command
            .metadata
            .insert("command_description".into(), "查看样机内存".into());

        slot.push_event(command, true);

        assert_eq!(slot.agent_records.len(), 1);
        assert_eq!(slot.agent_records[0].status, None);
        assert_eq!(slot.agent_records[0].description, "查看样机内存");
        assert_eq!(slot.agent_records[0].commands.len(), 1);
        assert_eq!(
            slot.agent_records[0].commands[0].text(),
            "cat /proc/meminfo"
        );
    }

    #[test]
    fn command_sequence_is_one_purpose_with_steps_sorted_by_step_index() {
        let mut slot = SlotViewModel::default();
        let run_id = Uuid::new_v4();
        let sequence_id = Uuid::new_v4();
        let actor = Actor {
            id: "agent:test".into(),
            label: "Test Agent".into(),
            kind: ActorKind::Agent,
        };
        for (seq, step_index, command) in [
            (10, 1, b"password\r".as_slice()),
            (11, 0, b"admin\r".as_slice()),
        ] {
            let mut step = event(seq, EventKind::Tx, Direction::Tx, command);
            step.actor = Some(actor.clone());
            step.run_id = Some(run_id);
            step.operation_id = Some(Uuid::new_v4());
            step.metadata.insert(
                "command_description".into(),
                serde_json::json!(if step_index == 0 {
                    "输入账号"
                } else {
                    "输入密码"
                }),
            );
            step.metadata.insert(
                "command_sequence_description".into(),
                serde_json::json!("登录样机控制台"),
            );
            step.metadata.insert(
                "command_sequence_id".into(),
                serde_json::json!(sequence_id.to_string()),
            );
            step.metadata.insert(
                "command_sequence_step_index".into(),
                serde_json::json!(step_index),
            );
            slot.push_event(step, true);
        }

        assert_eq!(slot.agent_records.len(), 1);
        let record = &slot.agent_records[0];
        assert_eq!(record.sequence_id, Some(sequence_id));
        assert_eq!(record.description, "登录样机控制台");
        assert_eq!(record.commands.len(), 2);
        assert_eq!(record.commands[0].step_index, Some(0));
        assert_eq!(record.commands[0].text(), "admin");
        assert_eq!(record.commands[1].step_index, Some(1));
        assert_eq!(record.commands[1].text(), "password");
        let expanded = record
            .commands
            .iter()
            .map(AgentCommand::text)
            .collect::<String>();
        assert!(!expanded.contains('\u{2705}'));
        assert!(!expanded.contains('\u{274c}'));
    }

    #[test]
    fn ordinary_commands_remain_independent_single_command_records() {
        let mut slot = SlotViewModel::default();
        let actor = Actor {
            id: "agent:test".into(),
            label: "Test Agent".into(),
            kind: ActorKind::Agent,
        };
        let version_operation = Uuid::new_v4();
        for (seq, description, command, operation_id) in [
            (20, "读取版本", b"ver".as_slice(), version_operation),
            (21, "读取版本", b"sion\r".as_slice(), version_operation),
            (22, "读取内存", b"free -m\r".as_slice(), Uuid::new_v4()),
        ] {
            let mut event = event(seq, EventKind::Tx, Direction::Tx, command);
            event.actor = Some(actor.clone());
            event.operation_id = Some(operation_id);
            event
                .metadata
                .insert("command_description".into(), serde_json::json!(description));
            slot.push_event(event, false);
        }

        assert_eq!(slot.agent_records.len(), 2);
        assert!(
            slot.agent_records
                .iter()
                .all(|record| { record.sequence_id.is_none() && record.commands.len() == 1 })
        );
        assert_eq!(slot.agent_records[0].commands[0].text(), "version");
        assert_eq!(slot.agent_records[1].commands[0].text(), "free -m");
    }

    #[test]
    fn binary_control_bytes_are_visible_but_not_interpreted() {
        assert_eq!(display_bytes(b"ok\x1b[2J\n"), "ok�[2J\n");
    }

    #[test]
    fn run_history_uses_plain_language_status_without_command_icons() {
        let mut slot = SlotViewModel::default();
        let mut completed = event(8, EventKind::RunEnded, Direction::None, b"");
        completed.run_id = Some(Uuid::new_v4());
        completed
            .metadata
            .insert("run".into(), serde_json::json!({"label": "检查启动日志"}));

        slot.push_event(completed, false);

        assert_eq!(slot.agent_records[0].status, Some("完成"));
        assert_eq!(slot.agent_records[0].description, "检查启动日志");
    }
}
