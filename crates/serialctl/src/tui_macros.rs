//! Local Macro catalog/editor. Saves never execute; runs use a fixed revision.
use super::*;
use serial_protocol::{
    MacroDefinition, MacroExecutionInfo, MacroListQuery, MacroRunSpec, MacroSaveRequest,
    MacroSummary,
};

pub(super) enum Action {
    None,
    Close,
    List(bool),
    Load(String),
    Save(MacroSaveRequest),
    Run(MacroRunSpec),
    Stop,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> MacroDefinition {
        MacroDefinition {
            id: "enter_uboot".into(),
            name: "进入 U-Boot".into(),
            description: "已审核的机型宏".into(),
            language_version: 1,
            revision: 9,
            parameters: Default::default(),
            script: "cmd(\"help\");\n".into(),
            shared: true,
            applies_to: None,
            updated_at_ns: 1,
        }
    }

    #[test]
    fn new_macro_stays_a_draft_and_save_is_never_run() {
        let mut panel = Panel::new("COM3".into());
        panel.busy = false;
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE)),
            Action::None
        ));
        let Action::Save(request) =
            panel.key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL))
        else {
            panic!("save only");
        };
        assert_eq!(request.shared, Some(false));
        assert_eq!(request.expected_revision, None);
        assert!(request.script.contains("cmd(\"help\")"));
    }

    #[test]
    fn saved_macro_runs_pinned_revision_and_bad_args_never_execute() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(definition()));
        panel.args = Editor::from("{\"attempts\":3}".into());
        let Action::Run(spec) = panel.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE))
        else {
            panic!("run fixed revision");
        };
        assert_eq!(spec.macro_id.as_deref(), Some("enter_uboot"));
        assert_eq!(spec.revision, Some(9));
        assert!(spec.script.is_none());
        assert_eq!(spec.args["attempts"], 3);
        panel.args = Editor::from("not JSON".into());
        assert!(matches!(panel.run_selected(), Action::None));
        assert!(panel.message.contains("JSON"));
    }

    #[test]
    fn macro_edit_retains_unsaved_draft_and_cannot_override_revision_guard() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(definition()));
        panel.open_editor(panel.definition.clone());
        panel.script = Editor::from("cmd(\"version\");".into());
        let mut properties: serde_json::Value =
            serde_json::from_str(&panel.metadata.value()).unwrap();
        properties["expected_revision"] = 100.into();
        panel.metadata = Editor::from(properties.to_string());
        panel.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        panel.open_editor(panel.definition.clone());
        assert_eq!(panel.script.value(), "cmd(\"version\");");
        let Action::Save(request) =
            panel.key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL))
        else {
            panic!("save");
        };
        assert_eq!(request.expected_revision, Some(9));
    }

    #[test]
    fn macro_modal_never_converts_enter_or_ctrl_d_to_a_run() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(definition()));
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            Action::None
        ));
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL)),
            Action::None
        ));
        panel.open_editor(panel.definition.clone());
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            Action::None
        ));
        assert!(panel.script.value().starts_with('\n'));
    }
}

pub(super) enum IoCommand {
    List(bool),
    Load(String),
    Save(MacroSaveRequest),
}
pub(super) enum IoEvent {
    Catalog(Vec<MacroSummary>),
    Definition(MacroDefinition),
    Saved(MacroDefinition),
    Failed(String),
}
pub(super) struct Io {
    pub commands: mpsc::Sender<IoCommand>,
    pub events: mpsc::Receiver<IoEvent>,
}

pub(super) fn spawn(api: ApiClient) -> Io {
    let (commands, mut input) = mpsc::channel(4);
    let (output, events) = mpsc::channel(4);
    tokio::spawn(async move {
        while let Some(command) = input.recv().await {
            let result: Result<IoEvent> = async {
                match command {
                    IoCommand::List(include_drafts) => {
                        let mut entries = Vec::new();
                        let mut offset = None;
                        loop {
                            let page = api
                                .macros(&MacroListQuery {
                                    include_drafts,
                                    offset,
                                    limit: Some(100),
                                    ..Default::default()
                                })
                                .await?;
                            entries.extend(page.macros);
                            if page.next_offset.is_none() {
                                break;
                            }
                            ensure!(
                                page.next_offset != offset,
                                "macro catalog pagination did not advance"
                            );
                            offset = page.next_offset;
                        }
                        Ok(IoEvent::Catalog(entries))
                    }
                    IoCommand::Load(id) => {
                        let value = api
                            .macros(&MacroListQuery {
                                id: Some(id),
                                include_drafts: true,
                                ..Default::default()
                            })
                            .await?;
                        Ok(IoEvent::Definition(
                            value.definition.context("macro no longer exists")?,
                        ))
                    }
                    IoCommand::Save(request) => {
                        Ok(IoEvent::Saved(api.save_macro(&request).await?.definition))
                    }
                }
            }
            .await;
            let event = result.unwrap_or_else(|error| IoEvent::Failed(format!("{error:#}")));
            if output.send(event).await.is_err() {
                break;
            }
        }
    });
    Io { commands, events }
}

#[derive(Default)]
struct Editor {
    text: Vec<char>,
    cursor: usize,
}
impl Editor {
    fn from(text: String) -> Self {
        Self {
            text: text.chars().collect(),
            cursor: 0,
        }
    }
    fn value(&self) -> String {
        self.text.iter().collect()
    }
    fn paste(&mut self, text: &str) {
        if self.text.len().saturating_add(text.chars().count())
            > serial_protocol::MAX_MACRO_SOURCE_BYTES
        {
            return;
        }
        let chars = text
            .chars()
            .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
            .collect::<Vec<_>>();
        let count = chars.len();
        self.text.splice(self.cursor..self.cursor, chars);
        self.cursor += count;
    }
    fn key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Left => self.cursor = previous_grapheme(&self.text, self.cursor),
            KeyCode::Right => self.cursor = next_grapheme(&self.text, self.cursor),
            KeyCode::Home => {
                self.cursor = self.text[..self.cursor]
                    .iter()
                    .rposition(|c| *c == '\n')
                    .map_or(0, |i| i + 1)
            }
            KeyCode::End => {
                self.cursor += self.text[self.cursor..]
                    .iter()
                    .position(|c| *c == '\n')
                    .unwrap_or(self.text.len() - self.cursor)
            }
            KeyCode::Up | KeyCode::Down => {
                let start = self.text[..self.cursor]
                    .iter()
                    .rposition(|c| *c == '\n')
                    .map_or(0, |i| i + 1);
                let column = self.cursor - start;
                if key.code == KeyCode::Up && start > 0 {
                    let previous = self.text[..start - 1]
                        .iter()
                        .rposition(|c| *c == '\n')
                        .map_or(0, |i| i + 1);
                    self.cursor = (previous + column).min(start - 1);
                } else if key.code == KeyCode::Down
                    && let Some(end) = self.text[self.cursor..].iter().position(|c| *c == '\n')
                {
                    let next = self.cursor + end + 1;
                    let length = self.text[next..]
                        .iter()
                        .position(|c| *c == '\n')
                        .unwrap_or(self.text.len() - next);
                    self.cursor = next + column.min(length);
                }
            }
            KeyCode::Backspace if self.cursor > 0 => {
                let start = previous_grapheme(&self.text, self.cursor);
                self.text.drain(start..self.cursor);
                self.cursor = start;
            }
            KeyCode::Delete if self.cursor < self.text.len() => {
                let end = next_grapheme(&self.text, self.cursor);
                self.text.drain(self.cursor..end);
            }
            KeyCode::Enter => self.paste("\n"),
            KeyCode::Tab => self.paste("  "),
            KeyCode::Char(c)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.paste(&c.to_string())
            }
            _ => {}
        }
    }
}

pub(super) struct Panel {
    pub port: String,
    pub busy: bool,
    pub message: String,
    pub execution: Option<MacroExecutionInfo>,
    entries: Vec<MacroSummary>,
    selected: usize,
    read_scroll: usize,
    definition: Option<MacroDefinition>,
    include_drafts: bool,
    editing: bool,
    metadata_focus: bool,
    args_focus: bool,
    script: Editor,
    metadata: Editor,
    args: Editor,
    drafts: HashMap<String, (String, String)>,
}

impl Panel {
    pub fn new(port: String) -> Self {
        Self {
            port,
            busy: true,
            message: "正在读取宏目录…".into(),
            execution: None,
            entries: Vec::new(),
            selected: 0,
            read_scroll: 0,
            definition: None,
            include_drafts: false,
            editing: false,
            metadata_focus: false,
            args_focus: false,
            script: Editor::default(),
            metadata: Editor::default(),
            args: Editor::from("{}".into()),
            drafts: HashMap::new(),
        }
    }
    pub fn reopen(&mut self, port: String) -> Action {
        self.port = port;
        self.busy = true;
        Action::List(self.include_drafts)
    }
    fn editor(&mut self) -> &mut Editor {
        if self.args_focus {
            &mut self.args
        } else if self.metadata_focus {
            &mut self.metadata
        } else {
            &mut self.script
        }
    }
    pub fn paste(&mut self, value: &str) {
        if !self.busy && (self.editing || self.args_focus) {
            self.editor().paste(value);
        }
    }
    fn open_editor(&mut self, definition: Option<MacroDefinition>) {
        let request = definition.as_ref().map_or_else(
            || MacroSaveRequest {
                id: "new_macro".into(),
                name: "新宏".into(),
                description: "说明此宏的用途".into(),
                parameters: Default::default(),
                script: "// cmd 自动追加当前机型的换行符\ncmd(\"help\");\n".into(),
                expected_revision: None,
                shared: Some(false),
                applies_to: None,
            },
            |value| MacroSaveRequest {
                id: value.id.clone(),
                name: value.name.clone(),
                description: value.description.clone(),
                parameters: value.parameters.clone(),
                script: value.script.clone(),
                expected_revision: Some(value.revision),
                shared: Some(value.shared),
                applies_to: value.applies_to.clone(),
            },
        );
        self.script = Editor::from(request.script.clone());
        let mut metadata = serde_json::to_value(&request).unwrap();
        metadata.as_object_mut().unwrap().remove("script");
        self.metadata = Editor::from(serde_json::to_string_pretty(&metadata).unwrap());
        self.definition = definition;
        let key = self
            .definition
            .as_ref()
            .map_or("__new", |value| value.id.as_str());
        if let Some((metadata, script)) = self.drafts.get(key) {
            self.metadata = Editor::from(metadata.clone());
            self.script = Editor::from(script.clone());
        }
        self.editing = true;
        self.metadata_focus = self.definition.is_none();
        self.args_focus = false;
        self.message = "F2 切换脚本/属性；Ctrl-S 校验并保存；Esc 保留草稿返回目录".into();
    }
    pub fn event(&mut self, event: IoEvent) -> Action {
        self.busy = false;
        match event {
            IoEvent::Catalog(entries) => {
                let selected_id = self.definition.as_ref().map(|value| value.id.clone());
                self.entries = entries;
                self.selected = selected_id
                    .and_then(|id| self.entries.iter().position(|entry| entry.id == id))
                    .unwrap_or(0);
                self.message = format!(
                    "{} 个宏 · {}",
                    self.entries.len(),
                    if self.include_drafts {
                        "包含草稿"
                    } else {
                        "仅共享"
                    }
                );
                if let Some(entry) = self.entries.get(self.selected) {
                    return Action::Load(entry.id.clone());
                }
                self.definition = None;
            }
            IoEvent::Definition(value) => {
                self.script = Editor::from(value.script.clone());
                self.read_scroll = 0;
                self.definition = Some(value);
            }
            IoEvent::Saved(value) => {
                self.drafts.remove(
                    self.definition
                        .as_ref()
                        .map_or("__new", |value| value.id.as_str()),
                );
                self.message = format!(
                    "已保存 {} · v{}（校验通过，不代表真机已验证）",
                    value.name, value.revision
                );
                self.include_drafts |= !value.shared;
                self.editing = false;
                self.definition = Some(value);
                return Action::List(self.include_drafts);
            }
            IoEvent::Failed(error) => self.message = error,
        }
        Action::None
    }
    pub fn key(&mut self, key: KeyEvent) -> Action {
        if self.busy {
            return if key.code == KeyCode::Esc {
                Action::Close
            } else {
                Action::None
            };
        }
        if self.args_focus {
            if key.code == KeyCode::Esc {
                self.args_focus = false;
                return Action::None;
            }
            if key.code == KeyCode::Char('r') && key.modifiers.contains(KeyModifiers::CONTROL) {
                return self.run_selected();
            }
            self.args.key(key);
            return Action::None;
        }
        if self.editing {
            if key.code == KeyCode::Esc {
                let key = self
                    .definition
                    .as_ref()
                    .map_or("__new", |value| value.id.as_str())
                    .to_owned();
                self.drafts
                    .insert(key, (self.metadata.value(), self.script.value()));
                self.editing = false;
                return Action::None;
            }
            if key.code == KeyCode::F(2) {
                self.metadata_focus = !self.metadata_focus;
                return Action::None;
            }
            if key.code == KeyCode::Char('s') && key.modifiers.contains(KeyModifiers::CONTROL) {
                let request = (|| -> Result<MacroSaveRequest> {
                    let mut value: serde_json::Value =
                        serde_json::from_str(&self.metadata.value()).context("属性 JSON 无效")?;
                    value
                        .as_object_mut()
                        .context("属性必须为 JSON 对象")?
                        .insert("script".into(), self.script.value().into());
                    let mut request: MacroSaveRequest =
                        serde_json::from_value(value).context("宏属性缺失或类型不正确")?;
                    request.expected_revision =
                        self.definition.as_ref().map(|value| value.revision);
                    Ok(request)
                })();
                return match request {
                    Ok(request) => Action::Save(request),
                    Err(error) => {
                        self.message = format!("{error:#}");
                        Action::None
                    }
                };
            }
            self.editor().key(key);
            return Action::None;
        }
        match key.code {
            KeyCode::Esc => Action::Close,
            KeyCode::PageUp => {
                self.read_scroll = self.read_scroll.saturating_sub(10);
                Action::None
            }
            KeyCode::PageDown => {
                self.read_scroll = self
                    .read_scroll
                    .saturating_add(10)
                    .min(self.script.text.iter().filter(|c| **c == '\n').count());
                Action::None
            }
            KeyCode::Up | KeyCode::Down => {
                if key.code == KeyCode::Up {
                    self.selected = self.selected.saturating_sub(1);
                } else {
                    self.selected = (self.selected + 1).min(self.entries.len().saturating_sub(1));
                }
                self.entries
                    .get(self.selected)
                    .map_or(Action::None, |entry| Action::Load(entry.id.clone()))
            }
            KeyCode::Char('n') => {
                self.open_editor(None);
                Action::None
            }
            KeyCode::Char('e') => {
                if self.definition.is_some() {
                    self.open_editor(self.definition.clone());
                }
                Action::None
            }
            KeyCode::Char('a') => {
                self.args_focus = true;
                self.message = "编辑本次参数 JSON；Ctrl-R 执行，Esc 返回".into();
                Action::None
            }
            KeyCode::Char('r') => self.run_selected(),
            KeyCode::Char('x') => Action::Stop,
            KeyCode::Char('v') => {
                self.include_drafts = !self.include_drafts;
                Action::List(self.include_drafts)
            }
            KeyCode::F(5) => Action::List(self.include_drafts),
            _ => Action::None,
        }
    }
    fn run_selected(&mut self) -> Action {
        let Some(definition) = &self.definition else {
            return Action::None;
        };
        match serde_json::from_str(&self.args.value()) {
            Ok(args) => {
                self.args_focus = false;
                Action::Run(MacroRunSpec {
                    macro_id: Some(definition.id.clone()),
                    revision: Some(definition.revision),
                    script: None,
                    description: None,
                    args,
                    timeout_seconds: 30,
                })
            }
            Err(error) => {
                self.message = format!("参数 JSON 无效：{error}");
                Action::None
            }
        }
    }
    pub fn draw(&self, frame: &mut Frame<'_>, visible_cursor: bool) {
        let area = centered_rect(
            frame.area().width.saturating_sub(4),
            frame.area().height.saturating_sub(2),
            frame.area(),
        );
        frame.render_widget(Clear, area);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(format!(
                " 宏 · {} · n 新建 / e 编辑 / a 参数 / r 运行 / x 停止 / v 草稿 / Esc 返回 ",
                safe_inline(&self.port)
            ));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let chunks = Layout::vertical([Constraint::Min(3), Constraint::Length(2)]).split(inner);
        let panes = Layout::horizontal([Constraint::Percentage(28), Constraint::Percentage(72)])
            .split(chunks[0]);
        let start = self
            .selected
            .saturating_sub(panes[0].height.saturating_sub(1) as usize);
        let rows = self
            .entries
            .iter()
            .enumerate()
            .skip(start)
            .map(|(index, entry)| {
                selected_menu_line(
                    index,
                    self.selected,
                    format!(
                        "{} · v{}{}",
                        safe_inline(&entry.name),
                        entry.revision,
                        if entry.shared { "" } else { " [草稿]" }
                    ),
                )
            })
            .collect::<Vec<_>>();
        frame.render_widget(Paragraph::new(rows), panes[0]);
        let editor = if self.args_focus {
            &self.args
        } else if self.editing && self.metadata_focus {
            &self.metadata
        } else {
            &self.script
        };
        let title = if self.args_focus {
            "本次参数 JSON · Ctrl-R 运行"
        } else if self.editing {
            if self.metadata_focus {
                "属性 JSON · F2 脚本 · Ctrl-S 保存"
            } else {
                "脚本 · F2 属性 · Ctrl-S 保存"
            }
        } else {
            "脚本（只读）"
        };
        let block = Block::default()
            .borders(Borders::LEFT | Borders::TOP)
            .title(title);
        let content = block.inner(panes[1]);
        frame.render_widget(block, panes[1]);
        let before: String = editor.text[..editor.cursor].iter().collect();
        let cursor_row = before.chars().filter(|c| *c == '\n').count();
        let cursor_chars = before.rsplit('\n').next().unwrap_or("").chars().count();
        let first = if self.editing || self.args_focus {
            cursor_row.saturating_sub(content.height.saturating_sub(2) as usize)
        } else {
            self.read_scroll
        };
        let rows = editor
            .value()
            .split('\n')
            .enumerate()
            .skip(first)
            .take(content.height as usize)
            .map(|(index, text)| {
                if index == cursor_row && (self.editing || self.args_focus) {
                    let (projected, cursor) = line_input_projection(
                        &text.chars().collect::<Vec<_>>(),
                        cursor_chars,
                        content.width,
                    );
                    line_with_software_cursor(projected, cursor, visible_cursor)
                } else {
                    Line::from(clip_display(&format!("  {text}"), content.width as usize))
                }
            })
            .collect::<Vec<_>>();
        frame.render_widget(Paragraph::new(rows), content);
        let execution = self
            .execution
            .as_ref()
            .map(|execution| {
                format!(
                    "{:?} · 行{} · {}次写入 · {}",
                    execution.status,
                    execution.line,
                    execution.writes,
                    execution.message.as_deref().unwrap_or("")
                )
            })
            .unwrap_or_default();
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(safe_inline(&self.message)),
                Line::from(safe_inline(&execution)),
            ]),
            chunks[1],
        );
    }
}
