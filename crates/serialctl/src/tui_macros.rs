//! Local Macro catalog/editor. Saves never execute; runs use a fixed revision.
use super::*;
use serial_protocol::{
    MAX_MACRO_TIMEOUT_SECONDS, MacroDefinition, MacroExecutionInfo, MacroListQuery, MacroParameter,
    MacroParameterType, MacroRunSpec, MacroSaveRequest, MacroSummary,
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

    fn parameterized_definition() -> MacroDefinition {
        let mut value = definition();
        value.parameters = serde_json::from_value(serde_json::json!({
            "attempts": {"type": "integer", "default": 3, "minimum": 1, "maximum": 10},
            "enabled": {"type": "boolean", "default": false},
            "path": {"type": "string", "description": "待查询路径"}
        }))
        .unwrap();
        value
    }

    fn set_parameter(panel: &mut Panel, name: &str, text: &str) {
        let field = panel
            .selected_options()
            .unwrap()
            .parameters
            .iter_mut()
            .find(|field| field.name == name)
            .unwrap();
        field.editor = Editor::from(text.into());
        field.supplied = true;
    }

    #[test]
    fn run_form_sends_typed_values_with_pinned_revision_and_custom_timeout() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(parameterized_definition()));
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE)),
            Action::None
        ));
        assert!(panel.args_focus, "reviewing arguments never executes");
        panel.selected_options().unwrap().selected = 0;
        panel.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        panel.paste("90");
        panel.selected_options().unwrap().selected = 3;
        panel.paste("\"; cmd(\"reboot\"); // 中文");
        let Action::Run(spec) = panel.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL))
        else {
            panic!("run fixed revision");
        };
        assert_eq!(spec.macro_id.as_deref(), Some("enter_uboot"));
        assert_eq!(spec.revision, Some(9));
        assert!(spec.script.is_none());
        assert_eq!(spec.timeout_seconds, 90);
        assert_eq!(spec.args["attempts"], 3);
        assert_eq!(spec.args["enabled"], false);
        assert_eq!(spec.args["path"], "\"; cmd(\"reboot\"); // 中文");
    }

    #[test]
    fn invalid_or_missing_parameters_never_start_a_macro() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(parameterized_definition()));
        panel.open_options(true);
        assert!(matches!(panel.run_selected(), Action::None));
        assert!(panel.message.contains("请填写参数 path"));
        assert_eq!(panel.selected_options().unwrap().selected, 3);
        // Enter explicitly accepts an empty string but cannot execute.
        assert!(matches!(
            panel.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            Action::None
        ));
        assert!(matches!(panel.run_selected(), Action::Run(_)));
        for text in ["0", "11", "1.5", "5e2", "9223372036854775808"] {
            set_parameter(&mut panel, "attempts", text);
            assert!(matches!(panel.run_selected(), Action::None), "{text}");
            assert!(panel.message.contains("attempts"));
        }
        set_parameter(&mut panel, "attempts", "5");
        set_parameter(&mut panel, "enabled", "yes");
        assert!(matches!(panel.run_selected(), Action::None));
        assert!(panel.message.contains("true 或 false"));
        set_parameter(&mut panel, "enabled", "true");
        set_parameter(&mut panel, "path", &"中".repeat(1366));
        assert!(matches!(panel.run_selected(), Action::None));
        assert!(panel.message.contains("4096"));
    }

    #[test]
    fn timeout_is_editable_and_validated_before_start() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(definition()));
        panel.key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE));
        assert!(panel.args_focus);
        assert_eq!(panel.selected_options().unwrap().selected, 0);
        for value in ["", "0", "121", "-1", "1.5", "no", "18446744073709551616"] {
            panel.selected_options().unwrap().timeout = Editor::from(value.into());
            assert!(matches!(panel.run_selected(), Action::None), "{value}");
            assert!(panel.message.contains("1–120"));
        }
        for value in [1, 30, 120] {
            panel.selected_options().unwrap().timeout = Editor::from(value.to_string());
            let Action::Run(spec) = panel.run_selected() else {
                panic!("valid timeout");
            };
            assert_eq!(spec.timeout_seconds, value);
        }
    }

    #[test]
    fn run_options_support_keyboard_navigation_boolean_toggle_and_safe_paste() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(parameterized_definition()));
        panel.open_options(true);
        assert_eq!(panel.selected_options().unwrap().selected, 1);
        panel.key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        panel.key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        assert_eq!(
            panel.selected_options().unwrap().parameters[1]
                .editor
                .value(),
            "true"
        );
        panel.key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        assert_eq!(
            panel.selected_options().unwrap().parameters[1]
                .editor
                .value(),
            "false"
        );
        panel.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        panel.paste("one\ntwo");
        assert!(!panel.selected_options().unwrap().parameters[2].supplied);
        panel.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
        assert!(!panel.selected_options().unwrap().parameters[2].supplied);
        panel.paste("hello");
        panel.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!panel.args_focus);
        panel.open_options(true);
        assert_eq!(
            panel.selected_options().unwrap().parameters[2]
                .editor
                .value(),
            "hello"
        );
        panel.key(KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT));
        assert_eq!(panel.selected_options().unwrap().selected, 0);
        panel.key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(panel.selected_options().unwrap().selected, 3);
    }

    #[test]
    fn run_values_are_scoped_to_macro_and_reset_when_revision_changes() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(parameterized_definition()));
        set_parameter(&mut panel, "path", "original");
        panel.selected_options().unwrap().timeout = Editor::from("65".into());
        let mut other = definition();
        other.id = "other_macro".into();
        panel.event(IoEvent::Definition(other));
        let options = panel.selected_options().unwrap();
        assert!(options.parameters.is_empty());
        assert_eq!(options.timeout.value(), "30");
        panel.event(IoEvent::Definition(parameterized_definition()));
        let options = panel.selected_options().unwrap();
        assert_eq!(options.parameters[2].editor.value(), "original");
        assert_eq!(options.timeout.value(), "65");
        let mut newer = parameterized_definition();
        newer.revision += 1;
        panel.event(IoEvent::Definition(newer));
        let options = panel.selected_options().unwrap();
        assert!(!options.parameters[2].supplied);
        assert_eq!(options.timeout.value(), "30");
        assert_eq!(options.revision, 10);
    }

    #[test]
    fn run_form_shows_schema_and_scrolls_to_selected_field() {
        let mut panel = Panel::new("COM3".into());
        panel.event(IoEvent::Definition(parameterized_definition()));
        panel.open_options(false);
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(110, 22)).unwrap();
        terminal.draw(|frame| panel.draw(frame, true)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        let rendered = rendered.replace(' ', "");
        assert!(rendered.contains("超时（秒）"));
        assert!(rendered.contains("attempts"));
        assert!(rendered.contains("待查询路径"));
        panel.selected_options().unwrap().selected = 3;
        let mut small = Terminal::new(ratatui::backend::TestBackend::new(60, 10)).unwrap();
        small.draw(|frame| panel.draw(frame, true)).unwrap();
        let rendered = small
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("path"));
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

struct RunParameter {
    name: String,
    parameter: MacroParameter,
    editor: Editor,
    supplied: bool,
}

struct RunOptions {
    revision: u64,
    timeout: Editor,
    parameters: Vec<RunParameter>,
    selected: usize,
}

impl RunOptions {
    fn new(definition: &MacroDefinition) -> Self {
        Self {
            revision: definition.revision,
            timeout: Editor::from("30".into()),
            parameters: definition
                .parameters
                .iter()
                .map(|(name, parameter)| RunParameter {
                    name: name.clone(),
                    parameter: parameter.clone(),
                    editor: Editor::from(parameter.default.as_ref().map_or_else(
                        String::new,
                        |value| {
                            value
                                .as_str()
                                .map_or_else(|| value.to_string(), str::to_owned)
                        },
                    )),
                    supplied: parameter.default.is_some(),
                })
                .collect(),
            selected: 0,
        }
    }

    fn editor(&mut self) -> &mut Editor {
        if self.selected == 0 {
            &mut self.timeout
        } else {
            let field = &mut self.parameters[self.selected - 1];
            field.supplied = true;
            &mut field.editor
        }
    }

    fn paste(&mut self, text: &str) {
        // Run fields are single-line inputs; pasted newlines must not submit or
        // silently combine separate values into a different serial command.
        if !text.chars().any(char::is_control) {
            self.editor().paste(text);
        }
    }

    fn key(&mut self, key: KeyEvent) {
        let count = self.parameters.len() + 1;
        match key.code {
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                *self.editor() = Editor::default();
            }
            KeyCode::BackTab | KeyCode::Up => self.selected = (self.selected + count - 1) % count,
            KeyCode::Tab | KeyCode::Down => self.selected = (self.selected + 1) % count,
            KeyCode::Enter => {
                // Explicitly accepting an empty string is different from not
                // supplying a required value. Enter never starts a macro.
                if self.selected > 0 {
                    self.parameters[self.selected - 1].supplied = true;
                }
                self.selected = (self.selected + 1) % count;
            }
            KeyCode::Char(' ') | KeyCode::Left | KeyCode::Right
                if self.selected > 0
                    && self.parameters[self.selected - 1].parameter.value_type
                        == MacroParameterType::Boolean =>
            {
                let field = &mut self.parameters[self.selected - 1];
                let value = if field.editor.value() == "true" {
                    "false"
                } else {
                    "true"
                };
                field.editor = Editor::from(value.into());
                field.supplied = true;
            }
            KeyCode::Char(_) | KeyCode::Backspace | KeyCode::Delete
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.editor().key(key);
            }
            KeyCode::Home | KeyCode::End | KeyCode::Left | KeyCode::Right => {
                // Moving the cursor does not fill a required parameter.
                if self.selected == 0 {
                    self.timeout.key(key);
                } else {
                    self.parameters[self.selected - 1].editor.key(key);
                }
            }
            _ => {}
        }
    }

    fn spec(&mut self, definition: &MacroDefinition) -> Result<MacroRunSpec> {
        let previous_selection = self.selected;
        self.selected = 0;
        let timeout_seconds = self
            .timeout
            .value()
            .parse::<u64>()
            .context("超时必须是 1–120 秒之间的整数")?;
        ensure!(
            (1..=MAX_MACRO_TIMEOUT_SECONDS).contains(&timeout_seconds),
            "超时必须是 1–120 秒之间的整数"
        );
        let mut args = std::collections::BTreeMap::new();
        for (index, field) in self.parameters.iter().enumerate() {
            self.selected = index + 1;
            let name = &field.name;
            ensure!(field.supplied, "请填写参数 {name}（空字符串按 Enter 确认）");
            let text = field.editor.value();
            let value = match field.parameter.value_type {
                MacroParameterType::String => {
                    ensure!(text.len() <= 4096, "参数 {name} 不能超过 4096 字节");
                    serde_json::Value::String(text)
                }
                MacroParameterType::Boolean => {
                    let value = match text.as_str() {
                        "true" => true,
                        "false" => false,
                        _ => bail!("参数 {name} 必须是 true 或 false（空格切换）"),
                    };
                    serde_json::Value::Bool(value)
                }
                MacroParameterType::Integer => {
                    let value = text
                        .parse::<i64>()
                        .with_context(|| format!("参数 {name} 必须是 64 位整数"))?;
                    ensure!(
                        field.parameter.minimum.is_none_or(|min| value >= min)
                            && field.parameter.maximum.is_none_or(|max| value <= max),
                        "参数 {name} 超出允许范围"
                    );
                    serde_json::Value::from(value)
                }
            };
            args.insert(name.clone(), value);
        }
        self.selected = previous_selection;
        Ok(MacroRunSpec {
            macro_id: Some(definition.id.clone()),
            revision: Some(definition.revision),
            script: None,
            description: None,
            args,
            timeout_seconds,
        })
    }

    fn draw(&self, frame: &mut Frame<'_>, area: Rect, visible_cursor: bool) {
        let block = Block::default()
            .borders(Borders::LEFT | Borders::TOP)
            .title(format!(
                " 本次运行 · v{} · Tab/↑↓ 切换 · Ctrl-R 运行 ",
                self.revision
            ));
        let content = block.inner(area);
        frame.render_widget(block, area);
        let field_height = 3;
        let visible = (content.height as usize / field_height).max(1);
        let first = self.selected.saturating_sub(visible - 1);
        let mut rows = Vec::new();
        for index in first..(first + visible).min(self.parameters.len() + 1) {
            let (label, hint, editor) = if index == 0 {
                (
                    "超时（秒）".to_owned(),
                    "总时限 1–120 秒；默认 30 秒".to_owned(),
                    &self.timeout,
                )
            } else {
                let field = &self.parameters[index - 1];
                let kind = match field.parameter.value_type {
                    MacroParameterType::String => "string",
                    MacroParameterType::Integer => "integer",
                    MacroParameterType::Boolean => "boolean · 空格切换",
                };
                let state = if field.supplied {
                    "已填写"
                } else {
                    "必填"
                };
                let mut hint = field.parameter.description.clone().unwrap_or_default();
                if let Some(min) = field.parameter.minimum {
                    hint.push_str(&format!(" 最小 {min}"));
                }
                if let Some(max) = field.parameter.maximum {
                    hint.push_str(&format!(" 最大 {max}"));
                }
                (
                    format!("{} · {kind} · {state}", field.name),
                    hint,
                    &field.editor,
                )
            };
            rows.push(Line::from(Span::styled(
                safe_inline(&label),
                if index == self.selected {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default()
                },
            )));
            if index == self.selected {
                let (projected, cursor) =
                    line_input_projection(&editor.text, editor.cursor, content.width);
                rows.push(line_with_software_cursor(projected, cursor, visible_cursor));
            } else {
                rows.push(Line::from(clip_display(
                    &format!("  {}", safe_inline(&editor.value())),
                    content.width as usize,
                )));
            }
            rows.push(Line::from(Span::styled(
                clip_display(&safe_inline(&hint), content.width as usize),
                Style::default().fg(Color::DarkGray),
            )));
        }
        frame.render_widget(Paragraph::new(rows), content);
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
    run_options: HashMap<String, RunOptions>,
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
            run_options: HashMap::new(),
            drafts: HashMap::new(),
        }
    }
    pub fn reopen(&mut self, port: String) -> Action {
        self.port = port;
        self.busy = true;
        Action::List(self.include_drafts)
    }
    fn editor(&mut self) -> &mut Editor {
        if self.metadata_focus {
            &mut self.metadata
        } else {
            &mut self.script
        }
    }
    pub fn paste(&mut self, value: &str) {
        if self.busy {
            return;
        }
        if self.args_focus {
            if let Some(options) = self.selected_options() {
                options.paste(value);
            }
        } else if self.editing {
            self.editor().paste(value);
        }
    }
    fn selected_options(&mut self) -> Option<&mut RunOptions> {
        let definition = self.definition.as_ref()?;
        let options = self
            .run_options
            .entry(definition.id.clone())
            .or_insert_with(|| RunOptions::new(definition));
        if options.revision != definition.revision {
            *options = RunOptions::new(definition);
        }
        Some(options)
    }
    fn open_options(&mut self, parameters: bool) {
        if let Some(options) = self.selected_options() {
            options.selected = usize::from(parameters && !options.parameters.is_empty());
            self.args_focus = true;
            self.message =
                "Tab 切换；空格切换布尔值；Ctrl-U 清空；Enter 确认字段；Ctrl-R 运行；Esc 保留返回"
                    .into();
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
                self.message = "本次参数和超时已保留，尚未执行".into();
                return Action::None;
            }
            if key.code == KeyCode::Char('r') && key.modifiers.contains(KeyModifiers::CONTROL) {
                return self.run_selected();
            }
            if let Some(options) = self.selected_options() {
                options.key(key);
            }
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
            KeyCode::Char('a') | KeyCode::Char('r') | KeyCode::Char('t')
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.open_options(key.code != KeyCode::Char('t'));
                Action::None
            }
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
        let Some(definition) = self.definition.clone() else {
            return Action::None;
        };
        let Some(options) = self.selected_options() else {
            return Action::None;
        };
        match options.spec(&definition) {
            Ok(spec) => {
                self.args_focus = false;
                Action::Run(spec)
            }
            Err(error) => {
                self.message = format!("{error:#}");
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
                " 宏 · {} · n 新建 / e 编辑 / r 参数与运行 / t 超时 / x 停止 / v 草稿 / Esc 返回 ",
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
        if self.args_focus {
            if let Some(options) = self
                .definition
                .as_ref()
                .and_then(|definition| self.run_options.get(&definition.id))
            {
                options.draw(frame, panes[1], visible_cursor);
            }
        } else {
            let editor = if self.editing && self.metadata_focus {
                &self.metadata
            } else {
                &self.script
            };
            let title = if self.editing {
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
        }
        let execution = self
            .execution
            .as_ref()
            .map(|execution| {
                format!(
                    "{:?} · 行{} · 发送{} · 输入核对{} · 只发送{} · {}",
                    execution.status,
                    execution.line,
                    execution.writes,
                    execution.input_verified_writes,
                    execution.send_only_writes,
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
