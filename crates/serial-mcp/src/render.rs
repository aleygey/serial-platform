use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use serial_protocol::{Direction, TimelineEvent};

pub struct RenderedEvents {
    pub text: String,
    pub events: Vec<Value>,
    pub text_truncated: bool,
    pub repeated_lines_collapsed: usize,
}

pub fn render_events(
    events: &[TimelineEvent],
    max_chars: usize,
    include_raw: bool,
    echo: Option<&str>,
) -> RenderedEvents {
    let mut rx_bytes = Vec::new();
    for event in events {
        if event.direction == Direction::Rx {
            rx_bytes.extend_from_slice(&event.data);
        }
    }
    let mut text = terminal_text(&rx_bytes);
    if let Some(echo) = echo {
        text = remove_leading_echo(text, echo);
    }
    let (text, repeated_lines_collapsed) = collapse_exact_repeats(&text);
    let (text, text_truncated) = limit_tail(text, max_chars);

    let events = events
        .iter()
        .map(|event| {
            let mut summary = json!({
                "seq": event.seq,
                "generation": event.generation,
                "kind": event.kind,
                "direction": event.direction,
                "actor": event.actor,
                "run_id": event.run_id,
                "operation_id": event.operation_id,
                "durable": event.durable,
                "byte_count": event.data.len(),
            });
            if include_raw && !event.data.is_empty() {
                summary["data_base64"] = Value::String(BASE64.encode(&event.data));
            }
            summary
        })
        .collect();

    RenderedEvents {
        text,
        events,
        text_truncated,
        repeated_lines_collapsed,
    }
}

fn terminal_text(bytes: &[u8]) -> String {
    let stripped = strip_ansi(bytes);
    let decoded = String::from_utf8_lossy(&stripped);
    let mut output = String::with_capacity(decoded.len());
    let mut chars = decoded.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                output.push('\n');
            }
            '\n' | '\t' => output.push(ch),
            '\u{8}' | '\u{7f}' => {
                if output.chars().next_back().is_some_and(|last| last != '\n') {
                    output.pop();
                }
            }
            ch if ch.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(output, "\\u{{{:04x}}}", ch as u32);
            }
            _ => output.push(ch),
        }
    }
    output
}

fn strip_ansi(bytes: &[u8]) -> Vec<u8> {
    #[derive(Clone, Copy)]
    enum State {
        Ground,
        Escape,
        Csi,
        Osc,
        OscEscape,
    }

    let mut state = State::Ground;
    let mut output = Vec::with_capacity(bytes.len());
    for &byte in bytes {
        state = match state {
            State::Ground if byte == 0x1b => State::Escape,
            State::Ground => {
                output.push(byte);
                State::Ground
            }
            State::Escape if byte == b'[' => State::Csi,
            State::Escape if byte == b']' => State::Osc,
            State::Escape => State::Ground,
            State::Csi if (0x40..=0x7e).contains(&byte) => State::Ground,
            State::Csi => State::Csi,
            State::Osc if byte == 0x07 => State::Ground,
            State::Osc if byte == 0x1b => State::OscEscape,
            State::Osc => State::Osc,
            State::OscEscape if byte == b'\\' => State::Ground,
            State::OscEscape if byte == 0x1b => State::OscEscape,
            State::OscEscape => State::Osc,
        };
    }
    output
}

fn remove_leading_echo(mut text: String, echo: &str) -> String {
    let normalized_echo = echo.replace("\r\n", "\n").replace('\r', "\n");
    let command = normalized_echo.trim_end_matches('\n');
    if text.starts_with(command) {
        let remainder = &text[command.len()..];
        if remainder.is_empty() || remainder.starts_with('\n') {
            text.drain(..command.len());
            while text.starts_with('\n') {
                text.remove(0);
            }
        }
    }
    text
}

fn collapse_exact_repeats(text: &str) -> (String, usize) {
    let mut output = String::new();
    let mut previous: Option<&str> = None;
    let mut count = 0usize;
    let mut collapsed = 0usize;

    let flush =
        |output: &mut String, previous: Option<&str>, count: usize, collapsed: &mut usize| {
            if let Some(line) = previous {
                output.push_str(line);
                output.push('\n');
                if count > 1 {
                    use std::fmt::Write as _;
                    let _ = writeln!(output, "[previous line repeated {} more times]", count - 1);
                    *collapsed += count - 1;
                }
            }
        };

    for line in text.lines() {
        if previous == Some(line) {
            count += 1;
        } else {
            flush(&mut output, previous, count, &mut collapsed);
            previous = Some(line);
            count = 1;
        }
    }
    flush(&mut output, previous, count, &mut collapsed);
    if !text.ends_with('\n') {
        output.pop();
    }
    (output, collapsed)
}

fn limit_tail(text: String, max_chars: usize) -> (String, bool) {
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return (text, false);
    }
    let tail: String = text.chars().skip(char_count - max_chars).collect();
    (format!("[earlier output omitted]\n{tail}"), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_rendering_removes_ansi_and_applies_controls() {
        assert_eq!(
            terminal_text(b"\x1b[31mERR\x1b[0m\r\nabc\x08d\0"),
            "ERR\nabd\\u{0000}"
        );
    }

    #[test]
    fn exact_repeats_are_collapsed_without_guessing_timestamp_equivalence() {
        let (text, count) = collapse_exact_repeats("a\na\na\nb\n[1] x\n[2] x\n");
        assert_eq!(count, 2);
        assert!(text.contains("repeated 2 more times"));
        assert!(text.contains("[1] x\n[2] x"));
    }

    #[test]
    fn output_limit_keeps_the_most_recent_context() {
        let (text, truncated) = limit_tail("abcdef".into(), 3);
        assert!(truncated);
        assert!(text.ends_with("def"));
    }
}
