use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use serial_protocol::{Direction, TimelineEvent};

pub struct RenderedEvents {
    pub text: String,
    pub events: Vec<Value>,
    pub text_truncated: bool,
    pub repeated_lines_collapsed: usize,
    pub match_excerpt: Option<MatchExcerptSummary>,
}

pub struct MatchExcerptSummary {
    pub matched_lines: usize,
    pub omitted_lines: usize,
    pub context_lines: usize,
}

pub struct MatchExcerptOptions<'a> {
    pub literal: &'a str,
    pub context_lines: usize,
}

pub struct RenderOptions<'a> {
    pub max_chars: usize,
    pub include_raw: bool,
    pub echo: Option<&'a str>,
    /// Collapse byte-identical adjacent lines. Disable when the exact line
    /// stream matters more than a compact rendering.
    pub collapse_repeats: bool,
    /// Populate the per-event summary array. Omitted by default because the
    /// array dominates token usage; cursor fields are always reported by the
    /// caller regardless of this flag. Raw bytes imply the array.
    pub include_events: bool,
    /// For search responses, keep only matching lines plus bounded surrounding
    /// context. Event summaries and raw bytes remain exact and unaffected.
    pub match_excerpt: Option<MatchExcerptOptions<'a>>,
}

pub fn render_events(events: &[TimelineEvent], options: RenderOptions) -> RenderedEvents {
    // Captures normally contain both the confirmed TX audit and device RX; in
    // that case present RX only. A TX-filtered read/search still needs useful
    // text, so fall back to TX when there is no RX event at all.
    let display_direction = if events.iter().any(|event| event.direction == Direction::Rx) {
        Direction::Rx
    } else {
        Direction::Tx
    };
    let mut stream_bytes = Vec::new();
    for event in events {
        if event.direction == display_direction {
            stream_bytes.extend_from_slice(&event.data);
        }
    }
    let mut text = terminal_text(&stream_bytes);
    if let Some(echo) = options.echo {
        text = remove_leading_echo(text, echo);
    }
    let (text, match_excerpt) = match options.match_excerpt {
        Some(excerpt) => {
            let (text, summary) =
                literal_match_excerpt(&text, excerpt.literal, excerpt.context_lines);
            (text, Some(summary))
        }
        None => (text, None),
    };
    let (text, repeated_lines_collapsed) = if options.collapse_repeats {
        collapse_exact_repeats(&text)
    } else {
        (text, 0)
    };
    let (text, text_truncated) = limit_tail(text, options.max_chars);

    let events = if options.include_events || options.include_raw {
        event_summaries(events, options.include_raw)
    } else {
        Vec::new()
    };

    RenderedEvents {
        text,
        events,
        text_truncated,
        repeated_lines_collapsed,
        match_excerpt,
    }
}

fn event_summaries(events: &[TimelineEvent], include_raw: bool) -> Vec<Value> {
    events
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
        .collect()
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
    if command.is_empty() {
        return text;
    }

    let mut text_chars = text.char_indices().peekable();
    let mut matched_any = false;
    for expected in command.chars() {
        if expected == '\n' {
            let Some(&(_, '\n')) = text_chars.peek() else {
                return text;
            };
            while text_chars.next_if(|(_, ch)| *ch == '\n').is_some() {}
            matched_any = true;
            continue;
        }

        if matched_any && text_chars.peek().is_some_and(|(_, ch)| *ch == '\n') {
            // Several UART consoles hard-wrap their echoed input as CR CR LF.
            // terminal_text normalizes that sequence to two newlines. Treat a
            // run of newlines inside an otherwise exact echoed command as a
            // visual wrap, not as command output.
            while text_chars.next_if(|(_, ch)| *ch == '\n').is_some() {}
        }
        let Some((_, actual)) = text_chars.next() else {
            return text;
        };
        if actual != expected {
            return text;
        }
        matched_any = true;
    }

    let matched_end = text_chars
        .peek()
        .map(|(index, _)| *index)
        .unwrap_or(text.len());
    let remainder = &text[matched_end..];
    if remainder.is_empty() || remainder.starts_with('\n') {
        text.drain(..matched_end);
        while text.starts_with('\n') {
            text.remove(0);
        }
    }
    text
}

fn literal_match_excerpt(
    text: &str,
    literal: &str,
    context_lines: usize,
) -> (String, MatchExcerptSummary) {
    let literal = literal.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    if lines.is_empty() || literal.is_empty() {
        return (
            text.to_string(),
            MatchExcerptSummary {
                matched_lines: 0,
                omitted_lines: 0,
                context_lines,
            },
        );
    }

    let mut matching = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.contains(&literal) {
            matching.push(index);
        }
    }

    // A literal may cross a journal/event or line boundary. The server has
    // already established the raw-byte match, so anchor the excerpt at the
    // line containing the first byte when the per-line scan cannot.
    if matching.is_empty()
        && let Some(byte_index) = text.find(&literal)
    {
        let mut consumed = 0usize;
        for (index, line) in lines.iter().enumerate() {
            if byte_index < consumed + line.len() {
                matching.push(index);
                break;
            }
            consumed += line.len();
        }
    }

    if matching.is_empty() {
        return (
            "[literal matched raw bytes but is not visible after terminal normalization; use include_events=true and include_raw=true for exact evidence]\n".into(),
            MatchExcerptSummary {
                matched_lines: 0,
                omitted_lines: lines.len(),
                context_lines,
            },
        );
    }

    let mut keep = vec![false; lines.len()];
    for &index in &matching {
        let start = index.saturating_sub(context_lines);
        let end = index
            .saturating_add(context_lines)
            .saturating_add(1)
            .min(lines.len());
        keep[start..end].fill(true);
    }

    let mut output = String::new();
    let mut omitted_lines = 0usize;
    let mut index = 0usize;
    while index < lines.len() {
        if keep[index] {
            output.push_str(lines[index]);
            index += 1;
            continue;
        }
        let start = index;
        while index < lines.len() && !keep[index] {
            index += 1;
        }
        let omitted = index - start;
        omitted_lines += omitted;
        use std::fmt::Write as _;
        let _ = writeln!(output, "[... {omitted} non-matching lines omitted ...]");
    }

    (
        output,
        MatchExcerptSummary {
            matched_lines: matching.len(),
            omitted_lines,
            context_lines,
        },
    )
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
    use serial_protocol::EventKind;

    fn rx_event(seq: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            slot_id: "bench".into(),
            daemon_epoch: uuid::Uuid::nil(),
            seq,
            generation: 1,
            wall_time_ns: 0,
            monotonic_time_ns: 0,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: data.to_vec(),
            metadata: Default::default(),
            durable: false,
        }
    }

    fn tx_event(seq: u64, data: &[u8]) -> TimelineEvent {
        let mut event = rx_event(seq, data);
        event.kind = EventKind::Tx;
        event.direction = Direction::Tx;
        event
    }

    #[test]
    fn terminal_rendering_removes_ansi_and_applies_controls() {
        assert_eq!(
            terminal_text(b"\x1b[31mERR\x1b[0m\r\nabc\x08d\0"),
            "ERR\nabd\\u{0000}"
        );
    }

    #[test]
    fn echoed_command_cleanup_tolerates_cr_cr_lf_hard_wraps() {
        let command = format!("printf '{}'", "x".repeat(120));
        assert!(command.len() > 64);
        let (first, rest) = command.split_at(64);
        let raw = format!("{first}\r\r\n{rest}\r\r\ni=1;\r\n[root@luckfox tmp]# ");
        let rendered = render_events(
            &[rx_event(990, raw.as_bytes())],
            RenderOptions {
                max_chars: 4096,
                include_raw: false,
                echo: Some(&command),
                collapse_repeats: false,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert_eq!(rendered.text, "i=1;\n[root@luckfox tmp]# ");
    }

    #[test]
    fn echo_cleanup_replays_a_nonmatching_prefix_losslessly() {
        assert_eq!(
            remove_leading_echo("prefix\nreal output\n".into(), "different command"),
            "prefix\nreal output\n"
        );
    }

    #[test]
    fn tx_only_query_has_human_readable_text() {
        let rendered = render_events(
            &[tx_event(1, b"progress-command\r")],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: false,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert_eq!(rendered.text, "progress-command\n");
    }

    #[test]
    fn search_excerpt_keeps_only_bounded_context_around_a_large_rx_match() {
        let output: String = (1..=600)
            .map(|index| format!("progress{index}\r\n"))
            .collect();
        let rendered = render_events(
            &[rx_event(1028, output.as_bytes())],
            RenderOptions {
                max_chars: 16_000,
                include_raw: false,
                echo: None,
                collapse_repeats: false,
                include_events: false,
                match_excerpt: Some(MatchExcerptOptions {
                    literal: "progress317",
                    context_lines: 5,
                }),
            },
        );
        assert!(rendered.text.contains("progress312\n"));
        assert!(rendered.text.contains("progress317\n"));
        assert!(rendered.text.contains("progress322\n"));
        assert!(!rendered.text.contains("progress311\n"));
        assert!(!rendered.text.contains("progress323\n"));
        let summary = rendered.match_excerpt.unwrap();
        assert_eq!(summary.matched_lines, 1);
        assert_eq!(summary.omitted_lines, 589);
        assert_eq!(summary.context_lines, 5);
    }

    #[test]
    fn exact_repeats_are_collapsed_without_guessing_timestamp_equivalence() {
        let (text, count) = collapse_exact_repeats("a\na\na\nb\n[1] x\n[2] x\n");
        assert_eq!(count, 2);
        assert!(text.contains("repeated 2 more times"));
        assert!(text.contains("[1] x\n[2] x"));
    }

    #[test]
    fn collapse_switch_leaves_the_line_stream_untouched() {
        let rendered = render_events(
            &[rx_event(1, b"a\na\na\n")],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: false,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert_eq!(rendered.text, "a\na\na\n");
        assert_eq!(rendered.repeated_lines_collapsed, 0);

        let collapsed = render_events(
            &[rx_event(1, b"a\na\na\n")],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert_eq!(collapsed.repeated_lines_collapsed, 2);
        assert!(collapsed.text.contains("repeated 2 more times"));
    }

    #[test]
    fn event_summaries_are_lean_by_default() {
        let lean = render_events(
            &[rx_event(1, b"hi\r\n")],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert!(lean.events.is_empty());
        assert_eq!(lean.text, "hi\n");

        let full = render_events(
            &[rx_event(1, b"hi\r\n")],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: true,
                match_excerpt: None,
            },
        );
        assert_eq!(full.events.len(), 1);
        assert_eq!(full.events[0]["seq"], 1);

        // Raw bytes need the event array even when include_events is false.
        let raw = render_events(
            &[rx_event(1, b"hi\r\n")],
            RenderOptions {
                max_chars: 1024,
                include_raw: true,
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
        );
        assert_eq!(raw.events.len(), 1);
        assert!(raw.events[0]["data_base64"].is_string());
    }

    #[test]
    fn output_limit_keeps_the_most_recent_context() {
        let (text, truncated) = limit_tail("abcdef".into(), 3);
        assert!(truncated);
        assert!(text.ends_with("def"));
    }
}
