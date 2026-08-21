use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use regex::Regex;
use serde_json::{Value, json};
use serial_protocol::{Direction, TimelineEvent};

pub struct RenderedEvents {
    pub text: String,
    #[cfg_attr(not(test), allow(dead_code))]
    pub events: Vec<Value>,
    pub text_truncated: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    pub repeated_lines_collapsed: usize,
    pub match_excerpt: Option<MatchExcerptSummary>,
    pub summary: TextSummary,
}

pub struct TextSummary {
    #[cfg_attr(not(test), allow(dead_code))]
    pub strategy: &'static str,
    pub omitted_chars: usize,
    pub omitted_lines: usize,
}

pub struct MatchExcerptSummary {
    pub matched_lines: usize,
    #[cfg_attr(not(test), allow(dead_code))]
    pub omitted_lines: usize,
    #[cfg_attr(not(test), allow(dead_code))]
    pub context_lines: usize,
}

pub enum MatchExcerptPattern<'a> {
    Literal(&'a str),
    Regex(&'a Regex),
}

pub struct MatchExcerptOptions<'a> {
    pub pattern: MatchExcerptPattern<'a>,
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
            let (text, summary) = match_excerpt(&text, excerpt.pattern, excerpt.context_lines);
            (text, Some(summary))
        }
        None => (text, None),
    };
    let (text, repeated_lines_collapsed) = if options.collapse_repeats {
        collapse_exact_repeats(&text)
    } else {
        (text, 0)
    };
    let (text, summary) = smart_limit(text, options.max_chars);
    let text_truncated = summary.omitted_chars > 0;

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
        summary,
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

pub(crate) fn terminal_text(bytes: &[u8]) -> String {
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

fn match_excerpt(
    text: &str,
    pattern: MatchExcerptPattern<'_>,
    context_lines: usize,
) -> (String, MatchExcerptSummary) {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    if lines.is_empty() {
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
        let is_match = match &pattern {
            MatchExcerptPattern::Literal(literal) => line.contains(*literal),
            MatchExcerptPattern::Regex(regex) => regex.is_match(line),
        };
        if is_match {
            matching.push(index);
        }
    }

    // A pattern may cross a journal/event or line boundary. The server has
    // already established the match, so anchor the excerpt at the line
    // containing its first byte when the per-line scan cannot.
    let cross_line_match = match &pattern {
        MatchExcerptPattern::Literal(literal) => {
            let literal = literal.replace("\r\n", "\n").replace('\r', "\n");
            (!literal.is_empty()).then(|| text.find(&literal)).flatten()
        }
        MatchExcerptPattern::Regex(regex) => regex.find(text).map(|matched| matched.start()),
    };
    if matching.is_empty()
        && let Some(byte_index) = cross_line_match
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
            "[match is not visible after terminal normalization]\n".into(),
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

fn smart_limit(text: String, max_chars: usize) -> (String, TextSummary) {
    let original_chars = text.chars().count();
    let original_lines = text.lines().count();
    if original_chars <= max_chars {
        return (
            text,
            TextSummary {
                strategy: "complete",
                omitted_chars: 0,
                omitted_lines: 0,
            },
        );
    }

    // Preserve enough of the beginning to identify the operation, always
    // preserve the newest output, and spend the remaining budget on bounded
    // warning/error context from the omitted middle.
    const MARKER_BUDGET: usize = 160;
    let content_budget = max_chars.saturating_sub(MARKER_BUDGET);
    let head_budget = content_budget / 4;
    let tail_budget = content_budget / 2;
    let notable_budget = content_budget.saturating_sub(head_budget + tail_budget);
    let head = take_chars(&text, head_budget);
    let tail = take_last_chars(&text, tail_budget);
    let middle_chars = original_chars
        .saturating_sub(head.chars().count())
        .saturating_sub(tail.chars().count());
    let middle: String = text
        .chars()
        .skip(head.chars().count())
        .take(middle_chars)
        .collect();
    let notable = notable_context(&middle, notable_budget);

    let mut output = String::new();
    output.push_str(&head);
    if !head.ends_with('\n') {
        output.push('\n');
    }
    output.push_str("[... middle output omitted ...]\n");
    if !notable.text.is_empty() {
        output.push_str(&notable.text);
        if !notable.text.ends_with('\n') {
            output.push('\n');
        }
        output.push_str("[... continuing at newest output ...]\n");
    }
    output.push_str(&tail);

    // The fixed marker budget keeps this below max_chars for ordinary text.
    // A single very long Unicode line can still make line accounting unusual,
    // but the character budget remains a hard bound.
    if output.chars().count() > max_chars {
        output = take_last_chars(&output, max_chars);
    }
    let preserved_chars = head
        .chars()
        .count()
        .saturating_add(tail.chars().count())
        .saturating_add(notable.source_chars);
    let preserved_lines = head
        .lines()
        .count()
        .saturating_add(tail.lines().count())
        .saturating_add(notable.source_lines);
    (
        output,
        TextSummary {
            strategy: "head_notable_tail",
            omitted_chars: original_chars.saturating_sub(preserved_chars),
            omitted_lines: original_lines.saturating_sub(preserved_lines),
        },
    )
}

struct NotableContext {
    text: String,
    source_chars: usize,
    source_lines: usize,
}

fn notable_context(text: &str, max_chars: usize) -> NotableContext {
    if max_chars == 0 {
        return NotableContext {
            text: String::new(),
            source_chars: 0,
            source_lines: 0,
        };
    }
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let mut keep = vec![false; lines.len()];
    for (index, line) in lines.iter().enumerate() {
        if has_notable_token(line) {
            let start = index.saturating_sub(1);
            let end = (index + 2).min(lines.len());
            keep[start..end].fill(true);
        }
    }

    let mut output = String::new();
    let mut source_chars = 0usize;
    let mut source_lines = 0usize;
    let mut omitted_between = false;
    for (line, keep) in lines.into_iter().zip(keep) {
        if !keep {
            omitted_between = !output.is_empty();
            continue;
        }
        if omitted_between {
            let marker = "[... unrelated lines omitted ...]\n";
            if output.chars().count() + marker.chars().count() <= max_chars {
                output.push_str(marker);
            }
            omitted_between = false;
        }
        let remaining = max_chars.saturating_sub(output.chars().count());
        if remaining == 0 {
            break;
        }
        let piece = take_chars(line, remaining);
        source_chars = source_chars.saturating_add(piece.chars().count());
        source_lines = source_lines.saturating_add(1);
        output.push_str(&piece);
        if piece.chars().count() < line.chars().count() {
            break;
        }
    }
    NotableContext {
        text: output,
        source_chars,
        source_lines,
    }
}

fn has_notable_token(line: &str) -> bool {
    const TOKENS: &[&str] = &[
        "error",
        "failed",
        "failure",
        "fatal",
        "panic",
        "warn",
        "warning",
        "timeout",
        "denied",
        "exception",
        "assert",
    ];
    let lower = line.to_ascii_lowercase();
    TOKENS
        .iter()
        .any(|token| contains_ascii_token(&lower, token))
}

fn contains_ascii_token(text: &str, token: &str) -> bool {
    text.match_indices(token).any(|(start, matched)| {
        let before = text[..start].chars().next_back();
        let after = text[start + matched.len()..].chars().next();
        before.is_none_or(|ch| !is_word_char(ch)) && after.is_none_or(|ch| !is_word_char(ch))
    })
}

fn is_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn take_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

fn take_last_chars(text: &str, limit: usize) -> String {
    let count = text.chars().count();
    text.chars().skip(count.saturating_sub(limit)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::EventKind;

    fn rx_event(seq: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            port: "bench".into(),
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
                    pattern: MatchExcerptPattern::Literal("progress317"),
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
    fn regex_search_excerpt_preserves_match_context() {
        let rendered = render_events(
            &[rx_event(
                1,
                b"before one\nbefore two\nbuild id=47 ok\nafter one\nafter two\n",
            )],
            RenderOptions {
                max_chars: 1024,
                include_raw: false,
                echo: None,
                collapse_repeats: false,
                include_events: false,
                match_excerpt: Some(MatchExcerptOptions {
                    pattern: MatchExcerptPattern::Regex(
                        &Regex::new(r"id=\d+\s+ok").expect("test regex"),
                    ),
                    context_lines: 1,
                }),
            },
        );
        assert!(rendered.text.contains("before two"));
        assert!(rendered.text.contains("build id=47 ok"));
        assert!(rendered.text.contains("after one"));
        assert!(!rendered.text.contains("before one"));
        assert!(!rendered.text.contains("after two"));
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
    fn output_limit_keeps_start_notable_context_and_recent_tail() {
        let mut source = String::from("operation-start\n");
        for index in 0..40 {
            source.push_str(&format!("progress-{index}\n"));
        }
        source.push_str("context-before\nERROR: device failed\ncontext-after\n");
        for index in 40..80 {
            source.push_str(&format!("progress-{index}\n"));
        }
        source.push_str("operation-end");

        let (text, summary) = smart_limit(source, 320);
        assert_eq!(summary.strategy, "head_notable_tail");
        assert!(summary.omitted_chars > 0);
        assert!(text.contains("operation-start"));
        assert!(text.contains("ERROR: device failed"));
        assert!(text.ends_with("operation-end"));
    }

    #[test]
    fn notable_keywords_require_token_boundaries() {
        assert!(has_notable_token("service ERROR: failed"));
        assert!(has_notable_token("[warn] retry"));
        assert!(!has_notable_token("xxxerror is a symbol name"));
        assert!(!has_notable_token("platform_info"));
    }
}
