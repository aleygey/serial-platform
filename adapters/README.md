# Agent adapters

`serial-mcp` is the shared stdio MCP adapter for OpenCode, Codex, and other
MCP-capable agents. It connects directly to `seriald`; it does not wrap
`serialctl` commands and does not depend on an Agent SDK.

## Topology

```text
Windows host: serial card -> seriald (192.168.56.1:3210)
                                  ^
Linux host-only VM: serialctl + serial-mcp -> OpenCode / Codex
```

Copy the Linux release's `serialctl` and `serial-mcp` to a directory on PATH.
Run `serialctl --endpoint http://192.168.56.1:3210 init` once. `serial-mcp`
reads the same `serialctl.toml` and operator token by default, so neither Agent
configuration contains a credential. You may instead provide `--config`,
`--endpoint`, and `--token-file`; the token itself is never a command argument.
Keep these clients on the same release as the Windows `seriald`: 0.3.x uses
WebSocket protocol v2 and cannot be mixed with 0.2.x protocol-v1 executables,
even though the HTTP route namespace remains `/api/v1`.

Use the examples in `opencode/` or `codex/`, replacing only executable/config
paths and the actor label. In OpenCode, the configured MCP server name `serial`
prefixes tools as `serial_devices`, `serial_command`, etc. In Codex they appear
under the `serial` MCP server namespace.

## Stable Agent workflow

1. `devices`: inspect authoritative Slot state and choose a Slot explicitly.
2. `run_start`: acquire queued control and create a task boundary owned by this
   adapter process. It isolates only the log/event interval and does not reset
   the target.
3. Initialize the target state explicitly with `command`, or use `trigger` when
   a bounded low-latency byte reaction is required.
4. Use `command` for bounded write/capture, `trigger` for daemon-local
   initial/repeated writes stopped by live RX, `wait` for future output, `read`
   for a cursor continuation, and `search` for a literal search in the current Run.
5. `run_end` when the task is finished. It stops renewal and best-effort
   releases control; a following `release` is optional and idempotent.

The adapter queues for control and never takes it over. An operator using
`serialctl` can observe everything and explicitly Takeover if needed. Agent
tools cannot close the serial port. A write holds a 60-second fenced lease and
the adapter renews it every 20 seconds while its process remains alive.
`run_start` control queueing is capped at 15 seconds so one Slot cannot starve
renewal of another active Run's lease. Only Slots with Runs started by this
adapter process are renewed. Disconnect or renewal failure forgets those Runs;
the adapter never reacquires control and writes outside the old boundary.

`command` requires that adapter-owned Run, attaches before writing, and sends
its ID as `expected_run_id`. `seriald` validates the exact active Run and owner
inside the same Slot actor as the control/fence; a missing, changed, or foreign
Run rejects the write with zero bytes. The confirmed TX is tagged with an
Operation UUID, and its sequence is the output window's lower bound. RX below
that sequence is excluded, so prompts replayed before the write cannot complete
the command. When the Profile says `echo=on`, a complete command plus EOL echo
is stripped and gives the strongest confidence; the matcher accepts the
target's observed `CR CR LF` hard wrap. An exact literal, regex, or prompt
observed after confirmed TX may still complete when that echo is missing or
incomplete. A logical line that is identical or close to a partial/damaged echo
is withheld in full, so neither a boundary embedded in a chunked echo nor the
same-line remainder after a damaged byte can complete early. Unrelated post-TX
lines are preserved even if they contain the command's first character.
One- to four-byte commands disable fuzzy echo classification and protect only
an exact partial/full prefix. A missing/incomplete result reports
`echo_missing=true`,
`suspected_input_loss=true`, and `delivery_confidence=uncertain`; it preserves
post-TX RX and never retries automatically. Quiet completion requires at least
one post-TX RX event. Results also expose `prewrite_activity`, discarded
pre-boundary RX, and `interfered`; any foreign TX from attach through completion
downgrades the boundary and the RX must not be treated as isolated.
Serial has no causal response envelope: if an already-buffered prompt is first
sequenced after confirmed TX and no recognizable echo prefix follows it, that
prompt can only be reported as lower-confidence post-TX evidence. Use a unique
literal/regex sentinel when false prompt completion would be unsafe.
Prompt and contains modes wait for the requested evidence or timeout; they are
not ended by a short `quiet_ms` gap. Quiet is used only when selected explicitly
or when auto mode has no configured prompt. Supplying `until_regex` makes that
regex the sole completion boundary and returns `completion_mode=regex`; a Shell
prompt that appears first (for example after starting a background command)
cannot pre-empt the later regex marker. Do not combine `until_regex` with
`until` or an explicit `completion=prompt|contains|quiet`.
`complete=true` means only that the capture boundary was observed:
`execution_status` remains `unknown` because the generic adapter does not
rewrite a command or assume a POSIX shell to inspect `$?`. Device RX has no
Operation UUID, so use the returned capture window, a Run, or an exact cursor
for response output; `operation_id` filters confirmed TX audit events only.
When the caller knows the target shell, it should emit a unique result sentinel
from the same command (for example a marker carrying the captured return code)
and use that marker as `until` or `until_regex`. The caller can then judge the
reported marker without confusing command completion with command success.
`serial-mcp` deliberately never inserts shell-specific syntax on its own.

`trigger` also requires the adapter-owned Run, but it is not a shell command or
flashing recipe. It arms exact live RX byte matchers inside `seriald`, performs
an optional one-time `initial_write` kickoff, optionally waits for a start
literal, then sends one bounded `action` at a time until a caller-supplied stop
literal or a hard terminal bound is reached. The kickoff is not an action fire
and does not increment `fires_confirmed`. This MCP v1 tool accepts explicit
UTF-8 `text` plus `eol`; it does not expose arbitrary binary/base64 Trigger
arguments. The lower seriald WebSocket protocol remains raw-byte capable and
encodes Trigger payloads and literals as base64. The adapter does not read a
Prompt or EOL from the device profile. Ordinary commands and Trigger
kickoff/action writes do inherit the Slot transport pacing. Pacing is
deliberately absent from the Agent schemas, so an Agent cannot bypass the
target's safe chunk/delay settings.
The start request returns as soon as the daemon has armed the Job, leaving the
session task available for lease renewal while a separate subscribed capture
waits for the terminal lifecycle event. Timeout, maximum fires, cancellation,
Control/Run/generation loss, write uncertainty, and RX gaps are distinct
outcomes and are never reported as a successful match.

For example:

```json
{
  "slot_id": "slot-1",
  "initial_write": {"text": "reboot", "eol": "\r"},
  "action": {"text": "slp", "eol": ""},
  "interval_ms": 20,
  "stop_contains": ["SigmaStar #"],
  "timeout_ms": 5000,
  "max_fires": 250
}
```

These values are not built in: the same primitive can wait for a banner and
send one ACK, repeat Ctrl-C until an arbitrary Prompt, or coordinate another
bounded serial handshake. After a confirmed action, `interval_ms` is the delay
before the next action becomes eligible; pacing and physical-write time are
additional, so this example does not promise one write every 20 ms. A timeout
stops new scheduling, but one already accepted physical write is allowed to
settle and be audited before the terminal result. Every confirmed
kickoff/action write is visible in the normal TX timeline with the Agent, Run,
Operation, Trigger, and attempt metadata. See the architecture guide's
[Trigger Jobs](../DOCUMENTATION.md#trigger-jobs) table for all request limits.

The lifecycle event `TriggerCompleted` covers `matched`, `timed_out`, and
`max_fires_reached`; always inspect the returned `outcome`. `matched=true`
means only that one supplied `stop_contains` literal was observed.
`capture_complete=true` separately means the bounded MCP capture reached the
terminal event without truncation or a reported gap. It is not proof of a
match, command success, or completion of a larger flashing/debug workflow.
Returned evidence is clipped to `start_seq..=end_seq`. If capture observed later
traffic while status polling caught up, `observed_through_seq` reports that fact
without folding the traffic into Trigger text/events or advancing the adapter's
live cursor past `end_seq`. `mcp_run_ownership_retained=false` means the status
lookup recovered evidence after this adapter lost its control connection; the
result remains valid evidence, but a new Run is required before another write.

An implicit `wait` does not sample a fresh head and risk losing output that
arrived after the preceding tool response. Within one `serial-mcp` process it
resumes a monotonic per-Slot live cursor recorded by `run_start`, `command`, and
`wait`. Filtered `read` and `search` calls deliberately do not advance this
cursor because their continuation can skip unmatched RX. The remembered cursor
includes delayed output that arrives between a `command` response and the next
`wait` attachment. An explicit `epoch` plus `after_seq` always overrides that
memory. If no compatible cursor exists (including after a daemon-epoch change),
`wait` starts at the current head. Inspect `cursor_source` and
`started_after_seq` in the result to audit the chosen boundary; the returned
`epoch`/`after_seq` is the next continuation. The memory is process-local, so
after restarting the adapter use an explicit cursor when an older exact
boundary matters.

One write RPC waits up to 20 seconds, covering seriald's 15-second legal write
budget plus response margin. `serial-mcp` leaves pacing unset in the wire
request so `seriald` applies the Slot transport settings. An explicit
daemon rejection that says those settings exceed the budget or the lease is too
short is a definite no-byte outcome and is reported as safe to retry after
configuration is corrected. Transport loss, timeout, missing echo, and partial
writes remain outcome-uncertain; inspect TX/RX before deciding whether to retry.

`search` defaults to `current_run`; it will not silently find a matching line
from an earlier test cycle. If that bounded query is `truncated`, repeat the
same search with its returned `scope=current_run`, `run_id`, `epoch`, and
`after_seq`; the Run constraint remains in force across pages, so continuation
cannot drift into another test cycle. An incomplete empty page is not evidence
that the literal is absent and returns continuation guidance instead of archive
guidance. `current_cursor` requires the exact `epoch` and `after_seq`, and
`archive` requires an explicit epoch. Raw base64 payload is opt-in. Search text
defaults to each matching line plus five surrounding lines, so one 4 KiB RX
event does not consume context merely because it contains many unrelated lines;
`context_lines` can adjust that bound and event/raw evidence remains exact.
Human-oriented text removes ANSI control sequences and folds only adjacent
byte-identical lines.

The executable speaks newline-delimited JSON-RPC over stdin/stdout. stdout is
reserved for MCP frames; diagnostics go to stderr. It supports MCP protocol
versions `2024-11-05`, `2025-03-26`, `2025-06-18`, and `2025-11-25`.
