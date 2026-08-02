# Agent adapters

`serial-mcp` is the shared stdio MCP adapter for OpenCode, Codex, and other
MCP-capable Agents. It connects directly to `seriald`; it does not wrap shell
calls to `serialctl` and does not depend on an Agent SDK.

## Topology and installation

```text
Windows host: serial card -> serial serve
                                  ^
                                  | HTTP + WebSocket on host-only network
Linux VM: serial console + serial mcp -> OpenCode / Codex
```

Run `serial setup --endpoint http://192.168.56.1:3210` once in the Agent
environment. `serial-mcp` reads the same saved endpoint and operator-token file,
so the MCP host configuration contains no bearer token. You may instead pass
`--config`, `--endpoint`, and `--token-file`; the token value itself is never a
command argument. For setup automation, use `--admin-token-file` and
`--operator-token-file` as inputs and `--save-operator-token-file` as the
destination. Setup rejects global `--token-file` and refuses input/output path
aliases so it cannot overwrite a supplied credential.

Use every component from the same release. v0.5.0 retains WebSocket protocol v3
from v0.4.0; the existing realtime surface is wire-compatible with v0.4, while
the Monitor HTTP APIs and MCP tools require v0.5 components. Protocol-v2 builds
from 0.3.x and protocol-v1 builds from 0.2.x are not compatible with v3.

Ready-to-copy examples are under `opencode/` and `codex/`. They invoke the
component executable directly; using the unified launcher is equivalent:

```text
command = serial
args = ["mcp", "--actor-label", "codex:workstation"]
```

The launcher starts only a sibling `serial-mcp` from the same release
directory. It never falls back to a different executable on `PATH`.

When an OpenCode configuration explicitly lists permissions, allow the
read-only tools (`devices`, `read`, `wait`, `search`, `monitor_list`,
`monitor_status`, and `monitor_incidents`) and require confirmation for
write/state-changing tools, including `input`, `signal`, `monitor_start`, and
`monitor_stop`.
OpenCode prefixes names as `serial_devices`, `serial_command`, and so on.
Codex exposes them under the configured `serial` MCP server.

## Stable core and Monitor tool surface

The original eleven tools keep their names and arguments. Five additive
Monitor tools manage persistent observation Jobs owned by `seriald`; restarting
or closing the stdio MCP process does not stop them.

| Tool | Required inputs | Optional inputs | Purpose |
|---|---|---|---|
| `devices` | — | `slot_id` | Authoritative Slot, Profile, state, ownership, Run, Trigger, and cursor head |
| `read` | `slot_id` | `scope=tail|continue|archive`, archive `epoch`/`after_seq` | Bounded retained text and continuation cursor |
| `command` | `slot_id`, `command` | one of `expect`/`regex`, `timeout_seconds` | Append effective EOL, write in the owned Run, capture post-TX RX |
| `input` | `slot_id`, `text` | — | Exact UTF-8 bytes with no EOL |
| `signal` | `slot_id`, `signal` | Break-only `duration_ms` | Ctrl-C/D/Z byte or physical serial Break |
| `trigger` | `slot_id`, `action` | `kickoff`, `start_contains`, `stop_contains`, interval/timeout/fire bounds | One bounded daemon-side low-latency reaction |
| `wait` | `slot_id` | one of `expect`/`regex`, `timeout_seconds` | Wait from the adapter's live cursor |
| `search` | `slot_id`, `query` | `regex`, scope, explicit continuation fields | Bounded search; current Run by default |
| `monitor_start` | `slot_id`, exactly one of `contains`/`regex` | `description` | Create a persistent Monitor and return its ID immediately |
| `monitor_list` | none | `slot_id` | List authoritative persistent Monitors |
| `monitor_status` | `monitor_id` | none | Inspect one Monitor's state, cursor, counts, and last error |
| `monitor_incidents` | `monitor_id` | opaque decimal `after` cursor | Read one bounded page of retained incidents and evidence references |
| `monitor_stop` | `monitor_id` | none | Stop future detection without deleting retained incidents |
| `run_start` | `slot_id`, `label` | — | Queue for Control and establish an evidence boundary |
| `run_end` | `slot_id` | — | End the owned Run and best-effort release Control |
| `release` | `slot_id` | `abort_run` | Release this adapter's lease without closing the port |

The adapter intentionally hides routine protocol mechanics: request and
Operation UUIDs, Control ID/fence, generation validation, pacing, effective
prompt selection, bounded capture settings, cursor memory, lease renewal, and
cleanup. This keeps Agent calls small while `seriald` retains the complete
auditable timeline.

## Persistent Monitor Jobs

`monitor_start` installs one literal or bounded-regex matcher in `seriald` and
returns immediately. It is not a long-running MCP request and does not keep an
Agent turn open. The daemon owns the Monitor state, live cursor, match window,
incident retention, and any configured notification/outbox policy; those
mechanics are deliberately absent from the Agent tool schema.
The adapter generates the idempotent creation ID internally and reuses it for
one transport retry, so a lost HTTP response cannot create a second Monitor.

For example:

```json
{
  "slot_id": "slot-1",
  "regex": "(?i)kernel panic|watchdog",
  "description": "Capture an intermittent DUT crash"
}
```

Use `monitor_status` or `monitor_list` to inspect whether it is still running.
`monitor_incidents` returns the recent bounded tail when `after` is omitted.
Use `after="0"` to page from the oldest retained incident, or pass the returned
decimal `next_after` back as `after` for the next forward page. Each incident includes a
short preview plus `evidence_ref`, `evidence_cursor`, and the exact serial
sequence range; fetch deeper serial evidence only when it is relevant instead
of placing an unbounded UART history in Agent context. For an exact follow-up,
call `read` with `scope=archive`, the incident Slot, and the returned
`evidence_cursor` epoch/after-sequence pair, plus `through_seq=seq_end` from
the Incident. That inclusive upper bound prevents output arriving after the
Incident from being mixed into its evidence. `truncated=true` means the
returned page is incomplete, not that no later incident exists.
`next_after` is also the server's observed incident high-water mark, so it can
advance even when a page is empty after ACK filtering; clients should persist
it exactly as returned rather than deriving a cursor from the result array.

Notification delivery is optional platform configuration. Without a message
center, incidents remain durable and queryable with these tools. With one
configured, the same persisted incident may also initiate a new Agent turn;
the model does not select a message-center endpoint or delivery strategy.
`monitor_stop` durably stops future matching but preserves existing incidents
for audit and later queries.

## Normal workflow

1. Call `devices`, choose a Slot explicitly, and inspect its online state and
   effective Profiles.
2. Call `run_start`. A Run scopes evidence only; initialize the DUT state
   explicitly.
3. Use `command` for ordinary shell/bootloader commands. Use `input` or
   `signal` only when exact raw input is required. Use `trigger` for a bounded
   timing-critical reaction that must run beside the serial actor.
4. Use `wait`, `read`, or `search` for additional evidence.
5. Call `run_end`. `release` is optional/idempotent cleanup.

The adapter queues for Control and cannot Takeover. A human can observe all
Agent TX in `serial console` and may explicitly Takeover; that revokes the
Agent's Control and aborts its Run. Agents cannot close the configured port.

Only Runs started by this adapter process receive lease renewal. Disconnect or
renewal failure clears local ownership; the adapter never silently reacquires
Control and continues an old task. One Slot has at most one active Run, and
per-Slot write locks prevent concurrent MCP calls from interleaving bytes.

## Command capture and compact results

`command` attaches before TX, then `seriald` validates the adapter-owned Run,
Control ID/fence, epoch, and generation before any byte reaches the port. The
confirmed TX event sequence becomes the capture's lower bound, so replayed RX
from before the write cannot complete the command.

Completion is selected with a small rule:

- `expect` waits for one literal.
- `regex` waits for one regular expression and disables prompt/quiet fallback.
- With neither, configured Device Profile Shell/U-Boot prompts are used.
- With no configured prompt, completion requires post-TX RX followed by a
  one-second quiet interval.

`expect` and `regex` are mutually exclusive. A returned prompt, literal, regex,
or quiet boundary is evidence that capture completed, not proof the command
exited successfully. The generic adapter does not inject POSIX shell syntax or
inspect `$?`; request a unique result sentinel when exit status matters.

With effective `echo=on`, an exact adjacent command+EOL echo is removed from
the returned text. Missing/incomplete echo, foreign TX, gaps, disconnects,
timeout, and truncation lower confidence and add warnings. Writes with uncertain
outcomes are never retried automatically.

Normal `command` results are intentionally compact:

```json
{
  "slot_id": "slot-1",
  "write": "confirmed",
  "capture": "prompt",
  "execution": "unknown",
  "confidence": "high",
  "text": "Linux ...\n[root@dut ~]# ",
  "truncated": false,
  "gap": false,
  "interfered": false,
  "cursor": {"epoch": "…", "after_seq": 812}
}
```

`warnings`, a summary/omitted count, and search continuation guidance appear
only when needed. Internal request/operation/TX IDs, duplicate completion
fields, and raw event arrays are not returned by default; they remain in
`seriald`'s journal for exact human/diagnostic inspection.

Rendered text is ANSI-cleaned, bounded, and folds only adjacent byte-identical
lines. A differing timestamp or payload keeps a row distinct. Folding saves
context but never changes cursor, gap, or durable event semantics.

## Read, wait, search, cursor, and epoch

An epoch is the UUID for one `seriald` process lifetime. Restarting the daemon
changes it. A continuation cursor is `(slot_id, epoch, after_seq)`, not a bare
sequence number.

- `read` defaults to a recent tail. `scope=continue` resumes the adapter's
  process-local cursor; `scope=archive` requires an explicit epoch.
- `wait` begins at the cursor remembered by `run_start`, `command`, or the
  previous `wait`, avoiding a loss window between calls. If no compatible
  cursor exists it begins at the current head.
- `search` defaults to `current_run`, so an old boot/test match cannot be
  mistaken for current evidence. `current_cursor` or `archive` must be
  explicit; archive requires an epoch.
- A truncated current-Run search returns a continuation containing the same
  `run_id`, epoch, and `after_seq`. Page until `truncated=false` before treating
  an empty result as absence.

Set `search.regex=true` to interpret `query` as a bounded server-side regular
expression. Literal and regex matches span adjacent same-direction RX/TX
events, so an OS read boundary cannot hide a pattern. Pattern size, compiled
automaton size, scanned bytes/time, and returned events/bytes remain bounded.
Regex does not enable default global history. Against an older daemon that
cannot advertise bounded server-side regex, MCP search fails closed and asks
for an upgrade or literal search; it never performs an unbounded local scan.

The adapter cursor is process-local. After restarting `serial-mcp`, pass an
explicit returned cursor when an older exact boundary matters.

## Raw input, control signals, and Break

`input` sends the UTF-8 encoding of `text` exactly and appends no Device
Profile EOL. `signal` accepts:

| Value | Physical action |
|---|---|
| `ctrl_c` | one byte `0x03` |
| `ctrl_d` | one byte `0x04` |
| `ctrl_z` | one byte `0x1a` |
| `break` | UART Break line condition, default 250 ms |

Break is not an encoded byte. It still requires the adapter-owned Run and
fenced Control, and success creates an audited `break` timeline event.
Unsupported drivers fail with `break_unsupported`; the adapter never silently
substitutes a NUL or another byte.

## Trigger

`trigger` is present in v0.4.0 and remains a generic daemon-native primitive.
It is not tied to SigmaStar, U-Boot, or flashing. It avoids repeated
Agent/VM/network round trips when a response window is shorter than one MCP
tool call:

```json
{
  "slot_id": "slot-1",
  "kickoff": {"text": "reboot", "eol": "\r"},
  "action": {"text": "slp", "eol": ""},
  "interval_ms": 20,
  "stop_contains": ["SigmaStar #"],
  "timeout_ms": 5000,
  "max_fires": 250
}
```

The daemon arms live literal matchers before the optional one-time `kickoff`,
then sends one `action` at a time. `start_contains` can gate the first action;
any `stop_contains` literal, timeout, or fire limit ends scheduling. Pacing and
physical-write time are additional to `interval_ms`; missed ticks never create
a catch-up burst.

Every kickoff/action uses the ordinary confirmed, fenced, Run-bound, audited
write path and inherits effective Device Profile pacing. The MCP schema exposes
explicit text/EOL but not chunk size/delay. A Trigger never starts another
physical write after Control/Run/generation loss, cancellation, terminal
match, or an observation gap.

The compact result reports `outcome`, `matched`, confirmed `fires`,
`confidence`, bounded `text`, truncation/gap, cursor, and warnings when needed.
`matched=true` proves only that one requested stop literal appeared, not that a
larger flashing/debug workflow succeeded.

## MCP transport behavior

The executable speaks newline-delimited JSON-RPC over stdin/stdout. stdout is
reserved for MCP frames; diagnostics go to stderr. Independent requests run
concurrently and completed frames are serialized through one writer.
`notifications/cancelled`/`$/cancelRequest` cancel only pure observations:
`devices`, `read`, `wait`, `search`, `monitor_list`, `monitor_status`, and
`monitor_incidents`. A command, input, signal, Trigger, Monitor mutation, Run
transition, or release may already have crossed a side-effect boundary, so it
is allowed to reach an authoritative result rather than hiding the outcome and
encouraging an unsafe retry. Per-Slot write serialization prevents physical
byte interleaving.

Supported MCP protocol versions are `2024-11-05`, `2025-03-26`, `2025-06-18`,
and `2025-11-25`.
