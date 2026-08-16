# serial-mcp tool reference

This document describes the complete Agent-facing tool surface implemented by
`serial-mcp` 0.6.2. The executable is the schema authority: run

```console
serial mcp --dump-tools
```

to print the exact `tools/list` JSON without loading credentials or connecting
to `seriald`.

## Session contract

`serial-mcp` is an MCP stdio server. It supports `initialize`, `ping`,
`tools/list`, `tools/call`, and cancellation. Tool arguments are strict JSON
objects: unknown fields are rejected. Schema `maxLength` values count JSON
string characters; where bytes cross the UART or enter daemon configuration,
the runtime also enforces the stated UTF-8 byte bound. All serial writes
require an active Run owned by this MCP process.

`run_start` returns a public audit `run_id` and a private, unpredictable
`run_token`. The token exists only in `serial-mcp` memory and in that one tool
result; it is never copied into Run metadata, timeline events, `devices`, or
daemon status. The initiating LLM session must retain the pair and pass it to
every Run-scoped call: `command`, `command_sequence`, `input`, `signal`,
`trigger`, `wait`, and `run_end`. An aborting `release` also requires the pair
while that Run remains active. A caller must never discover an active `run_id`
through `devices` or `search` and adopt it: the matching private token is
deliberately unavailable.

An authorized Run-scoped call pins its Run for that call's complete lifetime,
including a `wait`/command capture lasting up to 120 seconds or a validated
command sequence whose capture deadlines total up to 300 seconds. After the last pin is
dropped, the adapter permits five minutes of inactivity. At the next 20-second
renewal tick it actively releases control, which aborts an abandoned active Run;
if that best-effort release cannot reach `seriald`, the separately renewed,
fenced 60-second daemon lease still expires and aborts the Run. This prevents a
long-lived shared MCP
process from renewing a prior LLM session's abandoned Run indefinitely.

A successful `tools/call` result has `isError=false`, the documented object in
`structuredContent`, and the same object serialized as compact JSON in its
single text content item. A known tool's validation or execution failure uses
the same envelope with `isError=true` and `{error}` as structured content.
Malformed JSON-RPC, invalid `tools/call` framing, and unknown tool names use
JSON-RPC errors instead.

The adapter accepts multiple outstanding `tools/call` requests, but serial
mutations for one Slot are serialized. Independent Slots can progress
concurrently. `command` remains the ordinary one-command primitive.
`command_sequence` is the bounded alternative for a known dependent exchange,
such as waiting for `Password:` after a username before sending a password. It
defines a purpose, completion boundary, timeout, and result for each ordered
step and always stops before the first step whose predecessor did not complete.

Before connecting to or operating a DUT, an Agent must confirm that the model
assigned to the Slot matches the physical device. A saved model name is an
assignment, not evidence. Valid confirmation sources include serial evidence,
telnet, the device Web UI, and a human. A Run scopes evidence; it does not reset
or initialize the device.

The server currently exposes 19 tools:

| Tool | External mutation | Cancellable | Purpose |
|---|---:|---:|---|
| `devices` | No | Yes | List Slots, effective profiles, state, ownership, and Run |
| `device_models` | No | Yes | Read the hierarchical model catalog and Slot bindings |
| `device_model_set` | Yes | No | Assign/create a confirmed model, or guarded-update the Slot's current model node |
| `read` | No | Yes | Read a bounded live or archived serial window |
| `command` | Yes | No | Write a line and capture its bounded RX result |
| `command_sequence` | Yes | No | Execute 1–8 matcher-gated dependent commands in order |
| `input` | Yes | No | Write exact UTF-8 bytes without adding EOL |
| `signal` | Yes | No | Send Ctrl-C/D/Z or physical serial BREAK |
| `trigger` | Yes | No | Run a bounded daemon-side repeated-write matcher |
| `wait` | No | Yes | Wait for a literal, regex, prompt, or quiet boundary |
| `search` | No | Yes | Search the current Run/cursor or an archive |
| `monitor_start` | Yes | No | Start a persistent daemon-side RX Monitor |
| `monitor_list` | No | Yes | List persistent Monitors |
| `monitor_status` | No | Yes | Read one Monitor's authoritative state |
| `monitor_incidents` | No | Yes | Page through retained Monitor incidents |
| `monitor_stop` | Yes | No | Stop a Monitor while retaining incidents |
| `run_start` | Yes | No | Acquire control and open an evidence Run |
| `run_end` | Yes | No | End the Run and best-effort release control |
| `release` | Yes | No | Release control without closing the serial port |

The eight cancellable tools are exactly the read-only calls marked above.
`notifications/cancelled` and `$/cancelRequest` end one of those calls with
JSON-RPC error `-32800`. Mutation-capable calls deliberately ignore
cancellation once dispatched, and stdin shutdown waits for them to converge so
an unknown side-effect outcome is not hidden. In `tools/list`, only those eight
tools have `readOnlyHint=true` and `idempotentHint=true`. The physical-write
tools (`command`, `command_sequence`, `input`, `signal`, and `trigger`),
model-binding mutation, and `monitor_stop` declare `destructiveHint=true`.
Tools that observe or affect the
physical DUT (`devices`, `read`, `command`, `command_sequence`, `input`,
`signal`, `trigger`, `wait`, and `search`) declare `openWorldHint=true`; catalog
and daemon-local bookkeeping tools keep it false.

## Tool definitions

Connection authentication is outside individual tool schemas. With the
default loopback-only personal daemon, `serial-mcp` has no `token_file` and
omits Authorization on its HTTP, control-WebSocket, and capture-WebSocket
connections. Existing authenticated or remote installations continue to use
the serialctl-compatible operator `token_file`.

### `devices`

Lists authoritative Slot snapshots and effective serial/device behavior.

Input:

| Field | Required | Type | Meaning |
|---|---:|---|---|
| `slot_id` | No | string | Return only this Slot; an unknown ID is an error |

The result contains `daemon_epoch`, `model_config_revision`,
`device_model_capability`, `slots`, and `selection_note`. Against a daemon that
predates the additive model API, the capability is `"unavailable"`, the
revision is `null`, and every Slot model is `null`; the established `devices`
tool remains usable. Each compact Slot entry includes `slot_id`, disambiguated
`display_name`, `port`, `enabled`, `transport_profile`, `device_profile`,
`endpoint_present`, `session_state`, `state_code`, `state_reason`,
`target_activity`, `effective_transport`, `effective_device`, `cursor`,
`generation`, `control`, `active_run`, `active_trigger`, `logging`,
`rx_overflow_bytes`, and `device_model`. `control`, `active_run`, and
`active_trigger` are `null` when absent. `device_model` is `null` when the Slot
is unbound; otherwise it is `{id, name, parent_id, aliases,
confirmation_method, confirmation_note, confirmed_wall_time_ns, source}`.
`effective_transport` is `{baud_rate, data_bits, parity, stop_bits,
flow_control, dtr, rts, auto_open}` and `effective_device` is `{write_eol,
echo, shell_prompt, uboot_prompt, write_pacing:{chunk_size,chunk_delay_ms}}`.
An owner is `{id, label, kind}`. `control` is
`{owner,expires_wall_time_ns}`, `active_run` is
`{id,label,status,start_seq,owner}`, and `active_trigger` is
`{id,status,start_seq,end_seq,last_write_seq,fires_confirmed,
tx_bytes_confirmed,owner}`.
`device_profile` controls behavior while `device_model` records identity; one
must not be inferred from the other. The selection note repeats that the
configured model must be independently verified.

### `device_models`

Reads model identity metadata. This does not change UART behavior.

Input:

| Field | Required | Type | Meaning |
|---|---:|---|---|
| `slot_id` | No | string | Filter bindings to one known Slot |

The result contains `config_revision`, all `models`, filtered `bindings`,
`slot_id_filter` (string or `null`), and `identity_warning`. An unknown filter
Slot is an error; a known but unbound Slot returns an empty binding list. A
model is `{id, name, parent_id?, aliases?}`; empty aliases may be omitted. A
binding records `{slot_id, model_id, confirmation_method, note?,
updated_wall_time_ns, source}`. The warning says that this catalog is an
assignment record, not physical-device evidence, and requires
serial/telnet/Web/human confirmation on first connection or whenever identity
is uncertain.

### `device_model_set`

Records a model assignment through the narrow Operator endpoint. It never
changes a Device Profile, prompt, EOL, echo mode, pacing, or UART setting.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | non-empty string | Slot being assigned |
| `model_id` | Yes | string, 1..128 schema characters; max 128 UTF-8 bytes server-side | Existing model ID or ID to create |
| `confirmation_method` | Yes | `serial`, `telnet`, `web`, `human`, `other` | How physical identity was checked |
| `create_if_missing` | No | boolean, default `false` | Atomically create a missing leaf and bind it |
| `update_existing` | No | boolean, default `false` | Patch the existing node currently bound to this Slot; mutually exclusive with create |
| `name` | With create; optional with update | string, 1..128 schema characters; max 128 UTF-8 bytes server-side | Required creation name or replacement name |
| `parent` | Create/update only | string, 1..128 schema characters; max 128 UTF-8 bytes server-side | Creation or replacement parent model ID |
| `clear_parent` | Update only | boolean, default `false` | Remove the parent; conflicts with `parent` |
| `aliases` | Create/update only | array, default empty, at most 32 strings of 1..128; 128 UTF-8 bytes per alias server-side | Creation aliases or complete non-empty replacement |
| `clear_aliases` | Update only | boolean, default `false` | Remove all aliases; conflicts with non-empty `aliases` |
| `expected_current` | No | string of 1..128 schema characters, or `null` | Optimistic binding guard; omitted means guard the binding observed by the tool, `null` requires unbound, string requires that exact model |
| `note` | No | string, at most 2048 schema characters and UTF-8 bytes server-side | Confirmation evidence or context; NUL is rejected server-side |

Creation and update are mutually exclusive. An update requires at least one
model field, and is accepted only when `model_id` is the model currently bound
to `slot_id`; the tool always sends the just-read global revision and exact
current binding. Thus an Operator cannot use this route as a bulk catalog
editor. Updating a shared node changes its metadata for every Slot bound to it.
Model IDs, names, parents, and aliases must be trimmed and contain no control
characters; hierarchy cycles and duplicate aliases remain invalid. For create
on an existing ID, the supplied definition must exactly match the catalog
entry. The tool cannot detach and does not expose Admin bulk replacement. It sends fixed audit source
`agent:serial-mcp`, the catalog revision it just read, and the
explicit-or-observed current binding as concurrency guards.

The result contains `slot_id`, `previous_model_id`, resulting `binding` and
`model`, `created`, `affected_slots`, `shared_model_warning`, `config_revision`,
`identity_warning`, and `next_step`.
`previous_model_id` is a string or `null`; `binding` and `model` use the exact
shapes documented for `device_models`.
`shared_model_warning` is non-null only when `update_existing=true` changes a
catalog node currently shared by more than one Slot; ordinary binding does not
claim that metadata was changed.
Concurrent changes fail instead of silently replacing a newer assignment. The
warning and next step repeat that a saved name is not evidence and that the
physical DUT must be confirmed on first connection or whenever it remains in
doubt.

### `read`

Reads a bounded timeline window and returns rendered text plus a resumable
cursor.

Input:

| Field | Required | Type | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Slot to read |
| `scope` | No | `tail`, `continue`, `archive`; default `tail` | Window selection |
| `epoch` | With `archive` | UUID | Explicit daemon epoch for retained history; ignored by `tail` and `continue` |
| `after_seq` | No | integer >= 0 | Optional exclusive archive lower boundary; ignored by `tail` and `continue` |
| `through_seq` | No | integer >= 1 | Optional inclusive archive upper boundary; rejected outside `archive` |

`tail` queries after `head_seq - 200`. `continue` resumes this MCP process's
per-Slot cursor, or starts at the current head when none exists; a daemon-epoch
change also resets it to the current head. Those two scopes do not consume
caller-supplied `epoch`/`after_seq`. `archive` requires an explicit epoch and
is the only scope that accepts `through_seq`; omitting `after_seq` starts at the
archive beginning. One query is capped at 1,000 events and 512 KiB before
rendering. The result includes `slot_id`, `scope`, `confidence`, `text`,
`truncated`, `gap`, and `cursor`, plus optional `warnings` and
`omitted:{chars,lines}`.

### `command`

Writes `command + effective Slot EOL`, then captures RX after the authoritative
TX event. The MCP must own an active Run.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Private capability returned with that `run_id`; never obtain it from status |
| `command` | Yes | string, max 4096 schema characters | Command text; empty sends only EOL (Enter); command plus effective EOL must be at most 4096 UTF-8 bytes |
| `description` | Yes | non-empty trimmed string, max 256 schema characters and UTF-8 bytes | Concise human-readable purpose, for example `查看样机内存`; retained in the Run's confirmed TX audit history |
| `expect` | No | non-empty string | Literal completion boundary |
| `regex` | No | string, 1..4096 schema characters and UTF-8 bytes | Regex completion boundary |
| `timeout_seconds` | No | integer 1..120, default 10 | Capture deadline |

`expect` and `regex` are mutually exclusive. Without either, configured prompt
matching is preferred; if no prompt is configured, completion requires some
post-write RX followed by one second of quiet. An explicit literal or regex is
the sole requested boundary. If both command and effective EOL are empty, the
tool rejects the no-op write. The capture begins from the pre-attached current
head, discards pre-write RX from command output, and uses the accepted TX event
as its authoritative lower bound.

The result fields are `slot_id`, `write`, `capture`, `execution`, `confidence`,
`text`, `truncated`, `gap`, `interfered`, `run_id`, `operation_id`, `event_seq`,
`description`, and `cursor`, plus optional `warnings` and
`omitted:{chars,lines}`. `execution` is always `"unknown"`: seeing a prompt or
literal is not proof of the command's higher-level success. `write` is
`"confirmed"` after the accepted TX audit unless configured `echo=on` required
an echo that was not observed, in which case it is conservatively
`"uncertain"`. Echo `auto` does not require that echo proof. `capture` is one of
`literal`, `prompt`, `regex`, `quiet`, `timeout`, or `disconnected` on a normal
result. Human-takeover errors are described below.

After at least one byte is confirmed written, seriald stores `description` as
`command_description` in that TX event's metadata. The same event already
contains the authoritative `run_id`, `operation_id`, timestamp, sequence, and
the exact confirmed serial bytes in `data`; partial writes retain the
description together with `partial=true`. Pre-write failures produce no command
history entry. Legacy writes without a description remain valid at the wire
level and simply omit this metadata.

### `command_sequence`

Executes one known, dependent serial interaction inside a single MCP call. It
is intended for exchanges where the Agent already knows the next input but
must wait for device evidence before sending it—for example, username →
`Password:` → password → shell prompt. It is not a parallel command facility,
does not branch or loop, and never retries a step automatically.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability |
| `description` | Yes | non-empty trimmed string, max 256 schema characters and UTF-8 bytes | Human-readable purpose of the complete dependent interaction |
| `steps` | Yes | array of 1..8 strict step objects | Commands executed in array order; unknown step fields are rejected |

Each step is:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `command` | Yes | string, max 4096 schema characters | Command text; empty sends only effective EOL; command plus EOL is bounded to 4096 UTF-8 bytes |
| `description` | Yes | non-empty trimmed string, max 256 schema characters and UTF-8 bytes | This step's human-readable purpose in Run/TX history |
| `expect` | Every non-final step needs one matcher | string, 1..4096 schema characters and UTF-8 bytes | Literal boundary that must be observed before the next step may write |
| `regex` | Every non-final step needs one matcher | string, 1..4096 schema characters and UTF-8 bytes | Non-empty-stream regex boundary; mutually exclusive with `expect` |
| `timeout_seconds` | No | integer 1..120, default 10 | This step's capture deadline |

All steps are validated before the first write. Their effective EOL-inclusive
writes must total no more than 32768 bytes, and the sum of their effective
timeouts must not exceed 300 seconds. Every non-final step must supply exactly
one of `expect` or `regex`; the final step may omit both and then uses the same
configured-prompt/quiet fallback as `command`. An explicit matcher remains the
sole completion boundary for its step.

For example:

```json
{
  "slot_id": "slot-1",
  "run_id": "00000000-0000-0000-0000-000000000001",
  "run_token": "00000000-0000-0000-0000-000000000002",
  "description": "登录样机管理终端",
  "steps": [
    {
      "command": "admin",
      "description": "输入登录账号",
      "expect": "Password:",
      "timeout_seconds": 10
    },
    {
      "command": "example-password",
      "description": "输入登录密码",
      "regex": "[#$>]\\s*$",
      "timeout_seconds": 10
    }
  ]
}
```

The adapter holds its process-local Slot mutation path across the sequence.
Additionally, every step carries a daemon-enforced precondition derived from
the initial Slot snapshot or preceding capture cursor. The seriald Slot actor
atomically checks the epoch, generation, TX offset, and replay window before
the write enters the physical port queue. Pure RX is allowed; any intervening
TX, explicit Gap, or evicted replay window rejects that step with zero bytes
written. This closes the cross-client/cooperative Human race that a local MCP
lock cannot cover. A current adapter refuses the sequence before step 1 if the
daemon does not advertise this capability. Timeout, disconnect, evidence gap,
Run/Control loss, Human takeover, write uncertainty, or any other failed step
stops the sequence before every remaining write. A reported match is still
completion evidence rather than proof of higher-level command success.

The result is:

| Field | Meaning |
|---|---|
| `slot_id`, `run_id` | Authoritative Slot and Run |
| `sequence_id` | UUID grouping this interaction's TX events and terminal history |
| `description` | Overall sequence purpose supplied by the caller |
| `status` | `completed` when every step completed safely; otherwise `partial` |
| `execution` | Always `unknown`; serial boundaries do not prove application success |
| `requested_steps` | Validated plan length |
| `sent_steps` | Steps whose TX was accepted far enough to produce a step result |
| `completed_steps` | Leading steps that completed without a stop condition |
| `steps` | Ordered results for sent steps |
| `failure` | Present only for `partial`; `{step_index,phase,code,message,next_step_sent:false}` |
| `cursor` | Cursor from the last sent step, or the pre-write Slot head when none was sent |

Each entry in `steps` contains the ordinary `command` result plus
`sequence_id`, zero-based `step_index`, `step_count`, `status`, and
`safe_to_advance`. `safe_to_advance=true` occurs only on a completed non-final
step whose matcher made its following write eligible; the final step therefore
reports it as false even when the sequence completed. A capture-level stop
includes that sent step with `status=partial`. A daemon boundary rejection is
always a structured top-level `partial`, even before step 1, with
`code=sequence_boundary_changed` and `next_step_sent=false`. Other first-step
failures before a step result use the ordinary MCP error envelope. If a later
non-boundary failure occurs before TX/capture can yield a result, prior steps
are returned with top-level `status=partial` and failure `code=step_error`.

Capture stop codes are `timeout`, `disconnected`, `rx_gap`,
`capture_truncated`, `interfered`, `echo_uncertain`, `no_rx`,
`boundary_not_matched`, and `run_aborted`. `failure.phase` is `capture` for
those outcomes; a lower-level later-step failure reports its originating phase
such as `attach` or `write` with `code=step_error`. A write-phase atomic guard
failure uses `sequence_boundary_changed`. In every failure form,
`next_step_sent=false` is the explicit stop-before-next-write guarantee.

Every confirmed or partially confirmed step is a normal independent TX audit
event with its own Operation UUID and `command_description`. Its exact command
bytes are persisted in plaintext in the existing TX timeline, just like
`command`. Sequence audit metadata carries the `sequence_id`, overall purpose,
zero-based step index, and step count as `command_sequence_id`,
`command_sequence_description`, `command_sequence_step_index`, and
`command_sequence_step_count`, allowing the terminal command-history bar to
group the sequence and expand each step.

### `input`

Writes exact UTF-8 bytes without appending EOL. Requires this MCP's active Run.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability |
| `text` | Yes | string, 1..4096 schema characters and UTF-8 bytes | Exact UTF-8 payload |

The result is `{slot_id, write:"confirmed", kind:"input", bytes, cursor}`.
This confirms the physical write audit, not target-side execution.

### `signal`

Sends one control signal in the active Run.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability |
| `signal` | Yes | `ctrl_c`, `ctrl_d`, `ctrl_z`, `break` | Signal kind |
| `duration_ms` | BREAK only | integer 1..5000, default 250 | Physical BREAK duration; providing it for Ctrl-C/D/Z is an error |

Ctrl-C/D/Z are ordinary one-byte writes. BREAK is a driver line condition and
has an independent confirmed event. Ctrl results are `{slot_id,
write:"confirmed", kind, bytes:1, cursor}`. A BREAK result is `{slot_id,
write:"confirmed", kind:"break", cursor}`; it does not echo `duration_ms` or a
byte count. These results confirm the write/signal audit, not target-side
execution.

### `trigger`

Creates one bounded daemon-side Trigger tied to the current Run. The normal
form supplies `kickoff` and `action` but omits `start_contains`: once kickoff
is confirmed, the first action is immediately eligible. Use `start_contains`
only for the advanced case where a specific live RX literal must gate the
first action. A configured stop matcher keeps observing without additional
writes after the send budget is exhausted until it matches or the original
timeout expires.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability |
| `kickoff` | No | `{text, eol?}`; each string max 4096 schema characters, combined max 4096 UTF-8 bytes | One write after matchers are armed; confirmation immediately enables action unless an explicit start gate is present |
| `start_contains` | No | string, 1..256 schema characters and UTF-8 bytes | Advanced live-RX gate for the first action; omit for normal kickoff-then-action use |
| `action` | Yes | `{text, eol?}`; each string max 256 schema characters, combined max 256 UTF-8 bytes | Repeated non-empty write |
| `interval_ms` | No | integer 5..1000, default 20 | Action interval |
| `stop_contains` | No | array, default empty, max 8 strings of 1..256 schema characters and UTF-8 bytes | Any literal stops successfully; observation continues after the send budget is exhausted |
| `timeout_ms` | No | integer 100..30000, default 5000 | Hard deadline |
| `max_fires` | No | integer 1..1000, default 250 | Confirmed-action send budget, not an observation cutoff |

Each nested object requires `text`; `eol` defaults to an empty string, and the
two strings are concatenated exactly. Empty combined kickoff/action payloads
are rejected. Planned `kickoff + action * max_fires` is additionally capped at
65,536 bytes. Trigger writes always inherit Slot pacing; pacing overrides are
not tool arguments. Only one active Trigger is allowed on the Slot.

Typical call shape:

```json
{
  "slot_id": "bench",
  "run_id": "<run_start 返回的 UUID>",
  "run_token": "<同一次 run_start 返回的私有 UUID>",
  "kickoff": {"text": "reboot", "eol": "\r"},
  "action": {"text": "slp"},
  "stop_contains": ["prompt>"]
}
```

This does not wait for a separate start literal. Add `start_contains` only
when target output must explicitly authorize the first `action`.

The result fields are `slot_id`, terminal `outcome`, `matched`, confirmed
`fires`, configured `fire_budget`, boolean `send_budget_exhausted`,
`confidence`, evidence `text`, `truncated`, `gap`, and `cursor`, plus optional
`matched_pattern`, `warnings`, `omitted:{chars,lines}`, and `abort_diagnosis`.
`matched=true`
means a caller-supplied stop literal ended the Trigger; other authoritative
outcomes include `timed_out`, `max_fires_reached`, `cancelled`, `control_lost`,
`run_lost`, `generation_changed`, `port_closed`, `write_failed`, and `rx_gap`.
A control/session-loss result warns that the MCP no longer owns the Run. The
tool best-effort cancels a Trigger whose status cannot be made authoritative;
if cancellation/status identity remains uncertain, it returns an error rather
than presenting success.

With `stop_contains`, reaching `max_fires` sets
`send_budget_exhausted=true`, schedules no additional action, and leaves the
RX matcher armed. A later literal can still return `outcome="matched"`; if the
original deadline arrives first, the terminal outcome is
`max_fires_reached`. Without a stop literal, exhausting the budget completes
immediately. Confirmed TX is transport evidence only, so callers must inspect
the resulting RX/state before retrying or changing parameters.

When a `control_lost` or `run_lost` terminal state can be correlated with a
retained `RunAborted(reason="human takeover")`, `abort_diagnosis` is the stable
object `{code:"human_takeover", reason:"human takeover", taken_over_by,
run_id, no_bytes_written:false}`. `taken_over_by` is the retained Human Actor
object or `null` when the abort is retained but the following revocation actor
is unavailable. `false` is intentional: seriald had already accepted the
Trigger Job, so its kickoff or action may have physically executed before the
takeover. This field does not claim that a write definitely occurred; it means
the zero-write guarantee is unavailable.

### `wait`

Waits from the remembered live cursor without writing.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `run_id` | Yes | UUID | Public Run ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability; pins the Run for the entire wait |
| `expect` | No | non-empty string | Literal boundary |
| `regex` | No | string, 1..4096 schema characters and UTF-8 bytes | Regex boundary |
| `timeout_seconds` | No | integer 1..120, default 10 | Deadline |

`expect` and `regex` are mutually exclusive. Without an explicit matcher,
configured prompts are used; if none exist, completion requires observed RX
followed by one second of quiet, so an entirely empty stream times out rather
than claiming quiet completion. The start cursor is the valid remembered
per-Slot cursor, otherwise the current head. The result fields are `slot_id`,
`capture`, `confidence`, `text`, `truncated`, `gap`, and `cursor`, plus optional
`warnings` and `omitted:{chars,lines}`. `wait` requires the exact active Run
capability even though it never writes. A matching `RunAborted` immediately
ends the wait and exposes the reason/takeover diagnostic rather than timing
out. The capability pin lasts for the complete wait, so a legal 120-second
wait is not mistaken for adapter inactivity.

### `search`

Searches durable events with bounded server-side literal or regex matching.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Slot to search |
| `query` | Yes | string, 1..4096 schema characters; regex/literal server bound is 4096 UTF-8 bytes | Literal or regex; whitespace-only is rejected |
| `regex` | No | boolean, default `false` | Treat query as regex |
| `scope` | No | `current_run`, `current_cursor`, `archive`; default `current_run` | Evidence boundary |
| `run_id` | No | UUID | Optional Run filter; required with a `current_run` continuation cursor |
| `epoch` | Cursor/archive | UUID | Required with `after_seq` for `current_run`/`current_cursor`; always required for `archive` |
| `after_seq` | No | integer >= 0 | Exclusive lower sequence boundary; for live scopes, supply together with `epoch` |

For `current_run`, omitting `run_id` selects the active Run; without an active
Run, pass an explicit ID or choose another scope. A truncated continuation must
repeat its `epoch`, `after_seq`, and `run_id`. `current_cursor` uses an explicit
epoch/sequence pair or the MCP's remembered cursor and may optionally filter a
Run. `archive` requires an epoch and accepts optional sequence/Run filters. One
search is capped at 1,000 matched events and 1 MiB before rendering.

The result adds `matched` to the common rendered fields `slot_id`, `scope`,
`confidence`, `text`, `truncated`, `gap`, and `cursor`; matched excerpt lines,
`warnings`, `omitted:{chars,lines}`, `guidance`, and `archive_epochs` appear
when applicable. Matched excerpt lines use `matches`. When the server page is
truncated, `continuation` contains the scope/cursor and, for a Run-scoped
query, `run_id`; repeat the same query and that continuation unchanged.

### `monitor_start`

Starts a persistent Monitor owned by `seriald`; it survives this MCP process.

Input:

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Slot whose live RX is observed |
| `contains` | Exactly one matcher | string, 1..4096 schema characters and UTF-8 bytes | Literal matcher |
| `regex` | Exactly one matcher | string, 1..4096 schema characters and UTF-8 bytes | Regex matcher; must not match an empty stream |
| `description` | No | string, 1..1024 schema characters and UTF-8 bytes | Human-readable purpose |
| `idempotency_key` | No | UUID | Reuse the same value after an uncertain transport retry |

Omitting `idempotency_key` generates a UUID. The adapter automatically reuses
the same request ID for its one bounded transport retry. Other Monitor policy
is adapter-owned: observation starts strictly after the Slot head resolved at
creation, severity defaults to `warning`, debounce is 250 ms, cooldown is
30,000 ms, there is no duration limit, and notification freshness TTL is
600,000 ms.

The result is the compact Monitor record (`monitor_id`, `slot_id`, `status`,
`severity`, `description`, `matcher`, `current_cursor`, `incident_count`,
`unacked_incident_count`, `gap_count`, `expires_wall_time_ns`, `last_error`)
plus `persistent=true`,
`returns_immediately=true`, and polling `guidance`. Persistence means the Job
continues in `seriald` after the tool call or MCP process ends.

### `monitor_list`

Input: optional string `slot_id` filter, defaulting to all Slots. No fields are
required. The result is `{monitors, count}`; each compact Monitor has the same
fields documented for `monitor_start`.

### `monitor_status`

Input: required UUID `monitor_id`; there are no defaults. Returns one compact
authoritative Monitor with the fields documented for `monitor_start`.

### `monitor_incidents`

Reads a bounded retained incident page.

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `monitor_id` | Yes | UUID | Monitor to read |
| `after` | No | decimal string, 1..20 digits, fitting `u64` | Exclusive incident cursor; omit for recent tail |

Each API page requests at most 20 incidents and includes acknowledged items.
With `after` omitted, the mode is `recent_tail`; with it present, the mode is
forward from that exclusive cursor. The result fields are `monitor_id`,
`started_after`, `mode`, compact `incidents`, `count`, `next_after`,
`truncated`, `first_available_after`, `retention_gap`,
`has_older_retained`, `warning`, and `guidance`. Each incident includes its ID
and sequence, Slot/severity/description/preview, serial range, evidence
reference/cursor, wall-clock range, creation/expiry, and `acked`. Its exact
shape is `{incident_id, incident_seq, slot_id, severity, description, preview,
serial_range:{epoch,seq_start,seq_end}, evidence_ref, evidence_cursor,
wall_time_start_ns, wall_time_end_ns, created_wall_time_ns,
expires_wall_time_ns, acked}`. Reuse `next_after` as `after` to poll only newer
incidents; use `"0"` to page from the oldest retained item.

### `monitor_stop`

Input: required UUID `monitor_id`. The tool first reads the current Monitor
revision, then conditionally stops that exact revision. The result is the
compact Monitor plus `stopped=true` and `incidents_retained=true`; retained
incidents remain readable.

### `run_start`

Acquires queued fenced control without takeover and starts an evidence Run.

| Field | Required | Type / bound | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Online Slot |
| `label` | Yes | string, 1..128 schema characters and max 256 UTF-8 bytes server-side | Concrete task performed in this Run; must be trimmed and contain no control characters |

The adapter waits at most 15 seconds in the control queue; this fixed policy is
not a tool argument. An already-active Run is an error. The result is
`{slot_id, run_id, run_token, cursor, warning}`. `run_token` is returned only
to this caller, is not recorded by the daemon, and must be kept with `run_id`
for this Run's scoped calls. The warning repeats that the physical model must
be confirmed through serial/telnet/Web/human evidence and device state
initialized before commands.

### `run_end`

| Field | Required | Type | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Slot containing the Run |
| `run_id` | Yes | UUID | Public ID returned by this caller's `run_start` |
| `run_token` | Yes | UUID | Matching private Run capability |

Ends only that capability's active Run, then best-effort releases its lease;
it refuses to end or adopt another caller's Run. The result is `{slot_id,
ended, control_release:"best_effort"}`; `ended` is the completed Run UUID.

### `release`

Releases this MCP's control without closing the daemon-owned serial port.

| Field | Required | Type | Meaning |
|---|---:|---|---|
| `slot_id` | Yes | string | Slot whose lease should be released |
| `abort_run` | No | boolean, default `false` | Permit release to abort this MCP's active Run; otherwise the caller must use `run_end` first |
| `run_id` | With `run_token` | UUID | Required with `abort_run=true` when a Run is active; must match this caller's Run |
| `run_token` | With `run_id` | UUID | Matching private capability; one field without the other is rejected |

Inside the per-Slot lock, the tool first asks its serialized control session
whether this MCP actually holds a local lease. With no local lease, the call is
an immediate idempotent no-op (`already_released=true`) and does not inspect,
require a token for, or interfere with a foreign Run visible in daemon status.
When this MCP has a lease but authoritative status has no active Run, both
capability fields may be omitted: stale local Run bookkeeping is discarded and
the remaining lease is released with the default `abort_run=false`. Only this
MCP's matching active Run requires `abort_run=true` and the exact capability
pair. A mismatch between the local owned Run and the daemon's active Run fails
closed rather than releasing across the changed boundary. The result is
`{slot_id, released, already_released, serial_port_closed:false}`. Releasing
control never closes the daemon-owned serial port.

## Confidence and safety fields

Read/capture tools use `confidence` values instead of equating returned text
with proof:

| Value | Meaning |
|---|---|
| `high` | Bounded query/capture completed with complete retained evidence |
| `medium` | Useful RX exists, but configured echo evidence is missing |
| `low` | Quiet was the boundary, or a completed capture contained no RX |
| `incomplete` | Deadline elapsed before a completion boundary |
| `partial` | Capture or rendering hit a bound |
| `interfered` | Another actor wrote inside the command evidence window |
| `unreliable` | Gap, disconnect, or Run abort invalidated the evidence |

All returned cursors are `{epoch, after_seq}`. The epoch prevents a cursor from
silently crossing a daemon restart, while sequence/generation checks prevent a
write or Trigger from crossing a reopened physical serial session.

## Human takeover and control-loss diagnostics

A Human takeover authoritatively aborts the Agent Run, emits `RunAborted` with
reason `human takeover`, and revokes fenced control. `command`,
`command_sequence`, and `wait` subscribe to the current Run and stop promptly
on that timeline event rather than waiting for their ordinary boundary/timeout. Write-like tools also
diagnose renewal/write rejection by querying the Run and following
`ControlRevoked` event when available.

Tool failures use normal MCP error results (`isError=true`, with
`structuredContent.error` and the same compact JSON in text content). The
stable error string for confirmed takeover starts with `human_takeover:` and
contains:

- `taken_over_by`, rendered as the Human label and actor ID when retained
  timeline evidence is available, otherwise `unknown`;
- `run_id` for the aborted Run;
- `no_bytes_written=true|false`.

`no_bytes_written=true` means the rejected operation did not reach the serial
port (or, for `wait`, that the tool made no write). If `command` had already
received its authoritative TX acceptance before the takeover event, the error
uses `no_bytes_written=false`; it never rewrites that confirmed write as a
pre-write rejection. `StaleFence` and `ControlRequired` are definite pre-write
rejections. When the event reason or Human actor cannot be recovered, the
stable fallback begins `human_takeover_or_control_revoked:` and still carries
`taken_over_by=unknown`, `run_id`, and `no_bytes_written`. A non-Human abort
uses `run_aborted:` with its reason.

A daemon-side Trigger that reaches an authoritative ownership terminal state
returns `outcome=control_lost` or `run_lost` with warnings rather than claiming
a matcher success. A terminal Trigger correlated with confirmed Human takeover
also carries the structured `abort_diagnosis` described above and explicitly
uses `no_bytes_written=false`, because the accepted Job may already have
written. After any ownership loss, start a new Run only after the current owner
releases control and the physical model and DUT state are reconfirmed.
