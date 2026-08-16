# Serial Platform Protocol Reference

This document records the protocol surface implemented by the current source
tree. It covers the `serial-mcp` stdio transport, the `seriald` HTTP API, and
WebSocket protocol v3. The Rust DTOs in `crates/serial-protocol`, the routes in
`crates/seriald/src/api.rs`, and the MCP dispatcher in
`crates/serial-mcp/src/mcp.rs` remain the executable source of truth.

For the complete MCP tool schemas and result contracts, see
[MCP_TOOLS.md](./MCP_TOOLS.md). `serial mcp --dump-tools` emits the same
authoritative tool registry as JSON without starting the stdio server.

## Shared conventions

- New personal installations default to token-free loopback-only mode. In that
  mode HTTP and WebSocket requests omit Authorization and seriald grants the
  local connection the admin role. seriald rejects both configured and
  runtime bind overrides to non-loopback addresses while authentication is
  disabled.
- Existing configurations remain authenticated because a missing
  `auth_required` field defaults to `true`; those HTTP and WebSocket endpoints
  use `Authorization: Bearer <token>` and keep the three roles below.
- Roles are ordered `observer < operator < admin`; a stronger credential may
  use every weaker-role operation.
- JSON enum names and tagged-union `type` values use `snake_case`.
- UUID fields are JSON strings. Wall-clock timestamps ending in `_ns` are Unix
  nanoseconds; monotonic timestamps are meaningful only inside the current
  daemon process.
- Raw serial and Trigger bytes are base64 in JSON control objects. RX/TX data
  frames carry the bytes unchanged in the binary payload.
- A durable serial cursor is `(slot_id, daemon_epoch, after_seq)`. A sequence
  number without its epoch is not a cross-restart continuation.
- `server_id` identifies one installation, `daemon_epoch` one daemon process,
  and a Slot `generation` one successfully opened physical serial session.
- An HTTP error body is `{"code": <ErrorCode>, "message": <string>}`.
  WebSocket errors use `ServerMessage::Error` and additionally carry
  `request_id` and `retryable`.

## MCP stdio JSON-RPC

### Framing and lifecycle

`serial-mcp` uses newline-delimited JSON-RPC 2.0 on stdin/stdout: one complete
JSON object per line. Blank input lines are ignored. Requests execute
concurrently, while one dedicated writer serializes and flushes complete
stdout lines so responses cannot interleave.

The normal lifecycle is:

1. The host sends `initialize` with an optional MCP `protocolVersion`.
2. The server returns the selected version, fixed tool capability, server
   identity, and operating instructions.
3. The host may send the `notifications/initialized` notification. It has no
   response; unknown notifications are also ignored.
4. The host uses `ping`, `tools/list`, and `tools/call`.
5. Closing stdin cancels only cancellable observations. Mutating requests are
   allowed to converge to an authoritative result before the process exits.

Supported MCP protocol versions are `2025-11-25`, `2025-06-18`, `2025-03-26`,
and `2024-11-05`. `2025-11-25` is the current default. An unsupported requested
version falls back to that current version.

### Methods

| Method | Request/response |
|---|---|
| `initialize` | Accepts `params.protocolVersion`; returns `protocolVersion`, `capabilities.tools.listChanged=false`, `serverInfo`, and safety instructions. |
| `ping` | Returns an empty object. |
| `tools/list` | Returns the current registry under `tools`. Use this response or `serial mcp --dump-tools` instead of a copied tool count/schema. |
| `tools/call` | Requires `params.name`; `params.arguments` defaults to `{}`. Unknown tools and invalid arguments are rejected. |
| `notifications/cancelled` | Notification; reads the target ID from `params.requestId` or fallback `params.id`. |
| `$/cancelRequest` | LSP-compatible cancellation notification; reads `params.id` (or `requestId`). |

A successful tool call returns both a compact compatibility text block and the
same JSON value as `structuredContent`. A tool-level failure is still a
JSON-RPC success envelope whose tool result has `isError=true`; it is not
silently converted into a transport error.

### JSON-RPC errors

| Code | Meaning |
|---:|---|
| `-32700` | Input is not valid JSON or cannot be decoded as the implemented request envelope. |
| `-32600` | Invalid JSON-RPC version or a duplicate active request ID. |
| `-32601` | Method not found. |
| `-32602` | Invalid `tools/call` parameters or unknown tool. |
| `-32800` | A cancellable request was cancelled. |

Requests must carry `jsonrpc: "2.0"`. Notifications have no `id` and receive
no response. Reusing an ID while it is still active is rejected; the existing
request is never replaced.

### Cancellation boundary

Cancellation is deliberately restricted to pure observations. The currently
cancellable tool calls are `devices`, `device_models`, `read`, `wait`,
`search`, `monitor_list`, `monitor_status`, and `monitor_incidents`. All other
tool calls are non-cancellable because they can cross a serial, Control, Run,
Trigger, Monitor, or persistence side-effect boundary. Cancellation of those
mutations is ignored and their tasks continue toward an authoritative result.

When stdin closes, the dispatcher sends cancellation only to those read-only
tasks, waits for all non-cancellable tasks, closes the stdout writer, and then
exits. Per-Slot write serialization in the adapter still prevents concurrent
Agent calls from interleaving physical bytes.

## HTTP `/api/v1`

Every route enforces the minimum role below. In token-free loopback mode,
requests omit Authorization and receive the effective admin role. In
authenticated mode, every route—including health and WebSocket upgrade—needs
a bearer token meeting that role. `GET /api/v1/health` reports
`auth_required`; clients decode the field as `true` when an older daemon omits
it.

| Method and path | Minimum role | Request/query and result |
|---|---|---|
| `GET /api/v1/health` | observer | Process status, `server_id`, `daemon_epoch`, uptime, protocol version, and `auth_required`. |
| `GET /api/v1/status` | observer | All authoritative `SlotSnapshot`s plus identities, `config_revision`, and the additive `sequence_write_precondition_supported` capability. |
| `GET /api/v1/ports` | admin | Enumerates serial ports on the daemon host. |
| `PUT /api/v1/config/slots` | admin | Body `{slots, expected_revision?}`; full validated Slot replacement and resulting snapshots. |
| `GET /api/v1/config/transport-profiles` | observer | `{profiles, config_revision}`. |
| `PUT /api/v1/config/transport-profiles` | admin | Body `{profiles, expected_revision?}`; full catalog replacement. |
| `GET /api/v1/config/device-profiles` | observer | `{profiles, config_revision}`. |
| `PUT /api/v1/config/device-profiles` | admin | Body `{profiles, expected_revision?}`; full catalog replacement. |
| `GET /api/v1/config/device-models` | observer | `{models, bindings, config_revision}`. |
| `PUT /api/v1/config/device-models` | admin | Body `{models, expected_revision?}`; full identity-catalog replacement while retaining valid bindings. |
| `PUT /api/v1/slots/{slot_id}/device-model` | operator | Atomically attach/detach a model and optionally create a missing model; response includes binding, model, `created`, and revision. |
| `GET /api/v1/archives` | observer | Optional `slot_id`; bounded retained Slot/epoch archive catalog. |
| `GET /api/v1/diagnostics` | observer | Daemon, WebSocket, journal, and all per-Slot diagnostics. |
| `GET /api/v1/diagnostics/storage` | observer | Journal quota, queue, archive, and logging health. |
| `GET /api/v1/slots/{slot_id}/diagnostics` | observer | One snapshot plus subscriber count/lag. |
| `GET /api/v1/slots/{slot_id}/events` | observer | `EventQuery`; bounded durable event page, cursor, retention information, and gaps. |
| `POST /api/v1/monitors` | operator | Body `{request_id, spec}`; idempotently creates a persistent Monitor. |
| `GET /api/v1/monitors` | observer | Optional `slot_id` and `status`; lists Monitors. |
| `GET /api/v1/monitors/{monitor_id}` | observer | Gets one Monitor. |
| `PUT /api/v1/monitors/{monitor_id}` | operator | Body `{spec, expected_revision}`; replaces the Monitor under optimistic concurrency. |
| `DELETE /api/v1/monitors/{monitor_id}` | operator | Required query `expected_revision`; stops the Monitor. |
| `GET /api/v1/monitors/{monitor_id}/incidents` | observer | Optional `after_incident_seq`, `limit`, `include_acked`; bounded incident page. |
| `POST /api/v1/monitors/{monitor_id}/incidents/{incident_id}/ack` | operator | Idempotently ACKs the incident and returns it; a pending matching outbox event becomes acknowledged. |
| `GET /api/v1/monitor-events` | observer | Optional `after_outbox_seq` and `limit`; bounded notification-outbox page. |
| `POST /api/v1/monitor-events/{outbox_seq}/ack` | operator | Idempotently changes a pending outbox event to acknowledged and returns it. |
| `GET /api/v1/ws` | observer | Upgrades to WebSocket v3; observer may subscribe, while Slot commands require operator. |

### Event queries

`EventQuery` supports `epoch`, exclusive `after_seq`, inclusive
`through_seq`, `before_wall_time_ns`, `after_wall_time_ns`, `direction`,
`kind`, `actor_id`, `run_id`, `operation_id`, one of `contains` or `regex`,
`limit_events`, and `limit_bytes`. If `epoch` is omitted, the HTTP handler
inserts the current daemon epoch; archived history therefore requires an
explicit epoch. Invalid regexes are `regex_invalid`; exhausted query budgets
return HTTP 429 with `query_budget_exceeded`.

### Configuration concurrency and commit order

All Slot/Profile/model mutations share one daemon configuration mutation lock.
They validate a complete candidate and increment the monotonic
`config_revision`. If `expected_revision` is supplied and does not equal the
current revision, the request returns HTTP 409 / `config_revision_mismatch`
before staging or saving.

Slot and behavior-changing Profile replacements stage affected actors, save
the candidate atomically, commit runtime state, then publish it. Save failures
roll back staged actors; final commit failures attempt to restore the prior
persisted configuration. Device-model identity changes do not alter serial
behavior, but their catalog and bindings are still validated, saved, and
published as one configuration revision. Removing a Slot also removes its
model binding; replacing a retained Slot preserves its binding.

Monitor creation uses `request_id` as its stable idempotency key. Reuse with a
different spec is a conflict. Monitor update and stop require the current
Monitor `revision`. Monitor and ACK mutations serialize through the Monitor
persistence lock and restore the previous in-memory state if persistence
fails.

### Slot device-model request

`SetSlotDeviceModelRequest` has:

- `model_id`: string to attach; JSON `null`/omission detaches.
- `create_if_missing`: when true, `name`, `parent_id`, and `aliases` describe
  a model created in the same transaction before binding.
- `update_existing`: mutually exclusive with creation; patches only the node
  whose ID exactly matches this Slot's current binding. It requires both an
  exact `expected_revision` and string `expected_current`. `name` replaces the
  display name, `parent_id`/`clear_parent` replace/remove the parent, and a
  non-empty `aliases`/`clear_aliases` replaces/removes aliases.
- `confirmation_method`: `serial`, `telnet`, `web`, `human`, or `other`;
  required when attaching.
- `note`: optional audit context for how the assignment was established.
- `source`: required workflow/audit source string.
- `expected_revision`: optional global configuration guard.
- `expected_current`: a three-state per-Slot guard. Omitted means no binding
  guard, explicit JSON `null` requires the Slot to be unbound, and a string
  requires that exact current model ID.

An existing ID plus a conflicting `create_if_missing` definition returns HTTP
409. An unknown parent, hierarchy cycle, duplicate ID/alias, dangling binding,
or invalid field returns HTTP 400. Create/bind and guarded update/bind are
atomic. An update to a shared node changes the metadata observed by every Slot
listed in response `affected_slots`; it never changes those Slots' bindings.

### HTTP status/error mapping

- 400: invalid DTO/config/query/Monitor spec.
- 401: missing, malformed, or invalid bearer credential.
- 403: authenticated role is too weak.
- 404: unknown Slot, Monitor, Incident, outbox event, model, or archive target.
- 409: configuration/Monitor revision mismatch, model binding guard mismatch,
  or conflicting idempotent definition.
- 429: WebSocket/Monitor capacity or journal query budget exhausted.
- 503: the runtime registry is shutting down or degraded.
- 500: persistence, rollback, journal, Monitor runtime, or unexpected internal
  failure.

## WebSocket protocol v3

### Endpoint, envelope, and limits

Endpoint: `GET /api/v1/ws`. The upgrade requires at least Observer. At most
256 WebSocket connections are admitted; excess upgrades receive HTTP 429.
Each connection has a bounded 512-frame outbound queue. Incoming messages and
frames are capped at 64 KiB.

Binary frames use:

```text
[tag: u8][header_len: u32 big-endian][JSON header][raw payload]
```

| Tag | Meaning |
|---:|---|
| `0x01` | JSON `ClientMessage`/`ServerMessage`; payload must be empty. |
| `0x02` | RX `DataFrameHeader` plus unchanged serial bytes. |
| `0x03` | Confirmed TX `DataFrameHeader` plus unchanged serial bytes. |
| `0x04` | Reserved `WRITE_FRAME_TAG`; v3 does not decode or accept it. |

The shared codec bounds JSON headers to 256 KiB and payloads to 1 MiB; the
daemon's stricter 64 KiB incoming WebSocket cap applies first. After the
handshake, the daemon accepts either binary `0x01` envelopes or text JSON for
client control messages. Server control/state frames and RX/TX events are
binary envelopes. Ordinary WebSocket Ping receives Pong.

`DataFrameHeader` contains `protocol_version`, `slot_id`, `daemon_epoch`,
`seq`, `generation`, wall and monotonic times, `kind`, `direction`, optional
`actor`, `run_id`, `operation_id`, optional stream offsets, metadata,
`durable`, and `replay`. Non-byte events (`direction=none`) use a control
`timeline` message containing the complete `TimelineEvent` instead of an
RX/TX frame.

### Handshake and actor identity

The first application message, received within ten seconds, must be `hello`
with the exact `protocol_version=3`, `client_name`, declared `actor_kind`, and
`request_id`. Allowed client declarations are `human`, `agent`, and `script`;
`system` is reserved for the daemon. The server normalizes the label and
issues a fresh opaque actor ID bound to this authenticated connection.

Success produces, in order, `welcome` and a correlated `result` containing
`hello_accepted`. A second `hello` is rejected. Closing the connection removes
its subscriptions and queued acquisition, releases any lease held by that
actor, promotes the next valid waiter, and aborts an affected active Run.

### Attach, replay, and live delivery

An `attach` carries one or more `Subscription` values:
`{slot_id, cursor?, tail_events=200}`. For each Slot the daemon subscribes to
live events before capturing the head, then sends:

1. `snapshot` at head `H`;
2. optional `gap` if the requested cursor cannot be satisfied;
3. optional `replay_begin` and replay events through `H`;
4. `ready {head_seq: H}`;
5. live events with `seq > H`.

This ordering closes the attach/replay race. Attaching a Slot already attached
on the connection replaces its old subscription. `detach` removes only named
subscriptions. Subscriber lag sends `lagged {from_seq,to_seq}` and ends only
that Slot forwarder; the client must query durable history and reattach.

### ClientMessage

Every variant is tagged by `type` and carries `request_id`.

| Type | Minimum role | Fields and semantics |
|---|---|---|
| `hello` | observer | `protocol_version`, `client_name`, `actor_kind`; first message only. |
| `attach` | observer | `subscriptions[]`. |
| `detach` | observer | `slots[]`. |
| `acquire_control` | operator | `slot_id`, `mode` (`queue`/`takeover`), `ttl_ms`. |
| `renew_control` | operator | `slot_id`, `control_id`, `fence`, `ttl_ms`. |
| `release_control` | operator | `slot_id`, `control_id`, `fence`. |
| `cancel_acquire` | operator | `slot_id`, `control_id`; queued actors have no lease, so the daemon matches the authenticated actor and ignores this compatibility ID. |
| `write` | operator | `slot_id`, `control_id`, `fence`, base64 `data`, optional `operation_id`, `expected_run_id`, `pacing`, human-readable `description`, `command_sequence`, `sequence_precondition`, and `cooperative=false`. `expected_run_id` is required when cooperative. |
| `send_break` | operator | `slot_id`, `control_id`, `fence`, `duration_ms`, optional `operation_id` and `expected_run_id`. |
| `trigger_start` | operator | `slot_id`, Control ID/fence, `daemon_epoch`, `generation`, optional Operation/expected Run, and `spec`. |
| `trigger_status` | operator | `slot_id`, epoch, generation, `trigger_id`. |
| `trigger_cancel` | operator | `slot_id`, Control ID/fence, epoch, generation, `trigger_id`. |
| `start_run` | operator | `slot_id`, Control ID/fence, `label`, metadata map. |
| `end_run` | operator | `slot_id`, Control ID/fence, `run_id`. |
| `checkpoint` | operator | `slot_id`, Control ID/fence, `label`; requires an active Run. |
| `ping` | observer | No fields beyond `request_id`; returns daemon wall time. |

`WritePacing` is `{chunk_size, chunk_delay_ms}`; zero delay selects the
full-speed/no-sleep path (the daemon still chunks driver calls). The delay unit
is milliseconds and is inserted only after a complete planned chunk when more
payload remains. Thus `chunk_size=1, chunk_delay_ms=1` applies N-1 requested
gaps to N bytes; partial driver acceptance within one chunk adds no pacing gap,
and there is never a delay after the final chunk. Driver latency and scheduler
granularity are additional, so this is a minimum requested cadence rather than
an exact per-character clock. Ordinary writes resolve an explicit override
before the effective Slot/Device Profile pacing. `description` is an additive
optional wire field for legacy-client compatibility. When present it must be
non-empty, trimmed, free of control characters, and at most 256 UTF-8 bytes.
Current serial-mcp `command` calls require it. Empty writes are rejected. One
physical write is bounded to 4 KiB and a computed maximum 15-second
physical-write deadline.

`command_sequence` is optional additive audit context for one physical write:
`{sequence_id, description, step_index, step_count}`. Its `description` is the
overall sequence purpose; the write's ordinary `description` remains the
individual step purpose. The sequence UUID must be non-nil, `step_index` is
zero-based, `step_count` is in `[1,8]`, and `step_index < step_count`. A
sequence-tagged write must also have a valid per-step write description. This
context groups confirmed TX only and does not assert that the DUT executed a
step or completed the sequence.

`sequence_precondition` is an optional additive, daemon-enforced write guard:
`{cursor:{epoch,after_seq}, expected_generation, expected_tx_offset}`. It is
valid only alongside `command_sequence`. serial-mcp sends it on every sequence
step, including the first step using the initial status snapshot. Inside the
Slot actor, after lease/Run authorization and immediately before the write is
put in the physical port queue, seriald atomically verifies the daemon epoch,
serial generation, and TX offset. It also replays the timeline strictly after
the cursor and rejects an evicted replay window, an explicit `gap` event, or
any additional TX. Ordinary RX and non-TX control events are allowed and the
post-write command matcher still ignores replayed pre-write RX. A rejected
guard returns `sequence_boundary_changed`, is a definite zero-byte outcome,
creates no TX event, and causes MCP `command_sequence` to return `partial` with
`failure.next_step_sent=false`. The precondition participates in both
request-id fingerprints.

The HTTP status capability defaults to false when omitted by an older daemon.
A current serial-mcp refuses `command_sequence` before step 1 unless
`sequence_write_precondition_supported=true`; ordinary `command`, Human, and
legacy writes omit the precondition and retain their previous behavior.

`send_break.duration_ms` is in `[1,5000]`. BREAK is a UART line condition, not
a byte; Ctrl-C/D/Z are ordinary `write` payload bytes `03`, `04`, and `1a`.

### ServerMessage

| Type | Fields and meaning |
|---|---|
| `welcome` | `server_id`, `daemon_epoch`, protocol version, issued actor, authenticated role. |
| `snapshot` | One complete `SlotSnapshot`. |
| `replay_begin` | `slot_id`, first replay seq, inclusive replay head. |
| `ready` | `slot_id`, attachment head. |
| `timeline` | Complete non-byte event plus `replay`. |
| `result` | Correlated `request_id` and `CommandResult`. |
| `error` | Optional request ID, stable `ErrorCode`, message, and `retryable`. |
| `gap` | Slot, requested cursor, first available seq, current head, and `GapReason`. |
| `lagged` | Slot and missed inclusive sequence range; that Slot subscription ends. |

`SlotSnapshot` contains the Slot config, daemon epoch, head/ring oldest seq,
generation, endpoint/session state and stable reason code, target activity,
last RX time, RX/TX offsets and overflow count, current Control, active Run,
active Trigger, logging health, and effective Shell prompt, U-Boot prompt,
write EOL, echo, Transport settings, and write pacing.

### CommandResult

The exhaustive result `type` values are:

- `hello_accepted {actor, role}`
- `attached {slots}` and `detached {slots}`
- `control_granted {lease}`, `control_queued {position}`,
  `control_renewed {lease}`, and `control_released`
- `acquire_cancelled {removed}`
- `write_accepted {event_seq}` and `break_sent {event_seq}`
- `trigger_started {trigger}`, `trigger_status {trigger}`, and
  `trigger_cancelled {trigger}`
- `run_started {run}` and `run_ended {run}`
- `checkpoint_created {event_seq}`
- `pong {server_wall_time_ns}`

### ErrorCode, EventKind, and GapReason

The exhaustive stable `ErrorCode` values are:

`bad_request`, `unauthorized`, `forbidden`, `not_found`, `conflict`,
`control_required`, `stale_fence`, `port_offline`, `cursor_ahead`,
`sequence_boundary_changed`, `resource_exhausted`, `idempotency_expired`, `config_revision_mismatch`,
`profile_change_busy`, `port_not_found`, `port_busy`, `port_access_denied`,
`port_io`, `break_unsupported`, `regex_invalid`, `query_budget_exceeded`,
`unavailable`, and `internal`.

The exhaustive `EventKind` values are:

`rx`, `tx`, `serial_opening`, `serial_opened`, `serial_open_failed`,
`serial_closed`, `slot_reconfigured`, `slot_removed`, `control_granted`,
`control_released`, `control_revoked`, `control_expired`, `run_started`,
`run_ended`, `run_aborted`, `trigger_started`, `trigger_completed`,
`trigger_cancelled`, `trigger_failed`, `break`, `checkpoint`,
`logging_degraded`, and `gap`.

`Direction` is `rx`, `tx`, or `none`. `GapReason` is `epoch_changed`,
`ring_evicted`, `retention`, `corruption`, `logging_fault`, or
`sequence_discontinuity`.

## Control, takeover, and cooperative Human writes

Control is one fenced lease per Slot. A lease contains its ID, owner, daemon
epoch, generation, monotonically increasing fence, issued wall time, and
display expiry. Authorization and expiry use monotonic time. Requested TTL is
clamped to at least five seconds and the configured ceiling (60 seconds by
default; hard ceiling 24 hours).

`queue` grants immediately when free. Otherwise it inserts or refreshes one
waiter per actor and returns a one-based queue position. The queue is FIFO,
bounded (128 by default), and each waiter expires after the configured wait
timeout (60 seconds by default). Release, expiry, disconnect, or revocation
promotes the first non-expired waiter. `cancel_acquire` removes only the
calling actor's queued entry.

`takeover` immediately revokes the current lease, clears the taker's own queued
entry, increments the fence, grants the new lease, stops the old owner's
Trigger, and aborts the active Run with reason `human takeover`. The MCP adapter
never requests takeover, even though the lower protocol exposes the mode to an
Operator client. The revoked actor observes `control_revoked` and
`run_aborted`; stale Control ID/fence writes are rejected before touching the
port.

A `write` with `cooperative=true` is the narrow no-takeover Human injection
path. It is accepted only when:

- the caller's issued actor kind is `human`;
- the current lease owner is an `agent`;
- an active Run exists, is Agent-owned, and has the same owner as the lease;
- `expected_run_id` is present and exactly matches that active Agent Run;
- per-request `pacing` is omitted;
- no Trigger is active; and
- the current Agent lease's remaining monotonic TTL covers the complete
  physical-write budget plus the normal scheduling margin.

It neither transfers nor revokes Control. The request's ordinary lease fields
do not borrow the Agent fence. Confirmed TX is attributed to the Human, marked
`cooperative=true`, and includes the interfered Run/owner in metadata. It is
therefore visible as foreign interference to Agent capture. Every other use of
the flag is rejected as control-required/not-owner.

`actor_kind` is an authenticated connection's audit/cooperation declaration
within its bearer role, not an independently verified Human identity. A shared
Operator credential lets its holder declare `human`, `agent`, or `script` on a
new connection. Deployments that require a Human-only security boundary must
not share that Operator credential with untrusted automation; the cooperative
check itself is a coordination policy, not separate person authentication.

Human takeover and cooperative injection are different operations: takeover
ends the Agent ownership boundary; cooperative injection preserves it but
adds explicitly audited foreign TX.

If an already accepted MCP Trigger reaches `control_lost` or `run_lost`, the
adapter correlates that terminal state with the Run timeline. A confirmed
`run_aborted` reason of `human takeover` adds
`abort_diagnosis:{code:"human_takeover", taken_over_by, run_id,
no_bytes_written:false}` to the Trigger result. The value is never `true` for
an accepted Trigger: its kickoff or one or more actions may already have
reached the physical device before takeover became terminal. The Human Actor
is `null` only when the abort reason is retained but its following revocation
event is unavailable.

### Request idempotency

Ordinary Slot commands cache results by actor plus request ID and reject reuse
with different content. Physical-write operations (`write`, `send_break`, and
`trigger_start`) additionally use a daemon-epoch write history keyed by request
ID and a fingerprint that excludes reconnect-specific lease identity but
includes the intended bytes/action, Operation, expected Run, pacing, and
cooperative flag as applicable. A recent exact duplicate returns its cached
result. If execution is known but its result aged out, the daemon returns
`idempotency_expired` and does not repeat the physical operation. Partial or
transport-lost outcomes remain uncertain and are never safe for blind retry.
Because a cooperative write requires its Agent Run ID, retrying the same request
ID against a later Run has a different fingerprint and cannot return the old
Run's cached success.

## Runs and Trigger Jobs

### Runs

A Run is an explicit evidence interval, not a reset, reservation, or device
initialization. `start_run` requires current fenced Control, no active Trigger,
and no other active Run. It creates a server UUID, owner, label, `active`
status, start sequence, and bounded metadata. `checkpoint` adds an audited
label inside the active Run. `end_run` requires current Control and the exact
Run ID, emits `run_ended`, and returns a `completed` Run.

Control release, expiry, takeover, owner disconnect, serial close/generation
change, Slot reconfiguration/removal, or shutdown aborts the Run and emits
`run_aborted` with an explicit reason. Agent writes set `expected_run_id`; the
daemon verifies both the ID and the server-issued Run owner before pacing or
physical write. Missing, changed, or foreign Runs fail without writing bytes.

A described write that confirms at least one serial byte stores its purpose as
TX event metadata `command_description`. The TX event's existing `run_id`,
`operation_id`, sequence, timestamp, and `data` fields provide the Run grouping
and exact confirmed command bytes; `partial=true` marks a confirmed prefix.
For a sequence-tagged write, the same TX metadata additionally stores
`command_sequence_id`, `command_sequence_description`,
`command_sequence_step_index`, and `command_sequence_step_count`. Every sent
step has its own Operation ID and TX event. A step stopped before any byte is
written has no TX event; later sequence steps are not synthesized in history.
This keeps command history queryable through the existing event API without a
new event kind. A write rejected before any byte reaches the port creates no
command-history event, and legacy writes without `description` keep their
original event shape.

### TriggerSpec and lifecycle

A Trigger is one daemon-owned, bounded, device-agnostic reaction job:

| Field | Meaning/default/bound |
|---|---|
| `initial_write` | Optional one-time base64 write after matchers are armed; max 4 KiB. Once confirmed, action is immediately eligible unless an explicit start gate exists. |
| `start_contains` | Optional advanced base64 RX literal gating the first action; omission means no RX start gate. |
| `action` | Required non-empty base64 bytes for each fire; max 256 bytes. |
| `interval_ms` | Delay after one confirmed action before the next; default 20, range 5–1000. |
| `stop_contains` | Up to eight non-empty base64 RX literals, each max 256 bytes. |
| `timeout_ms` | Original observation deadline; default 5000, range 100–30000. |
| `max_fires` | Confirmed action-write send budget; default 250, range 1–1000. |
| `pacing` | Optional pacing for initial/action writes; otherwise effective Slot pacing. |

The total bounded Trigger write plan may not exceed 64 KiB. Start requires
current Control, exact daemon epoch and generation, an online port, no active
Trigger, and a valid optional expected Run. The daemon drains the ordered RX
barrier, arms matchers, emits `trigger_started`, and returns immediately.

The field shape is unchanged from protocol v2/v3. Its explicit omission rule
is: without `start_contains`, arming immediately enables `action` when
`initial_write` is absent, or a confirmed `initial_write` immediately enables
`action` when present. With `start_contains`, the Trigger instead enters
`waiting_for_start` after the kickoff and does not schedule its first action
until that literal is observed in live RX. This gate is optional; clients
should not synthesize one for routine kickoff-then-action jobs.

Exhausting `max_fires` prevents every later action write. With no
`stop_contains`, that immediately completes as `max_fires_reached`. With one
or more stop literals, the live matcher remains armed without scheduling more
writes until a literal matches or the original deadline expires. A deadline
after exhausting the send budget is reported as `max_fires_reached`; a
deadline reached before exhausting it is `timed_out`.

Nonterminal `TriggerStatus` values are `armed`, `waiting_for_start`, `running`,
and `stopping`. Terminal values are `matched`, `timed_out`,
`max_fires_reached`, `cancelled`, `control_lost`, `run_lost`,
`generation_changed`, `port_closed`, `write_failed`, and `rx_gap`.
`TriggerInfo` records identity/owner, epoch/generation, Control/fence,
Operation/expected Run, full spec, start/end/last-write sequences, confirmed
fire and TX-byte counts, and an optional matched pattern.

Status is bounded and does not require ownership, but the WebSocket command
still requires an Operator connection. Cancellation requires the Trigger
owner and its exact current Control/fence; cancelling an already terminal Job
returns its terminal state. A terminal cause stops new scheduling, while one
already accepted physical write may finish and be audited before the status
leaves `stopping`.

## Profiles and independent device-model identity

The configuration deliberately separates behavior from identity:

1. `SlotConfig` identifies the stable station channel and physical port.
2. `TransportProfile` owns baud rate, data bits, parity, stop bits, flow
   control, DTR/RTS, and `auto_open`.
3. Optional `DeviceProfile` owns Shell/U-Boot prompt presence and optional EOL,
   echo, write chunk size, and write delay overrides.
4. `DeviceModel`/`SlotModelBinding` records what DUT is believed to be connected
   without changing serial behavior.

For compatibility, `SlotConfig.profile` is the Transport Profile name and
`SlotConfig.device_profile` is the behavior Profile name. A model identity is
not stored in either field. Resolved behavior is published in snapshots as the
effective prompts, EOL, echo, Transport settings, and write pacing.

`DeviceModel` is `{id, name, parent_id?, aliases[]}`. `parent_id` forms an
arbitrary-depth tree; parent and child display names may intentionally repeat.
The daemon rejects unknown parents, cycles, duplicate IDs, and
case-insensitive duplicate aliases. `SlotModelBinding` is `{slot_id, model_id,
confirmation_method, note?, updated_wall_time_ns, source}` and is validated
against both catalogs.

Model identity is a maintained assertion, not automatic proof. Before an Agent
connects or writes, it must confirm that the configured model matches the
physical DUT using serial evidence, telnet, the device web UI, or a Human.
Recording `confirmation_method` and a note describes that evidence; it does
not make a stale label true. The MCP server repeats this requirement in its
initialization instructions and exposes only the narrow Operator binding path,
not Admin replacement of the entire catalog.

Transport changes can reopen the physical handle. Device Profile changes keep
the port open but are rejected while a Run or Trigger would otherwise continue
under changed prompt/pacing semantics. DeviceModel changes never alter prompts,
EOL, echo, pacing, port lifecycle, or Run state.

## Compatibility summary

- WebSocket protocol v3 requires an exact version match. Protocol v1/v2 peers
  are rejected rather than allowed to fail later on enum/field differences.
- HTTP remains namespaced `/api/v1`; that route namespace is independent of
  the WebSocket protocol number.
- Additive optional fields use Serde defaults where implemented, including
  legacy writes defaulting `description=null`, `command_sequence=null`,
  `sequence_precondition=null`, and `cooperative=false`; legacy HTTP status
  defaults `sequence_write_precondition_supported=false`.
- `0x04` is reserved only; clients must use the `write` control message.
- Exact schema inspection should use the Rust DTOs for HTTP/WebSocket and
  `tools/list` or `serial mcp --dump-tools` for MCP.
