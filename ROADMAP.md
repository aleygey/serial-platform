# serial-platform roadmap

This roadmap separates shipped behavior from possible future work for
`seriald`, the human `serialctl`/`serial console`, and `serial-mcp`. Items under
**Later / candidate** are ideas, not commitments; station feedback decides
whether they are promoted, changed, or removed.

## Product boundary

`seriald` is a device-agnostic shared serial control plane. It owns the
physical port, fans one byte stream out to multiple observers, serializes
writes, records authoritative events, and provides bounded realtime
primitives.

`serialctl` is the human setup, diagnosis, history, and terminal client.
`serial-mcp` is the thin Agent adapter. The `serial` executable is only a
unified launcher for those separate roles:

```text
serial serve    -> seriald
serial console  -> serialctl TUI
serial setup/profile/status/doctor/logs -> serialctl management
serial mcp      -> serial-mcp
```

Flashing recipes, board reset policy, Linux command semantics, and
model-specific workflows do not belong in `seriald`. A human, Agent adapter,
OpenChamber integration, or later workflow package may compose the generic
primitives.

## Status

- **Released before v0.4.0**: established platform behavior retained by the
  current release.
- **Released in v0.4.0**: the Profile/diagnostic/Agent-tool consolidation
  retained by v0.5.
- **Released in v0.5.0**: persistent serial Monitor Jobs and their additive
  MCP/notification surfaces.
- **Next**: practical validation or refinement work, not yet complete.
- **Later / candidate**: retained options with no delivery promise.

## seriald

### Released before v0.4.0

- Windows-hosted daemon with remote Windows/Linux clients over versioned HTTP
  and WebSocket protocols.
- Long-lived auto-open sessions with explicit online/waiting/backoff/disabled
  state and reconnect events.
- Multiple concurrent observers of exact RX, confirmed TX, state, Run,
  Operation, Control, and Trigger events.
- Fenced write-control leases with bounded queueing, TTL renewal, explicit
  Human Takeover, stale-writer rejection, and cross-reconnect idempotency.
- At most one active Run per Slot. Run/Operation boundaries scope evidence but
  never pretend to reset or reserve a clean DUT.
- Device-agnostic Trigger Jobs for bounded low-latency
  kickoff/repeated-action/live-literal reactions.
- Per-Slot sequence numbers, `(slot, epoch, seq)` cursors, explicit gaps,
  bounded replay/live queues, and slow-consumer isolation.
- Exact binary journal segments with CRC/torn-tail recovery, bounded scans,
  archive discovery, a gap ledger, and a default 10-GiB retention ceiling.
  Segment scanning is the current implementation; there is no SQLite index.
- Actor-labelled timeline records and observer/operator/admin bearer roles.

### Released in v0.4.0

- WebSocket protocol v3 for the additive Break/event/error enum extensions.
  v0.4 components intentionally reject v2/v1 peers instead of risking a
  runtime decode failure.
- Reusable Transport Profile catalog for baud, data/parity/stop bits, flow
  control, DTR/RTS, and auto-open.
- Device Profiles now also own optional chunk size/delay pacing, alongside
  Shell/U-Boot prompt, EOL, and echo. Existing Slot settings remain a complete
  compatibility snapshot.
- Authoritative `effective_transport` and `effective_write_pacing` in every
  Slot snapshot. A Transport change reopens the handle; a Device change does
  not, and is rejected during an active Run or Trigger.
- Optimistic `config_revision` on Slot and both Profile-catalog mutations,
  with atomic stage/save/commit and stale-update rejection.
- Stable error codes for missing, busy, access-denied, and general port I/O;
  Profile-change conflicts; regex failures; and unsupported Break.
- Read-only daemon/storage/Slot diagnostics including subscriber lag, RX
  overflow, journal quota/usage/queue health, and configuration revision.
- Bounded UTF-8 regular-expression history queries. Literal and regex matching
  span adjacent same-direction serial events while retaining scan/time/byte/
  result and compiled-automaton limits.
- Fenced, Run-bound physical serial Break with an audited timeline event.

### Released in v0.5.0

- Durable, daemon-owned Monitor Jobs over live Slot RX. A Job starts after an
  explicit cursor or the current head, persists independently of Agent turns,
  and resumes after daemon restart.
- Literal and bounded byte-regex matching across adjacent RX events, with
  explicit gap reset/recovery, 250-ms default burst grouping, 30-second default
  cooldown, optional duration, and bounded incident previews/evidence ranges.
- Idempotent Monitor creation keyed by `request_id`, authoritative status and
  cursor, retained incident acknowledgement, and recent-tail/cursor-forward
  pagination under additive `/api/v1/monitors` routes.
- Bounded local Monitor state and notification outbox with explicit capacity
  errors/gaps. Incident evidence remains in the serial journal rather than
  being copied without bound into notification payloads. Oldest summaries
  yield to newer evidence at hard bounds without requiring every Agent adapter
  to expose acknowledgement, and cursor pages surface the resulting gap.
- Optional station-configured HTTP CloudEvents 1.0-shaped sink. With no sink,
  Monitor incidents and outbox events remain available through pull APIs; with
  a sink, delivery retries with bounded exponential backoff until success or
  freshness-TTL expiry. Agent/session routing remains outside seriald.
- WebSocket protocol remains v3. The Monitor surface is additive HTTP/MCP, so
  protocol-v3 v0.4 peers remain wire-compatible for existing realtime features;
  Monitor use requires v0.5 seriald and adapter components.

### Next

- Exercise Monitor restart, replay-gap, burst grouping, regex-boundary,
  incident/outbox-capacity, sink outage/recovery, duplicate delivery, and TTL
  expiry paths against sustained real 115200-baud output.
- Run the two-Slot 115200-baud station matrix continuously: sustained RX,
  slow subscriber, repeated unplug/replug, daemon restart, disk pressure,
  Profile mutation conflicts, and Break support/unsupported drivers.
- Refine native open-error classification from additional Windows adapters and
  drivers without hiding the original OS reason.
- Measure retention and regex-query latency near the 10-GiB ceiling before
  choosing an index or compression format.

### Later / candidate

- TLS or a trusted reverse-proxy deployment profile for networks beyond
  loopback/host-only.
- Windows Service installation and service-account ACL guidance.
- A rebuildable sparse query catalog and optional sealed-segment compression
  after real 24/7 measurements. Raw segment bytes remain the source of truth.
- Explicit external-exclusive handoff for rare `screen`, `minicom`, vendor
  flasher, or diagnostics use.
- Optional reset/power-control resources if stations standardize the hardware.
- An explicit one-shot probe that runs only while the Slot is silent and has
  no Control owner, Run, Trigger, or recent human activity. It would never be a
  periodic guessed heartbeat.
- Device-pool reservations and asset identity if station scale grows beyond a
  small fixed set of frequently swapped boards.
- Compound Monitor rules, update/delete UX, or Monitor-aware diagnostics only
  after real station incidents show that one literal/regex plus status is
  insufficient.

### Non-goals

- Built-in firmware recipes, automatic flashing, or vendor-specific boot
  sequences.
- Automatically restoring a “clean board” when a Run starts or ends.
- Guessing liveness from silence or probing a busy target.
- Embedding OpenCode, Codex, OpenChamber, shell, or DUT semantics in the
  daemon.

## serialctl / serial console

### Released before v0.4.0

- One TUI that subscribes to and switches between multiple Slots.
- LINE and raw byte-entry modes, Human Control queueing, explicit Takeover, and
  visible ownership.
- Continuous prompt/TX/echo projection with exact durable RX/TX audit events.
- Actor-aware styling, bounded local viewport, high-rate rendering protection,
  Shift-free selection/copy, input paste, and serial-only wheel scrolling.
- Interactive setup, status, archive/history query, JSON output, and Windows
  x86-64 plus Ubuntu x86-64 clients.

### Released in v0.4.0

- Unified `serial` launcher in both release packages:
  `serve`, `console`, `setup`, `profile`, `status`, `doctor`, `archives`,
  `logs`, and `mcp`. Legacy component binaries remain available. Dispatch is
  restricted to sibling executables from the same package; there is no `PATH`
  fallback to a possibly different release.
- Ubuntu client release is built natively on the Ubuntu 20.04
  x86_64/glibc-2.31 baseline; it is not a musl or 32-bit build.
- Setup selects a reusable Transport Profile and optional Device Profile.
  Profile commands support list/show/create/update/clone/import/export/delete/
  attach/detach; interactive creation/update and admin-token-file automation
  are both supported. Partial update flags preserve omitted values, explicit
  prompt-clear flags remove prompts, and optional EOL/echo/pacing overrides
  can return to inherited behavior.
  Setup separates credential input/output flags, rejects global
  `--token-file`, and refuses aliased input/destination paths.
- Five diagnostic levels: connection, port, stream, storage, and authoritative
  Slot state. Stream diagnosis compares WebSocket RX with offsets and journal.
- `logs --regex` alongside literal `--contains`, with explicit Run/epoch/cursor
  scoping, bounded continuation, and fail-closed behavior against older
  daemons that cannot advertise bounded server-side regex.
- Full-width colored Run start/end/abort rules in the terminal transcript.
- Queued LINE command preview plus `Ctrl-] d` delete and `Ctrl-] e` return to
  editor. Bytes already entering a physical write cannot be recalled.
- RAW Ctrl-D and Ctrl-Z transmit `0x04`/`0x1a`; Ctrl-C transmits `0x03` in both
  modes without quitting the console.

### Released in v0.5.0

- The Windows and Ubuntu client packages track the v0.5 daemon/MCP release
  while preserving the existing console workflow. The first Monitor release
  does not add Monitor policy or message-center routing to the human TUI.

### Next

- Validate selection, wrapped/wide-character rows, high-rate output, queued
  editing, and Run boundaries in Windows Terminal and the Ubuntu station.
- Improve Profile setup prompts from first-time user feedback while preserving
  deterministic non-interactive flags and revision safety.
- Add a compact in-console detail view for active Run, Operation, Trigger,
  actor, and diagnostic counters only if it remains readable beside raw serial
  output.

### Later / candidate

- Export a selected sequence/time range as text plus authoritative metadata.
- Saved coloring themes and user rules with safe precedence over built-ins.
- A richer native OpenChamber serial panel that reuses the protocol/state
  model rather than embedding the TUI.
- A compact Monitor list/incident indicator only if operators need to manage
  Jobs without MCP or HTTP tooling; exact serial output remains the primary TUI.
- Full terminal emulation only if real DUT workflows require cursor
  addressing; the default remains a serial log console, not a PTY.

### Non-goals

- Using the mouse wheel for command history.
- Hiding Agent writes or reconstructing a cleaner transcript than observed
  bytes and audit events.
- Letting an Agent close a Slot; Agents release Run/Control while `seriald`
  keeps the configured port available.

## serial-mcp

### Released before v0.4.0

- One stdio adapter shared by OpenCode, Codex, and other MCP hosts.
- Adapter-owned Run and fenced-Control renewal with best-effort cleanup.
- Attach-before-write capture, confirmed TX lower bounds, prompt/literal/regex/
  quiet completion, interference/gap reporting, exact cursors, and bounded
  rendered text.
- Current-Run search by default and explicit cursor/archive scopes.
- Generic daemon-native Trigger access with no built-in SigmaStar, U-Boot, or
  flashing recipe.

### Released in v0.4.0

- Stable eleven-tool surface: `devices`, `read`, `command`, `input`, `signal`,
  `trigger`, `wait`, `search`, `run_start`, `run_end`, and `release`.
- Smaller Agent schemas: the adapter owns fencing, UUIDs, pacing, prompt
  selection, live cursors, capture bounds, rendering limits, and lease renewal.
- Compact results center on text, completion/confidence, warnings, truncation/
  gap/interference, and one continuation cursor instead of returning internal
  IDs and duplicate boundary fields by default.
- `input` sends exact UTF-8 bytes without EOL. `signal` exposes Ctrl-C/D/Z and
  physical Break so Agents need not fake raw control characters.
- `command` and `wait` accept one optional literal `expect` or regex boundary;
  prompt/quiet fallback is selected internally. `search` accepts bounded regex
  and remains current-Run scoped by default.
- Concurrent MCP request dispatch and serialized stdout frames. Cancellation
  applies only to pure read tools; a mutating call converges to its
  authoritative result after a possible side effect. Per-Slot write
  serialization prevents interleaved serial bytes.
- Repeated-line folding and bounded ANSI-cleaned evidence reduce context while
  preserving warnings, exact cursors, and durable raw events in `seriald`.

### Released in v0.5.0

- Five additive tools alongside the unchanged eleven core tools:
  `monitor_start`, `monitor_list`, `monitor_status`, `monitor_incidents`, and
  `monitor_stop`.
- `monitor_start` exposes only Slot, exactly one literal/regex matcher, and an
  optional description. The adapter owns the idempotency UUID and safe
  debounce/cooldown/TTL defaults; message-center and delivery policy never
  become routine Agent arguments.
- Monitor calls return immediately and leave the Job in seriald. Incident reads
  are capped at 20 compact entries and carry an opaque decimal continuation,
  exact serial range, bounded preview, and evidence reference/cursor.
- The same tools work without a message center by pull polling. When seriald's
  optional CloudEvents sink is configured, an external message center can
  initiate a new Agent turn while MCP remains the recovery/audit path.

### Next

- Add real OpenCode/Codex regression scenarios where a Monitor outlives the
  initiating turn, then verify pull-only recovery and Agent Message Center push
  delivery produce the same incident/evidence identity.
- Keep real-device regression fixtures for sustained output, delayed prompts,
  partial echo, target wrapping, foreign Human writes, reconnects, cancellation,
  raw signals, Break, and Trigger-based boot interruption.
- Tune context limits and recovery guidance from OpenCode/Codex use without
  adding routine parameters back to the tool surface.
- Publish tested host configuration snippets for Windows-local and
  Windows-daemon/Linux-VM layouts as Agent hosts evolve.

### Later / candidate

- Thin host-specific installers/permission helpers while preserving one shared
  adapter.
- Context-budget presets backed by the same exact event interval.
- Optional workflow packages outside the platform for repeatable vendor/test
  procedures.
- OpenChamber integration that displays the same actor, Run, Operation,
  Trigger, and sequence metadata.

### Non-goals

- Automatic retry after timeout, missing echo, transport loss, or uncertain
  delivery.
- Agent-selected physical pacing in ordinary tools.
- Default global-history search.
- Inferring command success from a returned prompt; request a unique result
  marker when exit status matters.
- Encoding flashing or DUT-specific recipes in MCP tool names or `seriald`.
