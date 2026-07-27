# serial-platform roadmap

This roadmap records the product direction discussed for `seriald`,
`serialctl`, and `serial-mcp`. It is intentionally ordered by value and
dependency rather than by date. Items under **Later / candidate** are ideas,
not commitments; they can be promoted, changed, or removed after station
testing.

## Product boundary

`seriald` is a device-agnostic shared serial control plane. It owns the
physical port, fans one byte stream out to multiple observers, serializes
writes, records authoritative events, and provides bounded low-latency
primitives.

`serialctl` is the human console for operating and monitoring those Slots.

`serial-mcp` is the thin Agent adapter. It translates a small MCP tool set into
the same protocol used by human clients and manages Agent-only state such as
Run ownership and compact context.

Flashing recipes, board reset policy, Linux command semantics, and
model-specific workflows do not belong in `seriald`. A human, Agent adapter,
OpenChamber integration, or later workflow layer may compose the generic
primitives.

## Status

- **Released**: available in v0.3.1 or earlier.
- **Released in v0.3.2**: first published in the current release.
- **Next**: the nearest useful work, not yet implemented.
- **Later / candidate**: retained for evaluation; no delivery promise.

## seriald

### Released

- Windows-hosted daemon with remote Windows/Linux clients over the same
  versioned HTTP and WebSocket protocol.
- Interactive COM discovery and persistent Slot configuration through
  `serialctl init`.
- Long-lived auto-open serial sessions with explicit
  online/waiting/backoff/disabled state and reconnect events.
- Multiple concurrent observers of exact RX, confirmed TX, state, Run,
  Operation, Control, and Trigger events.
- Fenced write-control leases with queueing, TTL renewal, explicit operator
  takeover, and stale-writer rejection.
- Run and Operation boundaries for task-local evidence without pretending to
  reset the physical board.
- Bounded daemon-native Trigger Jobs for low-latency literal matching and
  repeated writes, with timeout, fire-count, Control, Run, and generation
  bounds.
- Slot-safe physical write pacing. Slot settings are the authoritative
  baseline, with a conservative 1-byte/1-ms default for targets that lose
  characters on burst writes.
- Reusable device-model profiles for EOL, echo, Shell prompt, and U-Boot
  prompt. Attaching a model profile makes those effective values
  authoritative without guessing device behavior.
- Per-Slot sequence numbers, reconnect cursors, explicit gaps, bounded live
  queues, and slow-consumer isolation.
- Exact binary journal segments, SQLite index, corruption recovery, bounded
  search, archive queries, and a default 10-GiB retention ceiling.
- Actor-labelled TX audit records so a console can distinguish human, Agent,
  Trigger, and system activity.
- Bearer-token roles for observer, operator, and administrator access.

### Released in v0.3.2

- No new daemon protocol is required for the current console and MCP
  refinements. Ordinary Agent commands and Trigger Jobs use Slot transport
  pacing instead of selecting their own burst settings.

### Next

- Add `serialctl` commands for listing, validating, creating, cloning,
  attaching, importing, and exporting device profiles without hand-editing
  `seriald.toml`.
- Improve open-failure diagnostics with stable error codes and actionable
  owner/driver/port guidance while retaining the native OS error.
- Add administrative log-usage and retention diagnostics.

### Later / candidate

- A reusable transport-profile catalog for physical settings such as baud,
  8N1, flow control, DTR/RTS, pacing, and auto-open. Existing per-Slot
  settings would remain a compatible explicit snapshot/override.
- TLS or a trusted reverse-proxy deployment profile for networks beyond a
  host-only or otherwise trusted station network.
- Windows Service installation and service-account ACL guidance.
- Optional sealed-segment compression after measuring CPU, recovery, and
  query costs on 24/7 stations.
- Explicit external-exclusive handoff for rare `screen`, `minicom`, vendor
  flasher, or diagnostic-tool use.
- Optional hardware reset/power-control resources as separate generic
  capabilities, only if stations standardize the hardware.
- An explicit one-shot probe primitive that runs only while the Slot is
  silent and has no Control owner, Run, Trigger, or recent human activity.
  The caller would supply bounded request/response/timeout data; seriald would
  never probe periodically or guess a device-specific heartbeat.
- Device-pool reservations and asset identity, only if station scale grows
  beyond a small fixed set of frequently swapped boards.

### Non-goals

- Built-in firmware recipes, automatic flashing, or vendor-specific boot
  sequences.
- Automatically restoring a “clean board” state when a Run starts or ends.
- Guessing liveness from silence, inventing heartbeat bytes, or probing a busy
  target without an explicit policy.
- Embedding OpenCode, Codex, OpenChamber, shell, or device-model semantics in
  the daemon.

## serialctl

### Released

- One interactive console that switches between multiple live Slots without
  restarting or naming the Slot on every command.
- Human command mode and raw byte-entry mode.
- Operator Control acquisition, queued writes, explicit takeover, and visible
  Control ownership.
- Live actor markers and distinct styling for device RX, human TX, Agent TX,
  Trigger activity, state transitions, warnings, and errors.
- Interactive setup, status, doctor, port discovery, log query, and JSON
  diagnostics.
- Windows x86-64 and Ubuntu x86-64 baseline release artifacts that can connect
  to a Windows daemon.

### Released in v0.3.2

- Keyword colouring uses case-insensitive token boundaries across all
  severity/status classes: embedded words such as `information` or
  `errorCounter` are no longer partially coloured, while `[INFO]`,
  `level=info`, and underscore-separated log identifiers remain readable.
- The mouse wheel scrolls only the serial-output viewport.
- Output and input areas have explicit mouse focus. Output supports
  Shift-free drag selection and right-click copy. Windows input supports
  right-click paste; portable keyboard paste remains available on Linux.

### Next

- Add the device-profile management flow described under `seriald`.
- Refine selection, wrapped-line, wide-character, and very high-rate output
  behavior from Windows Terminal and Ubuntu station feedback.
- Add a compact in-console view for active Run, Operation, Trigger, and actor
  details without obscuring raw serial text.

### Later / candidate

- Export a selected sequence/time range as text plus authoritative event
  metadata.
- Saved colouring themes and user rules with safe precedence over built-in
  token and prompt highlighting.
- A richer native serial panel for OpenChamber that reuses the protocol and
  state model rather than embedding `serialctl`.
- Full terminal emulation only if real target workflows require cursor
  addressing; the default remains a serial log console, not a general PTY.

### Non-goals

- Giving mouse-wheel behavior to command history.
- Hiding Agent writes or reconstructing a cleaner transcript than the bytes
  and audit events actually observed.
- Letting an Agent close a Slot; Agents release their Run/Control while the
  daemon keeps the configured port available.

## serial-mcp

### Released

- One stdio MCP adapter shared by OpenCode, Codex, and other MCP-capable Agent
  hosts.
- A compact tool surface for device discovery, Run lifecycle, command,
  wait/read/search, Trigger, and release operations.
- Adapter-owned Run and fenced-Control renewal with best-effort cleanup on
  exit.
- Attach-before-write capture, TX confirmation, prompt/literal/regex/quiet
  completion modes, stale-prompt protection, interference reporting, exact
  cursors, explicit gaps, and bounded text.
- Current-Run search by default, with explicit cursor/archive scopes to avoid
  treating an earlier test cycle as current evidence.
- ANSI cleanup, bounded context, repeated-line folding, and optional raw
  evidence.
- Generic daemon-native Trigger access without built-in SigmaStar, U-Boot, or
  flashing recipes.

### Released in v0.3.2

- Command and Trigger schemas no longer expose per-call chunk size or delay.
  The adapter always uses the Slot's safe write cadence.
- Echo reconciliation no longer turns a missing echo into an automatic blind
  timeout when a valid post-TX requested boundary is observed. Results expose
  lower confidence, missing/incomplete echo, and suspected input loss without
  retrying a possibly executed command.
- Quiet completion requires post-TX evidence instead of succeeding from an
  empty interval.

### Next

- Exercise the adapter against sustained 115200-baud output, delayed prompts,
  partial echo, target line wrapping, foreign operator writes, reconnects, and
  Trigger-based boot interruption; keep these cases as regression fixtures.
- Improve Agent-facing recovery guidance for gap, interference, Run loss,
  uncertain delivery, and incomplete bounded searches while keeping tool
  parameters small.
- Publish copy-ready OpenCode and Codex configuration examples for both
  Windows-local and Windows-daemon/Linux-VM layouts.

### Later / candidate

- Thin host-specific packaging for OpenCode permissions and Codex setup while
  preserving one shared adapter implementation.
- Context-budget presets for different Agent hosts, backed by the same exact
  event interval.
- Optional higher-level workflow packages outside the platform for repeatable
  vendor/test procedures.
- OpenChamber terminal integration that displays the same actor, Run,
  Operation, Trigger, and sequence metadata.

### Non-goals

- Automatic command retry after timeout, missing echo, transport loss, or
  uncertain delivery.
- Agent-selected physical pacing in ordinary tools.
- Default global-history search.
- Inferring command success from a returned prompt; callers should request an
  explicit result marker when exit status matters.
- Encoding flashing or device-specific recipes in MCP tool names or in
  `seriald`.
