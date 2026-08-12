# Serial Platform

`serial-platform` is an independent shared serial-port control plane. `seriald`
runs on the machine physically connected to the serial card; any number of
`serialctl` clients can observe the same Slots, while a fenced control lease
serializes ordinary writers. A separately audited cooperative Human write may
be injected without taking over an Agent-owned Run.

It deliberately has no OpenCode or OpenChamber runtime dependency. The
`serial-mcp` adapter exposes the same platform to OpenCode, Codex, and other
MCP clients without making `seriald` Agent-specific.

v0.6.1 makes fresh personal installations token-free on loopback, switches the
terminal's default language to Chinese, reorganizes Help into scrollable task
groups, and clarifies that ordinary Trigger kickoff/action calls omit the
optional start gate. The current 18-tool MCP registry retains the persistent
Monitor Jobs introduced in v0.5.0 and the ordinary serial/Run/Trigger
operations. The unified launcher introduced in v0.4 remains: `serial serve`
starts the backend,
`serial console` opens the TUI, `serial setup` configures a station,
`serial profile ...` manages reusable profiles, `serial model ...` manages the
DUT identity tree, and `serial mcp` starts the MCP adapter. The component
executables remain in the release package and can still be invoked directly.

See [ROADMAP.md](ROADMAP.md) for released capabilities, the next practical
work, and longer-term candidates separated by `seriald`, `serialctl`, and
`serial-mcp`.

Protocol and integration references:

- [Complete MCP/HTTP/WebSocket protocol](./docs/PROTOCOL.md)
- [All MCP tool schemas and result contracts](./docs/MCP_TOOLS.md); inspect the
  installed executable directly with `serial mcp --dump-tools`.

## v0.6.1 release highlights

- The console defaults to Chinese when no saved language exists; `Ctrl-] g`
  still switches between Chinese and English and persists the choice.
- Help is grouped by navigation/display, Control and Agent cooperation,
  queued input, LINE mode, RAW mode, and safety/history. Its independent
  PgUp/PgDn/Home/End scroll state keeps the full reference usable on small
  terminals.
- A normal Trigger supplies optional `kickoff` plus required `action` and
  omits `start_contains`. Kickoff confirmation immediately enables actions;
  the explicit start literal remains available only when live RX must gate the
  first action.
- Fresh configurations set `auth_required=false`, contain no credentials, and
  are restricted to loopback. Older configurations without that field decode
  it as `true` and retain their observer/operator/admin tokens unchanged.
- Device Profile pacing is stated precisely: `write_chunk_size` bounds each
  driver step and `write_chunk_delay_ms` is requested between steps, not after
  the final chunk. It is neither a baud period nor an exact per-character
  clock; driver blocking and OS timer granularity may add latency.

## v0.6.0 release highlights

- Operators can select or create Profiles and arbitrarily deep DUT-model
  entries directly in the terminal, and can clone/edit common UART settings
  without leaving the console. The same model catalog remains scriptable from
  the CLI.
- A frozen visual-row scrollback snapshot no longer jumps when live RX/TX
  arrives. Short initial histories no longer scroll into blank or disappearing
  rows.
- Commands typed while an Agent owns the Run appear as ordered, individually
  editable/removable cards. Empty Enter only follows the live tail; ordinary
  Enter queues without takeover, Alt-Enter is an audited cooperative Human
  write, and explicit takeover remains separate.
- `device_models` and `device_model_set` bring the MCP registry to exactly
  18 tools. Agents must confirm that the configured model matches the physical
  DUT through serial, Telnet, web UI, or a human before model-specific work.
- Release archives are built and verified by Jenkins from one ARM64 Linux
  worker: cargo-zigbuild targets x86_64 GNU/Linux with a glibc 2.31 ceiling,
  while MinGW-w64 produces the Windows x86_64 GNU-ABI package.

## What v0.6 provides

- Interactive COM discovery and Slot configuration from `serial setup`.
- Two explicit reusable configuration layers: a Transport Profile for physical
  UART settings and an optional Device Profile for DUT behavior and safe write
  pacing.
- Interactive and scriptable Profile list/show/create/update/clone/import/export/
  attach/detach commands with optimistic `config_revision` protection.
- A separate hierarchical `DeviceModel` catalog with aliases, per-Slot
  confirmation records, CLI/TUI selection, and narrow Agent binding updates;
  model identity never silently changes Device Profile behavior.
- Long-lived serial ownership with auto-open and reconnect.
- In-place Slot reconfiguration: an existing Slot keeps its epoch, sequence,
  replay ring, and subscribers while the old OS handle is fully stopped before
  a changed port can open. Candidate configs and new actors stay paused and
  hidden until the atomic configuration save succeeds.
- Multiple subscribers and one WebSocket attachment for multiple Slots.
- Human write control with visible queueing, explicit takeover, a 60-second
  idle release policy, TTL, and fencing.
- LINE and RAW input modes in a full-screen terminal; switch Slot without
  leaving the current terminal.
- Full-width Run start/end/abort boundaries, visible queued LINE commands, and
  edit/delete controls for commands still waiting on Human Control.
- Source-aware TX/RX timeline with daemon epoch, per-Slot sequence, physical
  generation, byte offsets, Run, and Operation fields.
- Crash-recoverable binary journal, 64 MiB/1 hour segments, CRC validation,
  recovered gap-ledger tails, query-derived sequence-discontinuity gaps,
  bounded/concurrency-limited history queries, and a 10 GiB retention ceiling.
- Token-free loopback-only personal mode by default. Existing authenticated
  installations retain observer/operator/admin Bearer credentials; a remote
  bind is rejected unless authentication is enabled.
- Layered `doctor` checks for connection/authentication, a physical port, the
  live WebSocket stream, journal storage, and full Slot state.
- Windows daemon with a Windows or Linux/VM client over a trusted host-only
  network.
- One `serial-mcp` adapter for OpenCode and Codex, with Run-scoped search,
  cursor-safe reads, bounded command capture, Agent writes atomically bound to
  a Run owned by that adapter process, raw Ctrl-C/D/Z and serial Break support,
  and no Agent takeover/close.
- A protocol-v3, daemon-owned Trigger Job for bounded low-latency
  `initial_write -> repeated action -> live RX stop` reactions. It is
  byte-oriented and device-agnostic; every fire uses the normal fenced,
  confirmed, audited write path.
- Persistent Monitor Jobs that match one literal or bounded UTF-8 regular
  expression against live RX, group bursts into retained incidents, and keep
  an exact evidence cursor/range. Notification delivery is optional: without a
  message center, incidents remain pullable through HTTP and `serial-mcp`; with
  a configured sink, seriald also emits CloudEvents-shaped notifications from
  a durable bounded outbox.
- Bounded cross-reconnect write safety within one daemon epoch: recent duplicate
  request IDs return their cached result, older executed IDs are rejected
  instead of being written again, and an unacknowledged outcome remains
  visibly marked uncertain. Input is never automatically replayed.

The safe Transport default is 115200 8N1, no flow control, DTR and RTS low,
and `auto_open=true`. Generic device behavior uses command EOL `\r`, echo on,
no guessed Shell/U-Boot prompt, automatic probe disabled, and one-byte/1-ms
write pacing. Attach a Device Profile when a DUT needs different behavior.

Fresh installations write `auth_required = false`, omit the `[auth]` table,
and can listen only on `127.0.0.1` or `::1`. Existing configuration files lack
that field, decode it as `true`, and keep their existing three credentials.
To migrate an existing personal configuration, stop `seriald`, make sure its
persisted bind is loopback, run `serial auth disable`, then start it again. The
command atomically rewrites the configuration without an `[auth]` table; it
refuses a non-loopback bind.
Stop `seriald`, then run `seriald credentials` (or `serial credentials`) on a
token-free install to generate and persist observer/operator/admin credentials.
Do not run the command beside a live daemon: that process still owns its old
configuration snapshot and a later configuration save could overwrite the
change. Start it again before changing to a non-loopback bind. This explicit
transition prevents a runtime bind override from accidentally exposing an
unauthenticated daemon.

On Windows, the daemon uses native bounded synchronous COM reads/writes on
dedicated blocking workers. It does not route COM writes through Tokio's
named-pipe/overlapped backend. A Slot is not allowed to reopen until both
reader and writer handles have finished and been released, preventing a
timed-out write from leaving the port permanently locked with access denied.

With hardware flow control, the driver owns RTS and `rts=true` is rejected.
Linux drivers may transiently assert DTR during open even when the final
requested level is low; DTR-reset-sensitive targets should use validated
Windows-host behavior, avoid automatic reopen while the target must remain
untouched, or use electrical isolation/reset gating.

## Install from a release

Each release provides two x86_64 packages:

- `serial-platform-<version>-windows-x86_64.zip` contains the unified
  `serial.exe` plus `seriald.exe`, `serialctl.exe`, and `serial-mcp.exe`. These
  binaries use the GNU/MinGW-w64 ABI, not the MSVC ABI.
- `serial-platform-<version>-linux-x86_64-ubuntu20.04.tar.gz` contains
  `x86_64-unknown-linux-gnu`/glibc `serial`, `serialctl`, and `serial-mcp`
  clients cross-built with cargo-zigbuild against the Ubuntu 20.04 glibc 2.31
  ceiling. It is intended for 64-bit x86 Ubuntu 20.04 or newer. It does not
  contain `seriald`, because the Windows host owns the workstation COM ports,
  and it does not support 32-bit i386/i686 Ubuntu.

Use every executable from the same release across the Windows host and Linux
VM. v0.6.1 retains WebSocket protocol v3 from v0.4.0; existing v0.4 realtime
peers are wire-compatible for that protocol surface, but the Monitor HTTP APIs
and MCP tools require v0.5 components. Protocol-v2 executables from 0.3.x and
protocol-v1 executables from 0.2.x are not compatible with v3. The HTTP route
namespace remains `/api/v1`; the route namespace and WebSocket payload protocol
are versioned independently.

Extract the Windows package on the host connected to the serial card. Extract
the Linux package in the VM and make the client executable if the archive tool
did not preserve its mode:

```sh
tar -xzf serial-platform-v0.6.1-linux-x86_64-ubuntu20.04.tar.gz
cd serial-platform-v0.6.1-linux-x86_64-ubuntu20.04
chmod +x serial serialctl serial-mcp
./serial --version
```

Release checksums are published in `SHA256SUMS`. Each package also contains
`BUILD-INFO.json`, an internal `MANIFEST.sha256`, the complete `docs/` and
`adapters/` trees, and the top-level product documentation. Jenkins verifies
the target architecture, application version, exact 18-tool MCP registry,
Linux glibc ceiling, absence of unbundled MinGW runtime imports, archive
integrity, and both checksum layers before the artifacts are published. The
command examples below assume the executables are on `PATH`. From an extracted
package directory, use
`.\serial.exe` on Windows and `./serial` on Linux. Linux does not search the
current directory by default, so `serial setup` returns `command not found`
unless the binary was installed on `PATH`; from the extracted directory, run
`./serial setup`. The launcher itself resolves only sibling executables from
that same directory and never falls back to another `seriald`, `serialctl`, or
`serial-mcp` found on `PATH`; a partial/mixed installation fails visibly.

## Build

Install Rust 1.88 or newer. A local Windows MSVC source build needs the Visual
Studio C++ Build Tools and Windows SDK; this is distinct from the published
GNU/MinGW-w64 package. On Linux, install the distribution's C/C++ build tools.
Then run from this directory:

```sh
cargo build --release
cargo test --workspace
```

Build `seriald` on the Windows host that owns the COM ports. The `serial`,
`serialctl`, and `serial-mcp` clients can be built on Windows or natively
inside the Linux VM.

## First start

On Windows:

```powershell
serial serve
```

The first start creates a token-free configuration bound to loopback. No
credentials are generated or printed; local HTTP and WebSocket clients omit
Authorization and receive the effective admin role. A non-loopback bind is
rejected in this mode. Existing installations remain authenticated: a legacy
configuration without `auth_required` decodes that field as `true` and keeps
its observer/operator/admin credentials. Run `serial credentials` while the
daemon is stopped to enable authentication and generate those credentials for
a fresh installation before configuring a trusted host-only remote bind.

For non-interactive setup, `--admin-token-file` and
`--operator-token-file` are read-only credential inputs, while
`--save-operator-token-file` is the destination for the validated daily
operator credential. Global `--token-file` is rejected by `setup`, and all
input/output credential paths must resolve to different files; setup never
overwrites an input credential through a relative path, symlink, or
case-insensitive Windows alias.

In another terminal:

```sh
serial setup
```

The wizard connects to `seriald`, asks the daemon to enumerate its own COM
ports, lets you select the usual two ports, names them `slot-1`, `slot-2`, and
shows the available Transport and Device Profiles. Select
`generic-115200` plus `Generic` when the DUT behavior is unknown. Existing
Slots omitted from a new scan, including temporarily absent COM ports, are kept
by default; deletion requires an explicit confirmation. A Slot is the stable
station channel, not a device model or serial number. Replacing the sample
connected to the same serial-card channel normally means attaching a different
Device Profile, not changing the Slot.

Create a reusable profile before setup, or attach it later:

```sh
serial profile transport create --interactive
serial profile device create --interactive
serial profile device update my-dut --interactive
serial profile attach --slot slot-1 --transport generic-115200 --device my-dut
```

In default loopback mode, Profile mutations need no credential prompt. When
authentication is enabled, mutations request the administrator credential
interactively and never save it. For authenticated automation, use
`--admin-token-file`; list/show/export remain read-only. Every mutation carries
the revision it just read, so a second administrator cannot silently overwrite
a newer catalog or Slot update.

Start the console without naming a Slot:

```sh
serial console
```

Running plain `serial` is equivalent to `serial console`.

It restores the previous Slot and keeps all configured Slots subscribed. Main
keys:

| Key | Action |
|---|---|
| `Alt-1` … `Alt-9` | Switch Slot |
| `Ctrl-] 1` … `9` | Reliable Slot switch prefix |
| `Ctrl-] l` / `Ctrl-] r` | LINE / RAW input |
| `Ctrl-] s` | Select the next Slot |
| `Ctrl-] f` / `Ctrl-] End` | Resume following live output |
| `Ctrl-] PgUp` / `Ctrl-] PgDn` | Local scroll, including in RAW mode |
| `Ctrl-] v` | Toggle compact/detailed timeline |
| `Ctrl-] g` | Switch UI language and save it |
| `Ctrl-] m` | Open the Profile / device-model / serial-settings / help menu |
| `Alt-Enter` | Send the current LINE command cooperatively without taking the Agent lease |
| `Ctrl-] t` | Explicitly take over write control |
| `Ctrl-] c` | Release control or cancel queued input |
| `Ctrl-] d` | Delete the newest queued LINE command |
| `Ctrl-] e` | Return the newest queued LINE command to the editor |
| `Ctrl-] u` | Select any queued LINE command; Up/Down selects cards, PageUp/PageDown scrolls long text, and `d`/`e` deletes/edits |
| `Ctrl-] p` | Confirm a blocked multiline/large paste |
| `Ctrl-] ?` | Help |
| `Ctrl-] q` | Quit |
| `Ctrl-] Ctrl-]` | Send byte `0x1d` to the device |
| `Ctrl-C` | Send ETX byte `0x03` immediately in LINE or RAW mode |
| `Ctrl-D` / `Ctrl-Z` in RAW | Send `0x04` / `0x1a` exactly |

In LINE mode, plain `PageUp`/`PageDown` also scroll locally; RAW keeps those
keys for xterm byte sequences, so use the `Ctrl-]` prefix there.
`Ctrl-C` is the asynchronous interrupt exception to LINE editing: it discards
the unsent local draft, sends only ETX without the Profile EOL, and returns the
view to live output. It does not quit `serialctl` or release Human Control.
In RAW mode, Ctrl-D and Ctrl-Z are ordinary raw control bytes; they do not exit
the local console or suspend its process.

Mouse capture is on by default. The wheel scrolls only the serial-output
viewport; clicking the output or input pane changes focus; output left-drag
selects without `Shift`; releasing the left button resumes live output while
retaining the selected text; and output right-click copies that retained
selection. A missing mouse-up event is finalized after five seconds, so a
selection can never silently pin the live viewport indefinitely.
On Windows, input right-click pastes from the native Unicode clipboard.
`Ctrl+Shift+V` remains the portable paste path, including Ubuntu. Linux output
copy uses OSC 52 and may require clipboard access to be enabled in the terminal
emulator. Set `mouse_capture = false` in `serialctl.toml` to return mouse
selection to the terminal emulator; that also disables in-app wheel scrolling.
After confirmation, a LINE-mode multiline paste is queued as distinct logical
commands, each with its own Operation ID and effective EOL. RAW
paste remains one unmodified byte burst. Adjacent RAW keystrokes that have not
started a physical write are coalesced into bounded 4 KiB chunks, so waiting
behind another actor does not exhaust the 16-chunk queue after only 16
characters. The 64 KiB total local write bound still applies; input that would
cross it is rejected as one unit and is not partially appended.

Opening the console only subscribes. The first ordinary `Enter` asks for Human
Control. If another actor owns it, the request queues; only `Takeover` revokes
it. While an Agent Run is active, an empty `Enter` queues no bytes and simply
returns the output to its live tail. A non-empty command appears above the input
as its own oldest-first numbered card with the complete command text. Before a
queued LINE command starts sending, `Ctrl-] d` removes the newest command and
`Ctrl-] e` returns it to the editor; `Ctrl-] u` opens the queue selector so any
card can be deleted or returned for modification. Card text wraps by terminal
display width, including double-width CJK characters. A short terminal exposes
an explicit selected-card detail viewport; PageUp/PageDown scrolls that text
instead of silently truncating it. Bytes already in a physical write are locked
and cannot be recalled. Submitting a restored command places the edited version
at the queue tail.

`Alt-Enter` is a separate, explicit cooperative path available only while the
same Agent owns both the current lease and active Run. It writes immediately
without transferring or revoking that lease, binds the request and its retry
identity to that exact Run, remains attributed to the Human in the audit
timeline, and makes the Agent's overlapping evidence
`interfered`. `Ctrl-] t` remains the explicit takeover path and aborts the
Agent Run. These two choices are intentionally visible rather than inferred
from typing.

RAW bytes are not lossily converted into editable text; cancel their queue with
`Ctrl-] c`. Queued input expires after 60 seconds without human activity.
`Ctrl-] c` cancels it by reconnecting the actor connection; the protocol carries a dedicated
`cancel_acquire` message, but the current terminal still uses that reconnect, which
also releases this terminal's control on every other Slot, then automatically
restores all subscriptions.
An acquired human lease is renewed only while that Slot has recent manual
activity or an in-flight write, and is released after 60 seconds idle. LINE/RAW
mode and command history are independent for every Slot.

`Ctrl-] m` opens the extensible configuration menu. Its current pages are
Transport/Device Profiles, hierarchical DUT models, serial settings, and help.
Lists use Up/Down/Enter/Esc. Model parents expand before their indented children
become selectable. In the model tree, Enter toggles a parent or binds a leaf,
Left/Right collapses or expands, and `b` binds the selected node including a
parent model. Profile catalog changes and serial-setting clones ask for a
masked one-time administrator token. Its temporary values are dropped after
the request, and the token is never written to the client config. Device-model binding uses the
narrow operator API and records how the physical identity was confirmed.

When the output is scrolled upward, the console freezes the currently wrapped
visual rows. New output therefore cannot push the inspected page away. If the
history does not exceed the output viewport, upward scrolling is a no-op; this
also prevents the disappearing-text behavior immediately after a port opens.
The compact console is a derived terminal projection rather than one visual row
per audit event: a prompt, confirmed TX command, and exact device echo remain on
one row, including character-at-a-time RAW input. Actor-colored markers are
attached to the completed mixed row, while the durable journal retains separate
RX and TX events. Exact echo suppression is enabled only when the resolved
`effective_echo` is `on`; `auto` keeps all RX bytes until a future authoritative
probe can decide the mode. Echo reconciliation also recognizes the observed
target-TTY `CR CR LF` hard wrap inside an otherwise exact long-command echo;
on any mismatch the complete speculative RX is replayed and all pending
expectations are abandoned. Suppression applies only to an immediately
following exact echo; it never searches later boot or password output for
matching bytes. `Ready` preserves a replay-tail expectation because its echo
may already be in flight as live RX. A serial close/reopen
commits an unfinished TX row before any partial RX echo, so text from different
physical sessions is never merged. Detailed timeline mode narrows its source
column on an 80-column terminal to preserve useful command/output payload.
The projection accepts standard 7-bit `ESC` controls but treats isolated 8-bit
C1 values as untrusted UART data, rendering invalid UTF-8 with replacement
characters. Unterminated escape/control strings recover on a line boundary or
a strict size limit, so one malformed or binary RX event cannot hide all later
replay and live output. These presentation rules never alter the exact bytes
retained by `seriald`.
Wrapped rows are measured before the output viewport is scrolled, so following
live output always shows the newest prompt even when a long command or log line
occupies several 80-column screen rows. Local presentation remains bounded to
20,000 rows / 4 MiB per Slot. Once older local rows are evicted, the title and a
synthetic oldest-row boundary state that local display history was truncated
and direct the operator to `serial logs`; durable history is not presented
as though the retained local row were its true beginning.
Repeated device rows remain uncollapsed in v0.6: live monitoring stays faithful
to their exact order, while bounded log/search tools handle high-volume review.
Run lifecycle events are drawn as full-width colored start/end/abort rules, so
an operator can see exactly which output interval belongs to an Agent task
without confusing a Run with a board reset.
If the connection drops after a write was sent but before its acknowledgement,
the footer keeps an `OUTCOME UNCERTAIN` warning. Inspect the TX timeline before
deciding whether to retry; disconnected input is never replayed automatically.

## Windows host-only VM use

Bind `seriald` explicitly to the Windows host-only adapter address, for example:

```powershell
serial serve --bind 192.168.56.1:3210
```

`--bind` is a runtime-only override: repeat it on every launch (or stop the
daemon, update the persisted `bind` value, and restart). Use `serial paths` to
print the exact daemon configuration, data, and journal locations.

Then run inside the Linux VM:

```sh
./serial --endpoint http://192.168.56.1:3210 setup
./serial console
```

The endpoint supplied during `setup` is saved in the Linux user's local
configuration. Later launches can therefore use plain `./serial`; pass
`--endpoint` again only to select a different daemon.

Limit the Windows firewall rule to the host-only subnet. v1 accepts plain HTTP
only and is intended for loopback or a trusted host-only network. Do not expose
it to a normal LAN or the Internet; TLS/private-network support is on the
roadmap.

## Inspection and history

```sh
serial status
serial doctor
serial doctor port --slot slot-1
serial doctor stream --slot slot-1 --duration 10
serial doctor storage
serial doctor state --slot slot-1
serial archives
serial archives --slot slot-1
serial logs --slot slot-1 --limit 200
serial logs --slot slot-1 --after-seq 1200 --epoch <epoch-uuid>
serial logs --slot slot-1 --after-seq 1200 --through-seq 1250 --epoch <epoch-uuid>
serial logs --slot slot-1 --epoch <epoch-uuid> --direction rx
serial logs --slot slot-1 --after-time 2026-07-19T09:00:00+08:00
serial logs --slot slot-1 --contains panic
serial logs --slot slot-1 --regex "(?i)panic|watchdog"
serial logs --slot slot-1 --run <run-uuid> --contains panic
```

The default doctor verifies saved configuration, HTTP reachability,
authentication, and daemon identity. The subcommands then isolate port-open,
live-delivery, storage, and authoritative Slot-state failures. `doctor stream`
compares an independent WebSocket subscription, RX offsets, and the journal;
it distinguishes a legitimately silent target from a delivery fault.

`--contains` and `--regex` are mutually exclusive bounded historical queries.
Regular expressions are compiled with size/automaton limits and evaluated only
inside the query's existing event/byte/time scan budget; regex does not turn a
bounded query into an unbounded global search. Regex queries fail closed
against an older daemon that cannot advertise bounded server-side support;
they never fall back to an unbounded client-side scan. The interactive console
never silently substitutes an old global match for the current Run. Every
response carries its next cursor, truncation state, first available sequence,
and retention/corruption gaps. If `--epoch` is omitted, the CLI searches only
the current daemon epoch. Run `serial archives` first to discover retained
historical epoch UUIDs, then pass one to `logs --epoch`. `archives --json` and
`logs --json` retain exact nanosecond values and raw event fields; ordinary
output displays RFC3339 timestamps in the client's local timezone at
millisecond precision. Time filters require RFC3339 input with an explicit
timezone, and `--direction` accepts `rx`, `tx`, or `none`.
`--through-seq` is inclusive, so `--after-seq A --through-seq B` reads exactly
`(A, B]`; use that range when dereferencing a Monitor Incident so later serial
output cannot enter its evidence.
Without `--run`, `--operation`, or seq/time bounds, ordinary `logs` output
warns that the query spans the entire selected daemon epoch and may include
older test cycles. A text or regex filter only filters that selected range; it
does not make an old match current. Archive catalog times labeled
`segment-open` are segment creation bounds, not exact first/last event
timestamps.

An epoch is the UUID for one `seriald` process lifetime. It changes whenever
the daemon restarts. A durable continuation therefore uses
`(slot_id, epoch, after_seq)`: the same sequence number under a new epoch is a
different timeline and must not be treated as a continuation of the old one.

## OpenCode and Codex

Install `serial-mcp` on the same Windows or Linux machine where the Agent
platform runs. A new loopback-only personal installation needs only the
endpoint; `serial-mcp` sends no Authorization header. Existing authenticated
or remotely bound installations continue to reuse the operator token written
by `serial setup`, so Agent config contains no literal secret. Configure the MCP host to
run `serial mcp` (or the component `serial-mcp` directly). Ready-to-copy
OpenCode JSONC and Codex TOML examples and the expected Run workflow are in
[adapters/README.md](./adapters/README.md). The current 18-tool registry and
result contracts are documented in [MCP_TOOLS.md](./docs/MCP_TOOLS.md) and can
be inspected as authoritative JSON with `serial mcp --dump-tools`.

The adapter connects directly to `seriald`; the Agent is not asked to compose
shell commands around `serialctl`. OpenCode exposes the tools as
`serial_devices`, `serial_command`, and so on. Codex exposes the same tools in
the configured `serial` MCP namespace.

Ordinary calls expose only task-level inputs; the adapter owns
request/operation IDs, control fences, capture cursors, prompt selection,
bounded rendering, and lease renewal. Compact results keep the main evidence
(`text`, completion/confidence, warnings, and one continuation cursor) while
internal audit identifiers remain in the authoritative timeline. Model names
remain assertions: the Agent must confirm the physical DUT through serial,
telnet, the web UI, or a person before relying on a configured assignment.

`serial_monitor_start` takes a Slot plus exactly one `contains` or `regex`
matcher and returns a stable Monitor ID immediately. The Job starts at the
current Slot head by default, persists in `seriald`, and therefore survives the
stdio MCP process or Agent turn ending. `serial_monitor_incidents` returns at
most 20 compact incidents per call; omit `after` for the recent tail, use
`after="0"` to page from the oldest retained incident, or pass its decimal
`next_after` cursor to continue forward. Each incident includes a short
preview, exact `(epoch, seq_start, seq_end)` range, and `evidence_ref`/
`evidence_cursor` so the caller can fetch authoritative serial bytes without
putting an unbounded log in model context. An exact MCP follow-up uses
`read(scope=archive, epoch=evidence_cursor.epoch,
after_seq=evidence_cursor.after_seq, through_seq=seq_end)`.
`serial_monitor_stop` prevents future matches but retains already recorded
incidents.

`serial_trigger` handles short timing windows without making the Agent or the
Linux VM loop repeated `serial_command` or `serial_input` calls. It requires an
adapter-owned Run and takes explicit UTF-8 text plus EOL for an optional
one-time `kickoff` and the repeated `action`, along with an optional start
literal, stop literals, interval, timeout, and maximum-fire bounds.
`kickoff` is sent once after the RX matchers are armed; it does not increment
the action fire count. For normal kickoff-then-action use, omit
`start_contains`: successful kickoff confirmation enables the first action
immediately. Set `start_contains` only when a specific live RX literal must
explicitly gate that first action. The MCP surface is text-oriented, while
seriald's lower WebSocket protocol keeps every Trigger payload and literal as
raw bytes encoded with base64. Neither layer infers a Prompt or byte sequence
from the device profile. For example,
`kickoff={"text":"reboot","eol":"\r"}`,
`action={"text":"slp","eol":""}`, and
`stop_contains=["SigmaStar #"]` form one possible caller-supplied recipe; none
of those strings has special meaning to the platform. A matched result proves
only that the requested live RX literal was observed. `interval_ms` is the
delay after one action write is confirmed before the next becomes eligible;
pacing and physical-write time are additional. `timeout_ms` stops scheduling
new writes, but one already accepted physical write is allowed to settle and
be audited before the authoritative terminal result. See
[Trigger Jobs](./DOCUMENTATION.md#trigger-jobs) for hard limits and terminal
status semantics.

`max_fires` limits only confirmed action sends. If `stop_contains` is
configured, exhausting that budget stops additional writes but keeps the live
RX matcher armed until a stop literal appears or the original `timeout_ms`
expires. A confirmed Trigger TX therefore does not prove that the target
action either succeeded or failed.

`serial_command` requires a Run previously created by this `serial-mcp`
process. Every Agent write carries that Run ID; inside the same serialized Slot
actor that validates the control lease/fence, `seriald` rejects a missing,
changed, or foreign-owned Run before pacing-budget calculation or physical
write. Ordinary Human and legacy non-cooperative clients may omit this optional
boundary and retain lease-only writes. A Run isolates only its log/event interval—it does not reset,
initialize, or otherwise clean the device.

The adapter renews control only for Runs it currently owns. A lost connection
or failed renewal forgets all owned Run state and never reacquires control to
continue writing outside the old boundary. Successful `run_end` stops renewal
and immediately attempts a best-effort release; release failure cannot undo the
ended Run, and an unacknowledged lease expires by its TTL. A later explicit
`release` is idempotent when this adapter process has no local lease. None of
these operations closes `seriald`'s physical port.

Agent searches remain Run-scoped while paging: a `truncated` `current_run`
result returns the same `run_id` plus an exact `epoch`/`after_seq` continuation,
and an incomplete empty page is never presented as proof that no match exists.
For delayed markers such as background-test completion, `command.regex` is the
sole command boundary; a configured Shell prompt or quiet gap cannot finish the
capture first. `search` also accepts bounded regex matching. See the adapter
guide for valid parameter combinations and response fields.

`input` writes exact UTF-8 bytes without appending EOL. `signal` sends exact
Ctrl-C (`0x03`), Ctrl-D (`0x04`), Ctrl-Z (`0x1a`), or a physical serial Break
with a bounded duration. Break is a UART line condition rather than an encoded
byte and is still fenced, Run-bound, and audited.

MCP requests may run concurrently, but cancellation applies only to the
read-only observations `devices`, `device_models`, `read`, `wait`, `search`,
`monitor_list`, `monitor_status`, and `monitor_incidents`. Once a mutating
tool may have crossed a serial/Run/Control side-effect boundary, the adapter
waits for its authoritative result instead of hiding the outcome and inviting
an unsafe retry.

For commands where success matters, the Agent should make the target emit a
unique result sentinel carrying the command's status and wait for that sentinel.
The generic platform reports whether the requested boundary was observed; it
does not inject shell syntax or equate a returned prompt with successful
execution.

## Profile adjustments

Configuration has four explicit layers:

1. A Slot identifies the fixed station channel: stable ID, display name, and
   host port such as `COM4`.
2. Its Transport Profile defines the host-side physical UART:
   baud rate, data bits, parity, stop bits, flow control, DTR, RTS, and
   `auto_open`.
3. Its optional Device Profile defines DUT-facing behavior: Shell/U-Boot prompt
   literals, command EOL, echo policy, `write_chunk_size`, and
   `write_chunk_delay_ms`.
4. Its independent Device Model binding records the confirmed DUT identity in
   a hierarchical catalog without changing any serial behavior.

For wire/TOML compatibility the Slot field named `profile` is the Transport
Profile binding; `device_profile` is the optional DUT binding. `Generic` means
no Device Profile is attached—it is a safe fallback, not a guessed model.

Pacing belongs to the Device Profile because it compensates for how quickly a
DUT consumes input, not for the COM endpoint. `write_chunk_size` is the maximum
number of bytes passed to the driver in one paced step; the daemon waits
`write_chunk_delay_ms` before the next step. The Generic fallback is one byte
per 1 ms: an N-byte write has N driver chunks and N-1 requested 1-ms gaps,
never a trailing gap. The value is an inter-chunk delay in milliseconds, not a
baud period and not a guarantee of exact wall-clock cadence; driver blocking
and OS timer granularity add latency. A partial driver write does not introduce
an extra delay inside the same planned chunk. A zero delay removes the sleeps
but still applies `write_chunk_size` to driver calls. Existing Slot `settings` retain a full
compatibility snapshot, but current clients consume the daemon's published
`effective_transport` and `effective_write_pacing`.

The quickest interactive path is:

```sh
serial profile transport list
serial profile transport create --interactive
serial profile transport update uart-115200 --baud-rate 921600
serial profile device list
serial profile device create --interactive
serial profile device update my-dut --interactive
serial profile attach --slot slot-1 --transport uart-115200 --device my-dut
serial model tree
serial model attach --slot slot-1
serial doctor state --slot slot-1
```

`serial model add/update/list/tree/attach/detach` maintains the independent
identity catalog. Parent and child names may repeat, children render indented,
and interactive attach expands a parent before selecting a descendant. A
binding records whether identity was confirmed by serial, telnet, web UI, a
Human, or another documented method; the configured label alone is never
treated as proof.

`update` changes only the fields named on the command line; `--interactive`
walks every field while showing the existing values as defaults. Use `clone`
for a named variant, `import`/`export` for station sharing, and
`detach --device` to return a Slot to Generic behavior. `setup` prints the
available catalogs and asks which Transport and Device Profile to attach to
each selected port. It does not guess a DUT from boot text or a COM number.
Examples:

```sh
serial profile transport clone generic-115200 --name uart-921600 --baud-rate 921600
serial profile transport update uart-921600 --auto-open true
serial profile device clone my-dut --name my-dut-fast --write-chunk-size 8
serial profile device update my-dut-fast --shell-prompt "]# "
serial profile device update my-dut-fast --clear-shell-prompt
serial profile device update my-dut-fast --inherit-write-eol --inherit-echo
serial profile device update my-dut-fast --inherit-write-chunk-size --inherit-write-chunk-delay
serial profile device export my-dut --output my-dut.toml
serial profile device import my-dut.toml
serial profile detach --slot slot-1 --device
```

Prompt fields use explicit `--clear-shell-prompt` and
`--clear-uboot-prompt` flags because an absent prompt is an intentional Device
Profile value, not an instruction to consult legacy Slot data. Optional EOL,
echo, and pacing fields use `--inherit-*` to remove that Device Profile
override and fall back to the generic/compatibility setting.

Prompt matching is a literal substring, not a regular expression. Prefer a
stable suffix such as `"]# "` over a full prompt containing the current
directory. Omit an unknown Shell/U-Boot prompt instead of guessing. Profile
catalogs hold at most 128 unique names and cannot be deleted while attached.
A Transport change closes and reopens the physical port because its UART
parameters changed. A Device change keeps the handle open, but an active Run or
Trigger blocks the change so its evidence boundary cannot silently change
mid-operation.

Every configuration read returns `config_revision`; every write supplies the
revision it observed. If another setup/profile client committed first,
`seriald` returns `config_revision_mismatch` and the stale client must reload
before retrying. Catalog and Slot mutations are staged, atomically persisted,
then published. A validation, actor, or file-save failure leaves the previous
authoritative state active.

The low-level protocol still permits an explicit per-request pacing override
for trusted clients, but `serialctl` and `serial-mcp` use the effective Device
Profile pacing. Agent tool schemas intentionally omit chunk size and delay, so
an Agent cannot accidentally bypass a station-tested safe cadence. `seriald`
also rejects zero-byte writes; a command must contribute text or an EOL.

One physical write has a 15-second total budget and retains a separate
two-second timeout for any driver call that makes no progress. `seriald`
estimates the complete paced duration before enqueueing the write. A plan over
15 seconds is rejected explicitly before any byte reaches the port instead of
being truncated into a predictable partial write. With the conservative
1-byte/1-ms default this bounds one request to 591 bytes under the current
Windows scheduling allowance; use a target-validated chunk size such as 4 or 8,
or split a longer command. The current control lease must also have enough
monotonic time remaining for that complete budget plus a 100 ms scheduling
margin. Otherwise the daemon returns a retryable conflict before enqueueing any
bytes; renew control or shorten the request, then retry. An accepted write is
not cancelled midway merely because its lease reaches TTL.

For an Agent write with `expected_run_id`, the active Run ID and its
server-issued actor owner are checked before these pacing and lease budgets.
Run loss/mismatch is an explicit conflict containing “no bytes were written”.
The Run ID is part of the write idempotency fingerprint, so one request ID
cannot be reused under a different task boundary.

## Monitor persistence and optional notifications

Monitor Jobs and their retained incidents live in `seriald`, not in an MCP
process or Agent session. A new Monitor starts strictly after the Slot head at
creation time, so an old matching line cannot be reported as a new incident.
Matching spans adjacent live RX events only within one serial generation; close,
open, reconfigure, removal, generation changes, and explicit gaps reset the
matcher. It is bounded by pattern, compiled-regex,
window, preview, Job, incident, and outbox limits. A daemon restart reloads
running Jobs and resumes their workers; replay-safe checkpoints are throttled
to once per second, while Incident/cooldown state commits immediately so a
restart does not re-alert a match already suppressed by cooldown. Notification
TTL expires only outbox delivery, never retained incident evidence by itself.
At a hard bound the oldest incident yields to newer evidence even when no
consumer exposes ACK; bounded retention reports `retention_gap` and
`first_available_incident_seq` when a pull cursor is too old. The returned
cursor advances to the observed high-water mark even when ACK filtering makes a
page empty. Explicit gaps remain visible rather than being treated as an empty
observation.

No message center is required. The default configuration has no sink, while
`GET /api/v1/monitors/{monitor_id}/incidents` and the MCP
`monitor_incidents` tool continue to provide bounded pull access. To integrate a
message center such as Agent Message Center, configure a station-owned HTTP
sink in `seriald.toml`:

```toml
[monitor_event_sink]
endpoint = "http://127.0.0.1:8080/api/v1/events"
token_file = "message-center.token" # relative paths resolve under the config directory
retry_min_ms = 1000
retry_max_ms = 60000
```

The bearer secret is read from the file and is never placed in the event or
Agent tool arguments. Each incident is first persisted locally, then represented
as a CloudEvents 1.0-shaped event in a bounded outbox. Delivery retries use
bounded exponential backoff until success or the event's freshness TTL. The
event carries only routing/evidence metadata and a bounded preview; serial bytes
remain authoritative in the journal. `seriald` does not know Agent instance,
session, or return-route semantics—the message center and its adapters own those
policies.

## Control tuning

Write-control leases and queues are bounded by the optional `[control]`
section of `seriald.toml`:

```toml
[control]
max_ttl_ms = 60000      # 5s floor at runtime; configured ceiling <= 24h
wait_timeout_ms = 60000 # queued acquire lifetime; configured ceiling <= 1h
max_waiters = 128       # per-Slot wait-queue bound
```

The defaults match the compiled-in v0.6 behavior, so the section can be omitted
entirely; changes apply on the next daemon start. Configuration above the
24-hour lease or one-hour wait ceilings is rejected instead of reaching
platform `Instant` arithmetic.

## Current boundaries

v0.6 does not include device-pool Reservations, flashing recipes, automatic
probes, full VT100 emulation, external `screen/minicom` handoff, TLS,
compression, or a Windows Service installer. `serial serve` is the backend
entrypoint and `serial console` is a separate client process; putting both
behind one launcher does not merge the daemon with a terminal or prevent
multiple consumers. Service packaging can be added after station validation.
Explicit Windows ACL auditing/hardening for shared service accounts is also a
candidate; current per-user files inherit the Windows profile directory ACL.

The old OpenCode-only `serial_arm`/`serial_disarm` implementation was not
copied: it could continue writing after control loss and did not create normal
TX audit events. The protocol-v3 Trigger is Slot-owned, bound to
Control/fence/epoch/generation and an optional Run, uses bounded literal-byte
matching and hard limits, and sends its optional kickoff and every action fire
through the ordinary confirmed/audited write path. Device-specific recipes
remain explicit caller parameters rather than daemon or Profile behavior.

See [DOCUMENTATION.md](./DOCUMENTATION.md) for architecture, state, logging,
and correctness invariants; [PROTOCOL.md](./docs/PROTOCOL.md) for the complete
MCP/HTTP/WebSocket contract; and [MCP_TOOLS.md](./docs/MCP_TOOLS.md) for the
generated Agent tool surface.
