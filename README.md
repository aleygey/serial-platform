# Serial Platform

`serial-platform` is an independent shared serial-port control plane. `seriald`
runs on the machine physically connected to the serial card; any number of
`serialctl` clients can observe the same Slots, while a fenced control lease
ensures that only one actor writes to a Slot at a time.

It deliberately has no OpenCode or OpenChamber runtime dependency. The
`serial-mcp` adapter exposes the same platform to OpenCode, Codex, and other
MCP clients without making `seriald` Agent-specific.

## What v1 provides

- Interactive COM discovery and Slot configuration from `serialctl init`.
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
- Source-aware TX/RX timeline with daemon epoch, per-Slot sequence, physical
  generation, byte offsets, Run, and Operation fields.
- Crash-recoverable binary journal, 64 MiB/1 hour segments, CRC validation,
  recovered gap-ledger tails, query-derived sequence-discontinuity gaps,
  bounded/concurrency-limited history queries, and a 10 GiB retention ceiling.
- Observer, operator, and admin Bearer credentials.
- Windows daemon with a Windows or Linux/VM client over a trusted host-only
  network.
- One `serial-mcp` adapter for OpenCode and Codex, with Run-scoped search,
  cursor-safe reads, bounded command capture, Agent writes atomically bound to
  a Run owned by that adapter process, and no Agent takeover/close.
- A protocol-v2, daemon-owned Trigger Job for bounded low-latency
  `initial_write -> repeated action -> live RX stop` reactions. It is
  byte-oriented and device-agnostic; every fire uses the normal fenced,
  confirmed, audited write path.
- Bounded cross-reconnect write safety within one daemon epoch: recent duplicate
  request IDs return their cached result, older executed IDs are rejected
  instead of being written again, and an unacknowledged outcome remains
  visibly marked uncertain. Input is never automatically replayed.

The station defaults agreed for v1 are: 115200 8N1, no flow control, DTR and
RTS low, command EOL `\r`, echo on, no guessed Shell/U-Boot prompt, automatic
probe disabled, `auto_open=true`, and writes paced at one byte per 1 ms.

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

- `serial-platform-<version>-windows-x86_64.zip` contains `seriald.exe`,
  `serialctl.exe`, and `serial-mcp.exe`.
- `serial-platform-<version>-linux-x86_64-ubuntu20.04.tar.gz` contains native
  `x86_64-unknown-linux-gnu`/glibc `serialctl` and `serial-mcp` clients built on
  the Ubuntu 20.04 baseline (glibc 2.31). It is intended for 64-bit x86 Ubuntu
  20.04 or newer and does not include a Linux daemon because the Windows host
  owns the workstation COM ports. It does not support 32-bit i386/i686 Ubuntu.

Use `seriald`, `serialctl`, and `serial-mcp` from the same release across the
Windows host and Linux VM. Release 0.3.1 uses WebSocket protocol v2 and is not
wire-compatible with the protocol-v1 executables from 0.2.x. The HTTP route
namespace is intentionally still `/api/v1`; the route namespace and the
WebSocket payload protocol are versioned independently. Do not mix 0.2.x and
0.3.x executables in one station.

Extract the Windows package on the host connected to the serial card. Extract
the Linux package in the VM and make the client executable if the archive tool
did not preserve its mode:

```sh
tar -xzf serial-platform-v0.3.1-linux-x86_64-ubuntu20.04.tar.gz
cd serial-platform-v0.3.1-linux-x86_64-ubuntu20.04
chmod +x serialctl serial-mcp
./serialctl --version
```

Release checksums are published in `SHA256SUMS`. The command examples below
assume the executables are on `PATH`. From an extracted package directory, use
`.\seriald.exe` / `.\serialctl.exe` on Windows and `./serialctl` on Linux.
Linux does not search the current directory by default, so `serialctl init`
returns `command not found` unless the binary was installed on `PATH`; from the
extracted directory, run `./serialctl init`.

## Build

Install Rust 1.88 or newer. On Windows, the normal MSVC Rust target also needs
the Visual Studio C++ Build Tools and Windows SDK; on Linux, install the
distribution's C/C++ build tools. Then run from this directory:

```sh
cargo build --release
cargo test --workspace
```

Build `seriald` on the Windows host that owns the COM ports. `serialctl` can be
built on Windows or natively inside the Linux VM.

## First start

On Windows:

```powershell
seriald serve
```

The first start creates the daemon configuration and prints three credentials
once. `serialctl init` uses an admin credential only for setup and stores an
operator credential for normal interactive use. Use the observer credential
for read-only monitoring. Tokens are not accepted as command-line arguments by
`serialctl`.

In another terminal:

```sh
serialctl init
```

The wizard connects to `seriald`, asks the daemon to enumerate its own COM
ports, lets you select the usual two ports, names them `slot-1`, `slot-2`, and
persists the selected default Profile. Existing Slots omitted from a new scan,
including temporarily absent COM ports, are kept by default; deletion requires
an explicit confirmation. A Slot is the stable station channel; it is not a
device model or serial number. Replacing the sample connected to the same
serial-card channel requires no configuration change.

Start the console without naming a Slot:

```sh
serialctl
```

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
| `Ctrl-] t` | Explicitly take over write control |
| `Ctrl-] c` | Release control or cancel queued input |
| `Ctrl-] p` | Confirm a blocked multiline/large paste |
| `Ctrl-] ?` | Help |
| `Ctrl-] q` | Quit |
| `Ctrl-] Ctrl-]` | Send byte `0x1d` to the device |

In LINE mode, plain `PageUp`/`PageDown` also scroll locally; RAW keeps those
keys for xterm byte sequences, so use the `Ctrl-]` prefix there.

Mouse capture is off by default, so ordinary left-button drag selects terminal
text without holding Shift. Set `mouse_capture = true` in `serialctl.toml` only
if in-app wheel scrolling is more important; with capture enabled, terminal
selection follows the terminal emulator's usual Shift-drag behavior.
After confirmation, a LINE-mode multiline paste is queued as distinct logical
commands that share one Operation ID and each receive the effective EOL. RAW
paste remains one unmodified byte burst. Adjacent RAW keystrokes that have not
started a physical write are coalesced into bounded 4 KiB chunks, so waiting
behind another actor does not exhaust the 16-chunk queue after only 16
characters. The 64 KiB total local write bound still applies; input that would
cross it is rejected as one unit and is not partially appended.

Opening the console only subscribes. The first write asks for Human Control.
If another actor owns it, the request queues; only `Takeover` revokes it.
The footer keeps the queue position, age, and held chunk count visible. Queued
input expires after 60 seconds without human activity. `Ctrl-] c` cancels it by
reconnecting the v1 actor connection; the protocol now carries a dedicated
`cancel_acquire` message, but the v1 terminal still uses that reconnect, which
also releases this terminal's control on every other Slot, then automatically
restores all subscriptions.
An acquired human lease is renewed only while that Slot has recent manual
activity or an in-flight write, and is released after 60 seconds idle. LINE/RAW
mode and command history are independent for every Slot.
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
Wrapped rows are measured before the output viewport is scrolled, so following
live output always shows the newest prompt even when a long command or log line
occupies several 80-column screen rows. Local presentation remains bounded to
20,000 rows / 4 MiB per Slot. Once older local rows are evicted, the title and a
synthetic oldest-row boundary state that local display history was truncated
and direct the operator to `serialctl logs`; durable history is not presented
as though the retained local row were its true beginning.
Repeated device rows remain uncollapsed in v1: live monitoring stays faithful
to their exact order, while bounded log/search tools handle high-volume review.
If the connection drops after a write was sent but before its acknowledgement,
the footer keeps an `OUTCOME UNCERTAIN` warning. Inspect the TX timeline before
deciding whether to retry; disconnected input is never replayed automatically.

## Windows host-only VM use

Bind `seriald` explicitly to the Windows host-only adapter address, for example:

```powershell
seriald serve --bind 192.168.56.1:3210
```

`--bind` is a runtime-only override: repeat it on every launch (or stop the
daemon, update the persisted `bind` value, and restart). Use `seriald paths` to
print the exact daemon configuration, data, and journal locations.

Then run inside the Linux VM:

```sh
./serialctl --endpoint http://192.168.56.1:3210 init
./serialctl
```

The endpoint supplied during `init` is saved in the Linux user's local
configuration. Later launches can therefore use plain `./serialctl`; pass
`--endpoint` again only to select a different daemon.

Limit the Windows firewall rule to the host-only subnet. v1 accepts plain HTTP
only and is intended for loopback or a trusted host-only network. Do not expose
it to a normal LAN or the Internet; TLS/private-network support is on the
roadmap.

## Inspection and history

```sh
serialctl status
serialctl doctor
serialctl archives
serialctl archives --slot slot-1
serialctl logs --slot slot-1 --limit 200
serialctl logs --slot slot-1 --after-seq 1200 --epoch <epoch-uuid>
serialctl logs --slot slot-1 --epoch <epoch-uuid> --direction rx
serialctl logs --slot slot-1 --after-time 2026-07-19T09:00:00+08:00
serialctl logs --slot slot-1 --contains panic
serialctl logs --slot slot-1 --run <run-uuid> --contains panic
```

`contains` is an explicit bounded historical query. The interactive console
never silently substitutes an old global match for the current Run. Every
response carries its next cursor, truncation state, first available sequence,
and retention/corruption gaps. If `--epoch` is omitted, the CLI searches only
the current daemon epoch. Run `serialctl archives` first to discover retained
historical epoch UUIDs, then pass one to `logs --epoch`. `archives --json` and
`logs --json` retain exact nanosecond values and raw event fields; ordinary
output displays RFC3339 timestamps in the client's local timezone at
millisecond precision. Time filters require RFC3339 input with an explicit
timezone, and `--direction` accepts `rx`, `tx`, or `none`.
Without `--run`, `--operation`, or seq/time bounds, ordinary `logs` output
warns that the query spans the entire selected daemon epoch and may include
older test cycles. `--contains` only filters that range; it does not make an old
match current. Archive catalog times labeled `segment-open` are segment
creation bounds, not exact first/last event timestamps.

## OpenCode and Codex

Install `serial-mcp` on the same Windows or Linux machine where the Agent
platform runs. It reuses the endpoint and operator token written by
`serialctl init`, so Agent config contains no secret. Ready-to-copy OpenCode
JSONC and Codex TOML examples, the stable nine-tool surface, and the expected
Run workflow are in [adapters/README.md](./adapters/README.md).

The adapter connects directly to `seriald`; the Agent is not asked to compose
shell commands around `serialctl`. OpenCode exposes the tools as
`serial_devices`, `serial_command`, and so on. Codex exposes the same tools in
the configured `serial` MCP namespace.

`serial_trigger` handles short timing windows without making the Agent or the
Linux VM loop `serial_command`/`serial_write` calls. It requires an
adapter-owned Run and takes explicit UTF-8 text plus EOL for an optional
one-time `initial_write` and the repeated `action`, along with an optional start
literal, stop literals, interval, timeout, and maximum-fire bounds.
`initial_write` is the one-time kickoff after the RX matchers are armed; it does
not increment `fires_confirmed`. The MCP v1 surface is text-oriented, while
seriald's lower WebSocket protocol keeps every Trigger payload and literal as
raw bytes encoded with base64. Neither layer infers a Prompt or byte sequence
from the device profile. For example,
`initial_write={"text":"reboot","eol":"\r"}`,
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

`serial_command` requires a Run previously created by this `serial-mcp`
process. Every Agent write carries that Run ID; inside the same serialized Slot
actor that validates the control lease/fence, `seriald` rejects a missing,
changed, or foreign-owned Run before pacing-budget calculation or physical
write. Human and legacy clients may omit this optional boundary and retain
lease-only writes. A Run isolates only its log/event interval—it does not reset,
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
For delayed markers such as background-test completion, `until_regex` is the
sole command boundary; a configured Shell prompt or quiet gap cannot finish the
capture first. See the adapter guide for the valid parameter combinations and
response fields.

For commands where success matters, the Agent should make the target emit a
unique result sentinel carrying the command's status and wait for that sentinel.
The generic platform reports whether the requested boundary was observed; it
does not inject shell syntax or equate a returned prompt with successful
execution.

## Profile adjustments

`serialctl init` creates a `generic-115200` baseline snapshot inside each Slot
and keeps an existing same-port snapshot on later runs. In the current schema,
the Slot's `profile = "generic-115200"` value is a descriptive label rather
than a second catalog reference; the actual transport baseline is the Slot's
`settings`. Running `serialctl init` again is the safe path for COM discovery;
omitted Slots remain preserved unless deletion is explicitly confirmed.

Device-specific behavior belongs in the reusable device-profile catalog rather
than in the generic baseline. A `[[device_profiles]]` entry names the model:

```toml
[[device_profiles]]
name = "sigmastar-evb"
shell_prompt = "]# "
uboot_prompt = "SigmaStar =>"
write_eol = "\r"
echo = "on"
```

A Slot references the entry with `device_profile = "sigmastar-evb"`. Once a
model is attached, that profile is authoritative for whether Shell/U-Boot
prompts are configured at all; an omitted prompt stays unset instead of
falling back to stale legacy Slot data. Profile EOL and echo values, when
present, override the generic Slot baseline. The generic platform does not
guess a device-specific prompt. Prompt matching is a literal substring, not a
regular expression, so configure a stable suffix such as `"]# "` rather than
a full Shell prompt containing the current directory. Every snapshot and live
profile-change event publishes the authoritative effective prompts, EOL, and
echo policy, and clients use those values. The catalog is validated
on load — duplicate names, unknown Slot references, and more than 128 entries
are rejected — and it can be
replaced without a restart over HTTP: `GET /api/v1/config/device-profiles`
lists it (observer) and `PUT /api/v1/config/device-profiles` validates,
persists, and publishes a new catalog (admin). `serialctl` has no catalog
editor yet. As a temporary administrative path, run `seriald paths`, stop the
daemon, edit `seriald.toml`, and restart it; the file also contains credentials,
so never paste the whole file into logs or support messages.

When editing the generated TOML, replace the top-level
`device_profiles = []` with one or more `[[device_profiles]]` tables.
Place `device_profile = "sigmastar-evb"` inside the intended `[[slots]]`
table and before its `[slots.settings]` header; placing it after that header
would make it an invalid serial-setting key. `serialctl status --json` then
shows the binding and the four authoritative `effective_*` values.

Two Slot settings protect slow UART sinks from overruns: the writer sends
`write_chunk_size` bytes, then waits `write_chunk_delay_ms` before the next
chunk. The defaults (1 byte, 1 ms) produce a typewriter cadence; set
`write_chunk_delay_ms = 0` to send each write as one unthrottled burst. A
single write request can also carry its own pacing override. Note that
`seriald` rejects zero-byte writes, so clients must send at least the line
ending. On Windows, timer and asynchronous-driver scheduling can make the
conservative 1 byte/1 ms default behave closer to 15 ms per byte. Keep that
default until the target has been validated: new `serialctl init` profiles
still start at 1 byte/1 ms, and existing explicit settings are not migrated
silently. For long commands on a target known to accept bursts, set the Slot
to a chunk size of 4 or 8, or let an Agent request that per-write override;
both improve throughput without changing the protocol.

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

## Control tuning

Write-control leases and queues are bounded by the optional `[control]`
section of `seriald.toml`:

```toml
[control]
max_ttl_ms = 60000      # 5s floor at runtime; configured ceiling <= 24h
wait_timeout_ms = 60000 # queued acquire lifetime; configured ceiling <= 1h
max_waiters = 128       # per-Slot wait-queue bound
```

The defaults match the compiled-in v1 behavior, so the section can be omitted
entirely; changes apply on the next daemon start. Configuration above the
24-hour lease or one-hour wait ceilings is rejected instead of reaching
platform `Instant` arithmetic.

## Current boundaries

v1 does not include device-pool Reservations, flashing recipes,
automatic probes, full VT100 emulation,
external `screen/minicom` handoff, TLS, compression, or a Windows Service
installer. A daemon-level device-profile catalog exists and is managed over
HTTP; a `serialctl` editor for it is still roadmap work. `seriald serve` is
the backend CLI entrypoint; service packaging can be added after station
validation. Explicit Windows ACL auditing/hardening for shared service
accounts is also roadmap work; current per-user files inherit the Windows
profile directory ACL.

The old OpenCode-only `serial_arm`/`serial_disarm` implementation was not
copied: it could continue writing after control loss and did not create normal
TX audit events. The protocol-v2 Trigger is Slot-owned, bound to
Control/fence/epoch/generation and an optional Run, uses bounded literal-byte
matching and hard limits, and sends its optional kickoff and every action fire
through the ordinary confirmed/audited write path. Device-specific recipes
remain explicit caller parameters rather than daemon or Profile behavior.

See [DOCUMENTATION.md](./DOCUMENTATION.md) for protocol, state, logging, and
correctness invariants.
