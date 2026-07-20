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
  cursor-safe reads, bounded command capture, and no Agent takeover/close.
- Bounded cross-reconnect write safety within one daemon epoch: recent duplicate
  request IDs return their cached result, older executed IDs are rejected
  instead of being written again, and an unacknowledged outcome remains
  visibly marked uncertain. Input is never automatically replayed.

The station defaults agreed for v1 are: 115200 8N1, no flow control, DTR and
RTS low, command EOL `\r`, echo on, U-Boot prompt `SigmaStar #`, automatic
probe disabled, and `auto_open=true`.

With hardware flow control, the driver owns RTS and `rts=true` is rejected.
Linux drivers may transiently assert DTR during open even when the final
requested level is low; DTR-reset-sensitive targets should use validated
Windows-host behavior, avoid automatic reopen while the target must remain
untouched, or use electrical isolation/reset gating.

## Install from a release

Each release provides two x86_64 packages:

- `serial-platform-<version>-windows-x86_64.zip` contains `seriald.exe`,
  `serialctl.exe`, and `serial-mcp.exe`.
- `serial-platform-<version>-linux-x86_64-musl.tar.gz` contains statically
  linked `serialctl` and `serial-mcp` clients. It does not include a Linux
  daemon because the Windows host owns the workstation COM ports.

Extract the Windows package on the host connected to the serial card. Extract
the Linux package in the VM and make the client executable if the archive tool
did not preserve its mode:

```sh
tar -xzf serial-platform-v0.2.0-linux-x86_64-musl.tar.gz
cd serial-platform-v0.2.0-linux-x86_64-musl
chmod +x serialctl serial-mcp
```

Release checksums are published in `SHA256SUMS`. The command examples below
assume the executables are on `PATH`. From an extracted package directory, use
`.\seriald.exe` / `.\serialctl.exe` on Windows and `./serialctl` on Linux.

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
| `Ctrl-] f` | Resume following live output |
| `Ctrl-] PgUp` / `Ctrl-] PgDn` | Local scroll, including in RAW mode |
| `Ctrl-] t` | Explicitly take over write control |
| `Ctrl-] c` | Release control or cancel queued input |
| `Ctrl-] p` | Confirm a blocked multiline/large paste |
| `Ctrl-] ?` | Help |
| `Ctrl-] q` | Quit |
| `Ctrl-] Ctrl-]` | Send byte `0x1d` to the device |

Opening the console only subscribes. The first write asks for Human Control.
If another actor owns it, the request queues; only `Takeover` revokes it.
The footer keeps the queue position, age, and held chunk count visible. Queued
input expires after 60 seconds without human activity. `Ctrl-] c` cancels it by
reconnecting the v1 actor connection; because the protocol has no dedicated
cancel-acquire message yet, that reconnect also releases this terminal's
control on every other Slot, then automatically restores all subscriptions.
An acquired human lease is renewed only while that Slot has recent manual
activity or an in-flight write, and is released after 60 seconds idle. LINE/RAW
mode and command history are independent for every Slot.
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
JSONC and Codex TOML examples, the stable eight-tool surface, and the expected
Run workflow are in [adapters/README.md](./adapters/README.md).

The adapter connects directly to `seriald`; the Agent is not asked to compose
shell commands around `serialctl`. OpenCode exposes the tools as
`serial_devices`, `serial_command`, and so on. Codex exposes the same tools in
the configured `serial` MCP namespace.

## Profile adjustments

`serialctl init` creates the agreed `generic-115200` Profile snapshot and keeps
an existing same-port snapshot on later runs. v1 has no Profile editor. To set
a later shell prompt or a station-specific serial value, run `seriald paths`,
stop the daemon, edit that Slot's `settings` in `seriald.toml`, validate that the
file still contains the intended Slots, and restart `seriald`. The daemon file
also contains bearer credentials, so do not copy it into logs or support
messages. Running `serialctl init` again is the safer path for COM discovery;
omitted Slots remain preserved unless deletion is explicitly confirmed.

## Current boundaries

v1 does not include device-pool Reservations, flashing recipes,
server-side boot-interrupt triggers, a reusable Profile catalog/editor,
automatic probes, full VT100 emulation, external `screen/minicom` handoff,
TLS, compression, or a Windows Service installer. Slot configuration already
carries a complete Profile snapshot, so a catalog can be added without changing
the physical-port or event model. `seriald serve` is the backend CLI entrypoint;
service packaging can be added after station validation. Explicit Windows ACL
auditing/hardening for shared service accounts is also roadmap work; current
per-user files inherit the Windows profile directory ACL.

See [DOCUMENTATION.md](./DOCUMENTATION.md) for protocol, state, logging, and
correctness invariants.
