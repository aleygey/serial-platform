# Serial Platform Desktop

`serial-desktop` is the first native GUI client for serial-platform. It is a
thin Human client and local-process manager: `seriald` remains the only owner
of physical ports, journal persistence, Control leases, and configuration
transactions.

## Run the MVP from source

Build the unified launcher, daemon, and App together so the App can discover a
matching local service executable:

```sh
cargo build -p serial-cli -p seriald -p serial-desktop
cargo run -p serial-desktop
```

The App defaults to `http://127.0.0.1:3210`. Before auto-starting anything it
probes `/api/v1/health`. If that address already responds, the App only
connects and never treats the existing process as one it owns or may stop. If
the address is free, it starts a sibling `serial` or `seriald` process with
`serve --bind 127.0.0.1:3210`. A custom executable can be selected in the
configuration page.

Closing the App intentionally leaves a daemon it started running. This avoids
turning a GUI exit into an ungraceful journal shutdown. “Stop local service” is
explicit and confirmed; on Unix it sends SIGINT, waits up to three seconds for
normal shutdown, and only then force-stops an unresponsive owned child. The
button can never stop a daemon that was already running before this App.

## Current feature surface

- Connect to a local or remote `seriald` over the existing HTTP/WebSocket
  protocol without blocking the egui thread.
- Reconstruct up to 2,000 current-epoch durable events per Slot and then join
  the live stream at the journal cursor, with `(epoch, seq)` de-duplication and
  visible gap markers.
- View bounded serial output and the durable Agent Run/command-description
  history; expand a command purpose to inspect the confirmed bytes. An MCP
  `command_sequence` is one purpose card whose concrete commands are ordered by
  `step_index`; an ordinary command remains one card with one command. The App
  does not decorate commands with success/failure emoji. History is newest
  first and follows the top until the operator scrolls. Selecting a purpose or
  an individual sequence step moves the serial pane to that command's first
  timeline sequence; “返回最新” restores live following.
- Send Human lines through queued fenced Control. Drafts and the latest 100
  input-history entries per Slot survive App restarts; bearer tokens never do.
- Create a Slot from discovered ports and Transport Profiles, and open/close it
  through the authoritative full-Slot configuration transaction with
  `expected_revision`. The GUI never opens a device handle directly.
- Inspect and directly edit the selected Slot's port; every resolved Transport
  parameter (baud/data/parity/stop/flow, DTR/RTS, auto-open); and every resolved
  Device parameter (prompts, EOL, echo, write pacing). Saving never mutates a
  bound or shared Profile in place. It first prepares content-addressed,
  currently unbound `desktop-slot-*` Transport and Device Profiles, then
  switches both bindings in one revision-guarded Slots transaction. Identical
  content reuses the same entries. Changed content retains at most the current
  binding plus one candidate; after a successful switch the old unbound entries
  are removed best-effort and a later save retries cleanup. A stale form fails
  instead of rebasing over a concurrent change. Existing shared Profiles and a
  catalog model can still be attached or detached with
  revision/current-binding guards. General catalog CRUD remains in
  `serial setup/profile/model`.
- Use Chinese UI text, common keyboard shortcuts, and system/dark/light themes.

Desktop preferences are stored with user-only permissions where the platform
supports them. The exact path is shown on the configuration page.

## v0.7 CI and release packages

`serial-desktop` is a required release program, not an optional CI artifact.
Jenkins fails the build if it is missing, has the wrong target architecture,
requires a newer Linux glibc than the Ubuntu 20.04 baseline, or is absent from
the package manifest. GitHub's native Windows and Ubuntu validation jobs build,
run the headless smoke paths, package, extract, and checksum it independently.
The release gate still asserts the exact 19-tool MCP registry.

Both of these commands return before logging, opening a window, probing the
daemon, or loading a graphical backend, which makes them safe in headless CI:

```sh
serial-desktop --version
serial-desktop --help
```

The platform archives contain:

- **Windows x86_64:** `serial-desktop.exe` beside `serial.exe` and
  `seriald.exe`. The App can therefore start its packaged local backend without
  another install. Jenkins emits the GNU/MinGW-w64 build; GitHub also validates
  an MSVC build.
- **Ubuntu 20.04+ x86_64:** `serial-desktop`, `serial`, and `seriald` are
  siblings in the archive, so default local-backend startup works after
  extraction. The graphical path uses eframe's X11/glow backend and needs an
  X11 display plus the system X11, XKB, and OpenGL libraries (on Ubuntu,
  packages such as `libx11-6`, `libxcursor1`, `libxi6`, `libxkbcommon0`,
  `libxrandr2`, and `libgl1`).
- **macOS 11+ arm64 and x86_64:** each architecture-specific ZIP contains the
  bare `serial-desktop` binary and a double-clickable `Serial Platform.app`.
  `Contents/MacOS` contains `serial-desktop`, `serial`, and `seriald`, so the
  App's sibling-process discovery works when launched from Finder. These are
  thin, architecture-specific Apps rather than one universal binary.

The macOS App is deliberately **unsigned and not notarized**. After verifying
the release checksum, a tester may remove only that downloaded App's quarantine
attribute and open it:

```sh
xattr -dr com.apple.quarantine "Serial Platform.app"
open "Serial Platform.app"
```

Every archive contains `MANIFEST.sha256`, including every executable inside
the macOS App. Verify it from the extracted package directory with
`sha256sum --check MANIFEST.sha256` on Linux, or
`shasum -a 256 -c MANIFEST.sha256` on macOS. Jenkins also publishes a top-level
`SHA256SUMS` that covers all four platform archives.

## MVP boundaries

- The console renders safe text rather than emulating ANSI terminal control
  sequences; control bytes are displayed but never executed by the UI.
- Automatic startup accepts HTTP endpoints only. A production remote setup
  should add TLS termination and a secure OS credential store before persisting
  credentials.
- The initial console restores only the current daemon epoch. Older retained
  epochs remain journal archives and are not silently mixed with a newly
  attached device.
