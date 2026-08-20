# serialctl console notes

The v0.4 release presents this component through the unified launcher:

```text
serial                 # same as serial console
serial setup           # interactive port/Slot/Profile setup
serial profile ...     # Transport and Device Profile management
serial status
serial doctor ...
serial archives
serial logs ...
```

`serialctl` remains available as the direct component executable. `serial` is a
thin launcher, so the console is still a separate client process connected to
the long-running `seriald` backend; multiple consoles and MCP clients can
attach simultaneously. The launcher resolves only sibling components from its
own release directory and never falls back to another installation on `PATH`.

## Profile and diagnostic commands

Setup discovers ports on the daemon host and selects two independent layers:

- Transport Profile: baud, data/parity/stop bits, flow control, DTR/RTS, and
  auto-open.
- Device Profile: optional DUT Shell/U-Boot prompts, EOL, echo, and safe write
  chunk size/delay.

Use `Generic` when DUT behavior is unknown. A quick reusable configuration is:

```sh
serial profile transport create --interactive
serial profile device create --interactive
serial profile device update my-dut --interactive
serial profile attach --slot slot-1 --transport generic-115200 --device my-dut
serial doctor state --slot slot-1
```

Both Profile kinds support list/show/create/update/clone/import/export/delete.
An `update` changes only named fields, while `update --interactive` walks all
fields using current values as defaults. Device prompts are removed with
`--clear-shell-prompt` or `--clear-uboot-prompt`; EOL, echo, chunk size, and
chunk delay use their `--inherit-*` flags to remove an optional override.
`attach` changes either layer and `detach` returns Device behavior to Generic
and/or the Transport to the safe baseline. Mutations prompt once for the admin
credential or accept `--admin-token-file`; the credential is never saved.
Every mutation carries `config_revision`, so a stale setup/profile client is
rejected rather than overwriting a newer change.

Non-interactive setup reads `--admin-token-file` and
`--operator-token-file`, then saves the validated daily credential only to
`--save-operator-token-file` (or the saved/default destination). Global
`--token-file` is rejected by setup. Input and destination paths must be
distinct after normalization/canonicalization, preventing an input credential
from being overwritten through a path alias.

Diagnosis is deliberately layered:

```sh
serial doctor
serial doctor port --slot slot-1
serial doctor stream --slot slot-1 --duration 10
serial doctor storage
serial doctor state --slot slot-1
```

The stream check compares an independent WebSocket subscription with
authoritative RX offsets and the journal. An online but quiet target is
reported as silent, not dead. `serial logs` supports mutually exclusive
`--contains` and bounded `--regex` filters plus Run/epoch/cursor bounds. Regex
fails closed against an older daemon without the v0.4 capability signal; there
is no unbounded local fallback.

The interactive console captures mouse events by default:

- the wheel scrolls only the serial-output viewport;
- clicking the output or input pane changes the visible focus;
- left-drag in the output pane selects text without `Shift`;
- double-clicking anywhere in one word selects that whole word, allowing the
  small cell-to-cell movement reported by Windows Terminal;
- mouse-up automatically copies the selected text and resumes live output;
- right-click in the output pane repeats that retained copy;
- right-click in the input pane pastes from the Windows clipboard;
- `Ctrl+Shift+V` remains the portable paste path, including Ubuntu.

Only the active drag uses a stable visual snapshot. If a terminal loses the
mouse-up event, serialctl finalizes the drag after five seconds so selection
cannot leave the serial view looking permanently frozen.

In both LINE and RAW modes, `Ctrl-C` immediately sends ETX (`0x03`) to the
device. LINE mode first discards any unsent local draft and does not append the
Profile EOL, so a remote shell continuation or foreground process can be
interrupted without switching modes. `Ctrl-C` does not quit `serialctl` or
release Human Control.

RAW Ctrl-D and Ctrl-Z likewise send exact bytes `0x04` and `0x1a`; they do not
exit or suspend the local process. Physical serial Break is a distinct UART
line condition and is currently exposed through the MCP `signal` tool rather
than a console key.

When Human Control is queued, the input title shows the queued LINE count and
newest command preview. `Ctrl-] d` deletes the newest queued LINE command;
`Ctrl-] e` returns it to the editor. These actions cannot recall a command
whose physical write has started. RAW bytes are never converted back to text;
use `Ctrl-] c` to cancel their queue.

Run start/end/abort events are rendered as full-width colored rules. They make
an Agent task interval obvious to a human observer without implying that the
DUT was reset or cleaned at either boundary.

The LINE editor, search popup, and configuration prompts use a software block
cursor with a 600 ms visible/hidden phase. Typing or moving the cursor resets it
to visible. Live RX can redraw the screen up to 30 times per second, but those
frames neither reset this slow phase nor reposition a terminal emulator's
hardware cursor, so input remains visually stable while output is busy.

On terminals at least 22 rows tall, powerline-style separators frame a
full-width recent Agent task/command pane. Run rows show only state and task;
`command_sequence` steps are grouped under one purpose and expanded as clean
command text without success/failure emoji. Enter/Right expands the selected
purpose and jumps the main serial-output viewport to that operation (or its
sequence fallback) when the row is still in bounded local scrollback. The wheel
scrolls expanded detail without collapsing it. Newest records are at the top
and followed automatically until Up/Down/Enter explicitly pins an older
selection. Configure the inline content height from **Console configuration →
Terminal display settings**, or with `agent_history_rows = 3..20` in
`serialctl.toml` (default `5`). The page accepts an exact numeric value and
saves it immediately; short terminals use an on-demand popup. The serial-output
title shows the bound Device Model's exact catalog name and port, never its
internal ID, Slot name, or baud rate. Model bindings changed by another Human
or an MCP Agent are refreshed asynchronously (at most one catalog request is in
flight) and appear without restarting the console.

**Console configuration → Current port configuration** shows every active
field and edits it in place. Transport rows cover port/profile, baud, data
bits, parity, stop bits, flow control, DTR, RTS, and auto-open. Device rows
cover profile, EOL, echo, Shell/U-Boot prompts, and write chunk size/delay.
Edits remain a local draft until **Save and apply**. The save carries the
catalog revisions observed when editing began and refuses a stale write.
Transport and Device Profiles are reusable shared definitions: when either is
edited in place, a second confirmation lists **every** currently bound Slot
(including disabled Slots) before the credential/submission step. Cancelling
keeps the local draft. A binding change invalidates the guarded write instead
of applying against an out-of-date impact list. The daemon's transactional
profile endpoints apply physical UART changes through their normal busy/reopen
safety boundary; Device behavior appears in the refreshed authoritative
Snapshot. Model binding is an immediate revision-guarded operation. Renaming a
bound model changes one shared catalog node, so the terminal first lists every
Slot bound to that model and requires a separate confirmation; the guarded
response reports the affected Slot IDs and refreshes their output titles.

**Console configuration → Agent Run lifecycle settings** writes
`orphan_run_timeout_seconds` to the shared `serialctl.toml`: `0` means
unlimited, while every finite value must be at least `300` seconds and has no
artificial maximum. The default is `1800` seconds (30 minutes). In unlimited
mode, ownership ends only through explicit `run_end`, MCP process exit, or
Human takeover. This controls only the local
`serial-mcp` crash/abandonment fallback after the final Run-scoped call; normal
completion still uses `run_end`. It does not change the serial protocol or
`seriald`'s 60-second fenced Control lease. Restart already-running
`serial-mcp` processes after saving because adapters read it at startup.

The powerline separator caps require a Powerline or Nerd Font. Without one,
only those decorative cap glyphs may appear as missing-character boxes; the
pane layout and controls continue to work.

The former permanent status strip is not shown. Connection/control/write and
clipboard feedback instead temporarily replaces the one-line shortcut footer
for four seconds, then the normal help text returns.

Opening the console rebuilds each Slot's serial view from seriald's durable
journal before attaching to live traffic. Recovery is deliberately scoped to
the current daemon epoch, the most recent 20,000 sequence numbers, an 8 MiB
per-Slot processing budget, and a 10-second all-Slot startup deadline. The
WebSocket resumes at the final sequence the journal actually scanned, so its
replay ring closes any live-but-not-yet-durable tail without duplicating rows.
Retention gaps, recovery limits, and failures remain visible instead of being
presented as complete history. A freshly opened console loads only the current
daemon epoch; older epochs remain available through output search,
`serialctl archives`, or `serialctl logs --epoch`. If seriald restarts while
the console is already open, retained on-screen rows stay above an explicit
daemon-restart boundary and new-epoch traffic is appended below it.

Queued LINE commands use one oldest-first row each (`1. command`). `Ctrl-R`
searches LINE input history. `Ctrl-] /` searches seriald's durable RX/TX
journal in an independent popup: `F2`/`Tab` changes ordinary-text/regex
matching, `F3` changes case sensitivity, `F4` changes RX/TX direction, and
`F5` changes between the current daemon epoch, all retained epochs, and the
current Agent Run. Complete bounded results are newest-first and never inject
an older epoch into the live display. A partial result is ordered only within
the scanned portion and does not claim to contain the globally latest match.
Sequence numbers order matches within an epoch. Across epochs, results preserve
the archive catalog's newest-first rank, which is derived from each archive's
last-segment wall time; individual event wall times never reorder one epoch.
Every submit/retry refreshes the Slot's current epoch, head, and active Run;
opening the popup does not freeze those query boundaries. A journal scan that
ends before the requested live head is explicitly partial, even if its final
HTTP page was not marked truncated.

The interactive query is bounded to 200 displayed results, the latest four
archives, a 10,000-sequence tail per archive, eight journal requests, 16 MiB
per response, and a ten-second total deadline. Partial scans and journal gaps
are shown explicitly. A cross-event match points at the event that completed
the match, so one result row need not contain the complete expression. Use
`serialctl archives` plus `serialctl logs --epoch ...` when older data outside
those interactive bounds must be inspected.

On non-Windows terminals, output copy uses OSC 52 so the terminal emulator can
own the system clipboard without adding an X11 or Wayland runtime dependency.
The emulator may require OSC 52 clipboard access to be enabled. Set
`mouse_capture = false` in `serialctl.toml` to return selection to the terminal
emulator; this also disables in-app wheel scrolling.

Inline log highlighting is case-insensitive and token-based. Unicode letters,
digits, and `_` form an identifier; punctuation, brackets, colons, whitespace,
and `-` are boundaries. Thus `error`, `(error)`, and `[error]` match while
`get_data_error_name`, `xxxinfo`, `information`, and `errorCounter` do not.
Valid IPv4, IPv6, and colon- or hyphen-separated MAC addresses receive distinct
colors; malformed address-like text stays in the ordinary foreground.

The continuous terminal view recognizes 7-bit `ESC` controls but treats
isolated 8-bit C1 values as UART data. Invalid UTF-8 is shown with replacement
characters, and unterminated control sequences recover on a bounded line/byte
budget. A malformed historical event can therefore look corrupted without
poisoning all later replay or live RX; durable log bytes remain unchanged.
