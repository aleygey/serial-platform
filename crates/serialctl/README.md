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
- Device Model: concrete DUT identity which references a shared Device Profile;
  multiple product variants can therefore share the same behavior.

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

Inside the Console, `Ctrl-] m` opens the non-blocking Profile/model picker for
the selected Slot. Arrow keys and Enter select `Profile → model`; Generic and
Profile-only are explicit choices, and `N` adds and binds a metadata-only model
under the highlighted Profile. The daily Operator token is sufficient because
this path cannot edit COM/UART or Device Profile behavior. It is rejected
during an active Run or Trigger and never reopens the serial handle. The tab
keeps the stable station-channel name; the output title shows the concrete DUT.

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
- releasing left-drag resumes live output while retaining the selected text;
- right-click in the output pane copies that retained selection;
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

On non-Windows terminals, output copy uses OSC 52 so the terminal emulator can
own the system clipboard without adding an X11 or Wayland runtime dependency.
The emulator may require OSC 52 clipboard access to be enabled. Set
`mouse_capture = false` in `serialctl.toml` to return selection to the terminal
emulator; this also disables in-app wheel scrolling.

Inline log highlighting is case-insensitive and token-based. A keyword is
highlighted only when both sides are non-alphanumeric boundaries. Punctuation,
brackets, colons, whitespace, `_`, and `-` are valid boundaries, so `[INFO]`,
`level=info`, `xxx_info`, and `cloud_com_error_to_log` match while `xxxinfo`,
`information`, and `errorCounter` do not.

The continuous terminal view recognizes 7-bit `ESC` controls but treats
isolated 8-bit C1 values as UART data. Invalid UTF-8 is shown with replacement
characters, and unterminated control sequences recover on a bounded line/byte
budget. A malformed historical event can therefore look corrupted without
poisoning all later replay or live RX; durable log bytes remain unchanged.
