# serialctl console notes

The interactive console captures mouse events by default:

- the wheel scrolls only the serial-output viewport;
- clicking the output or input pane changes the visible focus;
- left-drag in the output pane selects text without `Shift`;
- right-click in the output pane copies the active selection;
- right-click in the input pane pastes from the Windows clipboard;
- `Ctrl+Shift+V` remains the portable paste path, including Ubuntu.

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
