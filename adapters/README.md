# Agent adapters

`serial-mcp` is the shared stdio MCP adapter for OpenCode, Codex, and other
MCP-capable agents. It connects directly to `seriald`; it does not wrap
`serialctl` commands and does not depend on an Agent SDK.

## Topology

```text
Windows host: serial card -> seriald (192.168.56.1:3210)
                                  ^
Linux host-only VM: serialctl + serial-mcp -> OpenCode / Codex
```

Copy the Linux release's `serialctl` and `serial-mcp` to a directory on PATH.
Run `serialctl --endpoint http://192.168.56.1:3210 init` once. `serial-mcp`
reads the same `serialctl.toml` and operator token by default, so neither Agent
configuration contains a credential. You may instead provide `--config`,
`--endpoint`, and `--token-file`; the token itself is never a command argument.

Use the examples in `opencode/` or `codex/`, replacing only executable/config
paths and the actor label. In OpenCode, the configured MCP server name `serial`
prefixes tools as `serial_devices`, `serial_command`, etc. In Codex they appear
under the `serial` MCP server namespace.

## Stable Agent workflow

1. `devices`: inspect authoritative Slot state and choose a Slot explicitly.
2. `run_start`: create a new task boundary. It does not reset the target.
3. Initialize the target state explicitly with `command`.
4. Use `command` for bounded write/capture, `wait` for future output, `read` for
   a cursor continuation, and `search` for a literal search in the current Run.
5. `run_end`, then `release` when the task is finished.

The adapter queues for control and never takes it over. An operator using
`serialctl` can observe everything and explicitly Takeover if needed. Agent
tools cannot close the serial port. A write holds a 60-second fenced lease and
the adapter renews it every 20 seconds while its process remains alive.

`command` attaches before writing and tags TX with an Operation UUID. It waits
for a configured prompt, an explicit literal, quiet time, or timeout and returns
an `interfered` flag if any other actor wrote during that window. Prompt and
quiet completion are bounded heuristics, so timeout, gap, truncation, and
interference fields must be considered before concluding a command succeeded.

`search` defaults to `current_run`; it will not silently find a matching line
from an earlier test cycle. `current_cursor` requires the exact `epoch` and
`after_seq`, and `archive` requires an explicit epoch. Raw base64 payload is
opt-in. Human-oriented text removes ANSI control sequences and folds only
adjacent byte-identical lines.

The executable speaks newline-delimited JSON-RPC over stdin/stdout. stdout is
reserved for MCP frames; diagnostics go to stderr. It supports MCP protocol
versions `2024-11-05`, `2025-03-26`, `2025-06-18`, and `2025-11-25`.

