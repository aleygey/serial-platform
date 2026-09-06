# Macro Script v1 core

This crate is a pure compiler and cooperative VM. It has no dependencies, timers,
threads, I/O, serial handles, host shell or arbitrary code execution facilities.
`seriald` drives its effects inside the existing Run and sole physical writer.

## Language

Statements end with semicolons; control-flow bodies use braces. Identifiers are
ASCII letters/underscores followed by letters/digits/underscores. Strings are
Unicode double-quoted literals; the supported escapes are `\"`, `\\`, `\n`,
`\r`, `\t`, `\0`, `\uXXXX` and `\u{...}` (Unicode scalar values).

```text
let boot = watch(prompt("uboot"));
cmd("reboot");
while (!boot.matched) {
    cmd("slp");
    wait(boot, args.interval_ms);
}

for (let i = 0; i < 3; i++) {
    let ready = watch(prompt("shell"));
    cmd("status");
    expect(ready, 10000);
}
```

Supported constructs are `let`, assignment (including `+=`, `-=`, `*=`, `/=`,
`%=`, statement `++`/`--`), scalar string/integer/boolean values, checked integer
arithmetic, string concatenation, comparisons, short-circuit `&&`/`||`, `!`,
`if/else`, `for(init; condition; update)`, `while`, `break`, `continue`, and `//`
comments. Variables have lexical scope and cannot change type. There are no
implicit conversions, objects, arrays, imports, functions, eval or host access.

| Built-in | Result | Driver obligation |
| --- | --- | --- |
| `cmd(string)` | void | Append Profile EOL; confirm TX, without waiting for a prompt |
| `watch(string)` | watcher | Register before the next TX; bind to that command's new RX |
| `prompt("shell" / "uboot")` | string | Supply the required Profile prompt before execution |
| `wait(watcher, ms)` | boolean | Return whether this deadline observed a trustworthy match |
| `expect(watcher, ms)` | void | Require a match and reliable evidence, or terminate the run |
| `delay(ms)` | void | Await a cancellable timer |

Only `watcher.matched` and declared `args.name` field access exists. Matchers are
literal strings in v1; terminal projection and prompt-specific interpretation
belong to the driver. Each independent command-response stage creates a fresh
watcher. Ordinary `cmd` text rejects every Unicode control character, including
embedded EOL; Ctrl-C/Ctrl-D remain on the existing signal path.

## API and execution boundary

1. `compile(source, &parameter_types, limits)` parses and type-checks all code,
   including branches that may not run. Prompt names must be static literals.
2. `program.start(args, prompts)` requires every declared argument, rejects extra
   arguments and type mismatches, and validates all required prompts. Parameter
   defaults and numeric range metadata are applied by the caller before this step.
3. `vm.advance()` returns `Step::Effect`, `Step::Yielded`, or `Step::Complete`.
   A repeated advance before acknowledgement returns the same pending effect ID:
   execute each ID at most once.
4. Register `Watch` and acknowledge it with `Done`. Feed subsequent authoritative
   RX match state through `mark_matched(watcher)`. The VM never treats old log text
   as new RX; binding and evidence correctness are driver responsibilities.
5. Complete Command/Watch/Delay with `resume(id, Done)`. Complete Wait with
   `resume(id, Matched(bool))`. `expect` uses `strict: true`; false is terminal.
   Even an already matched strict watcher still emits an effect so the driver
   can verify evidence integrity. A later match cannot overwrite a timed-out
   effect result.
6. Transport errors, uncertain writes, RX gaps or interference use
   `fail_effect(id, message)`, never a normal false wait result. `interrupt` works
   even between effects. Failure is sticky and never resumes at the next line.

The driver must keep renewing the existing Run, process RX and cancellation between
VM slices, enforce the total wall-clock deadline, and count physical bytes including
EOL. VM limits separately bound source and strings, nesting, variables, watchers,
instructions, per-wait duration and command text bytes. `commands_issued` counts
effects, not confirmed physical writes. Runtime errors can occur after earlier TX;
only compilation/start failures guarantee that no command effect was issued.
