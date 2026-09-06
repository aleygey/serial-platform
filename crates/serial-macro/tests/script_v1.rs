use std::collections::BTreeMap;

use serial_macro::{
    Effect, EffectKind, EffectResult, Error, Limits, Program, Step, Value, ValueType, Vm, compile,
};

fn program(source: &str) -> Program {
    compile(source, &BTreeMap::new(), Limits::default()).unwrap()
}
fn vm(source: &str) -> Vm {
    program(source)
        .start(BTreeMap::new(), BTreeMap::new())
        .unwrap()
}
fn next(vm: &mut Vm) -> Result<Option<Effect>, Error> {
    loop {
        match vm.advance()? {
            Step::Effect(effect) => return Ok(Some(effect)),
            Step::Yielded => {}
            Step::Complete => return Ok(None),
        }
    }
}
fn commands(source: &str) -> Vec<String> {
    let mut vm = vm(source);
    let mut output = Vec::new();
    while let Some(effect) = next(&mut vm).unwrap() {
        match &effect.kind {
            EffectKind::Command { text } => output.push(text.clone()),
            _ => panic!("unexpected effect {effect:?}"),
        }
        vm.resume(effect.id, EffectResult::Done).unwrap();
    }
    output
}
fn compile_error(source: &str) -> Error {
    compile(source, &BTreeMap::new(), Limits::default()).unwrap_err()
}

#[test]
fn line_commands_preserve_unicode_and_do_not_append_eol_in_vm() {
    assert_eq!(
        commands(r#"cmd("echo 机型🙂"); cmd(""); cmd("a\"b\\c\u{754c}\u754c");"#),
        ["echo 机型🙂", "", "a\"b\\c界界"]
    );
}

#[test]
fn nested_if_for_while_break_continue_and_compound_assignment() {
    assert_eq!(
        commands(
            r#"
        let sum = 0;
        for (let i = 0; i < 6; i++) {
            if (i == 1) { continue; }
            if (i == 4) { break; }
            sum += i;
        }
        let outer = 0;
        let count = 0;
        while (outer < 3) {
            outer++;
            for (let j = 0; j < 4; j += 1) {
                if (j == 2) { break; }
                count++;
            }
        }
        if (sum == 5 && count == 6) { cmd("ok"); }
        else if (true) { cmd("bad"); }
    "#
        ),
        ["ok"]
    );
}

#[test]
fn for_continue_runs_update_and_empty_for_can_break() {
    assert_eq!(
        commands(
            r#"
        let n = 0;
        for (let i = 0; i < 4; i = i + 1) { if (i < 3) { continue; } n = i; }
        for (;;) { n++; break; }
        if (n == 4) { cmd("ok"); }
    "#
        ),
        ["ok"]
    );
}

#[test]
fn while_continue_returns_to_condition() {
    assert_eq!(
        commands(r#"let n = 0; while (n < 3) { n++; continue; cmd("bad"); } cmd("ok");"#),
        ["ok"]
    );
}

#[test]
fn lexical_scopes_shadow_without_leaking() {
    assert_eq!(
        commands(r#"let x = "outer"; { let x = "inner"; cmd(x); } cmd(x);"#),
        ["inner", "outer"]
    );
    assert_eq!(
        compile_error("{ let x = 1; } let y = x;").code,
        "unknown_variable"
    );
    assert_eq!(
        compile_error("for (let i=0; i<1; i++) {} let x=i;").code,
        "unknown_variable"
    );
}

#[test]
fn short_circuit_does_not_evaluate_division_or_issue_wait() {
    let source = r#"
        let w = watch("ready");
        let a = false && (1 / 0 == 0);
        let b = true || wait(w, 100);
        if (!a && b) { cmd("ok"); }
    "#;
    let mut vm = vm(source);
    let first = next(&mut vm).unwrap().unwrap();
    assert!(matches!(first.kind, EffectKind::Watch { .. }));
    vm.resume(first.id, EffectResult::Done).unwrap();
    let second = next(&mut vm).unwrap().unwrap();
    assert_eq!(second.kind, EffectKind::Command { text: "ok".into() });
    vm.resume(second.id, EffectResult::Done).unwrap();
    assert!(next(&mut vm).unwrap().is_none());
}

#[test]
fn uboot_watch_command_repeat_and_match_sequence() {
    let source = r#"
        let boot = watch(prompt("uboot"));
        cmd("reboot");
        while (!boot.matched) { cmd("slp"); wait(boot, args.interval_ms); }
    "#;
    let parameters = BTreeMap::from([("interval_ms".into(), ValueType::Integer)]);
    let program = compile(source, &parameters, Limits::default()).unwrap();
    assert_eq!(program.required_prompts().collect::<Vec<_>>(), ["uboot"]);
    let mut vm = program
        .start(
            BTreeMap::from([("interval_ms".into(), Value::Integer(50))]),
            BTreeMap::from([("uboot".into(), "U-Boot>".into())]),
        )
        .unwrap();
    let mut writes = Vec::new();
    let mut waits = 0;
    let mut registered = false;
    while let Some(effect) = next(&mut vm).unwrap() {
        let result = match effect.kind {
            EffectKind::Watch { watcher, pattern } => {
                assert_eq!(watcher, 0);
                assert_eq!(pattern, "U-Boot>");
                registered = true;
                EffectResult::Done
            }
            EffectKind::Command { text } => {
                assert!(registered);
                writes.push(text);
                EffectResult::Done
            }
            EffectKind::Wait {
                watcher,
                timeout_ms,
                strict,
            } => {
                assert_eq!(timeout_ms, 50);
                assert!(!strict);
                waits += 1;
                if waits == 3 {
                    vm.mark_matched(watcher).unwrap();
                }
                EffectResult::Matched(waits == 3)
            }
            _ => panic!("unexpected effect"),
        };
        vm.resume(effect.id, result).unwrap();
    }
    assert_eq!(writes, ["reboot", "slp", "slp", "slp"]);
}

#[test]
fn watcher_match_can_arrive_before_command_ack() {
    let mut vm =
        vm(r#"let w=watch("ready"); cmd("go"); while (!w.matched) {cmd("again"); wait(w,1);}"#);
    let watch = next(&mut vm).unwrap().unwrap();
    vm.resume(watch.id, EffectResult::Done).unwrap();
    let command = next(&mut vm).unwrap().unwrap();
    vm.mark_matched(0).unwrap();
    vm.resume(command.id, EffectResult::Done).unwrap();
    assert!(next(&mut vm).unwrap().is_none());
    assert_eq!(vm.commands_issued(), 1);
}

#[test]
fn wait_timeout_can_be_handled_by_if_but_expect_timeout_is_terminal() {
    let mut vm = vm(
        r#"let w=watch("ready"); cmd("first"); if (!wait(w,20)) { cmd("retry"); } expect(w,30); cmd("never");"#,
    );
    let mut writes = Vec::new();
    loop {
        let effect = next(&mut vm).unwrap().unwrap();
        let result = match &effect.kind {
            EffectKind::Command { text } => {
                writes.push(text.clone());
                EffectResult::Done
            }
            EffectKind::Watch { .. } => EffectResult::Done,
            EffectKind::Wait { strict: false, .. } => EffectResult::Matched(false),
            EffectKind::Wait {
                strict: true,
                timeout_ms,
                ..
            } => {
                assert_eq!(*timeout_ms, 30);
                let error = vm
                    .resume(effect.id, EffectResult::Matched(false))
                    .unwrap_err();
                assert_eq!(error.code, "expect_timeout");
                assert_eq!(next(&mut vm).unwrap_err(), error);
                break;
            }
            _ => panic!("unexpected effect"),
        };
        vm.resume(effect.id, result).unwrap();
    }
    assert_eq!(writes, ["first", "retry"]);
}

#[test]
fn strict_expect_still_yields_for_driver_evidence_verification_after_match() {
    let mut vm = vm(r#"let w=watch("ready"); cmd("go"); expect(w,100); cmd("next");"#);
    let watch = next(&mut vm).unwrap().unwrap();
    vm.resume(watch.id, EffectResult::Done).unwrap();
    let command = next(&mut vm).unwrap().unwrap();
    vm.resume(command.id, EffectResult::Done).unwrap();
    vm.mark_matched(0).unwrap();
    let required = next(&mut vm).unwrap().unwrap();
    assert!(matches!(
        required.kind,
        EffectKind::Wait { strict: true, .. }
    ));
    let error = vm
        .fail_effect(required.id, "RX evidence has a gap")
        .unwrap_err();
    assert_eq!(error.code, "effect_failed");
    assert_eq!(vm.commands_issued(), 1);
    assert!(next(&mut vm).is_err());
}

#[test]
fn late_watch_match_cannot_rewrite_an_authoritative_wait_timeout() {
    let mut vm = vm("let w=watch(\"ready\"); cmd(\"go\"); expect(w,10); cmd(\"never\");");
    let watch = next(&mut vm).unwrap().unwrap();
    vm.resume(watch.id, EffectResult::Done).unwrap();
    let command = next(&mut vm).unwrap().unwrap();
    vm.resume(command.id, EffectResult::Done).unwrap();
    let wait = next(&mut vm).unwrap().unwrap();
    vm.mark_matched(0).unwrap();
    assert_eq!(
        vm.resume(wait.id, EffectResult::Matched(false))
            .unwrap_err()
            .code,
        "expect_timeout"
    );
    assert_eq!(vm.commands_issued(), 1);
}

#[test]
fn delay_is_an_external_effect_and_does_not_sleep_in_vm() {
    let mut vm = vm("delay(25); cmd(\"done\");");
    let delay = next(&mut vm).unwrap().unwrap();
    assert_eq!(delay.kind, EffectKind::Delay { duration_ms: 25 });
    assert_eq!(vm.advance().unwrap(), Step::Effect(delay.clone()));
    vm.resume(delay.id, EffectResult::Done).unwrap();
    assert!(matches!(
        next(&mut vm).unwrap().unwrap().kind,
        EffectKind::Command { .. }
    ));
}

#[test]
fn effect_acknowledgements_cannot_skip_or_duplicate_a_write() {
    let mut vm = vm("cmd(\"one\"); cmd(\"two\");");
    let effect = next(&mut vm).unwrap().unwrap();
    assert_eq!(
        vm.resume(effect.id + 1, EffectResult::Done)
            .unwrap_err()
            .code,
        "effect_protocol"
    );
    assert!(next(&mut vm).is_err());
    assert_eq!(vm.commands_issued(), 1);
    let mut second = vm_from("cmd(\"one\");");
    let effect = next(&mut second).unwrap().unwrap();
    second.resume(effect.id, EffectResult::Done).unwrap();
    assert_eq!(
        second
            .resume(effect.id, EffectResult::Done)
            .unwrap_err()
            .code,
        "effect_protocol"
    );
}
fn vm_from(source: &str) -> Vm {
    vm(source)
}

#[test]
fn wrong_effect_result_fails_closed() {
    let mut vm = vm("cmd(\"one\");");
    let effect = next(&mut vm).unwrap().unwrap();
    assert_eq!(
        vm.resume(effect.id, EffectResult::Matched(true))
            .unwrap_err()
            .code,
        "effect_protocol"
    );
}

#[test]
fn cancellation_between_cpu_slices_is_terminal() {
    let limits = Limits {
        instructions_per_yield: 2,
        ..Limits::default()
    };
    let mut vm = compile("while(true) {}", &BTreeMap::new(), limits)
        .unwrap()
        .start(BTreeMap::new(), BTreeMap::new())
        .unwrap();
    assert_eq!(vm.advance().unwrap(), Step::Yielded);
    let error = vm.interrupt("user sent Ctrl-D");
    assert_eq!(error.code, "interrupted");
    assert_eq!(vm.advance().unwrap_err(), error);
}

#[test]
fn empty_infinite_loops_exhaust_budget_without_hanging() {
    for source in ["while(true) {}", "for(;;) {}"] {
        let limits = Limits {
            max_instructions: 17,
            instructions_per_yield: 3,
            ..Limits::default()
        };
        let mut vm = compile(source, &BTreeMap::new(), limits)
            .unwrap()
            .start(BTreeMap::new(), BTreeMap::new())
            .unwrap();
        assert_eq!(next(&mut vm).unwrap_err().code, "instruction_budget");
        assert_eq!(vm.instructions_executed(), 17);
    }
}

#[test]
fn arguments_are_strictly_typed_and_unknown_fields_rejected_before_effects() {
    let parameters = BTreeMap::from([("name".into(), ValueType::String)]);
    let p = compile("cmd(args.name);", &parameters, Limits::default()).unwrap();
    assert_eq!(
        p.start(BTreeMap::new(), BTreeMap::new()).unwrap_err().code,
        "missing_parameter"
    );
    assert_eq!(
        p.start(
            BTreeMap::from([("name".into(), Value::Integer(1))]),
            BTreeMap::new()
        )
        .unwrap_err()
        .code,
        "parameter_type"
    );
    assert_eq!(
        p.start(
            BTreeMap::from([
                ("name".into(), Value::String("ok".into())),
                ("extra".into(), Value::Boolean(true))
            ]),
            BTreeMap::new()
        )
        .unwrap_err()
        .code,
        "unknown_parameter"
    );
    let mut vm = p
        .start(
            BTreeMap::from([("name".into(), Value::String("机型".into()))]),
            BTreeMap::new(),
        )
        .unwrap();
    assert_eq!(
        next(&mut vm).unwrap().unwrap().kind,
        EffectKind::Command {
            text: "机型".into()
        }
    );
}

#[test]
fn all_profile_prompts_are_preflighted_even_in_unreached_branches() {
    let p = program("cmd(\"first\"); if(false) {let w=watch(prompt(\"uboot\"));}");
    assert_eq!(
        p.start(BTreeMap::new(), BTreeMap::new()).unwrap_err().code,
        "missing_prompt"
    );
    assert_eq!(
        p.start(
            BTreeMap::new(),
            BTreeMap::from([("uboot".into(), "".into())])
        )
        .unwrap_err()
        .code,
        "empty_pattern"
    );
    assert_eq!(
        compile_error("let p=\"uboot\";let w=watch(prompt(p));").code,
        "static_prompt_required"
    );
    assert_eq!(
        compile_error("let w=watch(prompt(\"login\"));").code,
        "unknown_prompt"
    );
}

#[test]
fn unknown_variables_functions_members_and_parameter_references_fail_statically() {
    for (source, code) in [
        ("cmd(missing);", "unknown_variable"),
        ("cmd(args.missing);", "unknown_parameter"),
        ("system(\"rm\");", "unknown_function"),
        ("let w=watch(\"r\"); if(w.ready) {}", "unknown_field"),
        ("let s=\"r\"; if(s.matched) {}", "unknown_field"),
        ("let x=1; let x=2;", "duplicate_variable"),
        ("let cmd=1;", "duplicate_variable"),
        ("break;", "invalid_loop_control"),
        ("continue;", "invalid_loop_control"),
    ] {
        assert_eq!(compile_error(source).code, code, "{source}");
    }
}

#[test]
fn type_errors_in_unreached_branches_are_not_ignored() {
    for source in [
        "if(false){cmd(1);}",
        "let x=1;x=\"bad\";",
        "if(1){}",
        "let x=cmd(\"hi\");",
        "let x=1+true;",
        "let x=\"a\"<\"b\";",
        "let w=watch(\"r\");let x=w==w;",
        "delay(true);",
        "let x=false&&1;",
    ] {
        assert_eq!(compile_error(source).code, "type_error", "{source}");
    }
}

#[test]
fn syntax_rejects_missing_semicolons_braces_and_unavailable_language_features() {
    for source in [
        "cmd(\"x\")",
        "if(true) cmd(\"x\");",
        "while(true) cmd(\"x\");",
        "import x;",
        "eval(\"x\");",
        "fn f() {}",
        "let x=[1,2];",
        "let x=1.5;",
        "cmd('x');",
        "cmd(\"\\q\");",
        "cmd(\"\\uD800\");",
        "cmd(\"unterminated);",
    ] {
        assert!(
            compile(source, &BTreeMap::new(), Limits::default()).is_err(),
            "{source}"
        );
    }
}

#[test]
fn comments_unicode_columns_and_multiline_diagnostics() {
    let error = compile_error("// 中文🙂\ncmd(\"正常\");\n  cmd(missing);");
    assert_eq!((error.span.line, error.span.column), (3, 7));
    assert!(error.to_string().contains("3:7"));
    assert_eq!(
        commands("// cmd(\"never\");\ncmd(\"// literal\"); // trailing"),
        ["// literal"]
    );
}

#[test]
fn command_control_characters_are_rejected_in_literals_and_dynamic_values() {
    for source in [
        r#"cmd("x\n");"#,
        r#"cmd("\r");"#,
        r#"cmd("\u{04}");"#,
        r#"cmd("\t");"#,
        r#"cmd("a"+"\u007f");"#,
    ] {
        assert_eq!(compile_error(source).code, "command_control_character");
    }
    let p = compile(
        "cmd(\"first\"); cmd(args.text);",
        &BTreeMap::from([("text".into(), ValueType::String)]),
        Limits::default(),
    )
    .unwrap();
    let mut vm = p
        .start(
            BTreeMap::from([("text".into(), Value::String("bad\n".into()))]),
            BTreeMap::new(),
        )
        .unwrap();
    let first = next(&mut vm).unwrap().unwrap();
    vm.resume(first.id, EffectResult::Done).unwrap();
    assert_eq!(next(&mut vm).unwrap_err().code, "command_control_character");
    assert_eq!(vm.commands_issued(), 1);
}

#[test]
fn integer_overflow_division_and_remainder_errors_are_terminal() {
    for (source, code) in [
        ("let x=9223372036854775807+1;", "integer_overflow"),
        ("let x=3037000500*3037000500;", "integer_overflow"),
        ("let x=(-9223372036854775807-1)/-1;", "integer_overflow"),
        ("let x=-(-9223372036854775807-1);", "integer_overflow"),
        ("let x=1/0;", "division_by_zero"),
        ("let x=1%0;", "division_by_zero"),
    ] {
        assert_eq!(next(&mut vm(source)).unwrap_err().code, code, "{source}");
    }
    assert_eq!(
        compile_error("let x=9223372036854775808;").code,
        "integer_overflow"
    );
}

#[test]
fn arithmetic_precedence_and_scalar_equality() {
    assert_eq!(
        commands(r#"let x=2+3*4-8/2; x%=7; if(x==3 && "a"+"b"=="ab" && true!=false){cmd("ok");}"#),
        ["ok"]
    );
}

#[test]
fn finite_string_source_and_syntax_limits() {
    let limits = Limits {
        max_string_bytes: 5,
        ..Limits::default()
    };
    assert_eq!(
        compile("cmd(\"中文\");", &BTreeMap::new(), limits)
            .unwrap_err()
            .code,
        "string_limit"
    );
    assert_eq!(
        compile("cmd(\"abc\"+\"def\");", &BTreeMap::new(), limits)
            .unwrap_err()
            .code,
        "string_limit"
    );
    let mut vm = compile("let x=\"a\";while(true){x=x+x;}", &BTreeMap::new(), limits)
        .unwrap()
        .start(BTreeMap::new(), BTreeMap::new())
        .unwrap();
    assert_eq!(next(&mut vm).unwrap_err().code, "string_limit");
    let source = format!(
        "let x={};",
        std::iter::repeat_n("1", 5000).collect::<Vec<_>>().join("+")
    );
    assert_eq!(compile_error(&source).code, "nesting_limit");
    let source = format!("let x={}1{};", "(".repeat(5000), ")".repeat(5000));
    assert_eq!(compile_error(&source).code, "nesting_limit");
    let limits = Limits {
        max_source_bytes: 4,
        ..Limits::default()
    };
    assert_eq!(
        compile("cmd(\"x\");", &BTreeMap::new(), limits)
            .unwrap_err()
            .code,
        "source_limit"
    );
}

#[test]
fn durations_are_checked_before_effects() {
    assert_eq!(compile_error("delay(300001);").code, "duration_limit");
    assert_eq!(
        next(&mut vm("let n=-1;delay(n);")).unwrap_err().code,
        "duration_limit"
    );
    let mut zero = vm("delay(0);");
    assert_eq!(
        next(&mut zero).unwrap().unwrap().kind,
        EffectKind::Delay { duration_ms: 0 }
    );
}

#[test]
fn watcher_and_total_command_budgets_are_enforced() {
    let limits = Limits {
        max_watchers: 2,
        ..Limits::default()
    };
    let mut vm = compile(
        "while(true){let w=watch(\"ready\");}",
        &BTreeMap::new(),
        limits,
    )
    .unwrap()
    .start(BTreeMap::new(), BTreeMap::new())
    .unwrap();
    for _ in 0..2 {
        let effect = next(&mut vm).unwrap().unwrap();
        vm.resume(effect.id, EffectResult::Done).unwrap();
    }
    assert_eq!(next(&mut vm).unwrap_err().code, "watcher_budget");
    let limits = Limits {
        max_total_command_bytes: 3,
        ..Limits::default()
    };
    let mut vm = compile("cmd(\"abc\");cmd(\"d\");", &BTreeMap::new(), limits)
        .unwrap()
        .start(BTreeMap::new(), BTreeMap::new())
        .unwrap();
    let effect = next(&mut vm).unwrap().unwrap();
    vm.resume(effect.id, EffectResult::Done).unwrap();
    assert_eq!(next(&mut vm).unwrap_err().code, "tx_budget");
    assert_eq!(vm.commands_issued(), 1);
}

#[test]
fn variable_parameter_and_limits_validation() {
    let limits = Limits {
        max_variables: 1,
        ..Limits::default()
    };
    assert_eq!(
        compile("let x=1;let y=2;", &BTreeMap::new(), limits)
            .unwrap_err()
            .code,
        "variable_limit"
    );
    assert_eq!(
        compile(
            "",
            &BTreeMap::from([("not-valid".into(), ValueType::String)]),
            Limits::default()
        )
        .unwrap_err()
        .code,
        "invalid_parameter"
    );
    assert_eq!(
        compile(
            "",
            &BTreeMap::new(),
            Limits {
                instructions_per_yield: 0,
                ..Limits::default()
            }
        )
        .unwrap_err()
        .code,
        "invalid_limits"
    );
}

#[test]
fn malformed_generated_inputs_do_not_panic() {
    let pieces = [
        "let", "(", ")", "{", "}", ";", "=", "+", "!", "for", "if", "args.", "cmd", ",", "\"x\"",
        "true", "watch", "0",
    ];
    let mut seed = 7u64;
    for _ in 0..500 {
        let mut source = String::new();
        for _ in 0..30 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            source.push_str(pieces[(seed as usize) % pieces.len()]);
            source.push(' ');
        }
        let result = compile(&source, &BTreeMap::new(), Limits::default());
        if let Ok(program) = result {
            let mut vm = program.start(BTreeMap::new(), BTreeMap::new()).unwrap();
            let _ = vm.advance();
        }
    }
}
