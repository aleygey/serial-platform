use std::collections::BTreeMap;

use crate::compiler::{Builtin, Op};
use crate::syntax::{Binary, Unary};
use crate::{
    Effect, EffectKind, EffectResult, Error, Program, Span, Step, Value, WatcherId,
    validate_command,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeValue {
    Scalar(Value),
    Watcher(WatcherId),
}

/// A cooperative, deterministic VM. `advance` executes only a bounded CPU slice.
/// The caller must yield to RX/cancellation between slices and enforce wall time.
#[derive(Debug)]
pub struct Vm {
    program: Program,
    args: BTreeMap<String, Value>,
    prompts: BTreeMap<String, String>,
    variables: Vec<Option<RuntimeValue>>,
    stack: Vec<RuntimeValue>,
    watchers: Vec<bool>,
    pc: usize,
    instructions: u64,
    command_bytes: usize,
    command_effects: u64,
    next_effect: u64,
    pending: Option<Effect>,
    failure: Option<Error>,
    last_span: Span,
}

impl Vm {
    pub(crate) fn new(
        program: Program,
        args: BTreeMap<String, Value>,
        prompts: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let span = Span {
            line: 1,
            column: 1,
            ..Span::default()
        };
        for (name, expected) in &program.parameters {
            let value = args.get(name).ok_or_else(|| {
                Error::new(
                    "missing_parameter",
                    format!("missing argument {name}"),
                    span,
                )
            })?;
            if value.value_type() != *expected {
                return Err(Error::new(
                    "parameter_type",
                    format!("argument {name} must be {expected:?}"),
                    span,
                ));
            }
            if matches!(value, Value::String(value) if value.len() > program.limits.max_string_bytes)
            {
                return Err(Error::new(
                    "string_limit",
                    format!("argument {name} exceeds string byte limit"),
                    span,
                ));
            }
        }
        for name in args.keys() {
            if !program.parameters.contains_key(name) {
                return Err(Error::new(
                    "unknown_parameter",
                    format!("unknown argument {name}"),
                    span,
                ));
            }
        }
        for (name, span) in &program.prompts {
            let prompt = prompts.get(name).ok_or_else(|| {
                Error::new(
                    "missing_prompt",
                    format!("profile prompt {name} is not configured"),
                    *span,
                )
            })?;
            if prompt.is_empty() {
                return Err(Error::new(
                    "empty_pattern",
                    format!("profile prompt {name} is empty"),
                    *span,
                ));
            }
            if prompt.len() > program.limits.max_string_bytes {
                return Err(Error::new(
                    "string_limit",
                    format!("profile prompt {name} exceeds string byte limit"),
                    *span,
                ));
            }
        }
        Ok(Self {
            variables: vec![None; program.variables],
            program,
            args,
            prompts,
            stack: Vec::new(),
            watchers: Vec::new(),
            pc: 0,
            instructions: 0,
            command_bytes: 0,
            command_effects: 0,
            next_effect: 1,
            pending: None,
            failure: None,
            last_span: span,
        })
    }

    pub fn instructions_executed(&self) -> u64 {
        self.instructions
    }
    /// Number of command effects issued, not confirmed physical writes.
    pub fn commands_issued(&self) -> u64 {
        self.command_effects
    }
    pub fn command_bytes_issued(&self) -> usize {
        self.command_bytes
    }
    pub fn last_span(&self) -> Span {
        self.last_span
    }
    pub fn pending_effect(&self) -> Option<&Effect> {
        self.pending.as_ref()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }

    /// A pending effect is returned with the same ID until acknowledged. Drivers
    /// must execute each effect ID at most once and never retry uncertain writes.
    pub fn advance(&mut self) -> Result<Step, Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if let Some(effect) = &self.pending {
            return Ok(Step::Effect(effect.clone()));
        }
        let result = self.advance_inner();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn advance_inner(&mut self) -> Result<Step, Error> {
        for _ in 0..self.program.limits.instructions_per_yield {
            if self.pc >= self.program.code.len() {
                if !self.stack.is_empty() {
                    return Err(
                        self.error("vm_invariant", "completed program left values on stack")
                    );
                }
                return Ok(Step::Complete);
            }
            let instruction = self.program.code[self.pc].clone();
            self.last_span = instruction.span;
            if self.instructions >= self.program.limits.max_instructions {
                return Err(self.error(
                    "instruction_budget",
                    "macro exhausted its instruction budget",
                ));
            }
            self.instructions += 1;
            self.pc += 1;
            match instruction.op {
                Op::Push(value) => self.stack.push(RuntimeValue::Scalar(value)),
                Op::Arg(name) => self
                    .stack
                    .push(RuntimeValue::Scalar(self.args[&name].clone())),
                Op::Prompt(name) => self.stack.push(RuntimeValue::Scalar(Value::String(
                    self.prompts[&name].clone(),
                ))),
                Op::Load(index) => {
                    let value = self.variables[index].clone().ok_or_else(|| {
                        self.error("vm_invariant", "variable was not initialized")
                    })?;
                    self.stack.push(value);
                }
                Op::Store(index) => {
                    let value = self.pop()?;
                    self.variables[index] = Some(value);
                }
                Op::Pop => {
                    self.pop()?;
                }
                Op::Matched => {
                    let watcher = self.pop_watcher()?;
                    let matched = self.watcher_matched(watcher)?;
                    self.push_bool(matched);
                }
                Op::Unary(operator) => self.unary(operator)?,
                Op::Binary(operator) => self.binary(operator)?,
                Op::Jump(target) => self.pc = target,
                Op::JumpIfFalse(target) => {
                    if !self.pop_bool()? {
                        self.pc = target;
                    }
                }
                Op::And(target) => {
                    if !self.peek_bool()? {
                        self.pc = target;
                    } else {
                        self.pop()?;
                    }
                }
                Op::Or(target) => {
                    if self.peek_bool()? {
                        self.pc = target;
                    } else {
                        self.pop()?;
                    }
                }
                Op::Call(builtin) => {
                    let effect = self.call(builtin)?;
                    self.pending = Some(effect.clone());
                    return Ok(Step::Effect(effect));
                }
            }
            if self.stack.len()
                > self
                    .program
                    .limits
                    .max_nesting
                    .saturating_mul(4)
                    .saturating_add(16)
            {
                return Err(self.error("stack_limit", "expression stack limit exceeded"));
            }
        }
        Ok(Step::Yielded)
    }

    /// Resume only after the driver has confirmed the effect. `Matched(false)`
    /// terminates strict expect, but becomes false for ordinary wait.
    pub fn resume(&mut self, effect_id: u64, result: EffectResult) -> Result<(), Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.resume_inner(effect_id, result);
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    fn resume_inner(&mut self, effect_id: u64, result: EffectResult) -> Result<(), Error> {
        let effect = self
            .pending
            .as_ref()
            .ok_or_else(|| self.error("effect_protocol", "there is no pending effect"))?;
        if effect.id != effect_id {
            return Err(self.error(
                "effect_protocol",
                "effect acknowledgement ID does not match",
            ));
        }
        match (&effect.kind, result) {
            (EffectKind::Command { .. } | EffectKind::Delay { .. }, EffectResult::Done) => {}
            (EffectKind::Watch { watcher, .. }, EffectResult::Done) => {
                self.stack.push(RuntimeValue::Watcher(*watcher))
            }
            (
                EffectKind::Wait {
                    watcher, strict, ..
                },
                EffectResult::Matched(matched),
            ) => {
                let watcher = *watcher;
                let strict = *strict;
                // The driver's result is authoritative for this deadline. A late
                // RX match must not turn an already timed-out expect into success.
                self.watcher_matched(watcher)?;
                if matched {
                    self.watchers[watcher as usize] = true;
                }
                if strict {
                    if !matched {
                        return Err(self.error("expect_timeout", "required serial response was not observed; later commands were not issued"));
                    }
                } else {
                    self.push_bool(matched);
                }
            }
            _ => {
                return Err(self.error(
                    "effect_protocol",
                    "effect acknowledgement has the wrong result type",
                ));
            }
        }
        self.pending = None;
        Ok(())
    }

    /// Report a transport/evidence failure. This is terminal, including for wait;
    /// callers must not convert RX gaps or uncertain writes to Matched(false).
    pub fn fail_effect(&mut self, effect_id: u64, message: impl Into<String>) -> Result<(), Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let error = if self
            .pending
            .as_ref()
            .is_some_and(|effect| effect.id == effect_id)
        {
            self.error("effect_failed", message)
        } else {
            self.error(
                "effect_protocol",
                "failure ID does not match a pending effect",
            )
        };
        self.failure = Some(error.clone());
        Err(error)
    }

    /// Terminal cancellation also works while the VM is between CPU slices.
    pub fn interrupt(&mut self, message: impl Into<String>) -> Error {
        if let Some(error) = &self.failure {
            return error.clone();
        }
        let error = self.error("interrupted", message);
        self.failure = Some(error.clone());
        error
    }

    /// Feed only a match after the watch's authoritative TX boundary. A watcher
    /// is monotonic; create a fresh watch for each independent response window.
    pub fn mark_matched(&mut self, watcher: WatcherId) -> Result<(), Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let index = usize::try_from(watcher)
            .map_err(|_| self.error("unknown_watcher", "watcher ID is out of range"))?;
        if let Some(matched) = self.watchers.get_mut(index) {
            *matched = true;
            Ok(())
        } else {
            Err(self.error("unknown_watcher", "watcher has not been created"))
        }
    }
    pub fn watcher_matched(&self, watcher: WatcherId) -> Result<bool, Error> {
        usize::try_from(watcher)
            .ok()
            .and_then(|index| self.watchers.get(index))
            .copied()
            .ok_or_else(|| self.error("unknown_watcher", "watcher has not been created"))
    }

    fn call(&mut self, builtin: Builtin) -> Result<Effect, Error> {
        let kind = match builtin {
            Builtin::Cmd => {
                let text = self.pop_string()?;
                validate_command(&text, self.program.limits.max_string_bytes, self.last_span)?;
                let bytes = self
                    .command_bytes
                    .checked_add(text.len())
                    .ok_or_else(|| self.error("tx_budget", "command byte count overflowed"))?;
                if bytes > self.program.limits.max_total_command_bytes {
                    return Err(self.error("tx_budget", "macro exhausted its command byte budget"));
                }
                self.command_bytes = bytes;
                self.command_effects += 1;
                EffectKind::Command { text }
            }
            Builtin::Watch => {
                let pattern = self.pop_string()?;
                if pattern.is_empty() {
                    return Err(self.error("empty_pattern", "watch pattern must not be empty"));
                }
                if self.watchers.len() >= self.program.limits.max_watchers {
                    return Err(self.error("watcher_budget", "macro exhausted its watcher budget"));
                }
                let watcher = self.watchers.len() as WatcherId;
                self.watchers.push(false);
                EffectKind::Watch { watcher, pattern }
            }
            Builtin::Wait | Builtin::Expect => {
                let timeout_ms = self.pop_duration()?;
                let watcher = self.pop_watcher()?;
                EffectKind::Wait {
                    watcher,
                    timeout_ms,
                    strict: builtin == Builtin::Expect,
                }
            }
            Builtin::Delay => EffectKind::Delay {
                duration_ms: self.pop_duration()?,
            },
        };
        let id = self.next_effect;
        self.next_effect = self
            .next_effect
            .checked_add(1)
            .ok_or_else(|| self.error("effect_budget", "effect ID space exhausted"))?;
        Ok(Effect {
            id,
            span: self.last_span,
            kind,
        })
    }

    fn error(&self, code: &'static str, message: impl Into<String>) -> Error {
        Error::new(code, message, self.last_span)
    }
    fn pop(&mut self) -> Result<RuntimeValue, Error> {
        self.stack
            .pop()
            .ok_or_else(|| self.error("vm_invariant", "empty expression stack"))
    }
    fn pop_scalar(&mut self) -> Result<Value, Error> {
        if let RuntimeValue::Scalar(value) = self.pop()? {
            Ok(value)
        } else {
            Err(self.error("vm_invariant", "expected scalar value"))
        }
    }
    fn pop_integer(&mut self) -> Result<i64, Error> {
        if let Value::Integer(value) = self.pop_scalar()? {
            Ok(value)
        } else {
            Err(self.error("vm_invariant", "expected integer"))
        }
    }
    fn pop_bool(&mut self) -> Result<bool, Error> {
        if let Value::Boolean(value) = self.pop_scalar()? {
            Ok(value)
        } else {
            Err(self.error("vm_invariant", "expected boolean"))
        }
    }
    fn pop_string(&mut self) -> Result<String, Error> {
        if let Value::String(value) = self.pop_scalar()? {
            Ok(value)
        } else {
            Err(self.error("vm_invariant", "expected string"))
        }
    }
    fn pop_watcher(&mut self) -> Result<WatcherId, Error> {
        if let RuntimeValue::Watcher(value) = self.pop()? {
            Ok(value)
        } else {
            Err(self.error("vm_invariant", "expected watcher"))
        }
    }
    fn pop_duration(&mut self) -> Result<u64, Error> {
        let value = self.pop_integer()?;
        if value < 0 || value as u64 > self.program.limits.max_duration_ms {
            return Err(self.error(
                "duration_limit",
                format!(
                    "duration must be 0..={} milliseconds",
                    self.program.limits.max_duration_ms
                ),
            ));
        }
        Ok(value as u64)
    }
    fn peek_bool(&self) -> Result<bool, Error> {
        if let Some(RuntimeValue::Scalar(Value::Boolean(value))) = self.stack.last() {
            Ok(*value)
        } else {
            Err(self.error("vm_invariant", "expected boolean"))
        }
    }
    fn push_bool(&mut self, value: bool) {
        self.stack.push(RuntimeValue::Scalar(Value::Boolean(value)));
    }
    fn unary(&mut self, operator: Unary) -> Result<(), Error> {
        match operator {
            Unary::Not => {
                let value = self.pop_bool()?;
                self.push_bool(!value);
            }
            Unary::Negate => {
                let value = self
                    .pop_integer()?
                    .checked_neg()
                    .ok_or_else(|| self.error("integer_overflow", "integer negation overflowed"))?;
                self.stack.push(RuntimeValue::Scalar(Value::Integer(value)));
            }
        }
        Ok(())
    }
    fn binary(&mut self, operator: Binary) -> Result<(), Error> {
        let right = self.pop_scalar()?;
        let left = self.pop_scalar()?;
        let value = match (operator, left, right) {
            (Binary::Equal, left, right) => Value::Boolean(left == right),
            (Binary::NotEqual, left, right) => Value::Boolean(left != right),
            (Binary::Add, Value::String(mut left), Value::String(right)) => {
                if left.len().saturating_add(right.len()) > self.program.limits.max_string_bytes {
                    return Err(self.error(
                        "string_limit",
                        "concatenated string exceeds UTF-8 byte limit",
                    ));
                }
                left.push_str(&right);
                Value::String(left)
            }
            (operator, Value::Integer(left), Value::Integer(right)) => {
                let integer = match operator {
                    Binary::Add => left.checked_add(right),
                    Binary::Subtract => left.checked_sub(right),
                    Binary::Multiply => left.checked_mul(right),
                    Binary::Divide | Binary::Remainder if right == 0 => {
                        return Err(
                            self.error("division_by_zero", "integer division/remainder by zero")
                        );
                    }
                    Binary::Divide => left.checked_div(right),
                    Binary::Remainder => left.checked_rem(right),
                    Binary::Less | Binary::LessEqual | Binary::Greater | Binary::GreaterEqual => {
                        let value = match operator {
                            Binary::Less => left < right,
                            Binary::LessEqual => left <= right,
                            Binary::Greater => left > right,
                            _ => left >= right,
                        };
                        self.push_bool(value);
                        return Ok(());
                    }
                    _ => return Err(self.error("vm_invariant", "invalid integer operator")),
                }
                .ok_or_else(|| self.error("integer_overflow", "integer arithmetic overflowed"))?;
                Value::Integer(integer)
            }
            _ => return Err(self.error("vm_invariant", "operator operand types disagree")),
        };
        self.stack.push(RuntimeValue::Scalar(value));
        Ok(())
    }
}
