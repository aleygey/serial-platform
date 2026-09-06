use std::collections::BTreeMap;

use crate::syntax::{self, Binary, Expr, ExprKind, Statement, StatementKind, Unary};
use crate::{Error, Limits, Program, Span, Value, ValueType, validate_command};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Type {
    Scalar(ValueType),
    Watcher,
    Void,
}
impl From<ValueType> for Type {
    fn from(value: ValueType) -> Self {
        Self::Scalar(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Builtin {
    Cmd,
    Watch,
    Wait,
    Expect,
    Delay,
}

#[derive(Debug, Clone)]
pub(crate) struct Instruction {
    pub op: Op,
    pub span: Span,
}
#[derive(Debug, Clone)]
pub(crate) enum Op {
    Push(Value),
    Arg(String),
    Prompt(String),
    Load(usize),
    Store(usize),
    Pop,
    Matched,
    Unary(Unary),
    Binary(Binary),
    Jump(usize),
    JumpIfFalse(usize),
    And(usize),
    Or(usize),
    Call(Builtin),
}

#[derive(Debug, Clone, Copy)]
struct Variable {
    index: usize,
    kind: Type,
}
#[derive(Default)]
struct Loop {
    breaks: Vec<usize>,
    continues: Vec<usize>,
}

struct Compiler {
    code: Vec<Instruction>,
    scopes: Vec<BTreeMap<String, Variable>>,
    loops: Vec<Loop>,
    variables: usize,
    parameters: BTreeMap<String, ValueType>,
    prompts: BTreeMap<String, Span>,
    limits: Limits,
}

impl Compiler {
    fn emit(&mut self, op: Op, span: Span) -> usize {
        let index = self.code.len();
        self.code.push(Instruction { op, span });
        index
    }
    fn patch(&mut self, instruction: usize, target: usize) {
        match &mut self.code[instruction].op {
            Op::Jump(value) | Op::JumpIfFalse(value) | Op::And(value) | Op::Or(value) => {
                *value = target
            }
            _ => unreachable!("compiler only patches branch instructions"),
        }
    }
    fn lookup(&self, name: &str, span: Span) -> Result<Variable, Error> {
        self.scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name))
            .copied()
            .ok_or_else(|| Error::new("unknown_variable", format!("unknown variable {name}"), span))
    }
    fn require(&self, actual: Type, expected: Type, span: Span) -> Result<(), Error> {
        if actual == expected {
            Ok(())
        } else {
            Err(Error::new(
                "type_error",
                format!("expected {expected:?}, got {actual:?}"),
                span,
            ))
        }
    }
    fn statements(&mut self, statements: &[Statement]) -> Result<(), Error> {
        for statement in statements {
            self.statement(statement)?;
        }
        Ok(())
    }
    fn statement(&mut self, statement: &Statement) -> Result<(), Error> {
        let span = statement.span;
        match &statement.kind {
            StatementKind::Let(name, expression) => {
                if self.scopes.last().unwrap().contains_key(name)
                    || builtin_name(name)
                    || syntax::reserved(name)
                {
                    return Err(Error::new(
                        "duplicate_variable",
                        format!("identifier {name} is already declared or reserved"),
                        span,
                    ));
                }
                let kind = self.expression(expression)?;
                if kind == Type::Void {
                    return Err(Error::new(
                        "type_error",
                        "a void operation cannot initialize a variable",
                        expression.span,
                    ));
                }
                if self.variables >= self.limits.max_variables {
                    return Err(Error::new(
                        "variable_limit",
                        "variable limit exceeded",
                        span,
                    ));
                }
                let index = self.variables;
                self.variables += 1;
                self.scopes
                    .last_mut()
                    .unwrap()
                    .insert(name.clone(), Variable { index, kind });
                self.emit(Op::Store(index), span);
            }
            StatementKind::Assign(name, expression) => {
                let variable = self.lookup(name, span)?;
                let kind = self.expression(expression)?;
                self.require(kind, variable.kind, expression.span)?;
                self.emit(Op::Store(variable.index), span);
            }
            StatementKind::Expression(expression) => {
                if self.expression(expression)? != Type::Void {
                    self.emit(Op::Pop, span);
                }
            }
            StatementKind::Block(statements) => {
                self.scopes.push(BTreeMap::new());
                self.statements(statements)?;
                self.scopes.pop();
            }
            StatementKind::If(condition, body, alternative) => {
                let kind = self.expression(condition)?;
                self.require(kind, ValueType::Boolean.into(), condition.span)?;
                let skip = self.emit(Op::JumpIfFalse(0), condition.span);
                self.statement(body)?;
                if let Some(alternative) = alternative {
                    let end = self.emit(Op::Jump(0), span);
                    self.patch(skip, self.code.len());
                    self.statement(alternative)?;
                    self.patch(end, self.code.len());
                } else {
                    self.patch(skip, self.code.len());
                }
            }
            StatementKind::While(condition, body) => {
                let begin = self.code.len();
                let kind = self.expression(condition)?;
                self.require(kind, ValueType::Boolean.into(), condition.span)?;
                let stop = self.emit(Op::JumpIfFalse(0), condition.span);
                self.loops.push(Loop::default());
                self.statement(body)?;
                self.emit(Op::Jump(begin), span);
                let end = self.code.len();
                self.patch(stop, end);
                self.finish_loop(begin, end);
            }
            StatementKind::For(init, condition, step, body) => {
                self.scopes.push(BTreeMap::new());
                if let Some(init) = init {
                    self.statement(init)?;
                }
                let begin = self.code.len();
                let stop = if let Some(condition) = condition {
                    let kind = self.expression(condition)?;
                    self.require(kind, ValueType::Boolean.into(), condition.span)?;
                    Some(self.emit(Op::JumpIfFalse(0), condition.span))
                } else {
                    None
                };
                self.loops.push(Loop::default());
                self.statement(body)?;
                let continue_at = self.code.len();
                if let Some(step) = step {
                    if matches!(step.kind, StatementKind::Let(_, _)) {
                        return Err(Error::new(
                            "syntax_error",
                            "for update must not declare a variable",
                            step.span,
                        ));
                    }
                    self.statement(step)?;
                }
                self.emit(Op::Jump(begin), span);
                let end = self.code.len();
                if let Some(stop) = stop {
                    self.patch(stop, end);
                }
                self.finish_loop(continue_at, end);
                self.scopes.pop();
            }
            StatementKind::Break | StatementKind::Continue => {
                if self.loops.is_empty() {
                    return Err(Error::new(
                        "invalid_loop_control",
                        "break/continue requires an enclosing loop",
                        span,
                    ));
                }
                let jump = self.emit(Op::Jump(0), span);
                let current = self.loops.last_mut().unwrap();
                if matches!(statement.kind, StatementKind::Break) {
                    current.breaks.push(jump);
                } else {
                    current.continues.push(jump);
                }
            }
            StatementKind::Empty => {
                self.emit(Op::Jump(self.code.len() + 1), span);
            }
        }
        Ok(())
    }
    fn finish_loop(&mut self, continue_at: usize, end: usize) {
        let current = self.loops.pop().unwrap();
        for branch in current.breaks {
            self.patch(branch, end);
        }
        for branch in current.continues {
            self.patch(branch, continue_at);
        }
    }
    fn expression(&mut self, expression: &Expr) -> Result<Type, Error> {
        let span = expression.span;
        Ok(match &expression.kind {
            ExprKind::Literal(value) => {
                self.emit(Op::Push(value.clone()), span);
                value.value_type().into()
            }
            ExprKind::Variable(name) => {
                let variable = self.lookup(name, span)?;
                self.emit(Op::Load(variable.index), span);
                variable.kind
            }
            ExprKind::Member(base, field) => {
                if matches!(&base.kind, ExprKind::Variable(name) if name == "args") {
                    let kind = self.parameters.get(field).copied().ok_or_else(|| {
                        Error::new(
                            "unknown_parameter",
                            format!("unknown parameter args.{field}"),
                            span,
                        )
                    })?;
                    self.emit(Op::Arg(field.clone()), span);
                    kind.into()
                } else {
                    let kind = self.expression(base)?;
                    if kind != Type::Watcher || field != "matched" {
                        return Err(Error::new(
                            "unknown_field",
                            "only args.<declared parameter> and watcher.matched are supported",
                            span,
                        ));
                    }
                    self.emit(Op::Matched, span);
                    ValueType::Boolean.into()
                }
            }
            ExprKind::Unary(operator, operand) => {
                let kind = self.expression(operand)?;
                let expected = match operator {
                    Unary::Negate => ValueType::Integer,
                    Unary::Not => ValueType::Boolean,
                };
                self.require(kind, expected.into(), operand.span)?;
                self.emit(Op::Unary(*operator), span);
                kind
            }
            ExprKind::Binary(Binary::And | Binary::Or, left, right) => {
                let left_type = self.expression(left)?;
                self.require(left_type, ValueType::Boolean.into(), left.span)?;
                let is_and = matches!(expression.kind, ExprKind::Binary(Binary::And, _, _));
                let branch = self.emit(if is_and { Op::And(0) } else { Op::Or(0) }, span);
                let right_type = self.expression(right)?;
                self.require(right_type, ValueType::Boolean.into(), right.span)?;
                self.patch(branch, self.code.len());
                ValueType::Boolean.into()
            }
            ExprKind::Binary(operator, left, right) => {
                let left_type = self.expression(left)?;
                let right_type = self.expression(right)?;
                self.require(right_type, left_type, right.span)?;
                let output = match operator {
                    Binary::Add if left_type == Type::Scalar(ValueType::String) => left_type,
                    Binary::Add
                    | Binary::Subtract
                    | Binary::Multiply
                    | Binary::Divide
                    | Binary::Remainder => {
                        self.require(left_type, ValueType::Integer.into(), left.span)?;
                        left_type
                    }
                    Binary::Less | Binary::LessEqual | Binary::Greater | Binary::GreaterEqual => {
                        self.require(left_type, ValueType::Integer.into(), left.span)?;
                        ValueType::Boolean.into()
                    }
                    Binary::Equal | Binary::NotEqual => {
                        if !matches!(left_type, Type::Scalar(_)) {
                            return Err(Error::new(
                                "type_error",
                                "equality requires scalar operands of the same type",
                                span,
                            ));
                        }
                        ValueType::Boolean.into()
                    }
                    _ => unreachable!(),
                };
                self.emit(Op::Binary(*operator), span);
                output
            }
            ExprKind::Call(name, args) => self.call(name, args, span)?,
        })
    }
    fn call(&mut self, name: &str, args: &[Expr], span: Span) -> Result<Type, Error> {
        if name == "prompt" {
            if args.len() != 1 {
                return Err(Error::new(
                    "argument_count",
                    "prompt requires one literal name",
                    span,
                ));
            }
            let ExprKind::Literal(Value::String(name)) = &args[0].kind else {
                return Err(Error::new(
                    "static_prompt_required",
                    "prompt name must be the literal \"shell\" or \"uboot\"",
                    args[0].span,
                ));
            };
            if name != "shell" && name != "uboot" {
                return Err(Error::new(
                    "unknown_prompt",
                    "supported profile prompts are shell and uboot",
                    args[0].span,
                ));
            }
            self.prompts.entry(name.clone()).or_insert(args[0].span);
            self.emit(Op::Prompt(name.clone()), span);
            return Ok(ValueType::String.into());
        }
        let (builtin, expected, result): (Builtin, &[Type], Type) = match name {
            "cmd" => (Builtin::Cmd, &[Type::Scalar(ValueType::String)], Type::Void),
            "watch" => (
                Builtin::Watch,
                &[Type::Scalar(ValueType::String)],
                Type::Watcher,
            ),
            "wait" => (
                Builtin::Wait,
                &[Type::Watcher, Type::Scalar(ValueType::Integer)],
                ValueType::Boolean.into(),
            ),
            "expect" => (
                Builtin::Expect,
                &[Type::Watcher, Type::Scalar(ValueType::Integer)],
                Type::Void,
            ),
            "delay" => (
                Builtin::Delay,
                &[Type::Scalar(ValueType::Integer)],
                Type::Void,
            ),
            _ => {
                return Err(Error::new(
                    "unknown_function",
                    format!(
                        "unknown function {name}; only cmd/watch/prompt/wait/expect/delay are available"
                    ),
                    span,
                ));
            }
        };
        if args.len() != expected.len() {
            return Err(Error::new(
                "argument_count",
                format!("{name} requires {} arguments", expected.len()),
                span,
            ));
        }
        for (argument, expected) in args.iter().zip(expected) {
            let actual = self.expression(argument)?;
            self.require(actual, *expected, argument.span)?;
        }
        if builtin == Builtin::Cmd
            && let Some(text) = constant_string(&args[0], self.limits.max_string_bytes)?
        {
            validate_command(&text, self.limits.max_string_bytes, args[0].span)?;
        }
        if builtin == Builtin::Watch
            && constant_string(&args[0], self.limits.max_string_bytes)?
                .is_some_and(|s| s.is_empty())
        {
            return Err(Error::new(
                "empty_pattern",
                "watch pattern must not be empty",
                args[0].span,
            ));
        }
        if matches!(builtin, Builtin::Wait | Builtin::Expect | Builtin::Delay) {
            let duration = args.last().unwrap();
            if let ExprKind::Literal(Value::Integer(ms)) = duration.kind
                && (ms < 0 || ms as u64 > self.limits.max_duration_ms)
            {
                return Err(Error::new(
                    "duration_limit",
                    format!(
                        "duration must be 0..={} milliseconds",
                        self.limits.max_duration_ms
                    ),
                    duration.span,
                ));
            }
        }
        self.emit(Op::Call(builtin), span);
        Ok(result)
    }
}

fn builtin_name(name: &str) -> bool {
    matches!(
        name,
        "cmd" | "watch" | "prompt" | "wait" | "expect" | "delay"
    )
}

fn constant_string(expression: &Expr, max_bytes: usize) -> Result<Option<String>, Error> {
    match &expression.kind {
        ExprKind::Literal(Value::String(value)) => Ok(Some(value.clone())),
        ExprKind::Binary(Binary::Add, left, right) => {
            if let (Some(mut left), Some(right)) = (
                constant_string(left, max_bytes)?,
                constant_string(right, max_bytes)?,
            ) {
                if left.len().saturating_add(right.len()) > max_bytes {
                    return Err(Error::new(
                        "string_limit",
                        "constant string exceeds UTF-8 byte limit",
                        expression.span,
                    ));
                }
                left.push_str(&right);
                Ok(Some(left))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

pub(crate) fn compile(
    source: &str,
    parameters: &BTreeMap<String, ValueType>,
    limits: Limits,
) -> Result<Program, Error> {
    if limits.max_nesting == 0 || limits.max_nesting > 128 || limits.instructions_per_yield == 0 {
        return Err(Error::new(
            "invalid_limits",
            "max_nesting must be 1..=128 and instructions_per_yield must be positive",
            Span {
                line: 1,
                column: 1,
                ..Span::default()
            },
        ));
    }
    for name in parameters.keys() {
        let mut chars = name.chars();
        if !chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
            || syntax::reserved(name)
        {
            return Err(Error::new(
                "invalid_parameter",
                format!("invalid parameter name {name:?}"),
                Span {
                    line: 1,
                    column: 1,
                    ..Span::default()
                },
            ));
        }
    }
    if parameters.len() > limits.max_variables {
        return Err(Error::new(
            "parameter_limit",
            "parameter count exceeds variable limit",
            Span {
                line: 1,
                column: 1,
                ..Span::default()
            },
        ));
    }
    let statements = syntax::parse(source, limits)?;
    let mut compiler = Compiler {
        code: Vec::new(),
        scopes: vec![BTreeMap::new()],
        loops: Vec::new(),
        variables: 0,
        parameters: parameters.clone(),
        prompts: BTreeMap::new(),
        limits,
    };
    compiler.statements(&statements)?;
    Ok(Program {
        code: compiler.code,
        parameters: compiler.parameters,
        prompts: compiler.prompts,
        variables: compiler.variables,
        limits,
    })
}
