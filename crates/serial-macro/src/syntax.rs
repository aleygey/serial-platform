use crate::{Error, Limits, Span, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    Ident(String),
    String(String),
    Integer(i64),
    LParen,
    RParen,
    LBrace,
    RBrace,
    Semicolon,
    Comma,
    Dot,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Bang,
    Equal,
    EqualEqual,
    BangEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    AndAnd,
    OrOr,
    PlusPlus,
    MinusMinus,
    PlusEqual,
    MinusEqual,
    StarEqual,
    SlashEqual,
    PercentEqual,
    Eof,
}

#[derive(Debug, Clone)]
struct Token {
    kind: TokenKind,
    span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unary {
    Negate,
    Not,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Binary {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub(crate) struct Expr {
    pub kind: ExprKind,
    pub span: Span,
}
#[derive(Debug, Clone)]
pub(crate) enum ExprKind {
    Literal(Value),
    Variable(String),
    Member(Box<Expr>, String),
    Unary(Unary, Box<Expr>),
    Binary(Binary, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub kind: StatementKind,
    pub span: Span,
}
#[derive(Debug, Clone)]
pub(crate) enum StatementKind {
    Let(String, Expr),
    Assign(String, Expr),
    Expression(Expr),
    Block(Vec<Statement>),
    If(Expr, Box<Statement>, Option<Box<Statement>>),
    While(Expr, Box<Statement>),
    For(
        Option<Box<Statement>>,
        Option<Expr>,
        Option<Box<Statement>>,
        Box<Statement>,
    ),
    Break,
    Continue,
    Empty,
}

struct Lexer<'a> {
    source: &'a str,
    offset: usize,
    line: usize,
    column: usize,
    max_string: usize,
}

impl<'a> Lexer<'a> {
    fn peek(&self) -> Option<char> {
        self.source[self.offset..].chars().next()
    }
    fn next(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.offset += c.len_utf8();
        if c == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(c)
    }
    fn span(&self) -> Span {
        Span {
            offset: self.offset,
            end_offset: self.offset,
            line: self.line,
            column: self.column,
        }
    }
    fn error(&self, code: &'static str, message: impl Into<String>, span: Span) -> Error {
        Error::new(code, message, span)
    }
    fn take(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.next();
            true
        } else {
            false
        }
    }
    fn string(&mut self, start: Span) -> Result<String, Error> {
        let mut output = String::new();
        loop {
            let c = self.next().ok_or_else(|| {
                self.error(
                    "unterminated_string",
                    "expected closing double quote",
                    start,
                )
            })?;
            match c {
                '"' => return Ok(output),
                '\\' => {
                    let escaped = self.next().ok_or_else(|| {
                        self.error("invalid_escape", "incomplete string escape", start)
                    })?;
                    let c = match escaped {
                        '"' => '"',
                        '\\' => '\\',
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '0' => '\0',
                        'u' => {
                            let brace = self.take('{');
                            let mut hex = String::new();
                            if brace {
                                while self.peek().is_some_and(|c| c.is_ascii_hexdigit())
                                    && hex.len() < 6
                                {
                                    hex.push(self.next().unwrap());
                                }
                                if hex.is_empty() || !self.take('}') {
                                    return Err(self.error(
                                        "invalid_escape",
                                        "Unicode escape requires \\u{1 to 6 hex digits}",
                                        start,
                                    ));
                                }
                            } else {
                                for _ in 0..4 {
                                    let c = self
                                        .next()
                                        .filter(char::is_ascii_hexdigit)
                                        .ok_or_else(|| {
                                            self.error(
                                                "invalid_escape",
                                                "Unicode escape requires four hex digits",
                                                start,
                                            )
                                        })?;
                                    hex.push(c);
                                }
                            }
                            u32::from_str_radix(&hex, 16)
                                .ok()
                                .and_then(char::from_u32)
                                .ok_or_else(|| {
                                    self.error(
                                        "invalid_escape",
                                        "Unicode escape is not a scalar value",
                                        start,
                                    )
                                })?
                        }
                        _ => {
                            return Err(self.error(
                                "invalid_escape",
                                format!("unknown string escape \\{escaped}"),
                                start,
                            ));
                        }
                    };
                    output.push(c);
                }
                c if c.is_control() => {
                    return Err(self.error(
                        "invalid_string",
                        "literal control characters require an explicit escape",
                        start,
                    ));
                }
                c => output.push(c),
            }
            if output.len() > self.max_string {
                return Err(self.error(
                    "string_limit",
                    "string literal exceeds the UTF-8 byte limit",
                    start,
                ));
            }
        }
    }
    fn tokens(mut self) -> Result<Vec<Token>, Error> {
        let mut tokens = Vec::new();
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.next();
                continue;
            }
            if self.source[self.offset..].starts_with("//") {
                while self.peek().is_some_and(|c| c != '\n') {
                    self.next();
                }
                continue;
            }
            let mut span = self.span();
            self.next();
            let kind = match c {
                '(' => TokenKind::LParen,
                ')' => TokenKind::RParen,
                '{' => TokenKind::LBrace,
                '}' => TokenKind::RBrace,
                ';' => TokenKind::Semicolon,
                ',' => TokenKind::Comma,
                '.' => TokenKind::Dot,
                '+' => {
                    if self.take('+') {
                        TokenKind::PlusPlus
                    } else if self.take('=') {
                        TokenKind::PlusEqual
                    } else {
                        TokenKind::Plus
                    }
                }
                '-' => {
                    if self.take('-') {
                        TokenKind::MinusMinus
                    } else if self.take('=') {
                        TokenKind::MinusEqual
                    } else {
                        TokenKind::Minus
                    }
                }
                '*' => {
                    if self.take('=') {
                        TokenKind::StarEqual
                    } else {
                        TokenKind::Star
                    }
                }
                '/' => {
                    if self.take('=') {
                        TokenKind::SlashEqual
                    } else {
                        TokenKind::Slash
                    }
                }
                '%' => {
                    if self.take('=') {
                        TokenKind::PercentEqual
                    } else {
                        TokenKind::Percent
                    }
                }
                '!' => {
                    if self.take('=') {
                        TokenKind::BangEqual
                    } else {
                        TokenKind::Bang
                    }
                }
                '=' => {
                    if self.take('=') {
                        TokenKind::EqualEqual
                    } else {
                        TokenKind::Equal
                    }
                }
                '<' => {
                    if self.take('=') {
                        TokenKind::LessEqual
                    } else {
                        TokenKind::Less
                    }
                }
                '>' => {
                    if self.take('=') {
                        TokenKind::GreaterEqual
                    } else {
                        TokenKind::Greater
                    }
                }
                '&' if self.take('&') => TokenKind::AndAnd,
                '|' if self.take('|') => TokenKind::OrOr,
                '"' => TokenKind::String(self.string(span)?),
                c if c.is_ascii_digit() => {
                    while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                        self.next();
                    }
                    let value = self.source[span.offset..self.offset]
                        .parse::<i64>()
                        .map_err(|_| {
                            self.error(
                                "integer_overflow",
                                "integer literal exceeds signed 64-bit range",
                                span,
                            )
                        })?;
                    TokenKind::Integer(value)
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    while self
                        .peek()
                        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                    {
                        self.next();
                    }
                    TokenKind::Ident(self.source[span.offset..self.offset].to_owned())
                }
                _ => {
                    return Err(self.error(
                        "unexpected_character",
                        format!("unexpected character {c:?}"),
                        span,
                    ));
                }
            };
            span.end_offset = self.offset;
            tokens.push(Token { kind, span });
        }
        tokens.push(Token {
            kind: TokenKind::Eof,
            span: self.span(),
        });
        Ok(tokens)
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
    depth: usize,
    max_depth: usize,
}

impl Parser {
    // Bound the resulting tree as well as parser recursion. A long, flat chain
    // such as a+a+... is left-associative and otherwise bypasses recursion limits.
    fn check_expression_depth(&self, root: &Expr) -> Result<(), Error> {
        let mut pending = vec![(root, 1usize)];
        while let Some((expression, depth)) = pending.pop() {
            if depth > self.max_depth {
                return Err(Error::new(
                    "nesting_limit",
                    "expression nesting limit exceeded",
                    expression.span,
                ));
            }
            match &expression.kind {
                ExprKind::Member(base, _) | ExprKind::Unary(_, base) => {
                    pending.push((base, depth + 1))
                }
                ExprKind::Binary(_, left, right) => {
                    pending.push((left, depth + 1));
                    pending.push((right, depth + 1));
                }
                ExprKind::Call(_, args) => {
                    for arg in args {
                        pending.push((arg, depth + 1));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn current(&self) -> &Token {
        &self.tokens[self.index]
    }
    fn at(&self, kind: &TokenKind) -> bool {
        &self.current().kind == kind
    }
    fn keyword(&self, text: &str) -> bool {
        matches!(&self.current().kind, TokenKind::Ident(s) if s == text)
    }
    fn bump(&mut self) -> Token {
        let token = self.current().clone();
        if token.kind != TokenKind::Eof {
            self.index += 1;
        }
        token
    }
    fn take(&mut self, kind: &TokenKind) -> bool {
        if self.at(kind) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn expect(&mut self, kind: TokenKind) -> Result<(), Error> {
        if self.take(&kind) {
            Ok(())
        } else {
            Err(Error::new(
                "syntax_error",
                format!("expected {kind:?}"),
                self.current().span,
            ))
        }
    }
    fn enter(&mut self) -> Result<(), Error> {
        if self.depth >= self.max_depth {
            return Err(Error::new(
                "nesting_limit",
                "syntax nesting limit exceeded",
                self.current().span,
            ));
        }
        self.depth += 1;
        Ok(())
    }
    fn identifier(&mut self) -> Result<String, Error> {
        let token = self.bump();
        match token.kind {
            TokenKind::Ident(name) if !reserved(&name) => Ok(name),
            _ => Err(Error::new(
                "syntax_error",
                "expected a non-reserved identifier",
                token.span,
            )),
        }
    }
    fn statements(&mut self, block: bool) -> Result<Vec<Statement>, Error> {
        let mut statements = Vec::new();
        while !(self.at(&TokenKind::Eof) || block && self.at(&TokenKind::RBrace)) {
            statements.push(self.statement()?);
        }
        if block {
            self.expect(TokenKind::RBrace)?;
        }
        Ok(statements)
    }
    fn block(&mut self) -> Result<Statement, Error> {
        if !self.at(&TokenKind::LBrace) {
            return Err(Error::new(
                "syntax_error",
                "control-flow body requires braces",
                self.current().span,
            ));
        }
        self.statement()
    }
    fn statement(&mut self) -> Result<Statement, Error> {
        self.enter()?;
        let span = self.current().span;
        let kind = if self.take(&TokenKind::LBrace) {
            StatementKind::Block(self.statements(true)?)
        } else if self.keyword("if") {
            self.bump();
            self.expect(TokenKind::LParen)?;
            let condition = self.expression(0)?;
            self.expect(TokenKind::RParen)?;
            let body = Box::new(self.block()?);
            let alternative = if self.keyword("else") {
                self.bump();
                Some(Box::new(if self.keyword("if") {
                    self.statement()?
                } else {
                    self.block()?
                }))
            } else {
                None
            };
            StatementKind::If(condition, body, alternative)
        } else if self.keyword("while") {
            self.bump();
            self.expect(TokenKind::LParen)?;
            let condition = self.expression(0)?;
            self.expect(TokenKind::RParen)?;
            StatementKind::While(condition, Box::new(self.block()?))
        } else if self.keyword("for") {
            self.bump();
            self.expect(TokenKind::LParen)?;
            let init = if self.at(&TokenKind::Semicolon) {
                None
            } else {
                Some(Box::new(self.simple()?))
            };
            self.expect(TokenKind::Semicolon)?;
            let condition = if self.at(&TokenKind::Semicolon) {
                None
            } else {
                Some(self.expression(0)?)
            };
            self.expect(TokenKind::Semicolon)?;
            let step = if self.at(&TokenKind::RParen) {
                None
            } else {
                Some(Box::new(self.simple()?))
            };
            self.expect(TokenKind::RParen)?;
            StatementKind::For(init, condition, step, Box::new(self.block()?))
        } else if self.keyword("break") || self.keyword("continue") {
            let is_break = self.keyword("break");
            self.bump();
            self.expect(TokenKind::Semicolon)?;
            if is_break {
                StatementKind::Break
            } else {
                StatementKind::Continue
            }
        } else if self.take(&TokenKind::Semicolon) {
            StatementKind::Empty
        } else {
            let simple = self.simple()?;
            self.expect(TokenKind::Semicolon)?;
            simple.kind
        };
        self.depth -= 1;
        Ok(Statement { kind, span })
    }
    fn simple(&mut self) -> Result<Statement, Error> {
        let span = self.current().span;
        if self.keyword("let") {
            self.bump();
            let name = self.identifier()?;
            self.expect(TokenKind::Equal)?;
            return Ok(Statement {
                kind: StatementKind::Let(name, self.expression(0)?),
                span,
            });
        }
        if let TokenKind::Ident(name) = &self.current().kind {
            let name = name.clone();
            let next = self.tokens.get(self.index + 1).map(|t| t.kind.clone());
            let operator = match next {
                Some(TokenKind::Equal) => Some(None),
                Some(TokenKind::PlusEqual | TokenKind::PlusPlus) => Some(Some(Binary::Add)),
                Some(TokenKind::MinusEqual | TokenKind::MinusMinus) => Some(Some(Binary::Subtract)),
                Some(TokenKind::StarEqual) => Some(Some(Binary::Multiply)),
                Some(TokenKind::SlashEqual) => Some(Some(Binary::Divide)),
                Some(TokenKind::PercentEqual) => Some(Some(Binary::Remainder)),
                _ => None,
            };
            if let Some(operator) = operator {
                self.bump();
                let token = self.bump();
                let rhs = if matches!(token.kind, TokenKind::PlusPlus | TokenKind::MinusMinus) {
                    Expr {
                        kind: ExprKind::Literal(Value::Integer(1)),
                        span: token.span,
                    }
                } else {
                    self.expression(0)?
                };
                let value = if let Some(operator) = operator {
                    Expr {
                        kind: ExprKind::Binary(
                            operator,
                            Box::new(Expr {
                                kind: ExprKind::Variable(name.clone()),
                                span,
                            }),
                            Box::new(rhs),
                        ),
                        span,
                    }
                } else {
                    rhs
                };
                return Ok(Statement {
                    kind: StatementKind::Assign(name, value),
                    span,
                });
            }
        }
        Ok(Statement {
            kind: StatementKind::Expression(self.expression(0)?),
            span,
        })
    }
    fn expression(&mut self, minimum: u8) -> Result<Expr, Error> {
        self.enter()?;
        let mut left = self.primary()?;
        while let Some((operator, precedence)) = binary(&self.current().kind) {
            if precedence < minimum {
                break;
            }
            self.bump();
            let right = self.expression(precedence + 1)?;
            let span = left.span;
            left = Expr {
                kind: ExprKind::Binary(operator, Box::new(left), Box::new(right)),
                span,
            };
            self.check_expression_depth(&left)?;
        }
        self.depth -= 1;
        Ok(left)
    }
    fn primary(&mut self) -> Result<Expr, Error> {
        let token = self.bump();
        let kind = match token.kind {
            TokenKind::String(value) => ExprKind::Literal(Value::String(value)),
            TokenKind::Integer(value) => ExprKind::Literal(Value::Integer(value)),
            TokenKind::Ident(name) if name == "true" || name == "false" => {
                ExprKind::Literal(Value::Boolean(name == "true"))
            }
            TokenKind::Ident(name) if !reserved(&name) || name == "args" => {
                if self.take(&TokenKind::LParen) {
                    let mut args = Vec::new();
                    if !self.at(&TokenKind::RParen) {
                        loop {
                            args.push(self.expression(0)?);
                            if !self.take(&TokenKind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    ExprKind::Call(name, args)
                } else {
                    ExprKind::Variable(name)
                }
            }
            TokenKind::Bang | TokenKind::Minus => ExprKind::Unary(
                if token.kind == TokenKind::Bang {
                    Unary::Not
                } else {
                    Unary::Negate
                },
                Box::new(self.expression(7)?),
            ),
            TokenKind::LParen => {
                let expr = self.expression(0)?;
                self.expect(TokenKind::RParen)?;
                expr.kind
            }
            _ => {
                return Err(Error::new(
                    "syntax_error",
                    "expected literal, variable or built-in call",
                    token.span,
                ));
            }
        };
        let mut expr = Expr {
            kind,
            span: token.span,
        };
        while self.take(&TokenKind::Dot) {
            let field = self.identifier()?;
            expr = Expr {
                kind: ExprKind::Member(Box::new(expr), field),
                span: token.span,
            };
            self.check_expression_depth(&expr)?;
        }
        self.check_expression_depth(&expr)?;
        Ok(expr)
    }
}

pub(crate) fn reserved(name: &str) -> bool {
    matches!(
        name,
        "let"
            | "if"
            | "else"
            | "for"
            | "while"
            | "break"
            | "continue"
            | "true"
            | "false"
            | "args"
            | "return"
            | "fn"
            | "function"
            | "import"
            | "eval"
    )
}

fn binary(token: &TokenKind) -> Option<(Binary, u8)> {
    Some(match token {
        TokenKind::OrOr => (Binary::Or, 1),
        TokenKind::AndAnd => (Binary::And, 2),
        TokenKind::EqualEqual => (Binary::Equal, 3),
        TokenKind::BangEqual => (Binary::NotEqual, 3),
        TokenKind::Less => (Binary::Less, 4),
        TokenKind::LessEqual => (Binary::LessEqual, 4),
        TokenKind::Greater => (Binary::Greater, 4),
        TokenKind::GreaterEqual => (Binary::GreaterEqual, 4),
        TokenKind::Plus => (Binary::Add, 5),
        TokenKind::Minus => (Binary::Subtract, 5),
        TokenKind::Star => (Binary::Multiply, 6),
        TokenKind::Slash => (Binary::Divide, 6),
        TokenKind::Percent => (Binary::Remainder, 6),
        _ => return None,
    })
}

pub(crate) fn parse(source: &str, limits: Limits) -> Result<Vec<Statement>, Error> {
    if source.len() > limits.max_source_bytes {
        return Err(Error::new(
            "source_limit",
            "script exceeds source byte limit",
            Span {
                line: 1,
                column: 1,
                ..Span::default()
            },
        ));
    }
    let tokens = Lexer {
        source,
        offset: 0,
        line: 1,
        column: 1,
        max_string: limits.max_string_bytes,
    }
    .tokens()?;
    Parser {
        tokens,
        index: 0,
        depth: 0,
        max_depth: limits.max_nesting,
    }
    .statements(false)
}
