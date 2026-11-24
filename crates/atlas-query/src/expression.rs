//! The expression grammar: what a string given to `select()`, `having()`, an
//! aggregate, or `whereExpr()`'s extra expression may contain.
//!
//! Such a string is compiled as written, so the check has to read it the way
//! the server will. It is tokenised whole (words, quoted identifiers, numbers,
//! string literals, operators, parentheses, commas, dots) and every token must
//! then find its place in a small grammar: column references, literals,
//! arithmetic and comparisons, calls to allow-listed functions, `CASE`, and
//! the keywords a call owns (`EXTRACT(x FROM y)`, `FILTER (WHERE …)`,
//! `OVER (PARTITION BY … ORDER BY …)`), accepted inside that call only.
//!
//! Whatever the tokenizer has no token for refuses the fragment: a comment, a
//! lone `:` or `|`, a control character, a backslash. So does any token the
//! grammar leaves over. A word filter cannot give that guarantee: it accepted
//! `pg_sleep/**/(2)` as a bare word the server then called, and `FROM` /
//! `JOIN` / `TABLE` at depth zero, where they rewrote the statement.
//!
//! NAMED DEVIATION: the upstream query builder reads a string given to
//! `select()` as an identifier and nothing else; an expression there needs a
//! raw fragment. Atlas keeps the expression form (`count(*)`,
//! `coalesce(sum(total), 0)`) because it stays available in strict mode, and
//! this grammar is what makes that safe.

/// Parentheses, calls and `CASE` may nest this deep. The parser recurses once
/// per level, so the bound is what keeps a `((((…` input from exhausting the
/// stack.
const MAX_DEPTH: usize = 32;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bare word, lower-cased.
    Word(String),
    /// `"name"` or `` `name` ``, its contents already checked.
    Quoted,
    Number,
    /// A complete `'…'` literal.
    Str,
    Op(&'static str),
    LParen,
    RParen,
    Comma,
    Dot,
}

/// Every operator spelling, longest first: the scan takes the first match.
const OPERATORS: &[&str] = &[
    "->>", "->", "<>", "<=", ">=", "!=", "||", "::", "=", "<", ">", "+", "-", "*", "/", "%",
];

/// Operators that join two operands. `::` is a postfix cast, not one of them.
const BINARY: &[&str] = &[
    "->>", "->", "<>", "<=", ">=", "!=", "||", "=", "<", ">", "+", "-", "*", "/", "%",
];

/// The operators `whereExpr()`'s extra expression may use.
const ARITHMETIC: &[&str] = &["+", "-", "*", "/"];

/// Functions an expression may call. Aggregates, window functions and pure
/// scalar functions: nothing that reads a file, a setting, another table, or
/// that sleeps. `cast`, `extract`, `trim` and `substring` have their own
/// argument syntax and are parsed apart.
const FUNCTIONS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "coalesce",
    "nullif",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "array_agg",
    "string_agg",
    "json_agg",
    "date_trunc",
    "now",
    "length",
    "lower",
    "upper",
    "replace",
    "round",
    "ceil",
    "floor",
    "abs",
];

/// The scalar subset `whereExpr()`'s extra expression may call: an aggregate
/// or a window function has no place in a WHERE.
const ARITHMETIC_FUNCTIONS: &[&str] = &[
    "abs",
    "ceil",
    "floor",
    "round",
    "coalesce",
    "nullif",
    "length",
    "lower",
    "upper",
    "trim",
    "replace",
    "substring",
    "now",
];

/// Types `CAST(x AS type)` and `x::type` may name.
const TYPES: &[&str] = &[
    "int",
    "integer",
    "int2",
    "int4",
    "int8",
    "bigint",
    "smallint",
    "tinyint",
    "numeric",
    "decimal",
    "real",
    "float",
    "float4",
    "float8",
    "double",
    "text",
    "varchar",
    "char",
    "character",
    "boolean",
    "bool",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "datetime",
    "json",
    "jsonb",
    "uuid",
    "signed",
    "unsigned",
    "binary",
    "bytea",
    "interval",
];

/// Fields `EXTRACT(field FROM x)` and a MySQL `INTERVAL n unit` may name.
const DATE_FIELDS: &[&str] = &[
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "week",
    "quarter",
    "epoch",
    "doy",
    "dow",
    "isodow",
    "isoyear",
    "millisecond",
    "microsecond",
    "century",
    "decade",
];

/// Words that are never a column reference when written bare.
///
/// Two families. Statement and grammar keywords: the grammar already has no
/// place for them, they are listed so the refusal names them. And the words a
/// server evaluates without parentheses (`current_user`, `session_user`, …):
/// to a check that looks for `name(`, each one is an ordinary column name,
/// and `(current_user) as title` returned the database role. A column that is
/// really named after one of these is written quoted.
const RESERVED: &[&str] = &[
    // Evaluated without parentheses.
    "user",
    "current_user",
    "session_user",
    "system_user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_database",
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    // Statements and clauses.
    "select",
    "from",
    "where",
    "join",
    "on",
    "using",
    "natural",
    "inner",
    "outer",
    "left",
    "right",
    "full",
    "cross",
    "lateral",
    "table",
    "union",
    "intersect",
    "except",
    "minus",
    "limit",
    "offset",
    "fetch",
    "values",
    "into",
    "having",
    "group",
    "order",
    "by",
    "window",
    "with",
    "recursive",
    "returning",
    "for",
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "grant",
    "revoke",
    "exec",
    "execute",
    "call",
    "do",
    "set",
    "show",
    "explain",
    "analyze",
    "pragma",
    "attach",
    "detach",
    "vacuum",
    "copy",
    "load",
    "begin",
    "commit",
    "rollback",
    // Expression grammar.
    "and",
    "or",
    "not",
    "is",
    "null",
    "true",
    "false",
    "unknown",
    "in",
    "like",
    "ilike",
    "rlike",
    "regexp",
    "similar",
    "between",
    "exists",
    "any",
    "all",
    "some",
    "case",
    "when",
    "then",
    "else",
    "end",
    "as",
    "distinct",
    "cast",
    "interval",
    "collate",
    "over",
    "filter",
    "partition",
    "array",
    "default",
    "asc",
    "desc",
];

fn is_reserved(word: &str) -> bool {
    RESERVED.contains(&word)
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_word_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Split `expr` into tokens, or say why a character has no token.
fn tokenize(expr: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = expr.chars().collect();
    let mut toks: Vec<Tok> = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        // Only the whitespace every dialect reads as whitespace. A form feed
        // or a no-break space separates tokens on one server and is part of a
        // word on another.
        if matches!(c, ' ' | '\t' | '\n' | '\r') {
            i += 1;
            continue;
        }
        // A comment hides a `(` from whoever reads the word before it, and
        // nothing an expression says needs one.
        if (c == '-' && next == Some('-'))
            || (c == '/' && next == Some('*'))
            || (c == '*' && next == Some('/'))
        {
            return Err(
                "contains forbidden characters: a comment (`--`, `/*`, `*/`) has no place in an expression"
                    .to_string(),
            );
        }
        if c == '\'' {
            // A backslash inside the literal is refused: MySQL (default
            // sql_mode) and Postgres `E'…'` strings read `\'` as an escaped
            // quote, the SQL standard reads a backslash then the closing
            // quote. Where the two disagree on where the literal stops, what
            // this scan skips as "inside the string" runs as SQL.
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err("has an unterminated string literal".to_string()),
                    Some('\\') => {
                        return Err(
                            "contains forbidden characters: a backslash inside a string literal"
                                .to_string(),
                        )
                    }
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => i += 2,
                    Some('\'') => break,
                    Some(_) => i += 1,
                }
            }
            i += 1;
            toks.push(Tok::Str);
            continue;
        }
        if c == '"' || c == '`' {
            let start = i + 1;
            i += 1;
            while i < chars.len() && chars[i] != c {
                i += 1;
            }
            if i >= chars.len() {
                return Err("has an unterminated quoted identifier".to_string());
            }
            if start == i || !chars[start..i].iter().all(|&ch| is_word_part(ch)) {
                return Err(
                    "contains forbidden characters: a quoted identifier holds letters, digits and underscores only"
                        .to_string(),
                );
            }
            i += 1;
            toks.push(Tok::Quoted);
            continue;
        }
        if c.is_ascii_digit() {
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if chars.get(i) == Some(&'.') && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            // `1abc`, `0x41`, `1e5`: the dialects do not agree on where the
            // number stops and a word starts.
            if chars.get(i).is_some_and(|&ch| is_word_part(ch)) {
                return Err("contains forbidden characters: a number runs into a word".to_string());
            }
            toks.push(Tok::Number);
            continue;
        }
        if is_word_start(c) {
            let start = i;
            while i < chars.len() && is_word_part(chars[i]) {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            toks.push(Tok::Word(word.to_lowercase()));
            continue;
        }
        let single = match c {
            '(' => Some(Tok::LParen),
            ')' => Some(Tok::RParen),
            ',' => Some(Tok::Comma),
            '.' => Some(Tok::Dot),
            _ => None,
        };
        if let Some(tok) = single {
            toks.push(tok);
            i += 1;
            continue;
        }
        let rest: String = chars[i..chars.len().min(i + 3)].iter().collect();
        match OPERATORS.iter().find(|op| rest.starts_with(**op)) {
            Some(op) => {
                toks.push(Tok::Op(op));
                i += op.chars().count();
            }
            None => {
                return Err(format!(
                    "contains forbidden characters: {} has no place in an expression",
                    describe_char(c)
                ))
            }
        }
    }
    Ok(toks)
}

fn describe_char(c: char) -> String {
    if c.is_control() || c.is_whitespace() {
        format!("the character U+{:04X}", c as u32)
    } else {
        format!("'{c}'")
    }
}

fn describe(tok: Option<&Tok>) -> String {
    match tok {
        None => "the end of the expression".to_string(),
        Some(Tok::Word(w)) => format!("the word '{w}'"),
        Some(Tok::Quoted) => "a quoted identifier".to_string(),
        Some(Tok::Number) => "a number".to_string(),
        Some(Tok::Str) => "a string literal".to_string(),
        Some(Tok::Op(op)) => format!("'{op}'"),
        Some(Tok::LParen) => "'('".to_string(),
        Some(Tok::RParen) => "')'".to_string(),
        Some(Tok::Comma) => "','".to_string(),
        Some(Tok::Dot) => "'.'".to_string(),
    }
}

fn not_a_function(name: &str, allowed: &[&str]) -> String {
    format!("calls '{name}', which is not one of {}", allowed.join(" "))
}

struct Parser<'a> {
    toks: &'a [Tok],
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn new(toks: &'a [Tok]) -> Self {
        Parser {
            toks,
            pos: 0,
            depth: 0,
        }
    }

    fn peek(&self) -> Option<&'a Tok> {
        self.toks.get(self.pos)
    }

    fn peek_at(&self, ahead: usize) -> Option<&'a Tok> {
        self.toks.get(self.pos + ahead)
    }

    fn peek_word(&self) -> Option<&'a str> {
        match self.peek() {
            Some(Tok::Word(w)) => Some(w.as_str()),
            _ => None,
        }
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == Some(tok) {
            self.pos += 1;
            return true;
        }
        false
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if self.peek_word() == Some(word) {
            self.pos += 1;
            return true;
        }
        false
    }

    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Op(found)) if *found == op) {
            self.pos += 1;
            return true;
        }
        false
    }

    fn eat_any_word(&mut self, words: &[&str]) -> bool {
        if self.peek_word().is_some_and(|w| words.contains(&w)) {
            self.pos += 1;
            return true;
        }
        false
    }

    /// Why the current token cannot stand where it is.
    fn unexpected(&self) -> String {
        match self.peek() {
            None if self.depth > 0 => "has unbalanced parentheses".to_string(),
            Some(Tok::RParen) if self.depth == 0 => "has unbalanced parentheses".to_string(),
            Some(Tok::Word(w)) if is_reserved(w) => {
                format!("contains the SQL keyword '{w}' where the grammar has no place for it")
            }
            other => format!("is not a valid expression: unexpected {}", describe(other)),
        }
    }

    fn expect(&mut self, tok: &Tok) -> Result<(), String> {
        if self.eat(tok) {
            return Ok(());
        }
        if *tok == Tok::RParen && self.peek().is_none() {
            return Err("has unbalanced parentheses".to_string());
        }
        Err(self.unexpected())
    }

    fn expect_word(&mut self, word: &str) -> Result<(), String> {
        if self.eat_word(word) {
            return Ok(());
        }
        Err(self.unexpected())
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(format!("nests deeper than {MAX_DEPTH} levels"));
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    /// Nothing may follow a complete expression.
    fn end(&self) -> Result<(), String> {
        if self.peek().is_none() {
            return Ok(());
        }
        Err(self.unexpected())
    }

    // ----- the full grammar: select(), having(), aggregates -----

    /// `operand (binary-operator operand | postfix)*`.
    fn expr(&mut self) -> Result<(), String> {
        self.operand()?;
        loop {
            if self.eat_op("::") {
                self.type_name()?;
                continue;
            }
            match self.peek() {
                Some(Tok::Op(op)) if BINARY.contains(op) => {
                    self.bump();
                    self.operand()?;
                }
                Some(Tok::Word(w)) => match w.as_str() {
                    "and" | "or" | "like" | "ilike" | "between" => {
                        self.bump();
                        self.operand()?;
                    }
                    "not" => match self.peek_at(1) {
                        Some(Tok::Word(n))
                            if matches!(n.as_str(), "like" | "ilike" | "between") =>
                        {
                            self.pos += 2;
                            self.operand()?;
                        }
                        Some(Tok::Word(n)) if n == "in" => {
                            self.pos += 2;
                            self.parenthesised_list()?;
                        }
                        _ => return Ok(()),
                    },
                    "in" => {
                        self.bump();
                        self.parenthesised_list()?;
                    }
                    "is" => {
                        self.bump();
                        self.eat_word("not");
                        if !self.eat_any_word(&["null", "true", "false", "unknown"]) {
                            return Err(self.unexpected());
                        }
                    }
                    _ => return Ok(()),
                },
                _ => return Ok(()),
            }
        }
    }

    fn operand(&mut self) -> Result<(), String> {
        while self.eat_op("-") || self.eat_op("+") || self.eat_word("not") {}
        self.primary()
    }

    fn expr_list(&mut self) -> Result<(), String> {
        self.expr()?;
        while self.eat(&Tok::Comma) {
            self.expr()?;
        }
        Ok(())
    }

    /// `( expr, expr, … )`: the right-hand side of `IN`.
    fn parenthesised_list(&mut self) -> Result<(), String> {
        self.expect(&Tok::LParen)?;
        self.enter()?;
        self.expr_list()?;
        self.expect(&Tok::RParen)?;
        self.leave();
        Ok(())
    }

    fn primary(&mut self) -> Result<(), String> {
        match self.peek() {
            Some(Tok::Number) | Some(Tok::Str) => {
                self.bump();
                Ok(())
            }
            // One expression between the parentheses: `(a, b)` is a row
            // constructor, which compares to a whole row of a sub-select.
            Some(Tok::LParen) => {
                self.bump();
                self.enter()?;
                self.expr()?;
                self.expect(&Tok::RParen)?;
                self.leave();
                Ok(())
            }
            Some(Tok::Quoted) => {
                self.bump();
                self.column_tail(1)
            }
            Some(Tok::Word(w)) => self.word(w),
            _ => Err(self.unexpected()),
        }
    }

    fn word(&mut self, word: &'a str) -> Result<(), String> {
        let called = self.peek_at(1) == Some(&Tok::LParen);
        match word {
            "case" => {
                self.bump();
                return self.case();
            }
            // `INTERVAL '1 day'` and MySQL's `INTERVAL 1 DAY`. `interval(`
            // is a MySQL function and falls through to the call check.
            "interval" => match (self.peek_at(1), self.peek_at(2)) {
                (Some(Tok::Str), _) => {
                    self.pos += 2;
                    return Ok(());
                }
                (Some(Tok::Number), Some(Tok::Word(unit)))
                    if DATE_FIELDS.contains(&unit.as_str()) =>
                {
                    self.pos += 3;
                    return Ok(());
                }
                _ => {}
            },
            "null" | "true" | "false" | "current_date" | "current_time" | "current_timestamp"
                if !called =>
            {
                self.bump();
                return Ok(());
            }
            _ => {}
        }
        if called {
            self.pos += 2;
            self.enter()?;
            match word {
                "cast" => {
                    self.expr()?;
                    self.expect_word("as")?;
                    self.type_name()?;
                }
                "extract" => {
                    if !self.eat_any_word(DATE_FIELDS) {
                        return Err(self.unexpected());
                    }
                    self.expect_word("from")?;
                    self.expr()?;
                }
                "trim" => {
                    self.eat_any_word(&["leading", "trailing", "both"]);
                    if self.eat_word("from") {
                        self.expr()?;
                    } else {
                        self.expr()?;
                        if self.eat_word("from") {
                            self.expr()?;
                        } else {
                            while self.eat(&Tok::Comma) {
                                self.expr()?;
                            }
                        }
                    }
                }
                "substring" => {
                    self.expr()?;
                    if self.eat_word("from") {
                        self.expr()?;
                        if self.eat_word("for") {
                            self.expr()?;
                        }
                    } else if self.eat_word("for") {
                        self.expr()?;
                    } else {
                        while self.eat(&Tok::Comma) {
                            self.expr()?;
                        }
                    }
                }
                name if FUNCTIONS.contains(&name) => return self.call(),
                name => {
                    let mut allowed = FUNCTIONS.to_vec();
                    allowed.extend(["cast", "extract", "trim", "substring"]);
                    return Err(not_a_function(name, &allowed));
                }
            }
            self.expect(&Tok::RParen)?;
            self.leave();
            return Ok(());
        }
        if is_reserved(word) {
            return Err(self.unexpected());
        }
        self.bump();
        self.column_tail(1)
    }

    /// The rest of a column reference after its first segment: up to two more
    /// dot-separated names. A name that ends in `(` is a call under a spelling
    /// the allow-list never saw (`"pg_sleep"(1)`, `public.count(id)`), and is
    /// refused whatever it names.
    fn column_tail(&mut self, mut segments: usize) -> Result<(), String> {
        while self.eat(&Tok::Dot) {
            segments += 1;
            match self.peek() {
                Some(Tok::Word(_)) | Some(Tok::Quoted) if segments <= 3 => self.bump(),
                _ => return Err(self.unexpected()),
            }
        }
        if self.peek() == Some(&Tok::LParen) {
            return Err(
                "calls a quoted or qualified name: an allowed function is always written bare"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The arguments of an allow-listed function, the opening parenthesis
    /// already consumed, then the clauses that call owns.
    fn call(&mut self) -> Result<(), String> {
        let star =
            matches!(self.peek(), Some(Tok::Op("*"))) && self.peek_at(1) == Some(&Tok::RParen);
        if star {
            self.bump();
        } else if self.peek() != Some(&Tok::RParen) {
            self.eat_word("distinct");
            self.expr_list()?;
            if self.eat_word("order") {
                self.expect_word("by")?;
                self.order_list()?;
            }
        }
        self.expect(&Tok::RParen)?;
        self.leave();
        if self.peek_word() == Some("filter") && self.peek_at(1) == Some(&Tok::LParen) {
            self.pos += 2;
            self.enter()?;
            self.expect_word("where")?;
            self.expr()?;
            self.expect(&Tok::RParen)?;
            self.leave();
        }
        if self.peek_word() == Some("over") && self.peek_at(1) == Some(&Tok::LParen) {
            self.pos += 2;
            self.enter()?;
            if self.eat_word("partition") {
                self.expect_word("by")?;
                self.expr_list()?;
            }
            if self.eat_word("order") {
                self.expect_word("by")?;
                self.order_list()?;
            }
            if self.eat_any_word(&["rows", "range", "groups"]) {
                if self.eat_word("between") {
                    self.frame_bound()?;
                    self.expect_word("and")?;
                }
                self.frame_bound()?;
            }
            self.expect(&Tok::RParen)?;
            self.leave();
        }
        Ok(())
    }

    fn order_list(&mut self) -> Result<(), String> {
        loop {
            self.expr()?;
            self.eat_any_word(&["asc", "desc"]);
            if self.eat_word("nulls") && !self.eat_any_word(&["first", "last"]) {
                return Err(self.unexpected());
            }
            if !self.eat(&Tok::Comma) {
                return Ok(());
            }
        }
    }

    fn frame_bound(&mut self) -> Result<(), String> {
        if self.eat_word("current") {
            return self.expect_word("row");
        }
        if !self.eat_word("unbounded") && !self.eat(&Tok::Number) {
            return Err(self.unexpected());
        }
        if self.eat_any_word(&["preceding", "following"]) {
            return Ok(());
        }
        Err(self.unexpected())
    }

    /// `CASE [x] WHEN a THEN b … [ELSE c] END`, the `CASE` already consumed.
    fn case(&mut self) -> Result<(), String> {
        self.enter()?;
        if self.peek_word() != Some("when") {
            self.expr()?;
        }
        self.expect_word("when")?;
        loop {
            self.expr()?;
            self.expect_word("then")?;
            self.expr()?;
            if !self.eat_word("when") {
                break;
            }
        }
        if self.eat_word("else") {
            self.expr()?;
        }
        self.expect_word("end")?;
        self.leave();
        Ok(())
    }

    fn type_name(&mut self) -> Result<(), String> {
        let name = match self.peek_word() {
            Some(name) if TYPES.contains(&name) => name,
            Some(name) => {
                return Err(format!(
                    "casts to '{name}', which is not one of {}",
                    TYPES.join(" ")
                ))
            }
            None => return Err(self.unexpected()),
        };
        self.bump();
        match name {
            "double" => {
                self.eat_word("precision");
            }
            "character" => {
                self.eat_word("varying");
            }
            "signed" | "unsigned" => {
                self.eat_word("integer");
            }
            _ => {}
        }
        if self.eat(&Tok::LParen) {
            self.enter()?;
            self.expect(&Tok::Number)?;
            if self.eat(&Tok::Comma) {
                self.expect(&Tok::Number)?;
            }
            self.expect(&Tok::RParen)?;
            self.leave();
        }
        Ok(())
    }

    // ----- the arithmetic grammar: whereExpr()'s extra expression -----

    fn arithmetic_unexpected(&self) -> String {
        match self.peek() {
            Some(Tok::Str) | Some(Tok::Quoted) | Some(Tok::Dot) => format!(
                "contains forbidden characters: {} has no place in arithmetic on columns and numbers",
                describe(self.peek())
            ),
            Some(Tok::Op(op)) if !ARITHMETIC.contains(op) => format!(
                "contains forbidden characters: '{op}' is not one of the arithmetic operators + - * /"
            ),
            Some(Tok::Word(w)) if is_reserved(w) => format!(
                "contains the SQL keyword '{w}': only arithmetic on columns and numbers is allowed"
            ),
            None if self.depth > 0 => "has unbalanced parentheses".to_string(),
            Some(Tok::RParen) if self.depth == 0 => "has unbalanced parentheses".to_string(),
            other => format!(
                "is not arithmetic on columns and numbers: unexpected {}",
                describe(other)
            ),
        }
    }

    fn arithmetic(&mut self) -> Result<(), String> {
        self.arithmetic_operand()?;
        while matches!(self.peek(), Some(Tok::Op(op)) if ARITHMETIC.contains(op)) {
            self.bump();
            self.arithmetic_operand()?;
        }
        Ok(())
    }

    fn arithmetic_operand(&mut self) -> Result<(), String> {
        while self.eat_op("-") || self.eat_op("+") {}
        match self.peek() {
            Some(Tok::Number) => {
                self.bump();
                Ok(())
            }
            Some(Tok::LParen) => {
                self.bump();
                self.enter()?;
                self.arithmetic()?;
                self.arithmetic_close()
            }
            Some(Tok::Word(w)) if self.peek_at(1) == Some(&Tok::LParen) => {
                if !ARITHMETIC_FUNCTIONS.contains(&w.as_str()) {
                    return Err(not_a_function(w, ARITHMETIC_FUNCTIONS));
                }
                self.pos += 2;
                self.enter()?;
                if self.peek() != Some(&Tok::RParen) {
                    self.arithmetic()?;
                    while self.eat(&Tok::Comma) {
                        self.arithmetic()?;
                    }
                }
                self.arithmetic_close()
            }
            Some(Tok::Word(w)) if !is_reserved(w) => {
                self.bump();
                Ok(())
            }
            _ => Err(self.arithmetic_unexpected()),
        }
    }

    fn arithmetic_close(&mut self) -> Result<(), String> {
        if !self.eat(&Tok::RParen) {
            return Err(self.arithmetic_unexpected());
        }
        self.leave();
        Ok(())
    }
}

/// Check a `select()` / aggregate expression: one expression, then an optional
/// alias (`AS name`, or a bare name that is not a keyword). `Err` carries the
/// reason, phrased to follow the quoted expression in a message.
pub fn check_select_expression(expr: &str) -> Result<(), String> {
    let toks = tokenize(expr)?;
    let mut parser = Parser::new(&toks);
    parser.expr()?;
    if parser.eat_word("as") {
        match parser.peek() {
            Some(Tok::Word(_)) | Some(Tok::Quoted) => parser.bump(),
            _ => return Err(parser.unexpected()),
        }
    } else {
        match parser.peek() {
            Some(Tok::Word(w)) if !is_reserved(w) => parser.bump(),
            Some(Tok::Quoted) => parser.bump(),
            _ => {}
        }
    }
    parser.end()
}

/// Check a `having()` column expression: one expression, nothing after it.
pub fn check_having_expression(expr: &str) -> Result<(), String> {
    let toks = tokenize(expr)?;
    let mut parser = Parser::new(&toks);
    parser.expr()?;
    parser.end()
}

/// Check the extra expression of `whereExpr(column, extra, operator, value)`.
///
/// It is appended to a quoted column, so it is a tail: one or more
/// `operator operand` pairs, where an operand is a number, a column, a
/// parenthesised operand or a call to a scalar function of
/// [`ARITHMETIC_FUNCTIONS`]. No literal, no quoted or qualified name, no
/// keyword. A tail that opened with `(` would turn the quoted column before
/// it into a function name.
pub fn check_arithmetic_tail(expr: &str) -> Result<(), String> {
    let toks = tokenize(expr)?;
    let mut parser = Parser::new(&toks);
    loop {
        match parser.peek() {
            Some(Tok::Op(op)) if ARITHMETIC.contains(op) => parser.bump(),
            _ => return Err(parser.arithmetic_unexpected()),
        }
        parser.arithmetic_operand()?;
        if parser.peek().is_none() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select_ok(expr: &str) {
        if let Err(reason) = check_select_expression(expr) {
            panic!("{expr} should be accepted, refused because it {reason}");
        }
    }

    fn select_err(expr: &str) -> String {
        match check_select_expression(expr) {
            Ok(()) => panic!("{expr} should be refused"),
            Err(reason) => reason,
        }
    }

    #[test]
    fn the_expressions_the_guides_show_are_accepted() {
        for expr in [
            "COUNT(*)",
            "count(*) as n",
            "COUNT(*) AS \"n\"",
            "count(*) total",
            "SUM(amount)",
            "sum(\"orders\".\"total\")",
            "max(`amount`) as top",
            "count (\"x\")",
            "count(distinct user_id)",
            "coalesce(sum(total), 0)",
            "COALESCE(name, 'unknown')",
            "coalesce(name, 'it''s')",
            "coalesce(\"a\", 0) + (\"b\" * 2)",
            "EXTRACT(year FROM created_at)",
            "upper(a.id::text)",
            "cast(total as numeric(10, 2))",
            "string_agg(name, ',')",
            "string_agg(name, ', ' order by name desc)",
            "array_agg(id order by created_at desc nulls last)",
            "count(*) filter (where status = 'paid')",
            "sum(total) over (partition by user_id order by created_at)",
            "row_number() over (order by id desc) as rn",
            "sum(x) over (order by id rows between unbounded preceding and current row)",
            "avg(x) over (order by id rows 2 preceding)",
            "case when total > 100 then 'big' else 'small' end",
            "case status when 'a' then 1 when 'b' then 2 end",
            "sum(case when status in ('paid', 'sent') then total else 0 end)",
            "count(*) filter (where deleted_at is not null)",
            "trim(both ' ' from name)",
            "trim(name)",
            "substring(name from 1 for 3)",
            "substring(name, 1, 3)",
            "date_trunc('day', created_at)",
            "now() - interval '1 day'",
            "(created_at + interval 1 day)",
            "round(avg(score), 2)",
            "lower(data->>'name')",
            "(price * quantity) as line_total",
            "(a between 1 and 10)",
            "(a not like 'x%')",
            "(a not in (1, 2))",
            "(not a)",
            "(-price)",
            "coalesce(deleted_at, current_timestamp)",
            "length(\"year\")",
            "sum(year)",
        ] {
            select_ok(expr);
        }
    }

    #[test]
    fn a_comment_never_hides_a_call() {
        for expr in [
            "count(pg_sleep/**/(1))",
            "count(pg_sleep /* x */ (1))",
            "length(version/**/())",
            "count(1) -- x",
            "count(1) */",
        ] {
            let reason = select_err(expr);
            assert!(reason.contains("comment"), "{expr}: {reason}");
            assert!(check_having_expression(expr).is_err(), "{expr}");
        }
    }

    #[test]
    fn statement_keywords_have_no_place_at_any_depth() {
        for expr in [
            "(password) as title from users where 1 is distinct",
            "(password) as title from users join articles on not articles is distinct",
            "(table api_keys limit 1) as title",
            "(table api_keys limit 1)",
            "coalesce((table api_keys limit 1), '')",
            "(values (1))",
            "(a) union",
            "count(*) from users",
            "count(*) as n from users",
            "(1) limit 1",
            "(a) is distinct",
            "(a is distinct from b)",
            "coalesce((select 1), 0)",
            "count(*) into outfile",
            "(with x as (a) a)",
        ] {
            select_err(expr);
            assert!(check_having_expression(expr).is_err(), "{expr}");
        }
        // `FROM`, `WHERE`, `ORDER BY` belong to the call that owns them, and
        // only there.
        select_ok("extract(epoch from created_at)");
        select_err("coalesce(epoch from created_at)");
        select_err("count(*) filter (order by id)");
        select_err("(a where b)");
        select_err("(a order by b)");
        select_err("count(*) over (where a = 1)");
    }

    #[test]
    fn a_row_constructor_is_refused() {
        select_err("(1, 'root@x.io', 'H') <= (a)");
        select_err("sum(id) + (case when true = ((1, 'a') <= (b)) then 1000 else 0 end) as total");
        select_err("count(*), version()");
        select_err("max(id) as a, min(id) as b");
    }

    #[test]
    fn a_word_the_server_evaluates_bare_is_not_a_column() {
        for word in [
            "current_user",
            "CURRENT_USER",
            "session_user",
            "system_user",
            "user",
            "current_role",
            "current_catalog",
            "current_schema",
            "localtime",
            "utc_timestamp",
        ] {
            let reason = select_err(&format!("({word}) as title"));
            assert!(reason.contains("SQL keyword"), "{word}: {reason}");
            select_err(&format!("length({word})"));
            select_err(&format!("coalesce(name, {word})"));
            // Quoted, it is a column of that name and nothing else.
            select_ok(&format!("length(\"{word}\")"));
        }
        // An implicit alias cannot be one either: it would be the word itself.
        select_err("count(*) current_user");
    }

    #[test]
    fn only_an_allow_listed_bare_name_is_called() {
        for expr in [
            "evil_func(1)",
            "cast(pg_read_file('/etc/passwd') as text)",
            "length(current_setting('is_superuser'))",
            "coalesce(lo_import('/etc/passwd'), 0)",
            "\"pg_sleep\"(1)",
            "\"pg_sleep\" (1)",
            "`sleep`(1)",
            "pg_catalog.pg_sleep(1)",
            "public.count(id)",
            "\"public\".count(id)",
            "public . upper(name)",
            "interval(1, 2)",
            "exists(a)",
            "current_timestamp(6)",
            "count(a.b.c.d)",
        ] {
            select_err(expr);
            assert!(check_having_expression(expr).is_err(), "{expr}");
        }
        assert!(select_err("my_func(name)").starts_with("calls 'my_func'"));
        assert!(select_err("cast(a as regclass)").starts_with("casts to 'regclass'"));
        assert!(select_err("count(a::regproc)").starts_with("casts to 'regproc'"));
    }

    #[test]
    fn what_the_tokenizer_has_no_token_for_is_refused() {
        for expr in [
            "count(id);",
            "count($1)",
            "count(@@version)",
            "count(a ? b)",
            "count(a | b)",
            "count(a : b)",
            "count(a ! b)",
            "count(a[1])",
            "count({a})",
            "count(a\\b)",
            "count(a ~ b)",
            "count(a ^ b)",
            "count(a & b)",
            "count(a\u{000C}b)",
            "count(a\u{00A0}b)",
            "count(a\u{0000})",
            "count('\\'')",
            "coalesce(name, 'x)",
            "count(\"a b\")",
            "count(\"\")",
            "count(\"a)",
            "count(0x41)",
            "count(1e5)",
            "count(1abc)",
        ] {
            select_err(expr);
        }
        // Tokens the grammar has no place for.
        for expr in [
            "count(e'abc')",
            "count(x'41')",
            "count(_utf8'abc')",
            "count('a' 'b')",
            "count(a b c)",
            "count(1.)",
            "count(.5)",
            "count(a <=> b)",
            "count(a << 2)",
            "count(a ** 2)",
            "count(a == b)",
            "count((1)",
            "count(1))",
            "count()) + (1",
            "",
            "()",
        ] {
            select_err(expr);
        }
    }

    #[test]
    fn nesting_is_bounded() {
        let deep = format!("{}1{}", "(".repeat(40), ")".repeat(40));
        assert!(select_err(&deep).contains("nests deeper"));
        let fine = format!("{}1{}", "(".repeat(20), ")".repeat(20));
        select_ok(&fine);
        let tail = format!("+ {}1{}", "(".repeat(40), ")".repeat(40));
        assert!(check_arithmetic_tail(&tail)
            .unwrap_err()
            .contains("nests deeper"));
    }

    #[test]
    fn having_takes_one_expression_and_no_alias() {
        assert!(check_having_expression("count(*)").is_ok());
        assert!(check_having_expression("count(distinct user_id)").is_ok());
        assert!(check_having_expression("sum(total) / count(*)").is_ok());
        assert!(check_having_expression("count(*) as n").is_err());
        assert!(check_having_expression("count(*) n").is_err());
        assert!(check_having_expression("(table api_keys limit 1)").is_err());
        assert!(check_having_expression("count(*), version()").is_err());
    }

    #[test]
    fn an_arithmetic_tail_is_operators_and_operands() {
        for tail in [
            "+ 10",
            "+ tax",
            "+ tax * 2",
            "* 2 / 3",
            "* (1 + 0)",
            "- -1",
            "* 1.5",
            "+ coalesce(tax, 0)",
            "- round(abs(tax), 2)",
            "+ LENGTH (label)",
            "+ now()",
        ] {
            if let Err(reason) = check_arithmetic_tail(tail) {
                panic!("{tail} should be accepted, refused because it {reason}");
            }
        }
    }

    #[test]
    fn an_arithmetic_tail_calls_nothing_else_however_it_is_spelled() {
        for tail in [
            "+ pg_sleep(1)",
            "+ pg_sleep (1)",
            "+ pg_sleep/**/(1)",
            "+ pg_sleep /* x */ (1)",
            "+ length(version/**/())",
            "+ sleep/**/(1)",
            "+ length(concat/**/(pg_sleep/**/(2)))",
            "* 0 + length(randomblob/**/(1048576))",
            "+ length(sqlite_version())",
            "+ abs(pg_sleep(1))",
            "+ count(tax)",
            "+ length(current_user)",
            "+ length(current_user/**/)",
            "+ session_user",
            "+ current_date",
            "+ tax -- x",
            "+ \"pg_sleep\"(1)",
            "+ pg_catalog.pg_sleep(1)",
            "+ orders.tax",
            "+ 'a'",
            "+ tax % 2",
            "+ tax = 1",
            "+ tax || tax",
            "OR status",
            "AND 1",
            "+ 1 OR 1",
            "+ null",
            ") OR (1",
            "(tax)",
            "(1)",
            "tax",
            "+",
            "+ (1",
            "+ 1)",
            "+ abs(1",
            "+ abs(1,)",
            "+ 1 2",
            "; DROP TABLE orders--",
            "",
        ] {
            assert!(check_arithmetic_tail(tail).is_err(), "{tail}");
        }
        assert!(check_arithmetic_tail("+ pg_sleep(1)")
            .unwrap_err()
            .starts_with("calls 'pg_sleep', which is not one of abs ceil"));
        assert_eq!(
            check_arithmetic_tail(") OR (1").unwrap_err(),
            "has unbalanced parentheses"
        );
        assert!(check_arithmetic_tail("OR status")
            .unwrap_err()
            .starts_with("contains the SQL keyword 'or'"));
        assert!(check_arithmetic_tail("; DROP TABLE orders--")
            .unwrap_err()
            .starts_with("contains forbidden characters"));
        assert!(check_arithmetic_tail("+ pg_sleep/**/(1)")
            .unwrap_err()
            .contains("comment"));
    }
}
