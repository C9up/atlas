//! Placeholder grammar for SQL a caller typed: `whereRaw`, `havingRaw`,
//! `orderByRaw`, `groupByRaw`, `joinRaw`, `db.raw()` in a SELECT list, an
//! UPDATE SET or an upsert merge, and the top-level `rawQuery`.
//!
//! A fragment is written in one of two notations, never both:
//!
//! - `?` notation (Knex): `?` binds the next value, `??` inlines the next
//!   binding as a quoted identifier, `\?` is a literal `?` (the Postgres jsonb
//!   operators `?`, `?|`, `?&`). The number of `?`/`??` must equal the number
//!   of bindings.
//! - `$N` notation (what `RawSql.sql` and a compiled sub-query carry): `$k`
//!   binds the k-th binding. Every binding must be referenced.
//!
//! Placeholders are only recognised outside string literals, quoted
//! identifiers, comments and (Postgres) dollar-quoted bodies, so `'10:30?'` or
//! `'$5'` stay text. Whatever the notation, the fragment's bindings are
//! appended to the outer statement's parameters and its placeholders are
//! rewritten to continue the outer numbering. A fragment whose placeholders
//! and bindings disagree is refused: before this, a `$1` the compiler did not
//! rewrite pointed at the outer statement's first parameter and the
//! fragment's own bindings were dropped.

use crate::builder::CompileResult;
use crate::dialect::Dialect;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A whole statement written by the caller — `db.rawQuery(sql, bindings)`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RawSpec {
    pub sql: String,
    #[serde(default)]
    pub bindings: Vec<Value>,
}

/// Compile a caller-written statement with the fragment grammar, numbering
/// its placeholders from 1.
pub fn compile_raw(spec: &RawSpec, dialect: Dialect) -> Result<CompileResult, String> {
    let mut params = Vec::with_capacity(spec.bindings.len());
    let mut idx = 1;
    let sql = render_raw_fragment(
        "rawQuery",
        &spec.sql,
        spec.bindings.clone(),
        &mut params,
        &mut idx,
        dialect,
    )?;
    Ok(CompileResult { sql, params })
}

enum Token {
    /// Verbatim text (including literals and comments).
    Text(String),
    /// `?` — the next binding as a value.
    Value,
    /// `??` — the next binding as an identifier.
    Ident,
    /// `$k` — binding k (1-based) as a value.
    Numbered(usize),
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// Index just past the end of a quoted region opened at `start` by `quote`.
/// A doubled quote is an escaped quote; `backslash` also lets `\x` escape.
fn skip_quoted(chars: &[char], start: usize, quote: char, backslash: bool) -> usize {
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if backslash && c == '\\' {
            i += 2;
            continue;
        }
        if c == quote {
            if chars.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    chars.len()
}

/// A Postgres dollar-quote tag (`$tag$` or `$$`) starting at `start`, if any.
fn dollar_tag(chars: &[char], start: usize) -> Option<String> {
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == '$' {
            return Some(chars[start..=i].iter().collect());
        }
        let ok = if i == start + 1 {
            c.is_alphabetic() || c == '_'
        } else {
            c.is_alphanumeric() || c == '_'
        };
        if !ok {
            return None;
        }
        i += 1;
    }
    None
}

fn tokenize(fragment: &str, dialect: Dialect) -> Vec<Token> {
    let chars: Vec<char> = fragment.chars().collect();
    let mut tokens = Vec::new();
    let mut text = String::new();
    let mut i = 0;
    let flush = |text: &mut String, tokens: &mut Vec<Token>| {
        if !text.is_empty() {
            tokens.push(Token::Text(std::mem::take(text)));
        }
    };
    while i < chars.len() {
        let c = chars[i];
        let prev = if i > 0 { Some(chars[i - 1]) } else { None };
        let end = match c {
            '\'' => {
                // MySQL strings, and Postgres E'' strings, take backslash escapes.
                let escape_string = matches!(prev, Some('E') | Some('e'))
                    && (i < 2 || !is_ident_char(chars[i - 2]));
                let backslash = dialect == Dialect::Mysql || escape_string;
                Some(skip_quoted(&chars, i, '\'', backslash))
            }
            '"' => Some(skip_quoted(&chars, i, '"', dialect == Dialect::Mysql)),
            '`' if dialect == Dialect::Mysql => Some(skip_quoted(&chars, i, '`', false)),
            '-' if chars.get(i + 1) == Some(&'-') => Some(
                chars[i..]
                    .iter()
                    .position(|&ch| ch == '\n')
                    .map_or(chars.len(), |p| i + p + 1),
            ),
            '/' if chars.get(i + 1) == Some(&'*') => Some(
                (i + 2..chars.len().saturating_sub(1))
                    .find(|&j| chars[j] == '*' && chars[j + 1] == '/')
                    .map_or(chars.len(), |j| j + 2),
            ),
            '$' if dialect == Dialect::Postgres && !prev.is_some_and(is_ident_char) => {
                dollar_tag(&chars, i).map(|tag| {
                    let body_start = i + tag.chars().count();
                    let tail: String = chars[body_start..].iter().collect();
                    match tail.find(&tag) {
                        Some(pos) => body_start + tail[..pos].chars().count() + tag.chars().count(),
                        None => chars.len(),
                    }
                })
            }
            _ => None,
        };
        if let Some(end) = end {
            text.extend(&chars[i..end]);
            i = end;
            continue;
        }
        if c == '\\' && chars.get(i + 1) == Some(&'?') {
            text.push('?');
            i += 2;
            continue;
        }
        if c == '?' {
            flush(&mut text, &mut tokens);
            if chars.get(i + 1) == Some(&'?') {
                tokens.push(Token::Ident);
                i += 2;
            } else {
                tokens.push(Token::Value);
                i += 1;
            }
            continue;
        }
        if c == '$'
            && !prev.is_some_and(is_ident_char)
            && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit())
        {
            let digits: String = chars[i + 1..]
                .iter()
                .take_while(|d| d.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<usize>() {
                flush(&mut text, &mut tokens);
                tokens.push(Token::Numbered(n));
                i += 1 + digits.len();
                continue;
            }
        }
        text.push(c);
        i += 1;
    }
    flush(&mut text, &mut tokens);
    tokens
}

fn identifier_binding(what: &str, value: &Value, dialect: Dialect) -> Result<String, String> {
    let name = value
        .as_str()
        .ok_or_else(|| format!("{what}: a '??' binding must be an identifier string"))?;
    dialect.quote_ident(name)
}

/// Render a caller-written fragment: rewrite its placeholders into the
/// dialect's own, continuing from `*param_index`, and append its bindings to
/// `params` in placeholder order.
pub(crate) fn render_raw_fragment(
    what: &str,
    fragment: &str,
    bindings: Vec<Value>,
    params: &mut Vec<Value>,
    param_index: &mut u32,
    dialect: Dialect,
) -> Result<String, String> {
    let tokens = tokenize(fragment, dialect);
    let question = tokens
        .iter()
        .filter(|t| matches!(t, Token::Value | Token::Ident))
        .count();
    let numbered = tokens.iter().any(|t| matches!(t, Token::Numbered(_)));
    if question > 0 && numbered {
        return Err(format!(
            "{what} mixes '?' and '$N' placeholders — write the fragment in one notation"
        ));
    }
    if !numbered && question != bindings.len() {
        return Err(format!(
            "{what} expects {} binding(s) for its '?' placeholders, got {}",
            question,
            bindings.len()
        ));
    }
    let mut used = vec![false; bindings.len()];
    let mut next = bindings.iter();
    let mut out = String::with_capacity(fragment.len());
    for token in &tokens {
        match token {
            Token::Text(text) => out.push_str(text),
            Token::Ident => {
                // Counted above, so a binding is always there.
                if let Some(value) = next.next() {
                    out.push_str(&identifier_binding(what, value, dialect)?);
                }
            }
            Token::Value => {
                if let Some(value) = next.next() {
                    params.push(value.clone());
                    out.push_str(&dialect.placeholder(*param_index));
                    *param_index += 1;
                }
            }
            Token::Numbered(k) => {
                let value = k
                    .checked_sub(1)
                    .and_then(|i| bindings.get(i))
                    .ok_or_else(|| {
                        format!(
                            "{what} references ${k} but carries {} binding(s)",
                            bindings.len()
                        )
                    })?;
                used[k - 1] = true;
                params.push(value.clone());
                out.push_str(&dialect.placeholder(*param_index));
                *param_index += 1;
            }
        }
    }
    if numbered {
        if let Some(unused) = used.iter().position(|u| !u) {
            return Err(format!(
                "{what} carries binding {} that no '${}' references",
                unused + 1,
                unused + 1
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn render(
        fragment: &str,
        bindings: Vec<Value>,
        dialect: Dialect,
    ) -> Result<(String, Vec<Value>), String> {
        let mut params = vec![json!("outer")];
        let mut idx = 2;
        let sql = render_raw_fragment("t", fragment, bindings, &mut params, &mut idx, dialect)?;
        Ok((sql, params))
    }

    #[test]
    fn question_marks_continue_the_outer_numbering() {
        let (sql, params) = render(
            "lower(?) || ?",
            vec![json!("A"), json!("b")],
            Dialect::Postgres,
        )
        .unwrap();
        assert_eq!(sql, "lower($2) || $3");
        assert_eq!(params, vec![json!("outer"), json!("A"), json!("b")]);
    }

    #[test]
    fn numbered_fragment_is_rebased_not_aliased() {
        // `$1` of the fragment is the fragment's own binding, not the outer `$1`.
        let (sql, params) = render("lower($1)", vec![json!("USER")], Dialect::Postgres).unwrap();
        assert_eq!(sql, "lower($2)");
        assert_eq!(params, vec![json!("outer"), json!("USER")]);
        let (sql, params) =
            render("$2 || $1", vec![json!("a"), json!("b")], Dialect::Sqlite).unwrap();
        assert_eq!(sql, "? || ?");
        assert_eq!(params, vec![json!("outer"), json!("b"), json!("a")]);
    }

    #[test]
    fn a_numbered_fragment_without_bindings_is_refused() {
        assert!(render("$1", vec![], Dialect::Postgres).is_err());
        assert!(render("$1", vec![json!(1), json!(2)], Dialect::Postgres).is_err());
    }

    #[test]
    fn binding_count_mismatch_is_refused() {
        assert!(render("? + ?", vec![json!(1)], Dialect::Postgres).is_err());
        assert!(render("?", vec![json!(1), json!(2)], Dialect::Sqlite).is_err());
        assert!(render("now()", vec![json!(1)], Dialect::Mysql).is_err());
    }

    #[test]
    fn double_question_mark_is_a_quoted_identifier() {
        let (sql, params) = render(
            "?? = ?",
            vec![json!("users.name"), json!("x")],
            Dialect::Mysql,
        )
        .unwrap();
        assert_eq!(sql, "`users`.`name` = ?");
        assert_eq!(params, vec![json!("outer"), json!("x")]);
        assert!(render("?? = 1", vec![json!("a\"b")], Dialect::Postgres).is_err());
        assert!(render("?? = 1", vec![json!(1)], Dialect::Postgres).is_err());
    }

    #[test]
    fn escaped_question_mark_is_literal() {
        let (sql, params) = render(
            r#"data \? 'a' AND id = ?"#,
            vec![json!(5)],
            Dialect::Postgres,
        )
        .unwrap();
        assert_eq!(sql, "data ? 'a' AND id = $2");
        assert_eq!(params, vec![json!("outer"), json!(5)]);
    }

    #[test]
    fn placeholders_inside_literals_and_comments_are_text() {
        let (sql, _) = render(
            "x = '?' AND y = ? -- why?\n",
            vec![json!(1)],
            Dialect::Sqlite,
        )
        .unwrap();
        assert_eq!(sql, "x = '?' AND y = ? -- why?\n");
        let (sql, _) = render(
            "note = '$5' /* $1 ? */ AND id = $1",
            vec![json!(1)],
            Dialect::Postgres,
        )
        .unwrap();
        assert_eq!(sql, "note = '$5' /* $1 ? */ AND id = $2");
        let (sql, _) =
            render("$fn$ select ? $fn$ || ?", vec![json!(1)], Dialect::Postgres).unwrap();
        assert_eq!(sql, "$fn$ select ? $fn$ || $2");
        let (sql, _) = render(r"'it\'s ?' = ?", vec![json!(1)], Dialect::Mysql).unwrap();
        assert_eq!(sql, r"'it\'s ?' = ?");
        let (sql, _) = render(r"E'it\'s ?' = ?", vec![json!(1)], Dialect::Postgres).unwrap();
        assert_eq!(sql, r"E'it\'s ?' = $2");
        let (sql, _) = render(r#""odd?col" = ?"#, vec![json!(1)], Dialect::Postgres).unwrap();
        assert_eq!(sql, r#""odd?col" = $2"#);
    }

    #[test]
    fn mixed_notations_are_refused() {
        assert!(render("? + $1", vec![json!(1), json!(2)], Dialect::Postgres).is_err());
    }
}
