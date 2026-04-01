//! Query builder — compiles a query description into parameterized SQL.

use crate::identifier::{quote_identifier, quote_select_expr, quote_having_expr, validate_operator, validate_direction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    #[serde(rename = "type")]
    pub clause_type: String, // "and" or "or"
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExistsClause {
    pub kind: String, // "exists"
    pub subquery: Box<QueryDescription>,
    #[serde(rename = "type")]
    pub clause_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderByClause {
    pub column: String,
    pub direction: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CteDefinition {
    pub name: String,
    pub sql: String,
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnionDefinition {
    pub sql: String,
    pub params: Vec<serde_json::Value>,
    pub all: bool,
}

/// Full query description — sent from TypeScript to Rust.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDescription {
    pub table: String,
    #[serde(default = "default_select")]
    pub select: Vec<String>,
    #[serde(default)]
    pub wheres: Vec<serde_json::Value>, // WhereClause or ExistsClause
    #[serde(default)]
    pub order_by: Vec<OrderByClause>,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(default)]
    pub having: Vec<WhereClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
    #[serde(default)]
    pub unions: Vec<UnionDefinition>,
}

fn default_select() -> Vec<String> {
    vec!["*".to_string()]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompileResult {
    pub sql: String,
    pub params: Vec<serde_json::Value>,
}

/// Compile a QueryDescription into parameterized SQL.
pub fn compile_query(desc: &QueryDescription) -> Result<CompileResult, String> {
    let mut params: Vec<serde_json::Value> = Vec::new();
    let mut param_index = 1u32;
    let mut sql = String::new();

    // Remap $N params — boundary-aware (does not replace $1 inside $10)
    let remap_params = |sub_sql: &str, sub_params: &[serde_json::Value], params: &mut Vec<serde_json::Value>, idx: &mut u32| -> String {
        let mut remapped = sub_sql.to_string();
        for i in (1..=sub_params.len()).rev() {
            let old = format!("${}", i);
            let new_val = format!("${}", *idx + i as u32 - 1);
            let mut result = String::new();
            let mut rest = remapped.as_str();
            while let Some(pos) = rest.find(&old) {
                result.push_str(&rest[..pos]);
                let after = pos + old.len();
                let next_is_digit = rest.get(after..after + 1)
                    .and_then(|s| s.chars().next())
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false);
                if next_is_digit {
                    result.push_str(&old);
                } else {
                    result.push_str(&new_val);
                }
                rest = &rest[after..];
            }
            result.push_str(rest);
            remapped = result;
        }
        params.extend(sub_params.iter().cloned());
        *idx += sub_params.len() as u32;
        remapped
    };

    // CTEs
    if !desc.ctes.is_empty() {
        let cte_parts: Result<Vec<String>, String> = desc.ctes.iter().map(|cte| {
            let name = quote_identifier(&cte.name)?;
            let remapped = remap_params(&cte.sql, &cte.params, &mut params, &mut param_index);
            Ok(format!("{} AS ({})", name, remapped))
        }).collect();
        sql += &format!("WITH {} ", cte_parts?.join(", "));
    }

    // SELECT
    let select_cols: Result<Vec<String>, String> = desc.select.iter()
        .map(|c| quote_select_expr(c))
        .collect();
    let distinct = if desc.distinct { "DISTINCT " } else { "" };
    let table = quote_identifier(&desc.table)?;
    sql += &format!("SELECT {}{} FROM {}", distinct, select_cols?.join(", "), table);

    // WHERE
    if !desc.wheres.is_empty() {
        let mut clauses = Vec::new();
        for (i, w) in desc.wheres.iter().enumerate() {
            let prefix = if i == 0 { "WHERE" } else {
                let t = w.get("type").and_then(|v| v.as_str()).unwrap_or("and");
                if t == "or" { "OR" } else { "AND" }
            };

            // Check if EXISTS clause
            if let Some(kind) = w.get("kind").and_then(|v| v.as_str()) {
                if kind == "exists" {
                    let sub = w.get("subquery")
                        .ok_or_else(|| "EXISTS clause missing 'subquery' field".to_string())?;
                    let sub_desc: QueryDescription = serde_json::from_value(sub.clone())
                        .map_err(|e| format!("Invalid EXISTS subquery: {}", e))?;
                    let sub_result = compile_query(&sub_desc)?;
                    let remapped = remap_params(&sub_result.sql, &sub_result.params, &mut params, &mut param_index);
                    clauses.push(format!("{} EXISTS ({})", prefix, remapped));
                    continue;
                }
            }

            let column = w.get("column").and_then(|v| v.as_str()).unwrap_or("");
            let raw_op = w.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
            let operator = validate_operator(raw_op)?;
            let col = quote_identifier(column)?;

            match operator {
                "IS NULL" => clauses.push(format!("{} {} IS NULL", prefix, col)),
                "IS NOT NULL" => clauses.push(format!("{} {} IS NOT NULL", prefix, col)),
                "IN" | "NOT IN" => {
                    if let Some(arr) = w.get("value").and_then(|v| v.as_array()) {
                        if arr.is_empty() {
                            let expr = if operator == "IN" { "1 = 0" } else { "1 = 1" };
                            clauses.push(format!("{} {}", prefix, expr));
                        } else {
                            let placeholders: Vec<String> = arr.iter().map(|v| {
                                params.push(v.clone());
                                let p = format!("${}", param_index);
                                param_index += 1;
                                p
                            }).collect();
                            clauses.push(format!("{} {} {} ({})", prefix, col, operator, placeholders.join(", ")));
                        }
                    }
                }
                _ => {
                    if let Some(value) = w.get("value") {
                        params.push(value.clone());
                        clauses.push(format!("{} {} {} ${}", prefix, col, operator, param_index));
                        param_index += 1;
                    }
                }
            }
        }
        sql += &format!(" {}", clauses.join(" "));
    }

    // GROUP BY
    if !desc.group_by.is_empty() {
        let cols: Result<Vec<String>, String> = desc.group_by.iter()
            .map(|c| quote_identifier(c))
            .collect();
        sql += &format!(" GROUP BY {}", cols?.join(", "));
    }

    // HAVING
    if !desc.having.is_empty() {
        let mut clauses = Vec::new();
        for (i, h) in desc.having.iter().enumerate() {
            let prefix = if i == 0 { "HAVING" } else { "AND" };
            let col = quote_having_expr(&h.column)?;
            match h.operator.as_str() {
                "IS NULL" => clauses.push(format!("{} {} IS NULL", prefix, col)),
                "IS NOT NULL" => clauses.push(format!("{} {} IS NOT NULL", prefix, col)),
                _ => {
                    params.push(h.value.clone());
                    clauses.push(format!("{} {} {} ${}", prefix, col, h.operator, param_index));
                    param_index += 1;
                }
            }
        }
        sql += &format!(" {}", clauses.join(" "));
    }

    // ORDER BY
    if !desc.order_by.is_empty() {
        let cols: Result<Vec<String>, String> = desc.order_by.iter()
            .map(|o| {
                let col = quote_identifier(&o.column)?;
                let dir = validate_direction(&o.direction)?;
                Ok(format!("{} {}", col, dir))
            })
            .collect();
        sql += &format!(" ORDER BY {}", cols?.join(", "));
    }

    // LIMIT / OFFSET
    if let Some(limit) = desc.limit {
        sql += &format!(" LIMIT {}", limit);
    }
    if let Some(offset) = desc.offset {
        sql += &format!(" OFFSET {}", offset);
    }

    // UNIONS
    for u in &desc.unions {
        let remapped = remap_params(&u.sql, &u.params, &mut params, &mut param_index);
        let keyword = if u.all { "UNION ALL" } else { "UNION" };
        sql += &format!(" {} ({})", keyword, remapped);
    }

    Ok(CompileResult { sql, params })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_desc(table: &str) -> QueryDescription {
        QueryDescription {
            table: table.to_string(),
            select: vec!["*".to_string()],
            wheres: vec![],
            order_by: vec![],
            group_by: vec![],
            having: vec![],
            limit: None,
            offset: None,
            distinct: false,
            ctes: vec![],
            unions: vec![],
        }
    }

    #[test]
    fn test_simple_select() {
        let desc = simple_desc("orders");
        let result = compile_query(&desc).unwrap();
        assert_eq!(result.sql, "SELECT * FROM \"orders\"");
        assert!(result.params.is_empty());
    }

    #[test]
    fn test_select_with_where() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "=",
            "value": "active",
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert_eq!(result.sql, "SELECT * FROM \"orders\" WHERE \"status\" = $1");
        assert_eq!(result.params, vec![serde_json::json!("active")]);
    }

    #[test]
    fn test_select_with_where_in() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "IN",
            "value": ["active", "pending"],
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("IN ($1, $2)"));
        assert_eq!(result.params.len(), 2);
    }

    #[test]
    fn test_empty_in() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "IN",
            "value": [],
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("1 = 0"));
    }

    #[test]
    fn test_order_by_limit_offset() {
        let mut desc = simple_desc("orders");
        desc.order_by.push(OrderByClause { column: "createdAt".to_string(), direction: "desc".to_string() });
        desc.limit = Some(20);
        desc.offset = Some(40);
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("ORDER BY \"createdAt\" DESC"));
        assert!(result.sql.contains("LIMIT 20"));
        assert!(result.sql.contains("OFFSET 40"));
    }

    #[test]
    fn test_distinct() {
        let mut desc = simple_desc("orders");
        desc.distinct = true;
        desc.select = vec!["status".to_string()];
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("SELECT DISTINCT \"status\""));
    }

    #[test]
    fn test_group_by_having() {
        let mut desc = simple_desc("orders");
        desc.select = vec!["status".to_string(), "COUNT(*) AS count".to_string()];
        desc.group_by = vec!["status".to_string()];
        desc.having.push(WhereClause {
            column: "COUNT(*)".to_string(),
            operator: ">".to_string(),
            value: serde_json::json!(5),
            clause_type: "and".to_string(),
        });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("GROUP BY \"status\""));
        assert!(result.sql.contains("HAVING COUNT(*) > $1"));
        assert_eq!(result.params, vec![serde_json::json!(5)]);
    }

    #[test]
    fn test_cte() {
        let mut desc = simple_desc("active_orders");
        desc.ctes.push(CteDefinition {
            name: "active_orders".to_string(),
            sql: "SELECT * FROM \"orders\" WHERE \"status\" = $1".to_string(),
            params: vec![serde_json::json!("active")],
        });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WITH \"active_orders\" AS ("));
        assert!(result.sql.contains("WHERE \"status\" = $1"));
        assert_eq!(result.params, vec![serde_json::json!("active")]);
    }

    #[test]
    fn test_union() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "=",
            "value": "pending",
            "type": "and"
        }));
        desc.unions.push(UnionDefinition {
            sql: "SELECT * FROM \"orders\" WHERE \"status\" = $1".to_string(),
            params: vec![serde_json::json!("paid")],
            all: false,
        });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("UNION ("));
        assert_eq!(result.params, vec![serde_json::json!("pending"), serde_json::json!("paid")]);
    }

    #[test]
    fn test_rejects_sql_injection_in_table() {
        let mut desc = simple_desc("orders; DROP TABLE users--");
        let result = compile_query(&desc);
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_sql_injection_in_column() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "id; DROP TABLE orders--",
            "operator": "=",
            "value": 1,
            "type": "and"
        }));
        let result = compile_query(&desc);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_null() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "deleted_at",
            "operator": "IS NULL",
            "value": null,
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("\"deleted_at\" IS NULL"));
        assert!(result.params.is_empty());
    }

    #[test]
    fn test_or_where() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({ "column": "status", "operator": "=", "value": "active", "type": "and" }));
        desc.wheres.push(serde_json::json!({ "column": "status", "operator": "=", "value": "pending", "type": "or" }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE \"status\" = $1 OR \"status\" = $2"));
    }
}
