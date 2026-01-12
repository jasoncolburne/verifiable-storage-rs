//! Serde-based binding for PostgreSQL queries.
//!
//! This module provides functions to bind Storable types to PostgreSQL queries
//! using serde serialization, avoiding the need for type-specific derive macros.

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{Column, Row, postgres::PgRow};
use verifiable_storage::{Storable, StorageError};

/// Build INSERT SQL for a table with the given columns.
fn build_insert_sql(table: &str, columns: &[&str]) -> String {
    let cols = columns.join(", ");
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        cols,
        placeholders.join(", ")
    )
}

/// Bind a Storable type's values to a PostgreSQL INSERT query.
///
/// Serializes the item to JSON, extracts values in column order (matching
/// `Storable::columns()`), and executes the INSERT.
///
/// # Arguments
/// * `pool` - The PostgreSQL connection pool
/// * `item` - The item to insert (must implement Storable + Serialize)
///
/// # Returns
/// The number of rows affected (should be 1 on success)
pub async fn bind_insert_values<T: Storable + Serialize>(
    pool: &sqlx::PgPool,
    item: &T,
) -> Result<u64, StorageError> {
    bind_insert_with_table(pool, item, T::table_name()).await
}

/// Bind a Storable type's values to a PostgreSQL INSERT query with explicit table name.
///
/// Same as `bind_insert_values` but allows overriding the table name.
pub async fn bind_insert_with_table<T: Storable + Serialize>(
    pool: &sqlx::PgPool,
    item: &T,
    table: &str,
) -> Result<u64, StorageError> {
    let json = serde_json::to_value(item)
        .map_err(|e| StorageError::StorageError(format!("Serialization error: {}", e)))?;

    let obj = json.as_object().ok_or_else(|| {
        StorageError::StorageError("Expected JSON object for Storable type".to_string())
    })?;

    // Build arguments dynamically using json_keys() to find values in the JSON
    let mut args = sqlx::postgres::PgArguments::default();
    let column_types = T::column_types();

    for (idx, json_key) in T::json_keys().iter().enumerate() {
        let value = obj.get(*json_key).cloned().unwrap_or(Value::Null);
        let col_type = column_types.get(idx).copied().unwrap_or("text");
        bind_json_value(&mut args, &value, col_type)?;
    }

    let sql = build_insert_sql(table, T::columns());
    let result = sqlx::query_with(&sql, args)
        .execute(pool)
        .await
        .map_err(|e| StorageError::StorageError(e.to_string()))?;

    Ok(result.rows_affected())
}

/// Bind a Storable type's values to a PostgreSQL INSERT query within a transaction.
///
/// Same as `bind_insert_values` but works with a transaction.
pub async fn bind_insert_values_tx<'a, T: Storable + Serialize>(
    tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    item: &T,
) -> Result<u64, StorageError> {
    bind_insert_with_table_tx(tx, item, T::table_name()).await
}

/// Bind a Storable type's values to a PostgreSQL INSERT query within a transaction with explicit table name.
pub async fn bind_insert_with_table_tx<'a, T: Storable + Serialize>(
    tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
    item: &T,
    table: &str,
) -> Result<u64, StorageError> {
    let json = serde_json::to_value(item)
        .map_err(|e| StorageError::StorageError(format!("Serialization error: {}", e)))?;

    let obj = json.as_object().ok_or_else(|| {
        StorageError::StorageError("Expected JSON object for Storable type".to_string())
    })?;

    // Build arguments dynamically using json_keys() to find values in the JSON
    let mut args = sqlx::postgres::PgArguments::default();
    let column_types = T::column_types();

    for (idx, json_key) in T::json_keys().iter().enumerate() {
        let value = obj.get(*json_key).cloned().unwrap_or(Value::Null);
        let col_type = column_types.get(idx).copied().unwrap_or("text");
        bind_json_value(&mut args, &value, col_type)?;
    }

    let sql = build_insert_sql(table, T::columns());
    let result = sqlx::query_with(&sql, args)
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::StorageError(e.to_string()))?;

    Ok(result.rows_affected())
}

/// Deserialize a PostgreSQL row to a Storable type.
///
/// Extracts column values from the row using columns() and inserts them
/// into JSON using json_keys() to match serde's field naming.
/// Null values are omitted to match serde's skip_serializing_if behavior.
pub fn deserialize_row<T: Storable + DeserializeOwned>(row: &PgRow) -> Result<T, StorageError> {
    let mut obj = serde_json::Map::new();
    let columns = T::columns();
    let json_keys = T::json_keys();

    for (col_name, json_key) in columns.iter().zip(json_keys.iter()) {
        let value = extract_column_value(row, col_name)?;
        // Skip null values to match serde's skip_serializing_if behavior
        if !value.is_null() {
            obj.insert((*json_key).to_string(), value);
        }
    }

    serde_json::from_value(Value::Object(obj))
        .map_err(|e| StorageError::StorageError(format!("Deserialization error: {}", e)))
}

/// Bind a JSON value to PgArguments
fn bind_json_value(
    args: &mut sqlx::postgres::PgArguments,
    value: &Value,
    col_type: &str,
) -> Result<(), StorageError> {
    use sqlx::Arguments;

    match value {
        Value::Null => {
            // Use column type to bind the correct null type
            match col_type {
                "datetime" => args.add(None::<chrono::DateTime<chrono::Utc>>),
                "bigint" => args.add(None::<i64>),
                "integer" => args.add(None::<i32>),
                "boolean" => args.add(None::<bool>),
                "json" => args.add(None::<Value>),
                _ => args.add(None::<String>), // text and default
            }
            .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Bool(b) => {
            args.add(*b)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                args.add(i)
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            } else if let Some(u) = n.as_u64() {
                // PostgreSQL doesn't have unsigned, use i64
                args.add(u as i64)
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            } else if let Some(f) = n.as_f64() {
                args.add(f)
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            } else {
                // Fallback: store as string
                args.add(n.to_string())
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            }
        }
        Value::String(s) => {
            if col_type == "datetime" {
                // Parse and bind as timestamptz
                let dt = chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| StorageError::StorageError(format!("Invalid datetime: {}", e)))?;
                args.add(dt.with_timezone(&chrono::Utc))
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            } else {
                args.add(s.as_str())
                    .map_err(|e| StorageError::StorageError(e.to_string()))?;
            }
        }
        Value::Array(_) | Value::Object(_) => {
            // Store complex types as JSONB
            args.add(value.clone())
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
    }

    Ok(())
}

/// Extract a column value from a row as JSON
fn extract_column_value(row: &PgRow, col_name: &str) -> Result<Value, StorageError> {
    use sqlx::TypeInfo;

    // Find the column index
    let col_idx = row
        .columns()
        .iter()
        .position(|c| c.name() == col_name)
        .ok_or_else(|| StorageError::StorageError(format!("Column not found: {}", col_name)))?;

    let col = &row.columns()[col_idx];
    let type_name = col.type_info().name();

    // Handle based on PostgreSQL type
    let value = match type_name {
        "BOOL" => {
            let v: Option<bool> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            v.map(Value::Bool).unwrap_or(Value::Null)
        }
        "INT2" | "INT4" | "INT8" | "BIGINT" | "INTEGER" | "SMALLINT" => {
            let v: Option<i64> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            v.map(|n| Value::Number(n.into())).unwrap_or(Value::Null)
        }
        "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" => {
            let v: Option<f64> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            v.and_then(|n| serde_json::Number::from_f64(n).map(Value::Number))
                .unwrap_or(Value::Null)
        }
        "TIMESTAMPTZ" | "TIMESTAMP" => {
            let v: Option<chrono::DateTime<chrono::Utc>> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            // Use microsecond precision with Z to match StorageDatetime's serde format
            v.map(|dt| Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)))
                .unwrap_or(Value::Null)
        }
        "JSONB" | "JSON" => {
            let v: Option<Value> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            v.unwrap_or(Value::Null)
        }
        _ => {
            // Default: treat as string (VARCHAR, TEXT, CHAR, etc.)
            let v: Option<String> = row
                .try_get(col_idx)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
            v.map(Value::String).unwrap_or(Value::Null)
        }
    };

    Ok(value)
}
