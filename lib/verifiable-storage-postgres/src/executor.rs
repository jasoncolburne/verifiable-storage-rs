//! PostgreSQL implementation of QueryExecutor.

const DEFAULT_MAX_CONNECTIONS: u32 = 16;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::postgres::{PgArguments, PgPoolOptions};
use sqlx::{Arguments, Postgres, Transaction};
use std::ops::Deref;
use verifiable_storage::{
    Delete, Filter, Join, Order, Query, QueryExecutor, Storable, StorageError, TransactionExecutor,
    Value,
};

use crate::{bind_insert_values, bind_insert_values_tx, deserialize_row};

/// Wrapper around sqlx::PgPool that implements QueryExecutor.
#[derive(Clone, Debug)]
pub struct PgPool(sqlx::PgPool);

impl PgPool {
    /// Create a new PgPool from an sqlx PgPool.
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self(pool)
    }

    /// Connect to a PostgreSQL database.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect(url)
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;
        Ok(Self(pool))
    }

    /// Get the inner sqlx::PgPool.
    pub fn inner(&self) -> &sqlx::PgPool {
        &self.0
    }
}

impl Deref for PgPool {
    type Target = sqlx::PgPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Build a WHERE clause from filters and return the SQL and argument count.
fn build_where_clause(filters: &[Filter], start_param: usize) -> (String, usize) {
    if filters.is_empty() {
        return (String::new(), 0);
    }

    let mut clauses = Vec::new();
    let mut param_idx = start_param;

    for filter in filters {
        let clause = match filter {
            Filter::Eq(field, _) => {
                let c = format!("{} = ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::Ne(field, _) => {
                let c = format!("{} != ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::Gt(field, _) => {
                let c = format!("{} > ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::Gte(field, _) => {
                let c = format!("{} >= ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::Lt(field, _) => {
                let c = format!("{} < ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::Lte(field, _) => {
                let c = format!("{} <= ${}", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::In(field, _) => {
                let c = format!("{} = ANY(${})", field, param_idx);
                param_idx += 1;
                c
            }
            Filter::IsNull(field) => format!("{} IS NULL", field),
            Filter::IsNotNull(field) => format!("{} IS NOT NULL", field),
        };
        clauses.push(clause);
    }

    let param_count = param_idx - start_param;
    (format!(" WHERE {}", clauses.join(" AND ")), param_count)
}

/// Bind filter values to PgArguments.
fn bind_filters(args: &mut PgArguments, filters: &[Filter]) -> Result<(), StorageError> {
    for filter in filters {
        match filter {
            Filter::Eq(_, value)
            | Filter::Ne(_, value)
            | Filter::Gt(_, value)
            | Filter::Gte(_, value)
            | Filter::Lt(_, value)
            | Filter::Lte(_, value)
            | Filter::In(_, value) => {
                bind_value(args, value)?;
            }
            Filter::IsNull(_) | Filter::IsNotNull(_) => {
                // No binding needed
            }
        }
    }
    Ok(())
}

/// Bind a Value to PgArguments.
fn bind_value(args: &mut PgArguments, value: &Value) -> Result<(), StorageError> {
    match value {
        Value::String(s) => {
            args.add(s.as_str())
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Int(n) => {
            args.add(*n)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::UInt(n) => {
            args.add(*n as i64)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Float(n) => {
            args.add(*n)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Bool(b) => {
            args.add(*b)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Strings(v) => {
            args.add(v.as_slice())
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
        Value::Null => {
            args.add(None::<String>)
                .map_err(|e| StorageError::StorageError(e.to_string()))?;
        }
    }
    Ok(())
}

/// Build ORDER BY clause.
fn build_order_clause(order_by: &[(String, Order)]) -> String {
    if order_by.is_empty() {
        return String::new();
    }

    let clauses: Vec<String> = order_by
        .iter()
        .map(|(field, order)| {
            let dir = match order {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            };
            format!("{} {}", field, dir)
        })
        .collect();

    format!(" ORDER BY {}", clauses.join(", "))
}

/// Build JOIN clauses.
fn build_join_clause(main_table: &str, joins: &[Join]) -> String {
    if joins.is_empty() {
        return String::new();
    }

    joins
        .iter()
        .map(|join| {
            format!(
                " JOIN {} ON {}.{} = {}.{}",
                join.table, main_table, join.left_field, join.table, join.right_field
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

#[async_trait]
impl QueryExecutor for PgPool {
    type Transaction = PgTransaction;

    async fn fetch<T: Storable + DeserializeOwned + Send>(
        &self,
        query: Query<T>,
    ) -> Result<Vec<T>, StorageError> {
        let join_clause = build_join_clause(&query.table, &query.joins);
        let (where_clause, _) = build_where_clause(&query.filters, 1);
        let order_clause = build_order_clause(&query.order_by);

        // Build DISTINCT ON clause if specified
        let distinct_clause = if query.distinct_on.is_empty() {
            String::new()
        } else {
            format!("DISTINCT ON ({}) ", query.distinct_on.join(", "))
        };

        // Use table.* when joining to only return columns from the main table
        let select_cols = if query.joins.is_empty() {
            "*".to_string()
        } else {
            format!("{}.*", query.table)
        };

        let mut sql = format!(
            "SELECT {}{} FROM {}{}{}{}",
            distinct_clause, select_cols, query.table, join_clause, where_clause, order_clause
        );

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let mut args = PgArguments::default();
        bind_filters(&mut args, &query.filters)?;

        let rows = sqlx::query_with(&sql, args)
            .fetch_all(&self.0)
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        rows.iter().map(|row| deserialize_row::<T>(row)).collect()
    }

    async fn fetch_optional<T: Storable + DeserializeOwned + Send>(
        &self,
        query: Query<T>,
    ) -> Result<Option<T>, StorageError> {
        let mut q = query;
        q.limit = Some(1);

        let results = self.fetch(q).await?;
        Ok(results.into_iter().next())
    }

    async fn exists<T: Storable + Send>(&self, query: Query<T>) -> Result<bool, StorageError> {
        let (where_clause, _) = build_where_clause(&query.filters, 1);
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {}{})",
            query.table, where_clause
        );

        let mut args = PgArguments::default();
        bind_filters(&mut args, &query.filters)?;

        let row = sqlx::query_with(&sql, args)
            .fetch_one(&self.0)
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        use sqlx::Row;
        Ok(row.get::<bool, _>(0))
    }

    async fn delete<T: Storable + Send>(&self, delete: Delete<T>) -> Result<u64, StorageError> {
        let (where_clause, _) = build_where_clause(&delete.filters, 1);
        let sql = format!("DELETE FROM {}{}", delete.table, where_clause);

        let mut args = PgArguments::default();
        bind_filters(&mut args, &delete.filters)?;

        let result = sqlx::query_with(&sql, args)
            .execute(&self.0)
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn insert<T: Storable + Serialize + Send + Sync>(
        &self,
        item: &T,
    ) -> Result<u64, StorageError> {
        bind_insert_values(&self.0, item).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StorageError> {
        let tx = self
            .0
            .begin()
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;
        Ok(PgTransaction { tx })
    }
}

/// PostgreSQL transaction wrapper implementing TransactionExecutor.
pub struct PgTransaction {
    tx: Transaction<'static, Postgres>,
}

#[async_trait]
impl TransactionExecutor for PgTransaction {
    async fn insert<T: Storable + Serialize + Send + Sync>(
        &mut self,
        item: &T,
    ) -> Result<u64, StorageError> {
        bind_insert_values_tx(&mut self.tx, item).await
    }

    async fn commit(self) -> Result<(), StorageError> {
        self.tx
            .commit()
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.tx
            .rollback()
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))
    }
}
