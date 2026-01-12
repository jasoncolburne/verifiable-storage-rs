//! SurrealDB implementation of QueryExecutor.
//!
//! Note: Transactions are not implemented - the methods exist but don't create actual transactions.
//! This is sufficient for ADNS which doesn't require transactional guarantees.

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::Client;
use verifiable_storage::{
    Delete, Filter, Join, Order, Query, QueryExecutor, Storable, StorageError, TransactionExecutor,
};

/// Helper struct for deserializing count() results from SurrealDB.
#[derive(Debug, Deserialize)]
struct CountResult {
    count: u64,
}

/// Wrapper around SurrealDB client to enable trait implementations.
///
/// This wrapper exists to satisfy Rust's orphan rules - we can't implement
/// `QueryExecutor` directly on `Surreal<Client>` since both are external types.
#[derive(Clone)]
pub struct SurrealPool(Surreal<Client>);

impl SurrealPool {
    /// Create a new SurrealPool wrapper.
    pub fn new(db: Surreal<Client>) -> Self {
        Self(db)
    }

    /// Get the inner Surreal client.
    pub fn inner(&self) -> &Surreal<Client> {
        &self.0
    }
}

impl Deref for SurrealPool {
    type Target = Surreal<Client>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Build a WHERE clause from filters for SurrealQL.
fn build_where_clause(filters: &[Filter]) -> String {
    if filters.is_empty() {
        return String::new();
    }

    let clauses: Vec<String> = filters
        .iter()
        .enumerate()
        .map(|(i, filter)| {
            let param = format!("$p{}", i);
            match filter {
                Filter::Eq(field, _) => format!("{} = {}", field, param),
                Filter::Ne(field, _) => format!("{} != {}", field, param),
                Filter::Gt(field, _) => format!("{} > {}", field, param),
                Filter::Gte(field, _) => format!("{} >= {}", field, param),
                Filter::Lt(field, _) => format!("{} < {}", field, param),
                Filter::Lte(field, _) => format!("{} <= {}", field, param),
                Filter::In(field, _) => format!("{} CONTAINS {}", param, field),
                Filter::IsNull(field) => format!("{} IS NULL", field),
                Filter::IsNotNull(field) => format!("{} IS NOT NULL", field),
            }
        })
        .collect();

    format!(" WHERE {}", clauses.join(" AND "))
}

/// Build ORDER BY clause for SurrealQL.
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

/// Build JOIN clauses for SurrealQL.
fn build_join_clause(main_table: &str, joins: &[Join]) -> String {
    if joins.is_empty() {
        return String::new();
    }

    joins
        .iter()
        .map(|join| {
            format!(
                " INNER JOIN {} ON {}.{} = {}.{}",
                join.table, main_table, join.left_field, join.table, join.right_field
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Helper to bind a Value to a SurrealDB query.
fn bind_value<'a, C: surrealdb::Connection>(
    q: surrealdb::method::Query<'a, C>,
    param: &str,
    value: &verifiable_storage::Value,
) -> surrealdb::method::Query<'a, C> {
    match value {
        verifiable_storage::Value::String(s) => q.bind((param.to_owned(), s.clone())),
        verifiable_storage::Value::Int(n) => q.bind((param.to_owned(), *n)),
        verifiable_storage::Value::UInt(n) => q.bind((param.to_owned(), *n)),
        verifiable_storage::Value::Float(n) => q.bind((param.to_owned(), *n)),
        verifiable_storage::Value::Bool(b) => q.bind((param.to_owned(), *b)),
        verifiable_storage::Value::Strings(v) => q.bind((param.to_owned(), v.clone())),
        verifiable_storage::Value::Null => q.bind((param.to_owned(), Option::<String>::None)),
    }
}

#[async_trait]
impl QueryExecutor for SurrealPool {
    type Transaction = SurrealTransaction;

    async fn fetch<T: Storable + DeserializeOwned + Send>(
        &self,
        query: Query<T>,
    ) -> Result<Vec<T>, StorageError> {
        let join_clause = build_join_clause(&query.table, &query.joins);
        let where_clause = build_where_clause(&query.filters);
        let order_clause = build_order_clause(&query.order_by);

        // Build GROUP BY clause if distinct_on is specified
        // SurrealDB's GROUP BY returns one row per unique combination
        let group_clause = if query.distinct_on.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", query.distinct_on.join(", "))
        };

        // Use table.* when joining to only return columns from the main table
        let select_cols = if query.joins.is_empty() {
            "*".to_string()
        } else {
            format!("{}.*", query.table)
        };

        let mut sql = format!(
            "SELECT {} FROM {}{}{}{}{}",
            select_cols, query.table, join_clause, where_clause, group_clause, order_clause
        );

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = query.offset {
            sql.push_str(&format!(" START {}", offset));
        }

        let mut q = self.0.query(&sql);

        // Bind filter values
        for (i, filter) in query.filters.iter().enumerate() {
            let param = format!("p{}", i);
            q = match filter {
                Filter::Eq(_, v)
                | Filter::Ne(_, v)
                | Filter::Gt(_, v)
                | Filter::Gte(_, v)
                | Filter::Lt(_, v)
                | Filter::Lte(_, v)
                | Filter::In(_, v) => bind_value(q, &param, v),
                Filter::IsNull(_) | Filter::IsNotNull(_) => q,
            };
        }

        let result: Vec<T> = q
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?
            .take(0)
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        Ok(result)
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
        let where_clause = build_where_clause(&query.filters);
        let sql = format!(
            "SELECT count() FROM {}{} GROUP ALL",
            query.table, where_clause
        );

        let mut q = self.0.query(&sql);

        for (i, filter) in query.filters.iter().enumerate() {
            let param = format!("p{}", i);
            q = match filter {
                Filter::Eq(_, v)
                | Filter::Ne(_, v)
                | Filter::Gt(_, v)
                | Filter::Gte(_, v)
                | Filter::Lt(_, v)
                | Filter::Lte(_, v)
                | Filter::In(_, v) => bind_value(q, &param, v),
                Filter::IsNull(_) | Filter::IsNotNull(_) => q,
            };
        }

        let result: Option<CountResult> = q
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?
            .take(0)
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        Ok(result.map(|r| r.count > 0).unwrap_or(false))
    }

    async fn delete<T: Storable + Send>(&self, delete: Delete<T>) -> Result<u64, StorageError> {
        let where_clause = build_where_clause(&delete.filters);
        let sql = format!("DELETE FROM {}{}", delete.table, where_clause);

        let mut q = self.0.query(&sql);

        // Bind filter values
        for (i, filter) in delete.filters.iter().enumerate() {
            let param = format!("p{}", i);
            q = match filter {
                Filter::Eq(_, v)
                | Filter::Ne(_, v)
                | Filter::Gt(_, v)
                | Filter::Gte(_, v)
                | Filter::Lt(_, v)
                | Filter::Lte(_, v)
                | Filter::In(_, v) => bind_value(q, &param, v),
                Filter::IsNull(_) | Filter::IsNotNull(_) => q,
            };
        }

        q.await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        // SurrealDB doesn't return affected row count easily, return 0
        Ok(0)
    }

    async fn insert<T: Storable + Serialize + Send + Sync>(
        &self,
        item: &T,
    ) -> Result<u64, StorageError> {
        let table = T::table_name();
        let value =
            serde_json::to_value(item).map_err(|e| StorageError::StorageError(e.to_string()))?;

        self.0
            .query(format!("INSERT INTO {} $item", table))
            .bind(("item", value))
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        Ok(1)
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StorageError> {
        // SurrealDB transactions are not fully implemented here
        // Return a no-op transaction wrapper
        Ok(SurrealTransaction {
            db: self.0.clone(),
            committed: false,
        })
    }
}

/// SurrealDB transaction wrapper.
///
/// Note: This doesn't actually create a transaction - operations are executed immediately.
/// This is a placeholder to satisfy the QueryExecutor trait.
pub struct SurrealTransaction {
    db: Surreal<Client>,
    committed: bool,
}

#[async_trait]
impl TransactionExecutor for SurrealTransaction {
    async fn insert<T: Storable + Serialize + Send + Sync>(
        &mut self,
        item: &T,
    ) -> Result<u64, StorageError> {
        // Execute immediately (no actual transaction)
        let table = T::table_name();
        let value =
            serde_json::to_value(item).map_err(|e| StorageError::StorageError(e.to_string()))?;

        self.db
            .query(format!("INSERT INTO {} $item", table))
            .bind(("item", value))
            .await
            .map_err(|e| StorageError::StorageError(e.to_string()))?;

        Ok(1)
    }

    async fn commit(mut self) -> Result<(), StorageError> {
        self.committed = true;
        Ok(())
    }

    async fn rollback(self) -> Result<(), StorageError> {
        if self.committed {
            return Err(StorageError::StorageError(
                "Cannot rollback committed transaction".to_string(),
            ));
        }
        // No-op since we don't have real transactions
        Ok(())
    }
}
