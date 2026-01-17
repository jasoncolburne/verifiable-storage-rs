//! Database-agnostic query builder for verifiable storage.
//!
//! This module provides a query abstraction that can be translated to
//! different database backends (PostgreSQL, SurrealDB, etc.).

use crate::{Storable, StorageDatetime, StorageError};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// A value that can be bound to a query parameter.
#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Strings(Vec<String>),
    Datetime(StorageDatetime),
    Null,
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&String> for Value {
    fn from(s: &String) -> Self {
        Value::String(s.clone())
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::UInt(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<String>> for Value {
    fn from(v: Vec<String>) -> Self {
        Value::Strings(v)
    }
}

impl From<&[String]> for Value {
    fn from(v: &[String]) -> Self {
        Value::Strings(v.to_vec())
    }
}

impl<'a> From<Vec<&'a str>> for Value {
    fn from(v: Vec<&'a str>) -> Self {
        Value::Strings(v.into_iter().map(|s| s.to_string()).collect())
    }
}

impl<'a> From<&[&'a str]> for Value {
    fn from(v: &[&'a str]) -> Self {
        Value::Strings(v.iter().map(|s| s.to_string()).collect())
    }
}

impl From<StorageDatetime> for Value {
    fn from(dt: StorageDatetime) -> Self {
        Value::Datetime(dt)
    }
}

impl From<&StorageDatetime> for Value {
    fn from(dt: &StorageDatetime) -> Self {
        Value::Datetime(dt.clone())
    }
}

/// Filter conditions for queries.
#[derive(Debug, Clone)]
pub enum Filter {
    /// field = value
    Eq(String, Value),
    /// field != value
    Ne(String, Value),
    /// field > value
    Gt(String, Value),
    /// field >= value
    Gte(String, Value),
    /// field < value
    Lt(String, Value),
    /// field <= value
    Lte(String, Value),
    /// field IN (values) - for arrays
    In(String, Value),
    /// field IS NULL
    IsNull(String),
    /// field IS NOT NULL
    IsNotNull(String),
}

/// Sort order.
#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct Join {
    /// The table to join.
    pub table: String,
    /// The field on the left table (main table).
    pub left_field: String,
    /// The field on the right table (joined table).
    pub right_field: String,
}

/// A SELECT query builder.
#[derive(Debug, Clone)]
pub struct Query<T> {
    /// The table to query.
    pub table: String,
    /// JOIN clauses.
    pub joins: Vec<Join>,
    /// Filter conditions.
    pub filters: Vec<Filter>,
    /// Order by clauses.
    pub order_by: Vec<(String, Order)>,
    /// Maximum number of results.
    pub limit: Option<u64>,
    /// Offset for pagination.
    pub offset: Option<u64>,
    /// DISTINCT ON fields (PostgreSQL) / GROUP BY fields (SurrealDB).
    /// Returns one row per unique combination of these fields.
    pub distinct_on: Vec<String>,
    pub(crate) _marker: PhantomData<T>,
}

impl<T: Storable> Query<T> {
    /// Create a new query for the type's table.
    pub fn new() -> Self {
        Self {
            table: T::table_name().to_string(),
            joins: Vec::new(),
            filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct_on: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Create a new query with an explicit table name.
    pub fn for_table(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            joins: Vec::new(),
            filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct_on: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Add a JOIN clause.
    ///
    /// Joins `join_table` where `left_field` (on main table) equals `right_field` (on join table).
    pub fn join(
        mut self,
        join_table: impl Into<String>,
        left_field: impl Into<String>,
        right_field: impl Into<String>,
    ) -> Self {
        self.joins.push(Join {
            table: join_table.into(),
            left_field: left_field.into(),
            right_field: right_field.into(),
        });
        self
    }

    /// Add a filter condition.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add an equality filter (shorthand for Filter::Eq).
    pub fn eq(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Eq(field.into(), value.into()))
    }

    /// Add an IN filter (shorthand for Filter::In).
    pub fn r#in(self, field: impl Into<String>, values: impl Into<Value>) -> Self {
        self.filter(Filter::In(field.into(), values.into()))
    }

    /// Add a greater-than filter.
    pub fn gt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Gt(field.into(), value.into()))
    }

    /// Add a greater-than-or-equal filter.
    pub fn gte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Gte(field.into(), value.into()))
    }

    /// Add a less-than filter.
    pub fn lt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Lt(field.into(), value.into()))
    }

    /// Add a less-than-or-equal filter.
    pub fn lte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Lte(field.into(), value.into()))
    }

    /// Add an order-by clause.
    pub fn order_by(mut self, field: impl Into<String>, order: Order) -> Self {
        self.order_by.push((field.into(), order));
        self
    }

    /// Set the maximum number of results.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset for pagination.
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Add a DISTINCT ON field (PostgreSQL) / GROUP BY field (SurrealDB).
    ///
    /// Returns one row per unique combination of distinct_on fields.
    /// ORDER BY should start with these fields for deterministic results.
    pub fn distinct_on(mut self, field: impl Into<String>) -> Self {
        self.distinct_on.push(field.into());
        self
    }
}

impl<T: Storable> Default for Query<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A DELETE query builder.
#[derive(Debug, Clone)]
pub struct Delete<T> {
    /// The table to delete from.
    pub table: String,
    /// Filter conditions.
    pub filters: Vec<Filter>,
    pub(crate) _marker: PhantomData<T>,
}

impl<T: Storable> Delete<T> {
    /// Create a new delete query for the type's table.
    pub fn new() -> Self {
        Self {
            table: T::table_name().to_string(),
            filters: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Create a new delete query with an explicit table name.
    pub fn for_table(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filters: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Add a filter condition.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add an equality filter (shorthand).
    pub fn eq(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Eq(field.into(), value.into()))
    }

    /// Add a greater-than-or-equal filter.
    pub fn gte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Gte(field.into(), value.into()))
    }

    /// Add an IN filter.
    pub fn r#in(self, field: impl Into<String>, values: impl Into<Value>) -> Self {
        self.filter(Filter::In(field.into(), values.into()))
    }
}

impl<T: Storable> Default for Delete<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for executing queries against a database backend.
///
/// Implemented by database-specific pool types (e.g., PgPool, Surreal<Client>).
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// The transaction type for this executor.
    type Transaction: TransactionExecutor;

    /// Execute a SELECT query and return results.
    async fn fetch<T: Storable + DeserializeOwned + Send>(
        &self,
        query: Query<T>,
    ) -> Result<Vec<T>, StorageError>;

    /// Execute a SELECT query and return at most one result.
    async fn fetch_optional<T: Storable + DeserializeOwned + Send>(
        &self,
        query: Query<T>,
    ) -> Result<Option<T>, StorageError>;

    /// Check if any rows match the query (SELECT EXISTS).
    async fn exists<T: Storable + Send>(&self, query: Query<T>) -> Result<bool, StorageError>;

    /// Execute a DELETE query and return the number of rows affected.
    async fn delete<T: Storable + Send>(&self, delete: Delete<T>) -> Result<u64, StorageError>;

    /// Insert an item into the database.
    async fn insert<T: Storable + serde::Serialize + Send + Sync>(
        &self,
        item: &T,
    ) -> Result<u64, StorageError>;

    /// Begin a transaction. The returned executor can be used for queries within the transaction.
    async fn begin_transaction(&self) -> Result<Self::Transaction, StorageError>;
}

/// Trait for executing queries within a transaction.
#[async_trait]
pub trait TransactionExecutor: Send + Sync {
    /// Execute a SELECT query within the transaction.
    async fn fetch<T: Storable + DeserializeOwned + Send>(
        &mut self,
        query: Query<T>,
    ) -> Result<Vec<T>, StorageError>;

    /// Execute a DELETE query within the transaction.
    async fn delete<T: Storable + Send>(&mut self, delete: Delete<T>) -> Result<u64, StorageError>;

    /// Insert an item within the transaction.
    async fn insert<T: Storable + serde::Serialize + Send + Sync>(
        &mut self,
        item: &T,
    ) -> Result<u64, StorageError>;

    /// Acquire an advisory lock scoped to this transaction.
    /// The lock is automatically released on commit/rollback.
    /// Used to serialize operations on a logical key (e.g., a prefix).
    async fn acquire_advisory_lock(&mut self, key: &str) -> Result<(), StorageError>;

    /// Commit the transaction.
    async fn commit(self) -> Result<(), StorageError>;

    /// Rollback the transaction.
    async fn rollback(self) -> Result<(), StorageError>;
}
