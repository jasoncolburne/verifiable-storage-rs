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

/// A scalar subquery that resolves to a single value.
///
/// Generates SQL like: `(SELECT select_field FROM table WHERE ... LIMIT 1)`
#[derive(Debug, Clone)]
pub struct ScalarSubquery {
    /// The table to query.
    pub table: String,
    /// The field to select (single column).
    pub select_field: String,
    /// Filter conditions for the subquery.
    pub filters: Vec<Filter>,
}

impl ScalarSubquery {
    /// Create a new scalar subquery.
    pub fn new(
        table: impl Into<String>,
        select_field: impl Into<String>,
        filters: Vec<Filter>,
    ) -> Self {
        Self {
            table: table.into(),
            select_field: select_field.into(),
            filters,
        }
    }
}

/// A correlated subquery — one whose WHERE clause references columns from the outer query.
///
/// The "correlation" is the column-to-column join between the inner and outer tables.
/// For example, to find rows with no successor:
/// ```sql
/// NOT EXISTS (SELECT 1 FROM events _cs
///   WHERE _cs.previous = events.said    -- correlation: inner.previous = outer.said
///   AND _cs.prefix = $1)                -- filter: bound parameter
/// ```
/// Here `_cs.previous = events.said` is the correlation — it ties each inner-query
/// evaluation to a specific row of the outer query.
#[derive(Debug, Clone)]
pub struct CorrelatedSubquery {
    /// The table for the inner query.
    pub table: String,
    /// Alias for the inner table (to disambiguate when inner and outer are the same table).
    pub alias: String,
    /// The outer table name (for qualifying correlated column references).
    pub outer_table: String,
    /// Column correlations: each `(inner_col, outer_col)` generates
    /// `alias.inner_col = outer_table.outer_col`.
    pub correlations: Vec<(String, String)>,
    /// Additional filter conditions (field names should be alias-qualified).
    pub filters: Vec<Filter>,
}

impl CorrelatedSubquery {
    pub fn new(
        table: impl Into<String>,
        alias: impl Into<String>,
        outer_table: impl Into<String>,
        correlations: Vec<(String, String)>,
        filters: Vec<Filter>,
    ) -> Self {
        Self {
            table: table.into(),
            alias: alias.into(),
            outer_table: outer_table.into(),
            correlations,
            filters,
        }
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
    /// field >= (SELECT select_field FROM table WHERE ... LIMIT 1)
    GteScalarSubquery(String, ScalarSubquery),
    /// NOT EXISTS (SELECT 1 FROM table alias WHERE correlations AND filters)
    NotExists(CorrelatedSubquery),
}

/// Sort order.
#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

/// A term in an ORDER BY clause.
#[derive(Debug, Clone)]
pub enum OrderByTerm {
    /// A simple column name.
    Field(String),
    /// A CASE expression mapping field values to integer sort priorities.
    ///
    /// Generates SQL like: `CASE field WHEN 'val1' THEN 0 WHEN 'val2' THEN 1 ... ELSE 999 END`
    Case {
        field: String,
        mapping: Vec<(String, i64)>,
    },
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
    pub order_by: Vec<(OrderByTerm, Order)>,
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

    /// Add a not-equal filter (shorthand for Filter::Ne).
    pub fn ne(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Ne(field.into(), value.into()))
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

    /// Add a >= scalar subquery filter.
    ///
    /// Generates: `field >= (SELECT select_field FROM table WHERE ... LIMIT 1)`
    pub fn gte_scalar_subquery(self, field: impl Into<String>, subquery: ScalarSubquery) -> Self {
        self.filter(Filter::GteScalarSubquery(field.into(), subquery))
    }

    /// Add an order-by clause for a column name.
    pub fn order_by(mut self, field: impl Into<String>, order: Order) -> Self {
        self.order_by
            .push((OrderByTerm::Field(field.into()), order));
        self
    }

    /// Add an order-by clause using a CASE expression.
    ///
    /// Maps field values to integer sort priorities. Values not in the mapping
    /// sort last (ELSE 999).
    ///
    /// Example: `query.order_by_case("kind", &[("icp", 0), ("rot", 1)], Order::Asc)`
    /// generates: `CASE kind WHEN 'icp' THEN 0 WHEN 'rot' THEN 1 ELSE 999 END ASC`
    pub fn order_by_case(
        mut self,
        field: impl Into<String>,
        mapping: &[(&str, i64)],
        order: Order,
    ) -> Self {
        self.order_by.push((
            OrderByTerm::Case {
                field: field.into(),
                mapping: mapping.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            },
            order,
        ));
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

/// A query builder for fetching values from a single column.
///
/// Unlike `Query<T>` which returns deserialized objects, `ColumnQuery` returns
/// raw column values as strings. Useful for listing unique identifiers.
#[derive(Debug, Clone)]
pub struct ColumnQuery {
    /// The table to query.
    pub table: String,
    /// The column to select.
    pub column: String,
    /// Whether to return only distinct values.
    pub distinct: bool,
    /// Filter conditions.
    pub filters: Vec<Filter>,
    /// Sort order for the column.
    pub order: Option<Order>,
    /// Maximum number of results.
    pub limit: Option<u64>,
    /// GROUP BY columns.
    pub group_by: Vec<String>,
}

impl ColumnQuery {
    /// Create a new column query.
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            distinct: false,
            filters: Vec::new(),
            order: None,
            limit: None,
            group_by: Vec::new(),
        }
    }

    /// Return only distinct values.
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a filter condition.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add a greater-than filter on the column (for cursor-based pagination).
    pub fn gt(self, value: impl Into<Value>) -> Self {
        let column = self.column.clone();
        self.filter(Filter::Gt(column, value.into()))
    }

    /// Set sort order.
    pub fn order(mut self, order: Order) -> Self {
        self.order = Some(order);
        self
    }

    /// Set the maximum number of results.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Add a GROUP BY column.
    pub fn group_by(mut self, column: impl Into<String>) -> Self {
        self.group_by.push(column.into());
        self
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

    /// Add a less-than-or-equal filter.
    pub fn lte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.filter(Filter::Lte(field.into(), value.into()))
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

    /// Insert an item into a specific table.
    async fn insert_with_table<T: Storable + serde::Serialize + Send + Sync>(
        &self,
        item: &T,
        table: &str,
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

    /// Insert an item into a specific table within the transaction.
    async fn insert_with_table<T: Storable + serde::Serialize + Send + Sync>(
        &mut self,
        item: &T,
        table: &str,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_query_new() {
        let query = ColumnQuery::new("my_table", "my_column");
        assert_eq!(query.table, "my_table");
        assert_eq!(query.column, "my_column");
        assert!(!query.distinct);
        assert!(query.filters.is_empty());
        assert!(query.order.is_none());
        assert!(query.limit.is_none());
    }

    #[test]
    fn column_query_distinct() {
        let query = ColumnQuery::new("t", "c").distinct();
        assert!(query.distinct);
    }

    #[test]
    fn column_query_gt_filter() {
        let query = ColumnQuery::new("t", "prefix").gt("abc");
        assert_eq!(query.filters.len(), 1);
        assert!(matches!(
            &query.filters[0],
            Filter::Gt(field, Value::String(val)) if field == "prefix" && val == "abc"
        ));
    }

    #[test]
    fn column_query_order() {
        let query = ColumnQuery::new("t", "c").order(Order::Asc);
        assert!(matches!(query.order, Some(Order::Asc)));

        let query = ColumnQuery::new("t", "c").order(Order::Desc);
        assert!(matches!(query.order, Some(Order::Desc)));
    }

    #[test]
    fn column_query_limit() {
        let query = ColumnQuery::new("t", "c").limit(100);
        assert_eq!(query.limit, Some(100));
    }

    #[test]
    fn column_query_chained() {
        let query = ColumnQuery::new("events", "prefix")
            .distinct()
            .gt("Eabc")
            .order(Order::Asc)
            .limit(50);

        assert_eq!(query.table, "events");
        assert_eq!(query.column, "prefix");
        assert!(query.distinct);
        assert_eq!(query.filters.len(), 1);
        assert!(matches!(query.order, Some(Order::Asc)));
        assert_eq!(query.limit, Some(50));
    }

    #[test]
    fn column_query_filter() {
        let query = ColumnQuery::new("t", "c").filter(Filter::Eq(
            "status".to_string(),
            Value::String("active".to_string()),
        ));
        assert_eq!(query.filters.len(), 1);
        assert!(matches!(
            &query.filters[0],
            Filter::Eq(field, Value::String(val)) if field == "status" && val == "active"
        ));
    }

    #[test]
    fn scalar_subquery_new() {
        let subquery = ScalarSubquery::new(
            "events",
            "serial",
            vec![Filter::Eq(
                "said".to_string(),
                Value::String("abc".to_string()),
            )],
        );
        assert_eq!(subquery.table, "events");
        assert_eq!(subquery.select_field, "serial");
        assert_eq!(subquery.filters.len(), 1);
    }

    #[test]
    fn query_order_by_case() {
        use crate::Storable;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct TestItem {
            said: String,
        }

        impl Storable for TestItem {
            fn table_name() -> &'static str {
                "test_items"
            }
            fn columns() -> &'static [&'static str] {
                &["said"]
            }
            fn column_types() -> &'static [&'static str] {
                &["text"]
            }
            fn json_keys() -> &'static [&'static str] {
                &["said"]
            }
            fn insert_sql() -> &'static str {
                "INSERT INTO test_items (said) VALUES ($1)"
            }
            fn select_all_sql() -> &'static str {
                "SELECT * FROM test_items"
            }
            fn select_by_id_sql() -> &'static str {
                "SELECT * FROM test_items WHERE said = $1"
            }
            fn id(&self) -> &str {
                &self.said
            }
            fn is_chained() -> bool {
                false
            }
        }

        let query = Query::<TestItem>::new()
            .order_by("serial", Order::Asc)
            .order_by_case(
                "kind",
                &[("icp", 0), ("rot", 1), ("ixn", 2), ("cnt", 7)],
                Order::Asc,
            )
            .order_by("said", Order::Asc);

        assert_eq!(query.order_by.len(), 3);
        assert!(matches!(
            &query.order_by[0],
            (OrderByTerm::Field(f), Order::Asc) if f == "serial"
        ));
        assert!(matches!(
            &query.order_by[1],
            (OrderByTerm::Case { field, mapping }, Order::Asc)
                if field == "kind" && mapping.len() == 4
        ));
        assert!(matches!(
            &query.order_by[2],
            (OrderByTerm::Field(f), Order::Asc) if f == "said"
        ));
    }

    #[test]
    fn query_gte_scalar_subquery() {
        use crate::Storable;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct TestItem {
            said: String,
        }

        impl Storable for TestItem {
            fn table_name() -> &'static str {
                "test_items"
            }
            fn columns() -> &'static [&'static str] {
                &["said"]
            }
            fn column_types() -> &'static [&'static str] {
                &["text"]
            }
            fn json_keys() -> &'static [&'static str] {
                &["said"]
            }
            fn insert_sql() -> &'static str {
                "INSERT INTO test_items (said) VALUES ($1)"
            }
            fn select_all_sql() -> &'static str {
                "SELECT * FROM test_items"
            }
            fn select_by_id_sql() -> &'static str {
                "SELECT * FROM test_items WHERE said = $1"
            }
            fn id(&self) -> &str {
                &self.said
            }
            fn is_chained() -> bool {
                false
            }
        }

        let subquery = ScalarSubquery::new(
            "events",
            "serial",
            vec![Filter::Eq(
                "said".to_string(),
                Value::String("abc".to_string()),
            )],
        );
        let query = Query::<TestItem>::new().gte_scalar_subquery("serial", subquery);
        assert_eq!(query.filters.len(), 1);
        assert!(matches!(
            &query.filters[0],
            Filter::GteScalarSubquery(field, sq) if field == "serial" && sq.table == "events"
        ));
    }
}
