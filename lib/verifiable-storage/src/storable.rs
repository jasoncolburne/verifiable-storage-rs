//! Storable trait for database-agnostic storage operations.
//!
//! Types implementing `Storable` can be stored in any supported database backend.
//! Add `#[storable(table = "table_name")]` to a `#[derive(SelfAddressed)]` type
//! to generate the implementation.

/// Trait for types that can be stored in a database.
///
/// This trait provides the metadata and methods needed for database operations.
/// Generated automatically when `#[storable(table = "...")]` is present on a
/// `#[derive(SelfAddressed)]` type.
///
/// # Example
///
/// ```text
/// #[derive(SelfAddressed)]
/// #[storable(table = "adns_domains")]
/// #[serde(rename_all = "camelCase")]  // for JSON/SAID - DB uses snake_case
/// pub struct Domain {
///     #[said]
///     pub said: String,
///     #[prefix]
///     pub prefix: String,
///     #[previous]
///     pub previous: Option<String>,
///     #[version]
///     pub version: u64,
///     pub name: String,
///     // ...
/// }
/// ```
///
/// # Column Naming
///
/// Database columns use snake_case (Rust field names). JSON serialization
/// uses whatever serde is configured for (typically camelCase for SAID computation).
///
/// Use `#[column(skip)]` to exclude a field from database storage.
/// Use `#[column(name = "custom_name")]` to override the column name.
pub trait Storable: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync {
    /// The database table name for this type.
    fn table_name() -> &'static str;

    /// Column names in order (snake_case for DB).
    fn columns() -> &'static [&'static str];

    /// Column types in order (database-agnostic).
    /// Used by executors to bind null values with the correct type.
    /// Values: "text", "datetime", "bigint", "integer", "boolean", "json"
    fn column_types() -> &'static [&'static str];

    /// JSON key names in order (camelCase for serde).
    /// Corresponds 1:1 with columns().
    fn json_keys() -> &'static [&'static str];

    /// INSERT SQL with positional placeholders ($1, $2, ...).
    fn insert_sql() -> &'static str;

    /// SELECT * SQL for this table.
    fn select_all_sql() -> &'static str;

    /// SELECT by ID SQL.
    fn select_by_id_sql() -> &'static str;

    /// Number of columns.
    fn column_count() -> usize {
        Self::columns().len()
    }

    /// Get the primary key value (the SAID).
    fn id(&self) -> &str;

    /// Check if this type is versioned.
    fn is_versioned() -> bool;
}
