//! PostgreSQL implementation for verifiable-storage.
//!
//! This crate provides PostgreSQL-specific implementations for the verifiable-storage
//! system. It uses serde serialization for binding values, so types only need to
//! implement `Storable` (via `#[storable(table = "...")]` on `SelfAddressed` derive).
//!
//! # Usage
//!
//! Data types just need the storable attribute:
//!
//! ```text
//! use verifiable_storage::SelfAddressed;
//!
//! #[derive(SelfAddressed)]
//! #[storable(table = "my_table")]
//! #[serde(rename_all = "camelCase")]
//! pub struct MyType {
//!     #[said]
//!     pub said: String,
//!     pub name: String,
//! }
//! ```
//!
//! Repositories derive `Stored` for PostgreSQL repository implementations:
//!
//! ```text
//! use verifiable_storage_postgres::Stored;
//!
//! #[derive(Stored)]
//! #[stored(item_type = MyType, table = "my_table")]
//! pub struct MyRepository {
//!     pool: PgPool,
//! }
//! ```

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::unwrap_in_result)
)]

mod executor;
mod serde_bind;
mod time;

pub use executor::PgPool;
pub use serde_bind::{
    bind_insert_values, bind_insert_values_tx, bind_insert_with_table, bind_insert_with_table_tx,
    deserialize_row,
};
pub use time::PgStorageDatetime;

// Re-export the derive macro
pub use verifiable_storage_postgres_derive::Stored;

// Re-export sqlx migration types
pub use sqlx::migrate;
pub use sqlx::migrate::Migrator;

// Re-export core types for convenience
pub use verifiable_storage::{
    ConnectionConfig, Delete, Filter, Order, Query, QueryExecutor, RepositoryConnection,
    SelfAddressed, Storable, StorageDatetime, StorageError, TransactionExecutor,
    UnversionedRepository, Value, Versioned, VersionedRepository, compute_said,
};
