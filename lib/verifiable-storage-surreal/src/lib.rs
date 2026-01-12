//! SurrealDB implementation for verifiable-storage.
//!
//! This crate provides SurrealDB-specific implementations and types for
//! the verifiable-storage system.
//!
//! # Features
//!
//! - `SurrealStorageDatetime`: SurrealDB-compatible datetime wrapper
//! - `Stored` derive macro: Generates SurrealDB repository implementations
//!
//! # Example
//!
//! ```text
//! use verifiable_storage::{SelfAddressed, Versioned};
//! use verifiable_storage_surreal::Stored;
//!
//! #[derive(Stored)]
//! #[stored(item_type = MyType, table = "my_table", namespace = "my_ns")]
//! pub struct MyRepository {
//!     db: Surreal<Client>,
//! }
//! ```

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::unwrap_in_result)
)]

mod executor;
mod time;

pub use executor::{SurrealPool, SurrealTransaction};
pub use time::SurrealStorageDatetime;

// Re-export the derive macro
pub use verifiable_storage_surreal_derive::Stored;

// Re-export core types for convenience
pub use verifiable_storage::{
    ConnectionConfig, Delete, Filter, Order, Query, QueryExecutor, RepositoryConnection,
    SelfAddressed, Storable, StorageDatetime, StorageError, TransactionExecutor,
    UnversionedRepository, Value, Versioned, VersionedRepository, compute_said,
};
