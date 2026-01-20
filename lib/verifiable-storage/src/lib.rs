//! Verifiable Storage - Core traits for content-addressable storage using SAIDs.
//!
//! This crate provides the foundation for building verifiable, content-addressable
//! storage systems using Self-Addressing IDentifiers (SAIDs).
//!
//! # Core Concepts
//!
//! - **SAID** (Self-Addressing IDentifier): A content hash that serves as both
//!   the identifier and integrity check for data.
//! - **Versioned**: Data with a stable prefix (lineage identifier), version, and
//!   cryptographic linking between versions via previous pointers.
//!
//! # Traits
//!
//! - [`SelfAddressed`]: Types with a content-derived SAID
//! - [`Versioned`]: Versioned types with prefix, version, and previous pointer
//! - [`VersionedRepository`]: Storage for versioned types
//! - [`UnversionedRepository`]: Storage for simple SAID-addressed types

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::unwrap_in_result)
)]

mod error;
mod query;
mod repository;
mod said;
mod storable;
mod time;

pub use error::StorageError;
pub use query::{
    ColumnQuery, Delete, Filter, Join, Order, Query, QueryExecutor, TransactionExecutor, Value,
};
pub use repository::{
    ConnectionConfig, RepositoryConnection, UnversionedRepository, VersionedRepository,
};
pub use said::{SelfAddressed, Versioned, compute_said};
pub use storable::Storable;
pub use time::StorageDatetime;

// Re-export derive macro
// Note: SelfAddressed derive auto-detects versioning by presence of #[prefix], #[previous], #[version] fields
pub use verifiable_storage_derive::SelfAddressed;
