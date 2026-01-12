//! Repository traits for content-addressable data following the SAID pattern.
//!
//! - `VersionedRepository<T>`: For versioned types with prefix-based lookup
//! - `UnversionedRepository<T>`: For simple types with SAID-only lookup
//! - `RepositoryConnection`: Database connection and initialization

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::{SelfAddressed, StorageError, Versioned};

/// Connection configuration for database backends.
///
/// This enum is extensible for future authentication methods.
#[derive(Debug, Clone)]
pub enum ConnectionConfig {
    /// Connect using a database URL string.
    Url(String),
    // Future: Credentials { host, port, user, pass, database }
    // Future: WithCert { url, cert_path, key_path }
}

impl From<&str> for ConnectionConfig {
    fn from(url: &str) -> Self {
        ConnectionConfig::Url(url.to_string())
    }
}

impl From<String> for ConnectionConfig {
    fn from(url: String) -> Self {
        ConnectionConfig::Url(url)
    }
}

impl From<&String> for ConnectionConfig {
    fn from(url: &String) -> Self {
        ConnectionConfig::Url(url.clone())
    }
}

/// Trait for database connection and initialization.
///
/// This trait abstracts the database connection lifecycle, allowing
/// different backends (PostgreSQL, SurrealDB, etc.) to implement
/// their own connection and migration logic.
#[async_trait]
pub trait RepositoryConnection: Sized + Send + Sync {
    /// Connect to the database using the provided configuration.
    async fn connect(config: impl Into<ConnectionConfig> + Send) -> Result<Self, StorageError>;

    /// Initialize the database schema (run migrations).
    async fn initialize(&self) -> Result<(), StorageError>;
}

/// Repository trait for types that are SelfAddressed + Versioned.
///
/// This trait provides standard CRUD operations following the SAID versioning pattern:
/// - `create`: Creates the first version (calls `derive_prefix()`, then inserts)
/// - `update`: Creates a new version (calls `increment()`, then inserts)
/// - `get_by_said`: Retrieves by content address (SAID)
/// - `get_latest`: Gets the most recent version for a prefix
/// - `get_history`: Gets all versions for a prefix, ordered by version
///
/// # Type Bounds
///
/// The generic type `T` must implement:
/// - `SelfAddressed`: For computing content-based identifiers
/// - `Versioned`: For prefix, versioning (previous, version, increment)
/// - `Serialize + DeserializeOwned`: For storage
/// - `Clone + Send + Sync`: For async operations
#[async_trait]
pub trait VersionedRepository<T>
where
    T: SelfAddressed + Versioned + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Create the first version of an item.
    ///
    /// This method should:
    /// 1. Call `derive_prefix()` on the item to set said, prefix, and version=0
    /// 2. Insert the item into storage
    /// 3. Return the item with its computed identifiers
    async fn create(&self, item: T) -> Result<T, StorageError>;

    /// Create a new version of an existing item.
    ///
    /// This method should:
    /// 1. Call `increment()` on the item to update said, previous, and version+1
    /// 2. Insert the new version into storage
    /// 3. Return the item with its updated identifiers
    async fn update(&self, item: T) -> Result<T, StorageError>;

    /// Insert an item with pre-computed identifiers.
    ///
    /// This method inserts the item as-is without calling `derive_prefix()` or `increment()`.
    /// Use this when the SAID and other identifiers have already been computed and verified.
    ///
    /// The caller is responsible for ensuring the SAID is valid.
    async fn insert(&self, item: T) -> Result<T, StorageError>;

    /// Get an item by its SAID (Self-Addressing Identifier).
    ///
    /// Returns `None` if no item with the given SAID exists.
    async fn get_by_said(&self, said: &str) -> Result<Option<T>, StorageError>;

    /// Get the latest version for a prefix.
    ///
    /// Returns `None` if no items exist for the given prefix.
    async fn get_latest(&self, prefix: &str) -> Result<Option<T>, StorageError>;

    /// Get full history for a prefix (ordered by version ascending).
    ///
    /// Returns an empty vector if no items exist for the given prefix.
    async fn get_history(&self, prefix: &str) -> Result<Vec<T>, StorageError>;

    /// Check if any items exist for a prefix.
    ///
    /// Returns `true` if at least one item exists for the given prefix.
    async fn exists(&self, prefix: &str) -> Result<bool, StorageError>;
}

/// Repository trait for simple SelfAddressed types without versioning.
///
/// This trait provides basic CRUD operations for types that only need:
/// - Content-addressable storage via SAID
/// - No versioning or prefix-based lookups
///
/// # Type Bounds
///
/// The generic type `T` must implement:
/// - `SelfAddressed`: For computing content-based identifiers
/// - `Serialize + DeserializeOwned`: For storage
/// - `Clone + Send + Sync`: For async operations
#[async_trait]
pub trait UnversionedRepository<T>
where
    T: SelfAddressed + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Create an item with a computed SAID.
    ///
    /// This method should:
    /// 1. Call `derive_said()` on the item to compute its SAID
    /// 2. Insert the item into storage
    /// 3. Return the item with its computed identifier
    async fn create(&self, item: T) -> Result<T, StorageError>;

    async fn insert(&self, item: T) -> Result<T, StorageError>;

    /// Get an item by its SAID (Self-Addressing Identifier).
    ///
    /// Returns `None` if no item with the given SAID exists.
    async fn get_by_said(&self, said: &str) -> Result<Option<T>, StorageError>;
}
