//! Repository traits for content-addressable data following the SAID pattern.
//!
//! - `ChainedRepository<T>`: For versioned types with prefix-based lookup
//! - `UnversionedRepository<T>`: For simple types with SAID-only lookup
//! - `RepositoryConnection`: Database connection and initialization

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::{Chained, SelfAddressed, StorageError, TransactionExecutor};

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

/// Repository trait for types that are SelfAddressed + Chained.
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
/// - `Chained`: For prefix, versioning (previous, version, increment)
/// - `Serialize + DeserializeOwned`: For storage
/// - `Clone + Send + Sync`: For async operations
#[async_trait]
pub trait ChainedRepository<T>
where
    T: SelfAddressed + Chained + Serialize + DeserializeOwned + Clone + Send + Sync,
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

    /// Insert an item with pre-computed identifiers (auto-commits).
    ///
    /// This method inserts the item as-is without calling `derive_prefix()` or `increment()`.
    /// Use this when the SAID and other identifiers have already been computed and verified.
    ///
    /// The caller is responsible for ensuring the SAID is valid.
    async fn insert(&self, item: T) -> Result<T, StorageError>;

    /// Insert an item within an existing transaction (does not commit).
    ///
    /// Same as `insert()` but uses the provided transaction. The caller is
    /// responsible for committing or rolling back. The table name used is
    /// the same as the repository's configured table.
    async fn insert_in<Tx: TransactionExecutor>(
        &self,
        tx: &mut Tx,
        item: T,
    ) -> Result<T, StorageError>;

    /// Get an item by its SAID (Self-Addressing Identifier).
    ///
    /// Returns `None` if no item with the given SAID exists.
    async fn get_by_said(&self, said: &cesr::Digest) -> Result<Option<T>, StorageError>;

    /// Get the latest version for a prefix.
    ///
    /// Returns `None` if no items exist for the given prefix.
    async fn get_latest(&self, prefix: &cesr::Digest) -> Result<Option<T>, StorageError>;

    /// Get full history for a prefix (ordered by version ascending).
    ///
    /// Returns an empty vector if no items exist for the given prefix.
    async fn get_history(&self, prefix: &cesr::Digest) -> Result<Vec<T>, StorageError>;

    /// Check if any items exist for a prefix.
    ///
    /// Returns `true` if at least one item exists for the given prefix.
    async fn exists(&self, prefix: &cesr::Digest) -> Result<bool, StorageError>;

    /// Get history for a prefix starting from a given version serial (inclusive).
    ///
    /// Returns items with version >= `since_serial`, ordered by version ascending.
    /// Default implementation returns an error; repositories with a version field
    /// will override this with a generated implementation.
    async fn get_history_since(
        &self,
        prefix: &cesr::Digest,
        since_serial: u64,
    ) -> Result<Vec<T>, StorageError> {
        let _ = (prefix, since_serial);
        Err(StorageError::StorageError(
            "get_history_since not implemented for this repository".to_string(),
        ))
    }
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
pub trait UnchainedRepository<T>
where
    T: SelfAddressed + Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Create an item with a computed SAID (auto-commits).
    ///
    /// This method should:
    /// 1. Call `derive_said()` on the item to compute its SAID
    /// 2. Insert the item into storage
    /// 3. Return the item with its computed identifier
    async fn create(&self, item: T) -> Result<T, StorageError>;

    /// Insert an item with pre-computed SAID (auto-commits).
    async fn insert(&self, item: T) -> Result<T, StorageError>;

    /// Insert an item within an existing transaction (does not commit).
    ///
    /// Same as `insert()` but uses the provided transaction. The caller is
    /// responsible for committing or rolling back. The table name used is
    /// the same as the repository's configured table.
    async fn insert_in<Tx: TransactionExecutor>(
        &self,
        tx: &mut Tx,
        item: T,
    ) -> Result<T, StorageError>;

    /// Get an item by its SAID (Self-Addressing Identifier).
    ///
    /// Returns `None` if no item with the given SAID exists.
    async fn get_by_said(&self, said: &cesr::Digest) -> Result<Option<T>, StorageError>;

    /// Check if an item with the given SAID exists.
    async fn exists(&self, said: &cesr::Digest) -> Result<bool, StorageError>;
}
