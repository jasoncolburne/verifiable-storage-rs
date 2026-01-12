use cesr::Matter;
use serde::Serialize;

use crate::{StorageDatetime, StorageError};

/// Trait for types that have a Self-Addressing IDentifier (SAID).
///
/// The SAID is computed from the content hash of the serialized data,
/// providing content-addressable storage.
pub trait SelfAddressed: Sized {
    fn derive_said(&mut self) -> Result<(), StorageError>;
    fn verify_said(&self) -> Result<(), StorageError>;
    fn get_said(&self) -> String;
}

/// Trait for versioned types with prefix, version, and previous pointer.
///
/// The prefix is derived from the first SAID (at version 0) and provides a stable
/// lineage identifier. Subsequent versions increment the version and link via
/// the previous pointer, forming a cryptographically-linked chain.
///
/// Requires fields:
/// - `#[said]` - content hash (changes each version)
/// - `#[prefix]` - lineage identifier (set once from first SAID)
/// - `#[version]` - version number (0, 1, 2, ...)
/// - `#[previous]` - SAID of previous version (None for version 0)
/// - `#[created_at]` (optional) - timestamp, updated on increment
pub trait Versioned: SelfAddressed + Clone {
    fn derive_prefix(&mut self) -> Result<(), StorageError>;
    fn verify_prefix(&self) -> Result<(), StorageError>;
    fn get_prefix(&self) -> String;

    fn increment(&mut self) -> Result<(), StorageError>;

    /// Check if proposed update has no actual changes (only version/previous/created_at differ).
    /// Returns true if the proposed SAID matches what would be computed from self with
    /// only version metadata updated.
    fn verify_unchanged(&self, proposed: &Self) -> Result<bool, StorageError>;

    fn get_previous(&self) -> Option<String>;
    fn get_version(&self) -> u64;
    fn set_created_at(&mut self, created_at: StorageDatetime);
    fn get_created_at(&self) -> Option<StorageDatetime>;

    /// Verify the item based on its version:
    /// - version 0: verify_prefix() (said == prefix)
    /// - version > 0: verify_said() (said derived from content)
    fn verify(&self) -> Result<(), StorageError> {
        if self.get_version() == 0 {
            self.verify_prefix()
        } else {
            self.verify_said()
        }
    }
}

/// Compute a SAID (Self-Addressing IDentifier) from serializable data.
///
/// Uses Blake3-256 hash encoded as CESR.
pub fn compute_said<T: Serialize>(data: &T) -> Result<String, StorageError> {
    let bytes = serde_json::to_vec(data)?;

    let hash = blake3::hash(&bytes);
    let digest = cesr::Digest::from_raw(cesr::DigestCode::Blake3, hash.as_bytes().to_vec())?;

    Ok(digest.qb64())
}
