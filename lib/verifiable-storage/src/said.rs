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

/// Trait for chained types with prefix and previous pointer.
///
/// The prefix is derived from the first SAID (inception) and provides a stable
/// lineage identifier. Subsequent items link via the previous pointer, forming
/// a cryptographically-linked chain.
///
/// Requires fields:
/// - `#[said]` - content hash (changes each item)
/// - `#[prefix]` - lineage identifier (set once from first SAID)
/// - `#[previous]` - SAID of previous item (None for inception)
/// - `#[created_at]` (optional) - timestamp, updated on increment
pub trait Chained: SelfAddressed + Clone {
    fn derive_prefix(&mut self) -> Result<(), StorageError>;
    fn verify_prefix(&self) -> Result<(), StorageError>;
    fn get_prefix(&self) -> String;

    fn increment(&mut self) -> Result<(), StorageError>;

    /// Check if proposed update has no actual changes (only previous/created_at differ).
    /// Returns true if the proposed SAID matches what would be computed from self with
    /// only chain metadata updated.
    fn verify_unchanged(&self, proposed: &Self) -> Result<bool, StorageError>;

    fn get_previous(&self) -> Option<String>;
    fn set_created_at(&mut self, created_at: StorageDatetime);
    fn get_created_at(&self) -> Option<StorageDatetime>;

    /// Verify the item based on whether it has a previous pointer:
    /// - no previous: verify_prefix() (said == prefix, inception)
    /// - has previous: verify_said() (said derived from content)
    fn verify(&self) -> Result<(), StorageError> {
        if self.get_previous().is_none() {
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
