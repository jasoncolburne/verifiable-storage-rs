use cesr::Matter;
use serde::Serialize;

use crate::{StorageDatetime, StorageError};

const SAID_PLACEHOLDER: &str = "############################################";

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
    /// - no previous: verify_prefix() + verify_said() (inception — both must be valid)
    /// - has previous: verify_said() (said derived from content)
    fn verify(&self) -> Result<(), StorageError> {
        if self.get_previous().is_none() {
            self.verify_prefix()?;
        }
        self.verify_said()
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

/// Compute a SAID from a `serde_json::Value`.
///
/// Sets the `"said"` field to a placeholder, serializes, blake3 hashes, CESR encodes.
pub fn compute_said_from_value(value: &serde_json::Value) -> Result<String, StorageError> {
    let mut work = value.clone();
    let obj = work
        .as_object_mut()
        .ok_or_else(|| StorageError::StorageError("value must be an object".to_string()))?;

    obj.insert(
        "said".to_string(),
        serde_json::Value::String(SAID_PLACEHOLDER.to_string()),
    );

    let bytes = serde_json::to_vec(&work)?;
    let hash = blake3::hash(&bytes);
    let digest = cesr::Digest::from_raw(cesr::DigestCode::Blake3, hash.as_bytes().to_vec())?;
    Ok(digest.qb64())
}

impl SelfAddressed for serde_json::Value {
    fn derive_said(&mut self) -> Result<(), StorageError> {
        let said = compute_said_from_value(self)?;
        self.as_object_mut()
            .ok_or_else(|| StorageError::StorageError("value must be an object".to_string()))?
            .insert("said".to_string(), serde_json::Value::String(said));
        Ok(())
    }

    fn verify_said(&self) -> Result<(), StorageError> {
        let current = self
            .get("said")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StorageError::StorageError("missing said field".to_string()))?;
        let computed = compute_said_from_value(self)?;
        if current != computed {
            return Err(StorageError::StorageError(format!(
                "SAID mismatch: expected {}, got {}",
                computed, current
            )));
        }
        Ok(())
    }

    fn get_said(&self) -> String {
        self.get("said")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    }
}
