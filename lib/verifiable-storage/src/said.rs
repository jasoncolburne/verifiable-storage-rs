use cesr::{Digest256, Digest256Code, Matter};
use serde::Serialize;

use crate::StorageError;

const SAID_PLACEHOLDER: &str = "############################################";

/// Trait for types that have a Self-Addressing IDentifier (SAID).
///
/// The SAID is computed from the content hash of the serialized data,
/// providing content-addressable storage.
pub trait SelfAddressed: Sized {
    fn derive_said(&mut self) -> Result<(), StorageError>;
    fn verify_said(&self) -> Result<(), StorageError>;
    fn get_said(&self) -> Digest256;
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
    fn get_prefix(&self) -> Digest256;

    fn increment(&mut self) -> Result<(), StorageError>;

    /// Check if proposed update has no actual changes (only previous/created_at differ).
    /// Returns true if the proposed SAID matches what would be computed from self with
    /// only chain metadata updated.
    fn verify_unchanged(&self, proposed: &Self) -> Result<bool, StorageError>;

    fn get_previous(&self) -> Option<Digest256>;
    fn set_created_at(&mut self, created_at: crate::StorageDatetime);
    fn get_created_at(&self) -> Option<crate::StorageDatetime>;

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

/// Compute a SAID by blanking specified fields with placeholders, then hashing.
///
/// This is the core function for SAID/prefix computation. It:
/// 1. Serializes `data` to a JSON Value
/// 2. Replaces each named field with a placeholder string
/// 3. Serializes to bytes and computes Blake3-256
///
/// The placeholder is never assigned to the struct — it only exists in the
/// temporary JSON representation, allowing typed (non-String) SAID fields.
pub fn compute_said_for_fields<T: Serialize>(
    data: &T,
    fields: &[&str],
) -> Result<Digest256, StorageError> {
    let mut value = serde_json::to_value(data)?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| StorageError::StorageError("value must be an object".to_string()))?;

    for field in fields {
        obj.insert(
            (*field).to_string(),
            serde_json::Value::String(SAID_PLACEHOLDER.to_string()),
        );
    }

    let bytes = serde_json::to_vec(&value)?;
    let hash = blake3::hash(&bytes);
    Digest256::from_raw(Digest256Code::Blake3, hash.as_bytes().to_vec())
        .map_err(|e| StorageError::StorageError(e.to_string()))
}

/// Compute a SAID (Self-Addressing IDentifier) from serializable data.
///
/// Blanks the `"said"` field with a placeholder, serializes, and hashes.
/// Thin wrapper around `compute_said_for_fields`.
pub fn compute_said<T: Serialize>(data: &T) -> Result<Digest256, StorageError> {
    compute_said_for_fields(data, &["said"])
}

/// Compute a SAID from a `serde_json::Value`.
///
/// Blanks the `"said"` field with a placeholder, serializes, and hashes.
/// Thin wrapper around `compute_said_for_fields`.
pub fn compute_said_from_value(value: &serde_json::Value) -> Result<Digest256, StorageError> {
    compute_said_for_fields(value, &["said"])
}

impl SelfAddressed for serde_json::Value {
    fn derive_said(&mut self) -> Result<(), StorageError> {
        let said = compute_said_from_value(self)?;
        self.as_object_mut()
            .ok_or_else(|| StorageError::StorageError("value must be an object".to_string()))?
            .insert("said".to_string(), serde_json::Value::String(said.qb64()));
        Ok(())
    }

    fn verify_said(&self) -> Result<(), StorageError> {
        let current = self
            .get("said")
            .and_then(|v| v.as_str())
            .ok_or_else(|| StorageError::StorageError("missing said field".to_string()))?;
        let computed = compute_said_from_value(self)?;
        if current != computed.qb64() {
            return Err(StorageError::StorageError(format!(
                "SAID mismatch: expected {}, got {}",
                computed, current
            )));
        }
        Ok(())
    }

    fn get_said(&self) -> Digest256 {
        let qb64 = self
            .get("said")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        Digest256::from_qb64(qb64).unwrap_or_default()
    }
}
