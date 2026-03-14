use std::collections::HashMap;

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

/// Maximum default recursion depth for compaction.
pub const MAX_COMPACTION_DEPTH: usize = 32;

/// Walk a JSON value bottom-up, depth-first. At each object with a `"said"` key,
/// compact its children first, then compute its SAID, replace it in the parent
/// with the SAID string, and add the compacted form to the accumulator.
/// Uses `MAX_COMPACTION_DEPTH` as the default depth bound.
pub fn compact_value(
    value: &mut serde_json::Value,
    accumulator: &mut HashMap<String, serde_json::Value>,
) -> Result<(), StorageError> {
    compact_value_bounded(value, accumulator, MAX_COMPACTION_DEPTH)
}

/// Walk a JSON value bottom-up, depth-first with an explicit depth bound.
/// At each object with a `"said"` key, compact its children first, then compute
/// its SAID, replace it in the parent with the SAID string, and add the compacted
/// form to the accumulator.
pub fn compact_value_bounded(
    value: &mut serde_json::Value,
    accumulator: &mut HashMap<String, serde_json::Value>,
    remaining_depth: usize,
) -> Result<(), StorageError> {
    if remaining_depth == 0 {
        return Err(StorageError::StorageError(
            "maximum compaction depth exceeded".to_string(),
        ));
    }

    if let Some(obj) = value.as_object_mut() {
        let keys: Vec<String> = obj.keys().cloned().collect();
        for key in &keys {
            if key == "said" {
                continue;
            }
            if let Some(child) = obj.get_mut(key) {
                compact_value_bounded(child, accumulator, remaining_depth - 1)?;
            }
        }

        // If this object has a "said" field, it's a SelfAddressed chunk
        if obj.contains_key("said") {
            let said = compute_said_from_value(value)?;
            let obj = value
                .as_object_mut()
                .ok_or_else(|| StorageError::StorageError("lost object".to_string()))?;
            obj.insert("said".to_string(), serde_json::Value::String(said.clone()));
            accumulator.insert(said.clone(), value.clone());
            // Replace this object with its SAID string in the parent
            *value = serde_json::Value::String(said);
        }
    } else if let Some(arr) = value.as_array_mut() {
        for elem in arr.iter_mut() {
            compact_value_bounded(elem, accumulator, remaining_depth - 1)?;
        }
    }

    Ok(())
}
