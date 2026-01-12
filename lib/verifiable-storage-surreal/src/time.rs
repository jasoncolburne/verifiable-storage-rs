use std::ops::Add;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::sql::Datetime as SurrealDatetime;

// Note: verifiable_storage::StorageDatetime wraps SurrealDatetime when
// the surrealdb feature is enabled, which it is for this crate.

/// SurrealDB-compatible timestamp with microsecond precision.
///
/// Wraps SurrealDB's Datetime for database compatibility while providing
/// the same interface as `verifiable_storage::StorageDatetime`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SurrealStorageDatetime(pub SurrealDatetime);

impl SurrealStorageDatetime {
    pub fn now() -> Self {
        SurrealStorageDatetime(datetime_micros())
    }

    pub fn is_from_future(&self) -> bool {
        Self::now() < *self
    }

    pub fn inner(&self) -> &SurrealDatetime {
        &self.0
    }
}

impl Default for SurrealStorageDatetime {
    fn default() -> Self {
        Self::now()
    }
}

impl Add<Duration> for SurrealStorageDatetime {
    type Output = SurrealStorageDatetime;

    fn add(self, rhs: Duration) -> Self::Output {
        let inner: DateTime<Utc> = self.0.clone().into();
        let new_time = inner + chrono::Duration::from_std(rhs).unwrap_or(chrono::Duration::zero());
        SurrealStorageDatetime(SurrealDatetime::from(new_time))
    }
}

impl std::fmt::Display for SurrealStorageDatetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SurrealDatetime> for SurrealStorageDatetime {
    fn from(dt: SurrealDatetime) -> Self {
        SurrealStorageDatetime(dt)
    }
}

impl From<SurrealStorageDatetime> for SurrealDatetime {
    fn from(dt: SurrealStorageDatetime) -> Self {
        dt.0
    }
}

impl From<verifiable_storage::StorageDatetime> for SurrealStorageDatetime {
    fn from(dt: verifiable_storage::StorageDatetime) -> Self {
        // StorageDatetime wraps SurrealDatetime when surrealdb feature is enabled
        SurrealStorageDatetime(dt.0)
    }
}

impl From<SurrealStorageDatetime> for verifiable_storage::StorageDatetime {
    fn from(dt: SurrealStorageDatetime) -> Self {
        // StorageDatetime wraps SurrealDatetime when surrealdb feature is enabled
        verifiable_storage::StorageDatetime(dt.0)
    }
}

/// Create a SurrealDB Datetime truncated to microsecond precision (6 decimal places)
fn datetime_micros() -> SurrealDatetime {
    let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(time) => time,
        Err(_) => std::time::Duration::from_secs(0),
    };

    let timestamp_micros = (now.as_secs() as i64 * 1_000_000) + (now.subsec_micros() as i64);
    let timestamp = if let Some(time) = DateTime::from_timestamp_micros(timestamp_micros) {
        time
    } else {
        DateTime::<Utc>::from_timestamp_nanos(0)
    };

    SurrealDatetime::from(timestamp)
}
