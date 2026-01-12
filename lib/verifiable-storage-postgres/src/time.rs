//! PostgreSQL-compatible datetime wrapper.

use std::ops::Add;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;

/// PostgreSQL-compatible datetime with microsecond precision.
///
/// Wraps `chrono::DateTime<Utc>` and implements sqlx `Type` for direct
/// PostgreSQL TIMESTAMPTZ compatibility.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Type)]
#[sqlx(transparent)]
pub struct PgStorageDatetime(pub DateTime<Utc>);

impl PgStorageDatetime {
    pub fn now() -> Self {
        PgStorageDatetime(datetime_micros())
    }

    pub fn is_from_future(&self) -> bool {
        Self::now() < *self
    }

    pub fn inner(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl Default for PgStorageDatetime {
    fn default() -> Self {
        Self::now()
    }
}

impl Add<Duration> for PgStorageDatetime {
    type Output = PgStorageDatetime;

    fn add(self, rhs: Duration) -> Self::Output {
        let new_time = self.0 + chrono::Duration::from_std(rhs).unwrap_or(chrono::Duration::zero());
        PgStorageDatetime(new_time)
    }
}

impl std::fmt::Display for PgStorageDatetime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
    }
}

impl From<DateTime<Utc>> for PgStorageDatetime {
    fn from(dt: DateTime<Utc>) -> Self {
        PgStorageDatetime(dt)
    }
}

impl From<PgStorageDatetime> for DateTime<Utc> {
    fn from(dt: PgStorageDatetime) -> Self {
        dt.0
    }
}

/// Create a DateTime truncated to microsecond precision (6 decimal places)
fn datetime_micros() -> DateTime<Utc> {
    let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(time) => time,
        Err(_) => std::time::Duration::from_secs(0),
    };

    let timestamp_micros = (now.as_secs() as i64 * 1_000_000) + (now.subsec_micros() as i64);
    if let Some(time) = DateTime::from_timestamp_micros(timestamp_micros) {
        time
    } else {
        DateTime::<Utc>::from_timestamp_nanos(0)
    }
}
