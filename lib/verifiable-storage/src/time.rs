use std::ops::Add;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Verifiable storage timestamp with microsecond precision
//
// When surrealdb feature is enabled, wraps SurrealDB's Datetime for database compatibility.
// Otherwise uses chrono DateTime directly (for WASM builds).

#[cfg(feature = "surrealdb")]
mod inner {
    use super::*;
    use surrealdb::sql::Datetime as SurrealDatetime;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    pub struct StorageDatetime(pub SurrealDatetime);

    impl StorageDatetime {
        pub fn now() -> Self {
            StorageDatetime(datetime_micros())
        }

        pub fn is_from_future(&self) -> bool {
            Self::now() < *self
        }

        pub fn inner(&self) -> &SurrealDatetime {
            &self.0
        }
    }

    impl Add<Duration> for StorageDatetime {
        type Output = StorageDatetime;

        fn add(self, rhs: Duration) -> Self::Output {
            let inner: DateTime<Utc> = self.0.clone().into();
            let new_time =
                inner + chrono::Duration::from_std(rhs).unwrap_or(chrono::Duration::zero());
            StorageDatetime(SurrealDatetime::from(new_time))
        }
    }

    impl std::fmt::Display for StorageDatetime {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<SurrealDatetime> for StorageDatetime {
        fn from(dt: SurrealDatetime) -> Self {
            StorageDatetime(dt)
        }
    }

    impl From<StorageDatetime> for SurrealDatetime {
        fn from(dt: StorageDatetime) -> Self {
            dt.0
        }
    }

    impl Default for StorageDatetime {
        fn default() -> Self {
            Self::now()
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
}

#[cfg(not(feature = "surrealdb"))]
mod inner {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct StorageDatetime(pub DateTime<Utc>);

    // Custom serde to always use microsecond precision with Z timezone
    impl Serialize for StorageDatetime {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_str(&self.0.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        }
    }

    impl<'de> Deserialize<'de> for StorageDatetime {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            let s = String::deserialize(deserializer)?;
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| StorageDatetime(dt.with_timezone(&Utc)))
                .map_err(serde::de::Error::custom)
        }
    }

    impl StorageDatetime {
        pub fn now() -> Self {
            StorageDatetime(datetime_micros())
        }

        pub fn is_from_future(&self) -> bool {
            Self::now() < *self
        }

        pub fn inner(&self) -> &DateTime<Utc> {
            &self.0
        }
    }

    impl Default for StorageDatetime {
        fn default() -> Self {
            Self::now()
        }
    }

    impl Add<Duration> for StorageDatetime {
        type Output = StorageDatetime;

        fn add(self, rhs: Duration) -> Self::Output {
            let new_time =
                self.0 + chrono::Duration::from_std(rhs).unwrap_or(chrono::Duration::zero());
            StorageDatetime(new_time)
        }
    }

    impl std::fmt::Display for StorageDatetime {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
        }
    }

    impl From<DateTime<Utc>> for StorageDatetime {
        fn from(dt: DateTime<Utc>) -> Self {
            StorageDatetime(dt)
        }
    }

    impl From<StorageDatetime> for DateTime<Utc> {
        fn from(dt: StorageDatetime) -> Self {
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
}

pub use inner::StorageDatetime;
