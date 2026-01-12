use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Invalid SAID: {0}")]
    InvalidSaid(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("CESR error: {0}")]
    CesrError(#[from] cesr::CesrError),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

#[cfg(feature = "surrealdb")]
impl From<surrealdb::Error> for StorageError {
    fn from(e: surrealdb::Error) -> Self {
        StorageError::StorageError(e.to_string())
    }
}
