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

    #[error("Duplicate record: {0}")]
    DuplicateRecord(String),

    #[error("Not found: {0}")]
    NotFound(String),
}
