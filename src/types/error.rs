use std::string::FromUtf8Error;
use std::time::SystemTimeError;
use datafusion::arrow::error::ArrowError;
use ipfs_api_backend_hyper::Error as IpfsAPiBackendHyperError;
use datafusion::common::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use hyper::header::InvalidHeaderValue;
use hyper::http::uri::InvalidUri;
use validator::ValidationErrors;
use thiserror::Error;
use crate::types::value::TimeUnit;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error(transparent)]
    IpfsError(#[from] IpfsError),

    #[error(transparent)]
    QueryError(#[from] QueryError),

    #[error(transparent)]
    ContextError(#[from] ContextError),

    #[error(transparent)]
    SystemTimeError(#[from] SystemTimeError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Failed to parse")]
    ParseError,
}

#[derive(Error, Debug)]
pub enum ContextError {
    #[error("Config path `{0}` is not exist")]
    ConfigIsNotExist(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    ConfigParseError(#[from] serde_yaml::Error),

    #[error(transparent)]
    ValidationErrors(#[from] ValidationErrors),
}

#[derive(Error, Debug)]
pub enum IpfsError {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),

    #[error(transparent)]
    ParseError(#[from] FromUtf8Error),

    #[error(transparent)]
    InternalClient(#[from] IpfsAPiBackendHyperError),

    #[error(transparent)]
    InvalidHeaderValue(#[from] InvalidHeaderValue),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error(transparent)]
    IpfsError(#[from] IpfsError),

    #[error(transparent)]
    DataFusionError(#[from] DataFusionError),

    #[error(transparent)]
    ContextError(#[from] ContextError),

    #[error(transparent)]
    ConvertError(#[from] ConvertError),

    #[error("Requested unknown entity: {0}")]
    UnknownEntityError(String),

    #[error("Requested unknown field `{0}` of entity `{1}`")]
    UnknownEntityFieldError(String, String),

    #[error("Requested unknown entity `{0}` by id: `{1}`")]
    UnknownEntityIdError(String, u64),

    #[error("Requested invalid entity `{0}` by index: `{1}`")]
    InvalidEntityIndexError(String, usize),

    #[error("Expected invalid type of entities `{0}` field `{1}`")]
    ExpectedInvalidType(String, String),

    #[error("Entity {0} time field is not specified")]
    TimeFieldIsUnknownError(String),

    #[error("Entity {0} has no specified time field {1}")]
    TimeFieldIsNotPresentError(String, String)
}

#[derive(Error, Debug)]
pub enum DatasourceWriterError {
    #[error("Datasource blocks file not exists")]
    DataSourceBlocksFileNotExist,

    #[error(transparent)]
    ArrowError(#[from] ArrowError),

    #[error(transparent)]
    ParquetError(#[from] ParquetError),

    #[error(transparent)]
    SystemTimeError(#[from] SystemTimeError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    IpfsError(#[from] IpfsError),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error("Datasource entity `{0}` has no field named as `{1}`")]
    EntityWriterErrorUnknownField(String, String),

    #[error("Datasource entity `{0}` has field named as `{1}` with different builder")]
    EntityWriterErrorIncorrectFieldBuilder(String, String),

    #[error("Datasource entity `{0}` cant lock field named as `{1}`")]
    EntityWriterErrorLockFieldError(String, String),

    #[error("Datasource entity `{0}` struct field named as `{1}` has unknown field {2}")]
    EntityWriterErrorUnknownStructField(String, String, usize),

    #[error("Failed to parse arrow schema")]
    FailedToParseArrowSchema,

    #[error("Entity misses one of required fields: block_index(u64), time_index(u64)")]
    NoFieldsBlockIndexAndTimeIndex,

    #[error("Defined schema has unsupported data type")]
    UnsupportedDataType,

    #[error("Requested unknown entity: {0}")]
    UnknownEntityError(String),

    #[error("List field is not optional")]
    ListFieldIsNotOptional(String)
}

#[derive(Error, Debug)]
pub enum ConvertError {
    #[error("Field id has incorrect type")]
    ColumnIdHasIncorrectType,

    #[error("Requested invalid entity by index: `{0}`")]
    InvalidIndexError(usize),

    #[error("Unexpected time unit `{0}`")]
    UnsupportedTimeUnit(TimeUnit),

    #[error("Unsupported data type, create ticket to add support")]
    UnsupportedDataType,
}