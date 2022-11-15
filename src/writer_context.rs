use std::fs::File;
use std::path::PathBuf;
use std::io::Read;
use serde::Deserialize;
use validator::Validate;
use crate::types::error::ContextError;

#[derive(Clone, Deserialize, Validate, Debug)]
pub struct WriterContext {
    #[validate(url)]
    pub ipfs_daemon_url: String,
    pub ipns_key: String,
    pub data_store_path: String,
    #[serde(skip)]
    pub data_store: PathBuf,
    #[validate(range(min = 0))]
    pub blocks_per_export: u32,
    #[validate(range(min = 0))]
    pub parquet_page_size: u32,
    #[validate(range(min = 0))]
    pub parquet_row_group_size: u32,
}

impl WriterContext {
    pub fn new(
        ipfs_daemon_url: String,
        ipns_key: String,
        data_store_path: String,
        blocks_per_export: u32,
        parquet_page_size: u32,
        parquet_row_group_size: u32,
    ) -> WriterContext {
        Self {
            ipfs_daemon_url,
            ipns_key,
            data_store: PathBuf::from(data_store_path.clone()),
            data_store_path,
            blocks_per_export,
            parquet_page_size,
            parquet_row_group_size,
        }
    }

    pub fn read_from_file(config_path: PathBuf) -> Result<WriterContext, ContextError> {
        if ! config_path.exists() {
            return Err(ContextError::ConfigIsNotExist(config_path.to_str().unwrap().to_string()));
        }

        let mut file = File::open(config_path)?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut context = serde_yaml::from_str::<WriterContext>(contents.as_str())?;

        context.data_store = PathBuf::from(context.data_store_path.clone());

        context.validate()?;

        Ok(context)
    }
}