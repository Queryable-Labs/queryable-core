use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::prelude::SessionContext;
use log::debug;
use crate::ipfs::client::get_ipfs_client;
use crate::ipfs::object_store::IPFSFileSystem;
use crate::types::datasource::PublishedBlockItem;
use crate::types::entity::Entity;
use crate::types::error::QueryError;
use crate::types::filter::Expression;
use crate::utils::convert::{convert_filter, convert_filters};

pub struct EntityReader {
    pub ipfs_daemon_url: String,
    pub entity: Entity,
    pub arrow_schema_ref: SchemaRef,
}

impl EntityReader {
    pub fn new(ipfs_daemon_url: String, entity: Entity) -> Self {
        let arrow_schema = crate::utils::schema::gen_arrow_schema(
            entity.fields_schema.clone()
        );

        return EntityReader {
            ipfs_daemon_url: ipfs_daemon_url,
            entity,
            arrow_schema_ref: Arc::new(arrow_schema),
        }
    }

    pub async fn query(
        &self,
        block_items: &Vec<PublishedBlockItem>,
        filter: &Vec<Option<Expression>>,
        skip: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        debug!("Query IPFS entity: {} skip {:?} limit {:?}", self.entity.name, skip, limit);

        let ipfs_client = get_ipfs_client(self.ipfs_daemon_url.as_str())?;

        let file_format = Arc::new(
            ParquetFormat::default().with_enable_pruning(true)
        );

        let listing_options = ListingOptions {
            file_extension: "".to_owned(),
            format: Arc::clone(&file_format) as _,
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: block_items.len(),
        };

        let config = ListingTableConfig::new_with_multi_paths(
            block_items.iter()
                .map(|block_item|
                    ListingTableUrl::parse(format!("ipfs://{}", block_item.ipfs_path)).unwrap())
                .collect()
        )
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(&self.arrow_schema_ref));

        let table: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);

        let ctx = SessionContext::new();

        let ipfs_filesystem = Arc::new(
            IPFSFileSystem::new(
                ipfs_client,
                block_items.clone()
            )
        ) as _;

        ctx.runtime_env().register_object_store(
            "ipfs",
            "",
            ipfs_filesystem
        );

        ctx.register_table("table", Arc::clone(&table))?;

        let mut df = ctx.read_table(Arc::clone(&table))?;

        let converted_filter = convert_filters(
            filter,
        )?;

        if converted_filter.is_some() {
            df = df.filter(converted_filter.unwrap())?;
        }

        if skip.is_some() {
            df = df.limit(skip.clone().unwrap() as usize, limit)?;
        } else if limit.is_some() {
            df = df.limit(0, limit)?;
        }

        Ok(df.collect().await?)
    }

    pub async fn apply_query_to_result(
        &self,
        record_batches: Vec<RecordBatch>,
        filter: &Option<Expression>,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let ctx = SessionContext::new();

        let provider = MemTable::try_new(
            Arc::clone(&self.arrow_schema_ref),
            vec![record_batches]
        )?;
        ctx.register_table("table", Arc::new(provider))?;
        let mut df = ctx.table("table")?;

        if filter.is_some() {
            let converted_filter = convert_filter(
                filter.as_ref().unwrap(),
            )?;

            df = df.filter(converted_filter)?;
        }

        Ok(df.collect().await?)
    }
}