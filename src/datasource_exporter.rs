use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use ipfs_api_prelude::IpfsApi;
use log::debug;
use crate::constant::{PARQUET_METADATA_FIELD_BLOCKS_PAGINATION, PARQUET_METADATA_FIELD_ENGINE};
use crate::ipfs::client::get_ipfs_client;
use crate::ipfs::client::AddLocalOps;
use crate::ipfs::client::NamePublishOps;
use crate::types::datasource::{DatasourceBlocks, LocalBlockItem, PublishedBlockItem};
use crate::types::entity::Entity;
use crate::types::error::{DatasourceWriterError, IpfsError};
use crate::writer_context::WriterContext;

pub struct DatasourceExporter {
    context: WriterContext,

    engine: String,
    entities: Vec<Entity>,

    name: String,
    description: String,
    logo_url: String,
}

impl std::fmt::Debug for DatasourceExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasourceExporter")
            .field("context", &self.context)
            .field("engine", &self.engine)
            .field("entities", &self.entities)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("logoUrl", &self.logo_url)
            .finish()
    }
}

impl DatasourceExporter {
    pub fn new(
        context: WriterContext,

        engine: String,
        entities: Vec<Entity>,

        name: String,
        description: String,
        logo_url: String,
    ) -> Self {
        DatasourceExporter {
            context,

            engine,
            entities,

            name,
            description,
            logo_url,
        }
    }

    pub async fn export(
        &self,
        local_block_items: &HashMap<String, LocalBlockItem>,
        published_block_items: &mut HashMap<String, Vec<PublishedBlockItem>>,
        metadata: HashMap<String, String>,
    ) -> Result<(), DatasourceWriterError> {
        let ipfs_client = get_ipfs_client(self.context.ipfs_daemon_url.as_str())?;

        for (name, local_block_item) in local_block_items {
            let add_response = ipfs_client.add_local(PathBuf::from(local_block_item.local_path.clone())).await?;

            if ! published_block_items.contains_key(name) {
                published_block_items.insert(name.clone(),vec![]);
            }

            published_block_items.get_mut(name).unwrap().push(PublishedBlockItem {
                start_time: local_block_item.start_time,
                end_time: local_block_item.end_time,
                start_block: local_block_item.start_block.clone(),
                end_block: local_block_item.end_block.clone(),
                start_id: local_block_item.start_id,
                end_id: local_block_item.end_id,

                ipfs_path: format!("/ipfs/{}", add_response.hash).to_string(),
                size: local_block_item.size,
                created_at: local_block_item.created_at,
            })
        }

        let params = self.entities
            .iter()
            .map(|entity| (entity.name.clone(), entity.clone()))
            .collect::<Vec<(String, Entity)>>();

        let mut metadata: HashMap<String, String> = metadata.clone();

        metadata.insert(
            String::from(PARQUET_METADATA_FIELD_ENGINE),
            self.engine.clone()
        );
        metadata.insert(
            String::from(PARQUET_METADATA_FIELD_BLOCKS_PAGINATION),
            self.context.blocks_per_export.to_string()
        );

        let datasource = DatasourceBlocks {
            version: String::from("0.1"),
            name: self.name.clone(),
            description: self.description.clone(),
            logo_url: self.logo_url.clone(),
            entities: params.into_iter().collect(),
            data: published_block_items.clone(),
            metadata,
        };

        let datasource_serialized = serde_json::to_string(&datasource)?;

        let data = Cursor::new(datasource_serialized);

        let datasource_add_response = ipfs_client.add(data)
            .await
            .map_err(|err| IpfsError::InternalClient(err))?;

        debug!(
            "Uploaded datasource, hash: {}, size: {}",
            datasource_add_response.hash,
            datasource_add_response.size,
        );

        let name_publish_response = ipfs_client.name_publish_v2(
                datasource_add_response.hash,
                self.context.ipns_key.clone(),
        ).await?;

        debug!(
            "Published updated datasource, name: {}, value: {}",
            name_publish_response.name,
            name_publish_response.value
        );

        Ok(())
    }
}