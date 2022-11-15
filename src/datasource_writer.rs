use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use serde_json;
use ipfs_api_backend_hyper::IpfsApi;
use log::{debug, info, warn};
use crate::constant::{DATASOURCE_BLOCK_ITEMS_FILE_NAME, PARQUET_METADATA_FIELD_BLOCKS_PAGINATION, PARQUET_METADATA_FIELD_ENGINE};
use crate::ipfs::client::{NamePublishOps, get_ipfs_client};
use crate::entity_writer::EntityWriter;
use crate::types::error::DatasourceWriterError;
use crate::writer_context::WriterContext;
use crate::types::datasource::{BlockItem, DatasourceBlockItems, DatasourceBlocks};
use crate::types::entity::Entity;

pub struct DatasourceWriter {
    context: WriterContext,
    blocks_path: PathBuf,
    engine: String,
    entities: Vec<Entity>,

    name: String,
    description: String,
    logo_url: String,

    writers: HashMap<String, EntityWriter>,

    last_block_items: HashMap<String, Vec<BlockItem>>,
    last_exported_state: Option<String>,
    last_ids: HashMap<String, u64>,

    current_ids: HashMap<String, u64>,
}

impl std::fmt::Debug for DatasourceWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasourceWriter")
            .field("context", &self.context)
            .field("blocks_path", &self.blocks_path)
            .field("description", &self.description)
            .field("entities", &self.entities)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("logoUrl", &self.logo_url)
            .field("block_items", &self.last_block_items)
            .field("last_exported_state", &self.last_exported_state)
            .field("last_ids", &self.last_ids)
            .field("current_ids", &self.current_ids)
            .finish()
    }
}

impl DatasourceWriter {
    pub fn new(
        context: WriterContext,
        engine: String,
        name: String,
        description: String,
        logo_url: String,
        entities: Vec<Entity>,
    ) -> Result<DatasourceWriter, DatasourceWriterError> {
        let dir_path = Path::new(&context.data_store_path);

        if !dir_path.exists() {
            info!("Creating path {} for datasource", dir_path.display());
            std::fs::create_dir_all(dir_path).unwrap();
        }

        let mut writers: HashMap<String, EntityWriter> = HashMap::new();

        for entity in &entities {
            writers.insert(
                entity.name.clone(),
                EntityWriter::new(context.clone(), entity.clone())?
            );
        }

        let datasource_blocks_path = dir_path.join(
            Path::new(DATASOURCE_BLOCK_ITEMS_FILE_NAME)
        );

        let block_items: HashMap<String, Vec<BlockItem>>;
        let exported_state: Option<String>;
        let ids: HashMap<String, u64>;

        if !datasource_blocks_path.is_file() {
            warn!("Datasource blocks file is not exist, using empty");
            block_items = HashMap::new();
            exported_state = None;
            ids = HashMap::new();
        } else {
            let mut file = File::open(datasource_blocks_path.clone())?;

            let mut contents = String::new();
            file.read_to_string(&mut contents)?;

            let datasource_block_items: DatasourceBlockItems = serde_json::from_str(
                contents.as_str()
            )?;

            block_items = datasource_block_items.block_items;
            exported_state = datasource_block_items.exported_state;
            ids = datasource_block_items.ids;
        }

        Ok(
            Self {
                context,
                blocks_path: datasource_blocks_path,
                engine,

                name,
                description,
                logo_url,

                entities,
                writers,

                last_block_items: block_items,
                last_exported_state: exported_state,
                last_ids: ids.clone(),

                current_ids: ids,
            }
        )
    }

    pub fn set_block_index(&mut self, block_index: u64) {
        for entity_writer in self.writers.values_mut() {
            entity_writer.set_start_block(block_index);
            entity_writer.set_end_block(block_index);
        }
    }

    pub fn set_block_time(&mut self, block_time: u64) {
        for entity_writer in self.writers.values_mut() {
            entity_writer.set_start_time(block_time);
            entity_writer.set_end_time(block_time);
        }
    }

    pub fn get_next_id(&mut self, entity: String) -> Result<u64, DatasourceWriterError> {
        if ! self.writers.contains_key(&entity) {
            return Err(DatasourceWriterError::UnknownEntityError(entity));
        }

        let entity_writer = self.writers.get_mut(&entity).unwrap();

        let last_id;

        if ! self.current_ids.contains_key(&entity) {
            self.current_ids.insert(entity.clone(), 0);

            last_id = 0;
        } else {
            last_id = self.current_ids.get(&entity).unwrap() + 1;

            self.current_ids.insert(entity.clone(), last_id);
        }

        entity_writer.set_start_id(last_id);
        entity_writer.set_end_id(last_id);

        Ok(last_id)
    }

    pub fn get_last_exported_data(&self) -> Option<String> {
        self.last_exported_state.clone()
    }

    pub fn get_blocks_per_export(&self) -> u32 {
        self.context.blocks_per_export
    }

    pub fn get_writer(&mut self, entity_name: String) -> Option<&mut EntityWriter> {
        return self.writers.get_mut(&entity_name);
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn export(
        &mut self,
        exported_state: Option<String>,
        metadata: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let result = self.export_async(
            exported_state,
            metadata,
        ).await;

        match result {
            Ok(()) => {
                Ok(())
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    pub async fn export_async(
        &mut self,
        exported_state: Option<String>,
        metadata: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let mut new_block_items: HashMap<String, Vec<BlockItem>> = self.last_block_items.clone();

        for (name, writer) in self.writers.iter_mut() {
            if ! new_block_items.contains_key(name) {
                new_block_items.insert(name.clone(), vec![]);
            }

            let block_items = new_block_items.get_mut(name).unwrap();

            let block_item = writer.export_async(
                block_items.len()
            ).await.map_err(|err| {
                warn!("Failed to export data for entity {}", writer.entity().name);
                err
            })?;

            new_block_items.get_mut(name).unwrap().push(block_item);
        }

        let ipfs_client = get_ipfs_client(self.context.ipfs_daemon_url.as_str())?;

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
            data: new_block_items.clone(),
            metadata,
        };

        let datasource_serialized = serde_json::to_string(&datasource)?;

        let data = Cursor::new(datasource_serialized);

        let datasource_add_response = ipfs_client.add(data).await?;

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

        let datasource_block_items: DatasourceBlockItems = DatasourceBlockItems::new(
            new_block_items.clone(),
            exported_state.clone(),
            self.current_ids.clone(),
        );

        let serialized_datasource_block_items =
            serde_json::to_vec(&datasource_block_items)?;

        std::fs::write(self.blocks_path.clone(), serialized_datasource_block_items)?;

        debug!("Saved block items to FS");

        self.last_block_items = new_block_items;
        self.last_exported_state = exported_state;
        self.last_ids = self.current_ids.clone();

        Ok(())
    }

    pub fn clean(&mut self) {
        for writer in self.writers.values_mut() {
            writer.clean();
        }

        self.current_ids = self.last_ids.clone();
    }
}