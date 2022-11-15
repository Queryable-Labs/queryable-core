use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::types::entity::Entity;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockItem {
    pub start_time: u64,
    pub end_time: u64,
    pub start_block: Option<String>,
    pub end_block: Option<String>,
    pub start_id: u64,
    pub end_id: u64,
    pub ipfs_path: String,
    pub size: u64,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasourceBlockItems {
    pub block_items: HashMap<String, Vec<BlockItem>>,
    pub exported_state: Option<String>,
    pub ids: HashMap<String, u64>,
}

impl DatasourceBlockItems {
    pub fn new(
        block_items: HashMap<String, Vec<BlockItem>>,
        exported_state: Option<String>,
        ids: HashMap<String, u64>,
    ) -> Self {
        DatasourceBlockItems {
            block_items,
            exported_state,
            ids,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasourceBlocks {
    pub version: String,
    pub name: String,
    pub description: String,
    pub logo_url: String,
    pub entities: HashMap<String, Entity>,
    pub data: HashMap<String, Vec<BlockItem>>,
    pub metadata: HashMap<String, String>,
}