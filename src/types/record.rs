use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::record_batch::RecordBatch;
use crate::types::error::{ConvertError, QueryError};
use crate::types::value::Value;
use crate::utils::convert::convert_entities_record_batch;

// @TODO: Create references per block
pub struct Records {
    entities_records_batch: Arc<HashMap<String, Vec<RecordBatch>>>,
    references_records_batch: Arc<HashMap<String, Vec<RecordBatch>>>,

    entities: HashMap<String, Vec<Value>>,
    entities_ids: HashMap<String, HashMap<u64, usize>>,

    references: HashMap<String, Vec<Value>>,
    references_ids: HashMap<String, HashMap<u64, usize>>,
}

impl Records {
    pub fn new(
        entities_records_batch: Arc<HashMap<String, Vec<RecordBatch>>>,
        references_records_batch: Arc<HashMap<String, Vec<RecordBatch>>>,
    ) -> Result<Self, ConvertError> {
        let (entities, entities_ids) = convert_entities_record_batch(&entities_records_batch)?;

        let (references, references_ids) = convert_entities_record_batch(&references_records_batch)?;

        Ok(
            Records {
                entities_records_batch,
                references_records_batch,

                entities,
                entities_ids,

                references,
                references_ids,
            }
        )
    }

    pub(crate) fn get_entities_records_batch(&self) -> &Arc<HashMap<String, Vec<RecordBatch>>> {
        &self.entities_records_batch
    }

    pub(crate) fn get_references_records_batch(&self) -> &Arc<HashMap<String, Vec<RecordBatch>>> {
        &self.references_records_batch
    }

    pub fn get_entity(&self, entity_name: &String, index: usize) -> Option<&Value> {
        if let Some(entities) = self.entities.get(entity_name) {
            return entities.get(index);
        }

        None
    }

    pub fn get_entity_by_id(&self, entity_name: &String, id: u64) -> Option<&Value> {
        if let Some(index) = self.get_entity_index(entity_name, id.clone()) {
            if let Some(entities) = self.entities.get(entity_name) {
                return entities.get(index);
            }
        }

        None
    }

    pub fn entities_len(&self, entity_name: String) -> Result<usize, QueryError> {
        if let Some(indexes) = self.entities_ids.get(&entity_name) {
            Ok(indexes.keys().len())
        } else {
            Err(QueryError::UnknownEntityError(entity_name.clone()))
        }
    }

    pub fn get_reference(&self, entity_name: &String, index: usize) -> Option<&Value> {
        if let Some(entities) = self.references.get(entity_name) {
            return entities.get(index);
        }

        None
    }

    pub fn get_reference_by_id(&self, entity_name: &String, id: u64) -> Option<&Value> {
        if let Some(index) = self.get_reference_index(entity_name, id.clone()) {
            if let Some(entities) = self.references.get(entity_name) {
                return entities.get(index);
            }
        }

        None
    }

    pub fn references_len(&self, entity_name: String) -> Result<usize, QueryError> {
        if let Some(indexes) = self.references_ids.get(&entity_name) {
            Ok(indexes.keys().len())
        } else {
            Err(QueryError::UnknownEntityError(entity_name.clone()))
        }
    }

    fn get_entity_index(&self, entity_name: &String, id: u64) -> Option<usize> {
        if let Some(entity_ids) = self.entities_ids.get(entity_name) {
            if let Some(record) = entity_ids.get(&id) {
                return Some(record.clone());
            }
        }

        None
    }

    fn get_reference_index(&self, entity_name: &String, id: u64) -> Option<usize> {
        if let Some(references_ids) = self.references_ids.get(entity_name) {
            if let Some(record) = references_ids.get(&id) {
                return Some(record.clone());
            }
        }

        None
    }

    pub fn exists(&self, entity_name: String, id: u64) -> bool {
        if let Some(entity_record_indexes) = self.entities_ids.get(&entity_name) {
            if let Some(_record_index) = entity_record_indexes.get(&id) {
                return true;
            }
        }

        return false;
    }

    pub fn exists_reference(&self, entity_name: String, id: u64) -> bool {
        if let Some(entity_record_indexes) = self.references_ids.get(&entity_name) {
            if let Some(_record_index) = entity_record_indexes.get(&id) {
                return true;
            }
        }

        return false;
    }
}