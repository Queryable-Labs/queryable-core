use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use datafusion::arrow::array::{Array, ListArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use log::{debug, warn};
use crate::constant::ENTITY_ID_COLUMN_NAME;
use crate::entity_reader::EntityReader;
use crate::types::datasource::DatasourceBlocks;
use crate::types::entity::Entity;
use crate::types::entity::EntityRelationType;
use crate::types::error::QueryError;
use crate::types::filter::{Expression, ExpressionComparison, FilterConditional, FilterFieldRef, FilterLogicalExpressionType, FilterOperator};
use crate::types::record::Records;
use crate::types::value::Value;

pub struct DatasourceReader {
    datasource: DatasourceBlocks,
    entity_readers: HashMap<String, EntityReader>,
}

impl DatasourceReader {
    pub fn new(
        ipfs_daemon_url: String,
        datasource: DatasourceBlocks
    ) -> Self {
        let mut entity_readers: HashMap<String, EntityReader> = HashMap::new();

        for (entity_name, entity) in datasource.entities.iter() {
            entity_readers.insert(entity_name.clone(), EntityReader::new(
                ipfs_daemon_url.clone(),
                entity.clone(),
            ));
        }

        DatasourceReader {
            datasource,
            entity_readers,
        }
    }

    pub fn get_entity(&self, entity_name: String) -> Option<&Entity> {
        return self.datasource.entities.get(&entity_name);
    }

    pub async fn query(
        &self,
        entities_filters: HashMap<String, Vec<Option<Expression>>>,
        skip: HashMap<String, u64>,
        limit: Option<usize>,
    ) -> Result<Records, QueryError> {
        let mut referred_entities: HashMap<String, HashSet<u64>> = HashMap::new();

        let mut fetched_entities: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut referred_fetched_entities: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        debug!("Defining sequence of entities to fetch");

        for (entity_name, filters) in &entities_filters {
            let entity = self.datasource.entities.get(entity_name).unwrap();

            let entity_reader = self.entity_readers.get(entity_name).unwrap();

            let block_items = self.datasource.data.get(entity_name).unwrap();

            let record_batches = entity_reader.query(
                block_items,
                filters,
                match skip.get(entity_name) {
                    Some(v) => Some(v.clone()),
                    None => None
                },
                limit.clone(),
            ).await?;

            fetched_entities.insert(entity_name.clone(), record_batches);

            debug!("Defining plan to fetch references");

            for relation in entity.relations.iter() {
                if !relation.eager_fetch {
                    continue;
                }

                let referred_ids;

                if !referred_entities.contains_key(&relation.remote_entity_name) {
                    referred_entities.insert(
                        relation.remote_entity_name.clone(),
                        HashSet::new()
                    );
                }

                referred_ids = referred_entities.get_mut(&relation.remote_entity_name).unwrap();

                let record_batches = fetched_entities.get(entity_name).unwrap();

                match relation.relation_type {
                    EntityRelationType::ManyToOne | EntityRelationType::OneToOne => {
                        for batch in record_batches {
                            let (column_id, _) = batch.schema().column_with_name(
                                &relation.local_field_name
                            ).unwrap();

                            let column_id_index = batch.column(column_id);

                            let column_id_array_ref = column_id_index.as_any().downcast_ref::<UInt64Array>().unwrap();

                            for row in 0..batch.num_rows() {
                                if column_id_array_ref.is_null(row) {
                                    continue;
                                }

                                let value = column_id_array_ref.value(row);

                                referred_ids.insert(value);
                            }
                        }
                    },
                    EntityRelationType::OneToMany | EntityRelationType::ManyToMany => {
                        for batch in record_batches {
                            let (column_id, _) = batch.schema().column_with_name(
                                &relation.local_field_name
                            ).unwrap();

                            let column_id_index = batch.column(column_id);

                            let column_id_array_ref = column_id_index.as_any().downcast_ref::<ListArray>().unwrap();

                            for row in 0..batch.num_rows() {
                                if column_id_array_ref.is_null(row) {
                                    continue;
                                }

                                let array_ref = column_id_array_ref.value(row);

                                let array_ref = array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

                                for j in 0..array_ref.len() {
                                    referred_ids.insert(array_ref.value(j));
                                }
                            }
                        }
                    },
                }
            }
        }

        let mut referred_entities_count: HashMap<String, usize> = HashMap::new();

        debug!("Remove already fetched entities from references to fetch");

        for (entity_name, record_batches) in fetched_entities.iter() {
            if ! referred_entities.contains_key(entity_name) {
                continue;
            }

            let referred_ids = referred_entities.get_mut(entity_name).unwrap();

            for batch in record_batches {
                let (column_id, _) = batch.schema().column_with_name(ENTITY_ID_COLUMN_NAME).unwrap();

                let column_id_array_ref = batch.column(column_id);

                let column_id_array_ref = column_id_array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

                for row in 0..batch.num_rows() {
                    let id = column_id_array_ref.value(row);

                    referred_ids.remove(&id);
                }
            }

            referred_entities_count.insert(entity_name.clone(), referred_ids.len());
        }

        debug!("Fetching references, details: {:?}", referred_entities_count);

        for (entity_name, referred_ids) in referred_entities.iter_mut() {
            let entity_reader = self.entity_readers.get(entity_name).unwrap();

            let block_items = self.datasource.data.get(entity_name).unwrap();

            let record_batches = entity_reader.query(
                block_items,
                &vec![
                    Some(
                        Expression::Comparison(
                            ExpressionComparison {
                                field_ref: FilterFieldRef::IpfsColumn(String::from("_id")),
                                operator: FilterOperator::In,
                                value: Value::List(
                                    referred_ids.iter().map(
                                        |id| Value::UInt64(id.clone())
                                    ).collect()
                                )
                            }
                        )
                    )
                ],
                None,
                None,
            ).await?;

            for batch in &record_batches {
                let (column_id, _) = batch.schema().column_with_name(ENTITY_ID_COLUMN_NAME).unwrap();
                let column_id_array_ref = batch.column(column_id);
                let column_id_array_ref = column_id_array_ref.as_any().downcast_ref::<UInt64Array>().unwrap();

                for row in 0..batch.num_rows() {
                    let id = column_id_array_ref.value(row);

                    referred_ids.remove(&id);
                }
            }

            referred_fetched_entities.insert(entity_name.clone(), record_batches);

            if referred_ids.len() > 0 {
                warn!("Not all extra blocks were fetched!");
            }
        }

        let records = Records::new(
            Arc::new(fetched_entities),
            Arc::new(referred_fetched_entities),
        )?;

        Ok(records)
    }

    pub async fn timed_query(
        &self,
        entities_filters: HashMap<String, Vec<Option<Expression>>>,
        time_fields: HashMap<String, String>,
        start_time: u64,
        end_time: u64
    ) -> Result<Records, QueryError> {
        let mut entities_filters = entities_filters.clone();

        for (entity_name, filters) in entities_filters.iter_mut() {
            let time_field = time_fields.get(entity_name);

            if time_field.is_none() {
                warn!("Time field is not present in data source");

                return Err(QueryError::TimeFieldIsUnknownError(entity_name.clone()));
            }

            let time_field = time_field.unwrap().clone();

            filters.push(Some(
                Expression::Conditional(
                    Box::new(FilterConditional {
                        operator: FilterLogicalExpressionType::And,
                        left: Box::new(Expression::Comparison(
                            ExpressionComparison {
                                field_ref: FilterFieldRef::IpfsColumn(time_field.clone()),
                                operator: FilterOperator::GreaterOrEqualThan,
                                value: Value::UInt64(start_time)
                            }
                        )),
                        right: Box::new(Expression::Comparison(
                            ExpressionComparison {
                                field_ref: FilterFieldRef::IpfsColumn(time_field),
                                operator: FilterOperator::LessThan,
                                value: Value::UInt64(end_time)
                            }
                        ))
                    })
                )
            ));
        }

        self.query(
            entities_filters.clone(),
            HashMap::new(),
            None,
        ).await
    }

    pub async fn filter_query_result(
        &self,
        records: &Records,
        entity_name: String,
        filter: Option<Expression>
    ) -> Result<Records, QueryError> {
        let entity_reader = self.entity_readers.get(&entity_name).unwrap();
        let entities_records_batch = records.get_entities_records_batch();

        if let Some(record_batches) = entities_records_batch.get(&entity_name) {
            let filtered_record_batches = entity_reader.apply_query_to_result(
                record_batches.clone(),
                &filter,
            ).await?;

            let records = Records::new(
                Arc::new(
                    HashMap::from(
                        [
                            (entity_name.clone(), filtered_record_batches)
                        ]
                    )
                ),
                Arc::clone(
                    &records.get_references_records_batch()
                ),
            )?;

            Ok(records)
        } else {
            Err(QueryError::UnknownEntityError(entity_name))
        }
    }
}