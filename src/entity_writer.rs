use std::collections::HashMap;
use std::fs::{File, metadata};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use datafusion::arrow::array::{ArrayBuilder, ArrayRef, StructBuilder, ListBuilder, BinaryBuilder,
                               BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
                               Decimal256Builder, FixedSizeBinaryBuilder, Float32Builder,
                               Float64Builder, Int16Builder, Int32Builder, Int64Builder,
                               Int8Builder, LargeBinaryBuilder, LargeStringBuilder, make_builder,
                               StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder,
                               Time64MicrosecondBuilder, Time64NanosecondBuilder, UInt16Builder,
                               UInt32Builder, UInt64Builder, UInt8Builder, Array};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::decimal::{Decimal128, Decimal256};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding};
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use datafusion::parquet::schema::types::ColumnPath;
use log::{debug, info};
use crate::constant::{
    BLOCK_INDEX_COLUMN_NAME, PARQUET_METADATA_FIELD_ARROW_SCHEMA,
    PARQUET_METADATA_FIELD_END_BLOCK_SCHEMA, PARQUET_METADATA_FIELD_END_ID_SCHEMA,
    PARQUET_METADATA_FIELD_END_TIME_SCHEMA, PARQUET_METADATA_FIELD_PARQUET_SCHEMA,
    PARQUET_METADATA_FIELD_START_BLOCK_SCHEMA, PARQUET_METADATA_FIELD_START_ID_SCHEMA,
    PARQUET_METADATA_FIELD_START_TIME_SCHEMA, TIME_INDEX_COLUMN_NAME};
use crate::writer_context::WriterContext;
use crate::types::datasource::LocalBlockItem;
use crate::types::entity::{Entity, EntityFieldEncoding};
use crate::types::error::DatasourceWriterError;

macro_rules! add_primitive_append_method {
    ($name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<$builder_type>().unwrap();

                if let Some(value) = value {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };

    (AppendWithException, $name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<$builder_type>().unwrap();

                if let Some(value) = value {
                    builder.append_value(value)?;
                } else {
                    builder.append_null();
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };
}

macro_rules! add_list_primitive_append_method {
    ($name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, values: Option<Vec<Option<$type_>>>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<$builder_type>().unwrap();

                if let Some(values) = values {
                    for value in values {
                        if let Some(value) = value {
                            builder.values().append_value(value);
                        } else {
                            builder.values().append_null();
                        }
                    }

                    builder.append(true);
                } else {
                    builder.append(false);
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };

    (AppendWithException, $name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, values: Option<Vec<Option<$type_>>>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<$builder_type>().unwrap();

                if let Some(values) = values {
                    for value in values {
                        if let Some(value) = value {
                            builder.values().append_value(value)?;
                        } else {
                            builder.values().append_null();
                        }
                    }

                    builder.append(true);
                } else {
                    builder.append(false);
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };
}

macro_rules! add_struct_primitive_append_method {
    ($name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, field_index: usize, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<StructBuilder>().unwrap();

                let field_builder = builder.field_builder::<$builder_type>(field_index.clone());

                if let Some(field_builder) = field_builder {
                    if let Some(value) = value {
                        field_builder.append_value(value);
                    } else {
                        field_builder.append_null();
                    }
                } else {
                    return Err(
                        DatasourceWriterError::EntityWriterErrorUnknownStructField(
                            self.entity.name.clone(), key.clone(), field_index.clone()
                        )
                    )
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };

    (AppendWithException, $name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, field_index: usize, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<StructBuilder>().unwrap();

                let field_builder = builder.field_builder::<$builder_type>(field_index.clone());

                if let Some(field_builder) = field_builder {
                    if let Some(value) = value {
                        field_builder.append_value(value)?;
                    } else {
                        field_builder.append_null();
                    }
                } else {
                    return Err(
                        DatasourceWriterError::EntityWriterErrorUnknownStructField(
                            self.entity.name.clone(), key.clone(), field_index.clone()
                        )
                    )
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };
}

macro_rules! add_struct_list_primitive_append_method {
    ($name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, field_index: usize, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<StructBuilder>().unwrap();

                let field_builder = builder.field_builder::<$builder_type>(field_index);

                if let Some(field_builder) = field_builder {
                    if let Some(value) = value {
                        field_builder.values().append_value(value);
                        field_builder.append(true);
                    } else {
                        field_builder.append(false);
                    }
                } else {
                    return Err(
                        DatasourceWriterError::EntityWriterErrorUnknownStructField(
                            self.entity.name.clone(), key.clone(), field_index.clone()
                        )
                    )
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };

    (AppendWithException, $name: ident, $builder_type: ty, $type_: ty) => {
        pub fn $name(&mut self, key: String, field_index: usize, value: Option<$type_>) -> Result<(), DatasourceWriterError> {
            let builder = self.fields.get_mut(key.as_str());

            if let Some(builder) = builder {
                let mut builder = builder.write().unwrap();
                let builder = builder.as_any_mut().downcast_mut::<StructBuilder>().unwrap();

                let field_builder = builder.field_builder::<$builder_type>(field_index);

                if let Some(field_builder) = field_builder {
                    if let Some(value) = value {
                        field_builder.values().append_value(value)?;
                        field_builder.append(true);
                    } else {
                        field_builder.append(false);
                    }
                } else {
                    return Err(
                        DatasourceWriterError::EntityWriterErrorUnknownStructField(
                            self.entity.name.clone(), key.clone(), field_index.clone()
                        )
                    )
                }
            } else {
                return Err(
                    DatasourceWriterError::EntityWriterErrorUnknownField(
                        self.entity.name.clone(), key.clone()
                    )
                )
            }

            Ok(())
        }
    };
}

pub struct EntityWriter {
    context: WriterContext,
    entity: Entity,
    arrow_schema: Schema,

    fields: HashMap<String, RwLock<Box<dyn ArrayBuilder>>>,

    records_count: usize,

    start_block: Option<u64>,
    end_block: Option<u64>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    start_id: Option<u64>,
    end_id: Option<u64>,
}

fn make_list_builder(data_type: &DataType, capacity: usize) -> Result<Box<dyn ArrayBuilder>, DatasourceWriterError> {
    match data_type {
        DataType::Boolean => Ok(Box::new(
            ListBuilder::with_capacity(
                BooleanBuilder::with_capacity(capacity.clone()),
                capacity
            )
        )),
        DataType::Int8 => Ok(Box::new(
            ListBuilder::with_capacity(
                Int8Builder::with_capacity(capacity.clone()),
                capacity
            )
        )),
        DataType::Int16 => Ok(Box::new(
            ListBuilder::with_capacity(
                Int16Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Int32 => Ok(Box::new(
            ListBuilder::with_capacity(
                Int32Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Int64 => Ok(Box::new(
            ListBuilder::with_capacity(
                Int64Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::UInt8 => Ok(Box::new(
            ListBuilder::with_capacity(
                UInt8Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::UInt16 => Ok(Box::new(
            ListBuilder::with_capacity(
                UInt16Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::UInt32 => Ok(Box::new(
            ListBuilder::with_capacity(
                UInt32Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::UInt64 => Ok(Box::new(
            ListBuilder::with_capacity(
                UInt64Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Float32 => Ok(Box::new(
            ListBuilder::with_capacity(
                Float32Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Float64 => Ok(Box::new(
            ListBuilder::with_capacity(
                Float64Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Binary => {
            Ok(Box::new(
                ListBuilder::with_capacity(
                    BinaryBuilder::with_capacity(
                        1024, capacity
                    ),
                    capacity
                )
            ))
        },
        DataType::FixedSizeBinary(len) => {
            Ok(Box::new(
                ListBuilder::with_capacity(
                    FixedSizeBinaryBuilder::with_capacity(capacity, *len),
                    capacity
                )
            ))
        }
        DataType::Decimal128(precision, scale) => {
            Ok(Box::new(
                ListBuilder::with_capacity(
                    Decimal128Builder::with_capacity(capacity, *precision, *scale),
                    capacity
                )
            ))
        },
        DataType::Utf8 => {
            Ok(Box::new(
                ListBuilder::with_capacity(
                    StringBuilder::with_capacity(1024, capacity),
                    capacity
                )
            ))
        },
        DataType::Date32 => Ok(Box::new(
            ListBuilder::with_capacity(
                Date32Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Date64 => Ok(Box::new(
            ListBuilder::with_capacity(
                Date64Builder::with_capacity(capacity),
                capacity
            )
        )),
        DataType::Struct(fields) => {
            Ok(Box::new(
                ListBuilder::with_capacity(
                    StructBuilder::from_fields(fields.clone(), capacity),
                    capacity
                )
            ))
        },
        _ => {
            Err(DatasourceWriterError::UnsupportedDataType)
        }
    }
}

impl EntityWriter {
    pub fn new(context: WriterContext, entity: Entity) -> Result<Self, DatasourceWriterError> {
        let arrow_schema = crate::utils::schema::gen_arrow_schema(
            entity.fields_schema.clone()
        );

        let mut fields: HashMap<String, RwLock<Box<dyn ArrayBuilder>>> = HashMap::new();

        let mut has_block_index = false;
        let mut has_time_index = false;

        for field in &arrow_schema.fields {
            if field.name().eq(BLOCK_INDEX_COLUMN_NAME) {
                if field.data_type().equals_datatype(&DataType::UInt64) {
                    has_block_index = true;
                }
            }

            if field.name().eq(TIME_INDEX_COLUMN_NAME) {
                if field.data_type().equals_datatype(&DataType::UInt64) {
                    has_time_index = true;
                }
            }

            let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                DataType::List(field) => {
                    make_list_builder(
                        field.data_type(),
                        1024
                    )?
                },
                _ => {
                    make_builder(
                        field.data_type(),
                        1024
                    )
                }
            };

            fields.insert(field.name().clone(), RwLock::new(builder));
        }

        if !has_block_index && !has_time_index {
            return Err(DatasourceWriterError::NoFieldsBlockIndexAndTimeIndex);
        }

        return Ok(
            EntityWriter {
                context,
                entity,
                arrow_schema,
                fields,
                records_count: 0,

                start_block: None,
                end_block: None,
                start_time: None,
                end_time: None,
                start_id: None,
                end_id: None,
            }
        )
    }

    pub fn entity(&self) -> &Entity {
        &self.entity
    }

    pub fn records_count(&self) -> usize {
        self.records_count
    }

    pub fn set_start_block(&mut self, start_block: u64) {
        if self.start_block.is_none() {
            self.start_block = Some(start_block);
        }
    }

    pub fn set_end_block(&mut self, end_block: u64) {
        self.end_block = Some(end_block);
    }

    pub fn set_start_time(&mut self, start_time: u64) {
        if self.start_time.is_none() {
            self.start_time = Some(start_time);
        }
    }

    pub fn set_end_time(&mut self, end_time: u64) {
        self.end_time = Some(end_time);
    }

    pub fn set_start_id(&mut self, start_id: u64) {
        if self.start_id.is_none() {
            self.start_id = Some(start_id);
        }
    }

    pub fn set_end_id(&mut self, end_id: u64) {
        self.end_id = Some(end_id);
    }

    pub fn append_record(&mut self) {
        self.records_count += 1;
    }

    pub fn export(
        &mut self,
        export_iteration: usize,
    ) -> Result<LocalBlockItem, DatasourceWriterError> {
        let mut start_block: Option<String> = None;
        let mut end_block: Option<String> = None;
        let mut start_time: u64 = 0;
        let mut end_time: u64 = 0;
        let mut start_id: u64 = 0;
        let mut end_id: u64 = 0;

        if self.start_block.is_some() {
            start_block = Some(self.start_block.unwrap().to_string());
        }

        if self.end_block.is_some() {
            end_block = Some(self.end_block.unwrap().to_string());
        }

        if self.start_time.is_some() {
            start_time = self.start_time.unwrap();
        }

        if self.end_time.is_some() {
            end_time = self.end_time.unwrap();
        }

        if self.start_id.is_some() {
            start_id = self.start_id.unwrap();
        }

        if self.end_id.is_some() {
            end_id = self.end_id.unwrap();
        }

        let mut dictionary_enabled = false;

        for key in self.entity.encodings.keys() {
            dictionary_enabled = match self.entity.encodings.get(key).unwrap() {
                EntityFieldEncoding::PlainDictionary => true,
                EntityFieldEncoding::RLEDictionary => true,
                _ => false,
            };

            if dictionary_enabled {
                break;
            }
        }

        let mut writer_properties_builder = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(dictionary_enabled)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_pagesize_limit(self.context.parquet_page_size.clone() as usize)
            .set_max_row_group_size(self.context.parquet_row_group_size.clone() as usize)
            .set_compression(Compression::GZIP)
            .set_key_value_metadata(Some(
                vec![
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_ARROW_SCHEMA),
                        value: Some(self.entity.arrow_schema.clone()),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_PARQUET_SCHEMA),
                        value: Some(self.entity.parquet_schema.clone()),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_START_BLOCK_SCHEMA),
                        value: start_block.clone(),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_END_BLOCK_SCHEMA),
                        value: end_block.clone(),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_START_TIME_SCHEMA),
                        value: Some(start_time.to_string()),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_END_TIME_SCHEMA),
                        value: Some(end_time.to_string()),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_START_ID_SCHEMA),
                        value: Some(start_id.to_string()),
                    },
                    KeyValue {
                        key: String::from(PARQUET_METADATA_FIELD_END_ID_SCHEMA),
                        value: Some(end_id.to_string()),
                    },
                ]
            ));

        for key in self.entity.encodings.keys() {
            let encoding = self.entity.encodings.get(key).unwrap();

            let column_path = ColumnPath::from(
                key.split(".")
                    .map(|string| String::from(string))
                    .collect::<Vec<String>>()
            );

            match encoding {
                EntityFieldEncoding::PlainDictionary | EntityFieldEncoding::RLEDictionary => {
                    writer_properties_builder = writer_properties_builder
                        .set_column_dictionary_enabled(
                            column_path,
                            true,
                        );
                },
                _ => {
                    writer_properties_builder = writer_properties_builder
                        .set_column_dictionary_enabled(
                            column_path.clone(),
                            false,
                        );
                    writer_properties_builder = writer_properties_builder.set_column_encoding(
                        column_path,
                        match encoding {
                            EntityFieldEncoding::Plain => Encoding::PLAIN,
                            EntityFieldEncoding::PlainDictionary => Encoding::PLAIN_DICTIONARY,
                            EntityFieldEncoding::RLE => Encoding::RLE,
                            EntityFieldEncoding::BitPacked => Encoding::BIT_PACKED,
                            EntityFieldEncoding::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
                            EntityFieldEncoding::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
                            EntityFieldEncoding::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
                            EntityFieldEncoding::RLEDictionary => Encoding::RLE_DICTIONARY,
                            EntityFieldEncoding::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
                        },
                    );
                }
            };
        }

        let writer_properties = writer_properties_builder.build();

        let dir_path = Path::new(&self.context.data_store_path);

        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path).unwrap();
        }

        let path = dir_path
            .join(
                Path::new(
                    format!("{}-{}.parquet", self.entity.name, export_iteration).as_str()
                )
            );

        let file = File::create(&path).unwrap();

        // prepare data
        let mut fields_arrow_array: Vec<ArrayRef> = vec![];

        info!("Exporting entity `{}`", self.entity.name);

        for field in self.arrow_schema.fields.iter() {
            let key = field.name().clone();

            let mut array_builder = self.fields.get_mut(key.as_str()).unwrap().write().unwrap();

            let array = array_builder.finish();

            info!("Field `{}`, array len `{}`", field.name(), array.len());

            fields_arrow_array.push(array);
        }

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(self.arrow_schema.clone()),
            fields_arrow_array
        )?;

        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            batch.schema(),
            Some(writer_properties),
        )
            .expect("Unable to write file");

        writer.write(&batch)?;
        writer.close()?;

        let file_size = metadata(path.clone())?.len();

        let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        self.records_count = 0;

        self.start_block = None;
        self.end_block = None;
        self.start_time = None;
        self.end_time = None;
        self.start_id = None;
        self.end_id = None;

        Ok(LocalBlockItem {
            start_time,
            end_time,
            start_block,
            end_block,
            start_id,
            end_id,

            local_path: path.to_str().unwrap().to_string(),
            size: file_size,
            created_at,
        })
    }

    pub fn clean(&mut self) {
        self.records_count = 0;

        self.start_block = None;
        self.end_block = None;
        self.start_time = None;
        self.end_time = None;
        self.start_id = None;
        self.end_id = None;

        for builder in self.fields.values_mut() {
            builder.write().unwrap().finish();
        }
    }
}

impl EntityWriter {
    // Primitives
    add_primitive_append_method!(add_value_bool, BooleanBuilder, bool);

    add_primitive_append_method!(add_value_i8, Int8Builder, i8);
    add_primitive_append_method!(add_value_i16, Int16Builder, i16);
    add_primitive_append_method!(add_value_i32, Int32Builder, i32);
    add_primitive_append_method!(add_value_i64, Int64Builder, i64);
    add_primitive_append_method!(add_value_u8, UInt8Builder, u8);
    add_primitive_append_method!(add_value_u16, UInt16Builder, u16);
    add_primitive_append_method!(add_value_u32, UInt32Builder, u32);
    add_primitive_append_method!(add_value_u64, UInt64Builder, u64);
    add_primitive_append_method!(add_value_float32, Float32Builder, f32);
    add_primitive_append_method!(add_value_float64, Float64Builder, f64);

    add_primitive_append_method!(AppendWithException, add_value_decimal128, Decimal128Builder, Decimal128);
    add_primitive_append_method!(AppendWithException, add_value_decimal256, Decimal256Builder, &Decimal256);

    add_primitive_append_method!(add_value_date32, Date32Builder, i32);
    add_primitive_append_method!(add_value_date64, Date64Builder, i64);
    add_primitive_append_method!(add_value_time32_second, Time32SecondBuilder, i32);
    add_primitive_append_method!(add_value_time32_millisecond, Time32MillisecondBuilder, i32);
    add_primitive_append_method!(add_value_time64_microsecond, Time64MicrosecondBuilder, i64);
    add_primitive_append_method!(add_value_time64_nanosecond, Time64NanosecondBuilder, i64);

    add_primitive_append_method!(add_value_binary, BinaryBuilder, Vec<u8>);
    add_primitive_append_method!(AppendWithException, add_value_fixed_size_binary, FixedSizeBinaryBuilder, Vec<u8>);
    add_primitive_append_method!(add_value_large_binary, LargeBinaryBuilder, Vec<u8>);

    add_primitive_append_method!(add_value_utf8, StringBuilder, String);
    add_primitive_append_method!(add_value_large_utf8, LargeStringBuilder, String);

    // List
    add_list_primitive_append_method!(add_list_value_bool, ListBuilder<BooleanBuilder>, bool);

    add_list_primitive_append_method!(add_list_value_i8, ListBuilder<Int8Builder>, i8);
    add_list_primitive_append_method!(add_list_value_i16, ListBuilder<Int16Builder>, i16);
    add_list_primitive_append_method!(add_list_value_i32, ListBuilder<Int32Builder>, i32);
    add_list_primitive_append_method!(add_list_value_i64, ListBuilder<Int64Builder>, i64);
    add_list_primitive_append_method!(add_list_value_u8, ListBuilder<UInt8Builder>, u8);
    add_list_primitive_append_method!(add_list_value_u16, ListBuilder<UInt16Builder>, u16);
    add_list_primitive_append_method!(add_list_value_u32, ListBuilder<UInt32Builder>, u32);
    add_list_primitive_append_method!(add_list_value_u64, ListBuilder<UInt64Builder>, u64);
    add_list_primitive_append_method!(add_list_value_float32, ListBuilder<Float32Builder>, f32);
    add_list_primitive_append_method!(add_list_value_float64, ListBuilder<Float64Builder>, f64);

    add_list_primitive_append_method!(AppendWithException, add_list_value_decimal128, ListBuilder<Decimal128Builder>, Decimal128);

    // Not supported :(, trail ArrayBuilder is not implemented for Decimal256Builder
    // add_list_primitive_append_method!(add_list_value_decimal256, ListBuilder<Decimal256Builder>, &Decimal256);

    add_list_primitive_append_method!(add_list_value_date32, ListBuilder<Date32Builder>, i32);
    add_list_primitive_append_method!(add_list_value_date64, ListBuilder<Date64Builder>, i64);
    add_list_primitive_append_method!(add_list_time32_second, ListBuilder<Time32SecondBuilder>, i32);
    add_list_primitive_append_method!(add_list_time32_millisecond, ListBuilder<Time32MillisecondBuilder>, i32);
    add_list_primitive_append_method!(add_list_time64_microsecond, ListBuilder<Time64MicrosecondBuilder>, i64);
    add_list_primitive_append_method!(add_list_time64_nanosecond, ListBuilder<Time64NanosecondBuilder>, i64);

    add_list_primitive_append_method!(add_list_value_binary, ListBuilder<BinaryBuilder>, Vec<u8>);
    add_list_primitive_append_method!(AppendWithException, add_list_value_fixed_size_binary, ListBuilder<FixedSizeBinaryBuilder>, Vec<u8>);
    add_list_primitive_append_method!(add_list_value_large_binary, ListBuilder<LargeBinaryBuilder>, Vec<u8>);

    add_list_primitive_append_method!(add_list_value_utf8, ListBuilder<StringBuilder>, String);
    add_list_primitive_append_method!(add_list_value_large_utf8, ListBuilder<LargeStringBuilder>, String);

    // Struct
    add_struct_primitive_append_method!(add_struct_field_value_bool, BooleanBuilder, bool);

    add_struct_primitive_append_method!(add_struct_field_value_i8, Int8Builder, i8);
    add_struct_primitive_append_method!(add_struct_field_value_i16, Int16Builder, i16);
    add_struct_primitive_append_method!(add_struct_field_value_i32, Int32Builder, i32);
    add_struct_primitive_append_method!(add_struct_field_value_i64, Int64Builder, i64);
    add_struct_primitive_append_method!(add_struct_field_value_u8, UInt8Builder, u8);
    add_struct_primitive_append_method!(add_struct_field_value_u16, UInt16Builder, u16);
    add_struct_primitive_append_method!(add_struct_field_value_u32, UInt32Builder, u32);
    add_struct_primitive_append_method!(add_struct_field_value_u64, UInt64Builder, u64);
    add_struct_primitive_append_method!(add_struct_field_value_float32, Float32Builder, f32);
    add_struct_primitive_append_method!(add_struct_field_value_float64, Float64Builder, f64);

    add_struct_primitive_append_method!(AppendWithException, add_struct_field_value_decimal128, Decimal128Builder, Decimal128);

    // Not supported :(, trail ArrayBuilder is not implemented for Decimal256Builder
    // add_struct_primitive_append_method!(add_struct_field_value_decimal256, Decimal256Builder, &Decimal256);

    add_struct_primitive_append_method!(add_struct_field_value_date32, Date32Builder, i32);
    add_struct_primitive_append_method!(add_struct_field_value_date64, Date64Builder, i64);
    add_struct_primitive_append_method!(add_struct_field_value_time32_second, Time32SecondBuilder, i32);
    add_struct_primitive_append_method!(add_struct_field_value_time32_millisecond, Time32MillisecondBuilder, i32);
    add_struct_primitive_append_method!(add_struct_field_value_time64_microsecond, Time64MicrosecondBuilder, i64);
    add_struct_primitive_append_method!(add_struct_field_value_time64_nanosecond, Time64NanosecondBuilder, i64);

    add_struct_primitive_append_method!(add_struct_field_value_binary, BinaryBuilder, Vec<u8>);
    add_struct_primitive_append_method!(AppendWithException, add_struct_field_value_fixed_size_binary, FixedSizeBinaryBuilder, Vec<u8>);
    add_struct_primitive_append_method!(add_struct_field_value_large_binary, LargeBinaryBuilder, Vec<u8>);

    add_struct_primitive_append_method!(add_struct_field_value_utf8, StringBuilder, String);
    add_struct_primitive_append_method!(add_struct_field_value_large_utf8, LargeStringBuilder, String);

    add_struct_list_primitive_append_method!(add_struct_field_list_value_bool, ListBuilder<BooleanBuilder>, bool);

    add_struct_list_primitive_append_method!(add_struct_field_list_value_i8, ListBuilder<Int8Builder>, i8);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_i16, ListBuilder<Int16Builder>, i16);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_i32, ListBuilder<Int32Builder>, i32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_i64, ListBuilder<Int64Builder>, i64);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_u8, ListBuilder<UInt8Builder>, u8);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_u16, ListBuilder<UInt16Builder>, u16);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_u32, ListBuilder<UInt32Builder>, u32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_u64, ListBuilder<UInt64Builder>, u64);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_float32, ListBuilder<Float32Builder>, f32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_float64, ListBuilder<Float64Builder>, f64);

    add_struct_list_primitive_append_method!(AppendWithException, add_struct_field_list_value_decimal128, ListBuilder<Decimal128Builder>, Decimal128);

    // Not supported :(, trail ArrayBuilder is not implemented for Decimal256Builder
    // add_struct_list_primitive_append_method!(add_struct_field_list_value_decimal256, ListBuilder<Decimal256Builder>, &Decimal256);

    add_struct_list_primitive_append_method!(add_struct_field_list_value_date32, ListBuilder<Date32Builder>, i32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_date64, ListBuilder<Date64Builder>, i64);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_time32_second, ListBuilder<Time32SecondBuilder>, i32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_time32_millisecond, ListBuilder<Time32MillisecondBuilder>, i32);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_time64_microsecond, ListBuilder<Time64MicrosecondBuilder>, i64);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_time64_nanosecond, ListBuilder<Time64NanosecondBuilder>, i64);

    add_struct_list_primitive_append_method!(add_struct_field_list_value_binary, ListBuilder<BinaryBuilder>, Vec<u8>);
    add_struct_list_primitive_append_method!(AppendWithException, add_struct_field_list_value_fixed_size_binary, ListBuilder<FixedSizeBinaryBuilder>, Vec<u8>);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_large_binary, ListBuilder<LargeBinaryBuilder>, Vec<u8>);

    add_struct_list_primitive_append_method!(add_struct_field_list_value_utf8, ListBuilder<StringBuilder>, String);
    add_struct_list_primitive_append_method!(add_struct_field_list_value_large_utf8, ListBuilder<LargeStringBuilder>, String);
}

unsafe impl Sync for EntityWriter {}