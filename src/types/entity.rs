use std::collections::HashMap;
use log::error;
use serde::{Serialize, Deserialize};
use crate::types::error::DatasourceWriterError;
use crate::types::value::TimeUnit;
use crate::utils::schema::{stringify_arrow_schema, stringify_parquet_schema};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entity {
    pub name: String,
    pub relations: Vec<EntityRelation>,
    pub fields_schema: Vec<EntityField>,
    pub parquet_schema: String,
    pub arrow_schema: String,
    pub encodings: HashMap<String, EntityFieldEncoding>,
}

impl Entity {
    pub fn new(
        name: String,
        relations: Vec<EntityRelation>,
        fields_schema: Vec<EntityField>,
        encodings: HashMap<String, EntityFieldEncoding>,
    ) -> anyhow::Result<Entity> {
        let parquet_schema = stringify_parquet_schema(fields_schema.clone())?;
        let arrow_schema = stringify_arrow_schema(fields_schema.clone())?;

        Ok(
            Self {
                name,
                relations,
                fields_schema,
                parquet_schema,
                arrow_schema,
                encodings,
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityFieldType {
    #[serde(rename = "boolean")]
    Boolean(bool),
    #[serde(rename = "int8")]
    Int8(bool),
    #[serde(rename = "int16")]
    Int16(bool),
    #[serde(rename = "int32")]
    Int32(bool),
    #[serde(rename = "int64")]
    Int64(bool),
    #[serde(rename = "uint8")]
    Uint8(bool),
    #[serde(rename = "uint16")]
    Uint16(bool),
    #[serde(rename = "uint32")]
    Uint32(bool),
    #[serde(rename = "uint64")]
    Uint64(bool),
    #[serde(rename = "float32")]
    Float32(bool),
    #[serde(rename = "float64")]
    Float64(bool),
    #[serde(rename = "decimal128")]
    Decimal128(bool, u8, u8),
    #[serde(rename = "decimal256")]
    Decimal256(bool, u8, u8),
    #[serde(rename = "biginteger")]
    BigInteger(bool, u16),
    #[serde(rename = "bigdecimal")]
    BigDecimal(bool, u8, u8),
    #[serde(rename = "time32")]
    Time32(bool, TimeUnit),
    #[serde(rename = "time64")]
    Time64(bool, TimeUnit),
    #[serde(rename = "date32")]
    Date32(bool),
    #[serde(rename = "date64")]
    Date64(bool),
    #[serde(rename = "binary")]
    Binary(bool),
    #[serde(rename = "fixedsizebinary")]
    FixedSizeBinary(bool, i32),
    #[serde(rename = "largebinary")]
    LargeBinary(bool),
    #[serde(rename = "string")]
    String(bool),
    #[serde(rename = "largestring")]
    LargeString(bool),
    #[serde(rename = "list")]
    List(bool, Box<EntityFieldType>),
    #[serde(rename = "struct")]
    Struct_(bool, Vec<EntityField>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityField {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: EntityFieldType,
}

impl EntityField {
    pub fn create_field(name: String, type_: EntityFieldType) -> EntityField {
        Self {
            name,
            type_,
        }
    }

    pub fn create_list_field(name: String, required: bool, type_: EntityFieldType) -> Result<EntityField, DatasourceWriterError> {
        let field_required = match type_ {
            EntityFieldType::Boolean(required) => required,
            EntityFieldType::Int8(required) => required,
            EntityFieldType::Int16(required) => required,
            EntityFieldType::Int32(required) => required,
            EntityFieldType::Int64(required) => required,
            EntityFieldType::Uint8(required) => required,
            EntityFieldType::Uint16(required) => required,
            EntityFieldType::Uint32(required) => required,
            EntityFieldType::Uint64(required) => required,
            EntityFieldType::Float32(required) => required,
            EntityFieldType::Float64(required) => required,
            EntityFieldType::Decimal128(required, _, _) => required,
            EntityFieldType::Decimal256(required, _, _) => required,
            EntityFieldType::BigInteger(required, _) => required,
            EntityFieldType::BigDecimal(required, _, _) => required,
            EntityFieldType::Time32(required, _) => required,
            EntityFieldType::Time64(required, _) => required,
            EntityFieldType::Date32(required) => required,
            EntityFieldType::Date64(required) => required,
            EntityFieldType::Binary(required) => required,
            EntityFieldType::FixedSizeBinary(required, _) => required,
            EntityFieldType::LargeBinary(required) => required,
            EntityFieldType::String(required) => required,
            EntityFieldType::LargeString(required) => required,
            EntityFieldType::List(required, _) => required,
            EntityFieldType::Struct_(required, _) => required,
        };

        if field_required {
            error!("List field should be not required, because of https://github.com/apache/arrow-rs/blob/master/arrow-array/src/builder/generic_list_builder.rs#L132");

            return Err(
                DatasourceWriterError::ListFieldIsNotOptional(name.clone())
            );
        }

        Ok(
            Self {
                name,
                type_: EntityFieldType::List(required, Box::new(type_)),
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EntityRelationType {
    #[serde(rename = "one-to-one")]
    OneToOne,
    #[serde(rename = "one-to-many")]
    OneToMany,
    #[serde(rename = "many-to-one")]
    ManyToOne,
    #[serde(rename = "many-to-many")]
    ManyToMany,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRelation {
    pub local_field_name: String,
    #[serde(rename = "entity")]
    pub remote_entity_name: String,
    pub remote_field_name: String,
    #[serde(rename = "type")]
    pub relation_type: EntityRelationType,
    pub eager_fetch: bool,
    pub nullable: bool
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityFieldEncoding {
    Plain,
    PlainDictionary,
    RLE,
    BitPacked,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    RLEDictionary,
    ByteStreamSplit,
}