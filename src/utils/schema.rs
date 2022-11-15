use std::io::BufWriter;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::arrow_to_parquet_schema;
use datafusion::parquet::schema::types::SchemaDescriptor;
use datafusion::arrow::ipc::writer;
use datafusion::parquet::schema::printer::print_schema;
use crate::types::entity::{EntityField, EntityFieldType};
use crate::types::value::TimeUnit;

fn get_parquet_time_unit(unit: TimeUnit) -> datafusion::arrow::datatypes::TimeUnit {
    match unit {
        TimeUnit::Second => datafusion::arrow::datatypes::TimeUnit::Second,
        TimeUnit::Millisecond => datafusion::arrow::datatypes::TimeUnit::Millisecond,
        TimeUnit::Microsecond => datafusion::arrow::datatypes::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => datafusion::arrow::datatypes::TimeUnit::Nanosecond,
    }
}

fn convert_field(field: EntityField) -> Field {
    match field.type_ {
        EntityFieldType::Boolean(required) => Field::new(
            field.name.as_str(),
            DataType::Boolean,
            !required
        ),
        EntityFieldType::Int8(required) => Field::new(
            field.name.as_str(),
            DataType::Int8,
            !required
        ),
        EntityFieldType::Int16(required) => Field::new(
            field.name.as_str(),
            DataType::Int16,
            !required
        ),
        EntityFieldType::Int32(required) => Field::new(
            field.name.as_str(),
            DataType::Int32,
            !required
        ),
        EntityFieldType::Int64(required) => Field::new(
            field.name.as_str(),
            DataType::Int64,
            !required
        ),
        EntityFieldType::Uint8(required) => Field::new(
            field.name.as_str(),
            DataType::UInt8,
            !required
        ),
        EntityFieldType::Uint16(required) => Field::new(
            field.name.as_str(),
            DataType::UInt16,
            !required
        ),
        EntityFieldType::Uint32(required) => Field::new(
            field.name.as_str(),
            DataType::UInt32,
            !required
        ),
        EntityFieldType::Uint64(required) => Field::new(
            field.name.as_str(),
            DataType::UInt64,
            !required
        ),
        EntityFieldType::Float32(required) => Field::new(
            field.name.as_str(),
            DataType::Float32,
            !required
        ),
        EntityFieldType::Float64(required) => Field::new(
            field.name.as_str(),
            DataType::Float64,
            !required
        ),
        EntityFieldType::Decimal128(required, precision, scale) => Field::new(
            field.name.as_str(),
            DataType::Decimal128(precision, scale),
            !required
        ),
        EntityFieldType::Decimal256(required, precision, scale) => Field::new(
            field.name.as_str(),
            DataType::Decimal256(precision, scale),
            !required
        ),
        EntityFieldType::BigInteger(required, _bits) => Field::new(
            field.name.as_str(),
            DataType::Binary,
            !required
        ),
        EntityFieldType::BigDecimal(required, _precision, _scale) => Field::new(
            field.name.as_str(),
            DataType::Binary,
            !required
        ),
        EntityFieldType::Time32(required, unit) => Field::new(
            field.name.as_str(),
            DataType::Time32(get_parquet_time_unit(unit)),
            !required
        ),
        EntityFieldType::Time64(required, unit) => Field::new(
            field.name.as_str(),
            DataType::Time64(get_parquet_time_unit(unit)),
            !required
        ),
        EntityFieldType::Date32(required) => Field::new(
            field.name.as_str(),
            DataType::Date32,
            !required
        ),
        EntityFieldType::Date64(required) => Field::new(
            field.name.as_str(),
            DataType::Date64,
            !required
        ),
        EntityFieldType::Binary(required) => Field::new(
            field.name.as_str(),
            DataType::Binary,
            !required
        ),
        EntityFieldType::FixedSizeBinary(required, size) => Field::new(
            field.name.as_str(),
            DataType::FixedSizeBinary(size),
            !required
        ),
        EntityFieldType::LargeBinary(required) => Field::new(
            field.name.as_str(),
            DataType::LargeBinary,
            !required
        ),
        EntityFieldType::String(required) => Field::new(
            field.name.as_str(),
            DataType::Utf8,
            !required
        ),
        EntityFieldType::LargeString(required) => Field::new(
            field.name.as_str(),
            DataType::LargeUtf8,
            !required
        ),
        EntityFieldType::List(required, type_) => Field::new(
            field.name.as_str(),
            DataType::List(Box::new(
                convert_field(
                    EntityField {
                        name: String::from("item"),
                        type_: *type_
                    }
                )
            )),
            !required
        ),
        EntityFieldType::Struct_(required, entity_fields) => {
            let mut converted_fields: Vec<Field> = vec![];

            for entity_field in entity_fields {
                converted_fields.push(
                    convert_field(entity_field)
                )
            }

            Field::new(
                field.name.as_str(),
                DataType::Struct(converted_fields),
                !required
            )
        },
    }
}

pub fn gen_arrow_schema(fields: Vec<EntityField>) -> Schema {
    let mut converted_fields: Vec<Field> = vec![];

    for field in fields {
        let converted_field = convert_field(field);

        converted_fields.push(converted_field);
    }

    let arrow_schema = Schema::new(converted_fields);

    arrow_schema
}

// Copy-paste from crate `parquet`
pub fn stringify_arrow_schema(fields: Vec<EntityField>) -> anyhow::Result<String> {
    let arrow_schema = gen_arrow_schema(fields);

    let options = writer::IpcWriteOptions::default();
    let data_gen = writer::IpcDataGenerator::default();
    let mut serialized_schema = data_gen.schema_to_bytes(&arrow_schema, &options);

    // manually prepending the length to the schema as arrow uses the legacy IPC format
    // TODO: change after addressing ARROW-9777
    let schema_len = serialized_schema.ipc_message.len();
    let mut len_prefix_schema = Vec::with_capacity(schema_len + 8);
    len_prefix_schema.append(&mut vec![255u8, 255, 255, 255]);
    len_prefix_schema.append((schema_len as u32).to_le_bytes().to_vec().as_mut());
    len_prefix_schema.append(&mut serialized_schema.ipc_message);

    Ok(base64::encode(&len_prefix_schema))
}

pub fn gen_parquet_schema(fields: Vec<EntityField>) -> anyhow::Result<SchemaDescriptor> {
    let arrow_schema = gen_arrow_schema(fields);

    let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;

    Ok(parquet_schema)
}

pub fn stringify_parquet_schema(fields: Vec<EntityField>) -> anyhow::Result<String> {
    let parquet_schema = gen_parquet_schema(fields)?;

    let mut buf = BufWriter::new(Vec::new());

    print_schema(&mut buf, parquet_schema.root_schema());

    let bytes = buf.into_inner()?;
    let stringified_arrow_schema = String::from_utf8(bytes)?;

    Ok(stringified_arrow_schema)
}