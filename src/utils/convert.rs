use std::collections::HashMap;
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, Field, TimeUnit as ArrowTimeUnit};
use datafusion::arrow::array::{
    ArrayRef,
    BinaryArray, BooleanArray, Date32Array, Date64Array, DecimalArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, ListArray, StringArray,
    StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Operator;
use datafusion::prelude::{col, Expr, Expr::Literal};
use datafusion::common::ScalarValue;
use log::{error, warn};
use crate::types::error::ConvertError;
use crate::types::filter::{Expression, ExpressionComparison, FilterFieldRef, FilterLogicalExpressionType, FilterOperator};
use crate::constant::{BLOCK_INDEX_COLUMN_NAME, ENTITY_ID_COLUMN_NAME};
use crate::types::value::{PrimitiveDecimal128, PrimitiveDecimal256, TimeUnit, Value};

pub fn convert_array_ref_value(array_ref: &ArrayRef, index: usize) -> Result<Value, ConvertError> {
    let is_valid = array_ref.is_valid(index);

    if !is_valid {
        return Err(ConvertError::InvalidIndexError(index.clone()));
    }

    let is_null = array_ref.is_null(index);

    if is_null {
        return Ok(Value::Null);
    }

    let data_type = array_ref.data_type();

    let value = match data_type {
        DataType::Boolean => Value::Boolean(array_ref.as_any().downcast_ref::<BooleanArray>().unwrap().value(index)),

        DataType::Int8 => Value::Int8(array_ref.as_any().downcast_ref::<Int8Array>().unwrap().value(index)),
        DataType::Int16 => Value::Int16(array_ref.as_any().downcast_ref::<Int16Array>().unwrap().value(index)),
        DataType::Int32 => Value::Int32(array_ref.as_any().downcast_ref::<Int32Array>().unwrap().value(index)),
        DataType::Int64 => Value::Int64(array_ref.as_any().downcast_ref::<Int64Array>().unwrap().value(index)),

        DataType::UInt8 => Value::UInt8(array_ref.as_any().downcast_ref::<UInt8Array>().unwrap().value(index)),
        DataType::UInt16 => Value::UInt16(array_ref.as_any().downcast_ref::<UInt16Array>().unwrap().value(index)),
        DataType::UInt32 => Value::UInt32(array_ref.as_any().downcast_ref::<UInt32Array>().unwrap().value(index)),
        DataType::UInt64 => Value::UInt64(array_ref.as_any().downcast_ref::<UInt64Array>().unwrap().value(index)),

        DataType::Float32 => Value::Float32(array_ref.as_any().downcast_ref::<Float32Array>().unwrap().value(index)),
        DataType::Float64 => Value::Float64(array_ref.as_any().downcast_ref::<Float64Array>().unwrap().value(index)),

        DataType::Decimal128(precision, scale) => {
            let array_ref = array_ref.as_any().downcast_ref::<DecimalArray<Decimal128Type>>().unwrap();

            let value = array_ref.value(index);

            let value = value.raw_value();

            Value::Decimal128(
                PrimitiveDecimal128{
                    scale: scale.clone(),
                    precision: precision.clone(),
                    value: value.clone()
                }
            )
        },

        DataType::Decimal256(precision, scale) => {
            let array_ref = array_ref.as_any().downcast_ref::<DecimalArray<Decimal256Type>>().unwrap();

            let decimal = array_ref.value(index);

            let value = decimal.raw_value();

            Value::Decimal256(
                PrimitiveDecimal256 {
                    precision: precision.clone(),
                    scale: scale.clone(),
                    value: value.clone()
                }
            )
        },

        DataType::Date32 => Value::Date32(array_ref.as_any().downcast_ref::<Date32Array>().unwrap().value(index)),
        DataType::Date64 => Value::Date64(array_ref.as_any().downcast_ref::<Date64Array>().unwrap().value(index)),
        DataType::Time32(unit) => {
            match unit {
                ArrowTimeUnit::Second => {
                    let value = array_ref.as_any().downcast_ref::<Time32SecondArray>().unwrap().value(index);

                    Value::Time32(unit.clone().into(), value)
                },
                ArrowTimeUnit::Millisecond => {
                    let value = array_ref.as_any().downcast_ref::<Time32MillisecondArray>().unwrap().value(index);

                    Value::Time32(unit.clone().into(), value)
                },
                _ => {
                    return Err(ConvertError::UnsupportedTimeUnit(unit.clone().into()))
                }
            }
        },
        DataType::Time64(unit) => {
            match unit {
                ArrowTimeUnit::Microsecond => {
                    let value = array_ref.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap().value(index);

                    Value::Time64(unit.clone().into(), value)
                },
                ArrowTimeUnit::Nanosecond => {
                    let value = array_ref.as_any().downcast_ref::<Time64NanosecondArray>().unwrap().value(index);

                    Value::Time64(unit.clone().into(), value)
                },
                _ => {
                    return Err(ConvertError::UnsupportedTimeUnit(unit.clone().into()))
                }
            }
        },

        DataType::Binary => Value::Binary(array_ref.as_any().downcast_ref::<BinaryArray>().unwrap().value(index).into()),
        DataType::FixedSizeBinary(size) => Value::FixedSizeBinary(
            size.clone(), array_ref.as_any().downcast_ref::<BinaryArray>().unwrap().value(index).into()
        ),
        DataType::LargeBinary => Value::LargeBinary(array_ref.as_any().downcast_ref::<BinaryArray>().unwrap().value(index).into()),

        DataType::Utf8 => Value::Utf8(array_ref.as_any().downcast_ref::<StringArray>().unwrap().value(index).into()),
        DataType::LargeUtf8 => Value::LargeUtf8(array_ref.as_any().downcast_ref::<LargeStringArray>().unwrap().value(index).into()),
        DataType::List(_) => {
            let array_ref = array_ref.as_any().downcast_ref::<ListArray>().unwrap().value(index);

            let mut values = vec![];

            for i in 0..array_ref.len() {
                values.push(
                    convert_array_ref_value(&array_ref, i)?
                );
            }

            Value::List(values)
        },
        DataType::Struct(fields) => {
            let struct_array_ref = array_ref.as_any().downcast_ref::<StructArray>().unwrap();

            let mut map: HashMap<String, Value> = HashMap::new();

            for field in fields {
                let column_name = field.name();

                let column_array_ref = struct_array_ref.column_by_name(column_name).unwrap();

                map.insert(
                    column_name.clone(),
                    convert_array_ref_value(
                        column_array_ref,
                        index.clone()
                    )?
                );
            }

            Value::Struct(map)
        }
        _ => {
            return Err(ConvertError::UnsupportedDataType);
        }
    };

    Ok(value)
}

pub fn convert_entities_record_batch(
    entities_records_batch: &HashMap<String, Vec<RecordBatch>>
) -> Result<(
    HashMap<String, Vec<Value>>,
    HashMap<String, HashMap<u64, usize>>
), ConvertError> {
    let mut data: HashMap<String, Vec<HashMap<String, Value>>> = HashMap::new();
    let mut entities_ids: HashMap<String, HashMap<u64, usize>> = HashMap::new();

    for (entity_name, records_batch) in entities_records_batch {
        if data.contains_key(entity_name) {
            data.insert(entity_name.clone(), Vec::new());
            entities_ids.insert(entity_name.clone(),HashMap::new());
        }

        // entity name => array of map(string => value)

        let fields_map = data.get_mut(entity_name).unwrap();
        let records_map = entities_ids.get_mut(entity_name).unwrap();

        let mut record_index = 0;

        for batch_index in 0..records_batch.len() {
            let batch = &records_batch[batch_index];
            let schema_ref = batch.schema();

            let mut column_id = 0;

            let mut first_field = true;

            for field in schema_ref.fields.iter() {
                // iterate over array

                let column_array = batch.column(column_id);

                column_id += 1;

                for index in 0..column_array.len() {
                    if first_field {
                        fields_map.push(HashMap::new());
                    }

                    let fields = fields_map.get_mut(record_index).unwrap();

                    let value = convert_array_ref_value(
                        column_array,
                        index
                    )?;

                    if field.name().eq(ENTITY_ID_COLUMN_NAME) {
                        let id = match &value {
                            Value::UInt64(value) => value.clone(),
                            _ => {
                                return Err(ConvertError::ColumnIdHasIncorrectType)
                            }
                        };

                        records_map.insert(
                            id,
                            record_index
                        );

                        record_index += 1;
                    }

                    fields.insert(
                        field.name().clone(),
                        value
                    );
                }

                if first_field {
                    first_field = false;
                }
            }
        }
    }

    let mut struct_data: HashMap<String, Vec<Value>> = HashMap::new();

    for (entity_name, records) in data {
        let mut struct_records = vec![];

        for record in records {
            struct_records.push(
                Value::Struct(record)
            );
        }

        struct_data.insert(entity_name, struct_records);
    }

    Ok((struct_data, entities_ids))
}

pub fn convert_filter_field_value(value: &Value) -> Result<ScalarValue, ConvertError> {
    match value {
        Value::Result(_ok, _err, _value) => {
            error!("Can't convert filter field ResultValue");
            Err(ConvertError::UnsupportedDataType)
        },
        Value::Null => Ok(ScalarValue::Null),
        Value::Boolean(bool) => Ok(ScalarValue::Boolean(Some(bool.clone()))),
        Value::Int8(i8) => Ok(ScalarValue::Int8(Some(i8.clone()))),
        Value::Int16(i16) => Ok(ScalarValue::Int16(Some(i16.clone()))),
        Value::Int32(i32) => Ok(ScalarValue::Int32(Some(i32.clone()))),
        Value::Int64(i64) => Ok(ScalarValue::Int64(Some(i64.clone()))),
        Value::UInt8(u8) => Ok(ScalarValue::UInt8(Some(u8.clone()))),
        Value::UInt16(u16) => Ok(ScalarValue::UInt16(Some(u16.clone()))),
        Value::UInt32(u32) => Ok(ScalarValue::UInt32(Some(u32.clone()))),
        Value::UInt64(u64) => Ok(ScalarValue::UInt64(Some(u64.clone()))),
        Value::Float32(f32) => Ok(ScalarValue::Float32(Some(f32.clone()))),
        Value::Float64(f64) => Ok(ScalarValue::Float64(Some(f64.clone()))),

        Value::BigInteger(bits, vec) => {
            let mut arr = Vec::from(bits.to_le_bytes());
            arr.extend(vec);

            Ok(ScalarValue::Binary(Some(arr)))
        },
        Value::BigDecimal(scale, precision, vec) => {
            let mut arr = Vec::from(scale.to_le_bytes());
            arr.extend(precision.to_le_bytes());
            arr.extend(vec);

            Ok(ScalarValue::Binary(Some(arr)))
        },

        Value::Date32(i32) => Ok(ScalarValue::Date32(Some(i32.clone()))),
        Value::Date64(i64) => Ok(ScalarValue::Date64(Some(i64.clone()))),
        Value::Binary(vec) => Ok(ScalarValue::Binary(Some(vec.clone()))),
        Value::FixedSizeBinary(_, vec) => Ok(ScalarValue::Binary(Some(vec.clone()))),
        Value::LargeBinary(vec) => Ok(ScalarValue::LargeBinary(Some(vec.clone()))),
        Value::Utf8(str) => Ok(ScalarValue::Utf8(Some(str.clone()))),
        Value::LargeUtf8(str) => Ok(ScalarValue::LargeUtf8(Some(str.clone()))),
        Value::List(vec) => {
            let mut values = vec![];

            for value in vec {
                values.push(convert_filter_field_value(value)?);
            }

            Ok(ScalarValue::List(
                Some(values),
                Box::new(Field::new(
                    "",
                    DataType::Null,
                    true,
                ))
            ))
        },
        Value::Decimal128(decimal) => Ok(ScalarValue::Decimal128(
            Some(i128::from_be_bytes(decimal.value.clone())),
            decimal.precision.clone(),
            decimal.scale.clone(),
        )),
        Value::Decimal256(decimal) => {
            warn!("ScalarValue has no Decimal256 type, converting to Binary");

            Ok(ScalarValue::Binary(Some(decimal.value.clone().into())))
        },
        Value::Time32(unit, time) => {
            match unit {
                TimeUnit::Second => Ok(ScalarValue::TimestampSecond(Some(time.clone() as i64), None)),
                TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(Some(time.clone() as i64), None)),
                TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(Some(time.clone() as i64), None)),
                TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(Some(time.clone() as i64), None)),
            }
        },
        Value::Time64(unit, time) => {
            match unit {
                TimeUnit::Second => Ok(ScalarValue::TimestampSecond(Some(time.clone()), None)),
                TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(Some(time.clone()), None)),
                TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(Some(time.clone()), None)),
                TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(Some(time.clone()), None)),
            }
        },
        Value::Struct(_map) => {
            warn!("Can't convert filter field to ScalarValue::Struct, applying Null");
            Err(ConvertError::UnsupportedDataType)
        }
    }
}

pub fn convert_filter_field_value_expr(field_value: &Value) -> Result<Expr, ConvertError> {
    Ok(
        Expr::Literal(
            convert_filter_field_value(field_value)?
        )
    )
}

pub fn convert_comparison_filter(comparison: &ExpressionComparison) -> Result<Expr, ConvertError> {
    let field_ref = &comparison.field_ref;
    let field_value = &comparison.value;

    let col_value = convert_filter_field_value_expr(field_value)?;

    let col_name = match field_ref {
        FilterFieldRef::IpfsColumn(column) => column,
    };

    Ok(
        match comparison.operator {
            FilterOperator::Equals => col(col_name).eq(col_value),
            FilterOperator::NotEquals => col(col_name).not_eq(col_value),
            FilterOperator::GreaterThan => col(col_name).gt(col_value),
            FilterOperator::GreaterOrEqualThan => col(col_name).gt_eq(col_value),
            FilterOperator::LessThan => col(col_name).lt(col_value),
            FilterOperator::LessOrEqualThan => col(col_name).lt_eq(col_value),
            FilterOperator::In => {
                col(col_name).in_list(
                    match col_value {
                        Literal(scalar_value) => {
                            match scalar_value {
                                ScalarValue::List(list, _) =>
                                    list.unwrap().iter().map(
                                        |value| Expr::Literal(value.clone())
                                    ).collect(),
                                _ => todo!("IPFS::convert_comparison_filter Operator::In Not list value")
                            }
                        },
                        _ => todo!("IPFS::convert_comparison_filter not literal for Operator::In")
                    },
                    false
                )
            },
            FilterOperator::NotIn => {
                col(col_name).in_list(
                    match col_value {
                        Literal(scalar_value) => {
                            match scalar_value {
                                ScalarValue::List(list, _) =>
                                    list.unwrap().iter().map(
                                        |value| Expr::Literal(value.clone())
                                    ).collect(),
                                _ => todo!("IPFS::convert_comparison_filter Operator::In Not list value")
                            }
                        },
                        _ => todo!("IPFS::convert_comparison_filter not literal for Operator::In")
                    },
                    true
                )
            },
        }
    )
}

pub fn convert_filter(
    filter: &Expression
) -> Result<Expr, ConvertError> {
    Ok(match filter {
        Expression::Comparison(comparison) => {
            convert_comparison_filter(
                comparison
            )?
        },
        Expression::Conditional(conditional) => {
            let left = convert_filter(
                &(&*conditional.left).clone()
            )?;
            let right = convert_filter(
                &(&*conditional.right).clone()
            )?;

            Expr::BinaryExpr {
                left: Box::new(left),
                right: Box::new(right),
                op: match conditional.operator {
                    FilterLogicalExpressionType::And => Operator::And,
                    FilterLogicalExpressionType::Or => Operator::Or,
                }
            }
        },
    })
}


pub fn convert_filters(
    filters: &Vec<Option<Expression>>,
) -> Result<Option<Expr>, ConvertError> {
    if filters.len() > 0 {
        let first_filter = &filters[0];

        if first_filter.is_none() {
            return Ok(None);
        }

        let mut expr: Expr = convert_filter(
            first_filter.as_ref().unwrap(),
        )?;

        if filters.len() > 1 {
            for i in 1..filters.len() {
                let filter = &filters[i];
                if filter.is_none() {
                    return Ok(None);
                }

                let converted_expression = convert_filter(
                    filter.as_ref().unwrap(),
                )?;

                expr = expr.or(converted_expression);
            }
        }

        Ok(Some(expr))
    } else {
        Ok(None)
    }
}

pub fn extend_filter_with_scan_block(
    expression:  &Option<Expr>,
    start_block: u64,
    end_block: u64,
) -> Result<Expr, ConvertError> {
    let column_name = BLOCK_INDEX_COLUMN_NAME;

    let mut new_expression = col(column_name).gt_eq(
        Expr::Literal(
            ScalarValue::UInt64(
                Some(start_block)
            )
        )
    ).and(
        col(column_name).lt(
            Expr::Literal(
                ScalarValue::UInt64(
                    Some(end_block)
                )
            )
        )
    );

    if expression.is_some() {
        new_expression = new_expression.and(expression.as_ref().unwrap().clone());
    }

    Ok(new_expression)
}