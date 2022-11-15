use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use serde::{Serialize, Deserialize};
use datafusion::arrow::datatypes::TimeUnit as ArrowTimeUnit;
use datafusion::arrow::util::decimal::{Decimal128, Decimal256};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<ArrowTimeUnit> for TimeUnit {
    fn from(unit: ArrowTimeUnit) -> Self {
        match unit {
            ArrowTimeUnit::Second => TimeUnit::Second,
            ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
            ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
            ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimitiveDecimal128 {
    pub precision: u8,
    pub scale: u8,
    pub value: [u8; 16],
}

impl From<PrimitiveDecimal128> for Decimal128 {
    fn from(primitive: PrimitiveDecimal128) -> Self {
        Decimal128::new(
            primitive.precision,
            primitive.scale,
            &primitive.value,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimitiveDecimal256 {
    pub precision: u8,
    pub scale: u8,
    pub value: [u8; 32],
}

impl From<PrimitiveDecimal256> for Decimal256 {
    fn from(primitive: PrimitiveDecimal256) -> Self {
        Decimal256::new(
            primitive.precision,
            primitive.scale,
            &primitive.value,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Result(bool, Box<Value>, Box<Value>), // ok, err, value

    Null,

    Boolean(bool),

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),

    Float32(f32),
    Float64(f64),

    Decimal128(PrimitiveDecimal128),
    Decimal256(PrimitiveDecimal256),

    BigInteger(u16, Vec<u8>), // bits, value
    BigDecimal(u8, u8, Vec<u8>), // scale, precision, value

    Time32(TimeUnit, i32),
    Time64(TimeUnit, i64),

    Date32(i32),
    Date64(i64),

    Binary(Vec<u8>),
    FixedSizeBinary(i32, Vec<u8>), // size, value
    LargeBinary(Vec<u8>),

    Utf8(String),
    LargeUtf8(String),

    List(Vec<Value>),
    Struct(HashMap<String, Value>),
}
