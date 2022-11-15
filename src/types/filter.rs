use serde::{Serialize, Deserialize};
use crate::types::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Comparison(ExpressionComparison),
    Conditional(Box<FilterConditional>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConditional {
    pub operator: FilterLogicalExpressionType,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterLogicalExpressionType {
    And,
    Or
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpressionComparison {
    pub field_ref: FilterFieldRef,
    pub operator: FilterOperator,
    pub value: Value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterFieldRef {
    IpfsColumn(String)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    In,
    NotIn,
    LessThan,
    LessOrEqualThan,
    GreaterThan,
    GreaterOrEqualThan,
}
