use std::str::FromStr;

use crate::Result;

use crate::CsvColError;

#[derive(Clone, Debug)]
enum Operator {
    Gt,
    Ge,
    Lt,
    Le,
    Ne,
    Eq,
}

impl Operator {
    fn try_parse(value: &str) -> Result<Self> {
        let result = match value.trim() {
            ">=" => Self::Ge,
            "<=" => Self::Le,
            "==" => Self::Eq,
            "!=" => Self::Ne,
            "<" => Self::Lt,
            ">" => Self::Gt,
            _ => return Err(CsvColError::Filter("bad operator".to_string())),
        };
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub enum Operand {
    Column(String),
    Number(i64),
}

#[derive(Clone, Debug)]
pub struct Expression {
    left: Operand,
    operator: Operator,
    right: Operand,
}

impl Expression {
    pub fn check_by_name(&self, column_name: &str) -> bool {
        if let Operand::Column(value) = &self.left
            && value == column_name
        {
            true
        } else if let Operand::Column(value) = &self.right
            && value == column_name
        {
            true
        } else {
            false
        }
    }

    pub fn unchacked_validate(&self, value: &i64) -> bool {
        let (left, right) = if let Operand::Number(operand) = &self.left {
            (operand, value)
        } else if let Operand::Number(operand) = &self.right {
            (value, operand)
        } else {
            panic!("Expression was badly constructed");
        };

        match self.operator {
            Operator::Gt => left.gt(right),
            Operator::Ge => left.ge(right),
            Operator::Lt => left.lt(right),
            Operator::Le => left.le(right),
            Operator::Ne => left.ne(right),
            Operator::Eq => left.eq(right),
        }
    }
}

impl FromStr for Expression {
    type Err = CsvColError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let items: Vec<&str> = value.split(' ').collect();
        if items.len() != 3 {
            return Err(CsvColError::Filter(
                "Filter should have 2 operands and 1 operator".to_string(),
            ));
        }

        let operator = Operator::try_parse(items[1])?;

        let expression = if let Ok(value) = items[0].parse::<i64>() {
            Self {
                left: Operand::Number(value),
                operator,
                right: Operand::Column(items[2].to_string()),
            }
        } else if let Ok(value) = items[2].parse::<i64>() {
            Self {
                left: Operand::Column(items[0].to_string()),
                operator,
                right: Operand::Number(value),
            }
        } else {
            return Err(CsvColError::Filter(
                "Filter should have 2 operands and 1 operator".to_string(),
            ));
        };

        Ok(expression)
    }
}
