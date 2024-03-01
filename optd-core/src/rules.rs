mod ir;

use std::collections::HashMap;
use std::fmt;

use crate::{
    optimizer::Optimizer,
    rel_node::{RelNode, RelNodeTyp},
};

pub use ir::RuleMatcher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleType {
    /// Implementation rules are used to convert logical plan to physical plan
    Implementation,

    /// transformation rules are used to generate new logical plan, default type
    Transformation,

    /// normalization rules are like heuristics rules, which always apply when matched
    /// , are used to simplify the plan and new generated plan will replace original ones
    Normalization,
}

impl fmt::Display for RuleType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RuleType::Implementation => write!(f, "Implementation"),
            RuleType::Transformation => write!(f, "Transformation"),
            RuleType::Normalization => write!(f, "Normalization"),
        }
    }
}

pub trait Rule<T: RelNodeTyp, O: Optimizer<T>>: 'static + Send + Sync {
    fn matcher(&self) -> &RuleMatcher<T>;
    fn apply(&self, optimizer: &O, input: HashMap<usize, RelNode<T>>) -> Vec<RelNode<T>>;
    fn name(&self) -> &'static str;
    fn is_impl_rule(&self) -> bool {
        self.get_rule_type() == RuleType::Implementation
    }
    fn is_xform_rule(&self) -> bool {
        self.get_rule_type() == RuleType::Transformation
    }
    fn is_norm_rule(&self) -> bool {
        self.get_rule_type() == RuleType::Normalization
    }
    fn get_rule_type(&self) -> RuleType {
        RuleType::Transformation
    }
}
