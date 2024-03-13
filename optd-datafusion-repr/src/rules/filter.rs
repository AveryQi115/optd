use std::collections::HashMap;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{ConstantType, LogicalEmptyRelation, OptRelNode, OptRelNodeTyp, LogOpExpr, LogOpType};

use super::macros::define_rule;

define_rule!(
    SimplifyFilterRule,
    apply_simplify_filter,
    (Filter, child, [cond])
);

fn simplify_log_expr(log_expr: OptRelNodeRef) -> OptRelNodeRef{
    let log_expr = LogOpExpr::from_rel_node(log_expr).unwrap();
    let op = log_expr.op_type();
    let mut new_children = HashSet::new();
    for child in log_expr.children().to_vec() {
        let mut new_child = child;
        if let OptRelNodeTyp::LogOp(child_op) = child.typ() {
            new_child = simplify_log_expr(child);
        }
        if let OptRelNodeTyp::Constant(ConstantType::Bool) = new_child.typ(){
            let data = new_child.data.unwrap();
            // TrueExpr
            if data.as_bool(){
                if op == LogOpType::And{
                    // skip True in And
                    continue;
                }
                if op == LogOpType::Or{
                    // replace whole exprList with True
                    return OptRelNode::new_constant(true).into_rel_node().as_ref().clone();
                }
            }
            // FalseExpr
            if op == LogOpType::And{
                // replace whole exprList with False
                return OptRelNode::new_constant(false).into_rel_node().as_ref().clone();
            }
            if op == LogOpType::Or{
                // skip False in Or
                continue;
            }
        }
        
        new_children.insert(new_child);
    }
    if new_children.len() == 0{
        if op == LogOpType::And{
            return OptRelNode::new_constant(true).into_rel_node().as_ref().clone();
        }
        if op == LogOpType::Or{
            return OptRelNode::new_constant(false).into_rel_node().as_ref().clone();
        }
        unreachable!("no other type in logOp");
    }
    if new_children.len() == 1{
        return new_children.into_iter().next().unwrap();
    }
    return LogOpExpr::new(op, ExprList::new(new_children.into())).into_rel_node().as_ref().clone();
}


// SimplifySelectFilters simplifies the Filters operator in several possible
//  ways:
//    - Replaces the Or operator with True if any operand is True
//    - Replaces the And operator with False if any operand is False
//    - Removes Duplicates
fn apply_simplify_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    SimplifyFilterRulePicks { child, cond }: SimplifyFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    match cond.typ() {
        OptRelNodeTyp::LogOp(op) => {
            let log_expr = LogOpExpr::from_rel_node(cond.into()).unwrap();
            let new_log_expr = simplify_log_expr(log_expr);
            return vec![new_log_expr.into_rel_node().as_ref().clone()];
        }
        _ => {
            return vec![];
        }
    }
}

define_rule!(
    EliminateFilterRule,
    apply_eliminate_filter,
    (Filter, child, [cond])
);

/// Transformations:
///     - Filter node w/ false pred -> EmptyRelation
///     - Filter node w/ true pred  -> Eliminate from the tree
fn apply_eliminate_filter(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    EliminateFilterRulePicks { child, cond }: EliminateFilterRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::Constant(ConstantType::Bool) = cond.typ {
        if let Some(data) = cond.data {
            if data.as_bool() {
                // If the condition is true, eliminate the filter node, as it
                // will yield everything from below it.
                return vec![child];
            } else {
                // If the condition is false, replace this node with the empty relation,
                // since it will never yield tuples.
                let node = LogicalEmptyRelation::new(false);
                return vec![node.into_rel_node().as_ref().clone()];
            }
        }
    }
    vec![]
}