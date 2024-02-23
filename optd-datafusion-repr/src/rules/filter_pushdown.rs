//! This rule is designed to be applied heuristically (read: all the time, blindly).
//! However, pushing a filter is not *always* better (but it usually is). If cost is
//! to be taken into account, each transposition step can be done separately
//! (and are thus all in independent functions).
//! One can even implement each of these helper functions as their own transpose rule,
//! like Calcite does.
//!
//! At a high level, filter pushdown is responsible for pushing the filter node
//! further down the query plan whenever it is possible to do so.

use std::collections::HashMap;
use std::result;

use optd_core::rules::{Rule, RuleMatcher};
use optd_core::{optimizer::Optimizer, rel_node::RelNode};

use crate::plan_nodes::{
    ConstantType, Expr, LogicalEmptyRelation, LogicalFilter, LogicalSort, OptRelNode, OptRelNodeTyp,
};

use super::macros::define_rule;

define_rule!(
    FilterPushdownRule,
    apply_filter_pushdown,
    (Filter, child, [cond])
);

/// Projection and filter are commutable when TODO
fn filter_project_transpose(
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    vec![]
}

/// Filter and sort should always be commutable.
fn filter_sort_transpose(
    child: RelNode<OptRelNodeTyp>,
    cond: RelNode<OptRelNodeTyp>,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let old_sort = LogicalSort::from_rel_node(child.into()).unwrap();
    let cond_as_expr = Expr::from_rel_node(cond.into()).unwrap();
    let new_filter_node = LogicalFilter::new(old_sort.child(), cond_as_expr);
    // Exprs should be the same, no projections have occurred here.
    let new_sort = LogicalSort::new(new_filter_node.into_plan_node(), old_sort.exprs());
    vec![new_sort.into_rel_node().as_ref().clone()]
}

fn apply_filter_pushdown(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    FilterPushdownRulePicks { child, cond }: FilterPushdownRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    // Push filter down one node
    let result_from_this_step = match child.typ {
        OptRelNodeTyp::Projection => filter_project_transpose(child, cond),
        OptRelNodeTyp::Filter => todo!(), // @todo filter merge rule? Should we do that here?
        OptRelNodeTyp::Scan => todo!(),
        OptRelNodeTyp::Join(_) => todo!(),
        OptRelNodeTyp::Sort => filter_sort_transpose(child, cond),
        OptRelNodeTyp::Agg => todo!(),
        OptRelNodeTyp::Apply(_) => todo!(),
        OptRelNodeTyp::EmptyRelation => todo!(),
        OptRelNodeTyp::Limit => todo!(),
        _ => vec![],
    };

    // Apply rule recursively
    if let Some(new_node) = result_from_this_step.first() {
        // For all the children in our result,
        new_node.children = new_node
            .children
            .into_iter()
            .map(|child| {
                if child.typ == OptRelNodeTyp::Filter {
                    // If this node is a filter, apply the rule again to this node!
                    let child_as_filter = LogicalFilter::from_rel_node(child.clone()).unwrap();
                    let childs_child = child_as_filter.child().into_rel_node().as_ref().clone();
                    let childs_cond = child_as_filter.cond().into_rel_node().as_ref().clone();
                    // TODO: make this iterative?
                    let result = apply_filter_pushdown(
                        _optimizer,
                        FilterPushdownRulePicks {
                            child: childs_child,
                            cond: childs_cond,
                        },
                    );
                    // If we got a result, that is the replacement for this child
                    if let Some(&new_child) = result.first() {
                        return new_child.into();
                    }
                }
                // Otherwise, if there was no result from rule application or this is not a filter,
                // return the old child unmodified.
                child
            })
            .collect();
    }

    result_from_this_step
}
