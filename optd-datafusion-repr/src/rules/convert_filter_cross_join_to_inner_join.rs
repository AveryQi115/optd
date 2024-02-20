use std::collections::HashMap;
use std::collections::HashSet;
use std::vec;
use std::error::Error;
use std::sync::Arc;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

#[allow(unused_imports)]
use super::macros::{define_rule};

use crate::plan_nodes::LogOpExpr;
use crate::plan_nodes::OptRelNodeRef;
#[allow(unused_imports)]
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, JoinType, LogOpType, LogicalJoin, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};

#[allow(unused_imports)]
use crate::properties::schema::SchemaPropertyBuilder;

use std::fmt;

#[derive(Debug)]
struct MyError {
    details: String
}

impl MyError {
    fn new(msg: &str) -> MyError {
        MyError{details: msg.to_string()}
    }
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"{}",self.details)
    }
}

impl Error for MyError {
    fn description(&self) -> &str {
        &self.details
    }
}

define_rule!(
    ConvertFilterCrossJoinToInnerJoinRule,
    convert_filter_cross_join_to_inner_join,
    (Filter, child, [cond])
);

fn try_flatten_join_inputs(
    input_node: &RelNode<OptRelNodeTyp>,
    possible_join_keys: &mut HashSet<(OptRelNodeRef, OptRelNodeRef)>,
    all_inputs: &mut Vec<OptRelNodeRef>,
) -> Result<bool, MyError> {
    let join = LogicalJoin::from_rel_node(Arc::new(input_node.clone())).unwrap();

    let children = match join.join_type() {
        JoinType::Inner => {
            // TODO: Currently optd only supports equal condition as join condition for inner join
            // eg: select * from t join s on t.a = s.a
            // queries like `select * from t join s on t.a between s.min and s.max` are not supported
            // if we adds support for non equal conditions filter in join condition, this rule should
            //  be skipped when non equal conditions(filters) are present in join condition, otherwise
            //  those filters will be ignored to generate wrong results
            if !extract_possible_join_keys(&join.cond(), possible_join_keys){
                return Ok(false); // return false if joind cond has non equal conditions
            }
            vec![join.left().clone(), join.right().clone()]
        }
        JoinType::Cross => {
            let left = join.left().clone();
            let right = join.right().clone();
            vec![left, right]
        }
        _ => {
            return Err(MyError::new("try_flatten_join_inputs can only be called on cross join and inner join".into()));
        }
    };

    for child in children.into_iter() {
        match child.typ() {
            OptRelNodeTyp::Join(JoinType::Inner) | OptRelNodeTyp::Join(JoinType::Cross) => {
                let child_ref = child.into_rel_node();
                if !try_flatten_join_inputs(&child_ref, possible_join_keys, all_inputs)? {
                    return Ok(false);
                }
            }
            _ => {
                let child_ref = child.into_rel_node();
                all_inputs.push(child_ref);
            }
        }
    }
    Ok(true)
}

fn intersect(
    vec1: &mut HashSet<(OptRelNodeRef, OptRelNodeRef)>,
    vec2: &HashSet<(OptRelNodeRef, OptRelNodeRef)>
) {
    let tmp = vec1.clone();
    for (x1, x2) in tmp.iter() {
        if !(vec2.contains(&(x1.clone(), x2.clone())) || vec2.contains(&(x2.clone(), x1.clone()))) {
            vec1.remove(&(x1.clone(), x2.clone()));
        }
    }
}

fn extract_possible_join_keys(expr: &Expr, possible_join_keys: &mut HashSet<(OptRelNodeRef, OptRelNodeRef)>) -> bool {
    match expr.typ(){
        OptRelNodeTyp::BinOp(BinOpType::Eq) => {
            let bin_expr = BinOpExpr::from_rel_node(expr.clone().into_rel_node()).unwrap();
            let left = bin_expr.left_child().into_rel_node();
            let right = bin_expr.right_child().into_rel_node();
            // Ensure that we don't add the same Join keys multiple times
            if !(possible_join_keys.contains(&(left.clone(), right.clone()))
                || possible_join_keys.contains(&(right.clone(), left.clone())))
            {
                possible_join_keys.insert((left.clone(), right.clone()));
            }
        }
        OptRelNodeTyp::LogOp(log_op_type) => {
            let log_expr = LogOpExpr::from_rel_node(expr.clone().into_rel_node()).unwrap();
            let expr_list = log_expr.children();
            match log_op_type{
                LogOpType::And => {
                    let mut invalid:bool = false;
                    (0..expr_list.len()).for_each(|i| {
                        if !extract_possible_join_keys(&expr_list.child(i), possible_join_keys){
                            invalid = true;
                        }
                    });
                    if invalid{
                        return false;
                    }
                }
                LogOpType::Or => {
                    let mut initial_key_sets:HashSet<(OptRelNodeRef, OptRelNodeRef)> = HashSet::new();
                    let mut invalid:bool = false;
                    (0..expr_list.len()).for_each(&mut|i| {
                        let mut key_sets:HashSet<(OptRelNodeRef,OptRelNodeRef)> = HashSet::new();
                        if !extract_possible_join_keys(&expr_list.child(i), &mut key_sets){
                            invalid = true;
                        }
                        intersect(&mut initial_key_sets, &key_sets);
                    });
                    if invalid{
                        return false;
                    }
                }
            }
        }
        _ => {
            return false;
        }
    }
    return true;
}

// fn find_inner_join(
//     left_input: &LogicalPlan,
//     rights: &mut Vec<LogicalPlan>,
//     possible_join_keys: &mut Vec<(Expr, Expr)>,
//     all_join_keys: &mut HashSet<(Expr, Expr)>,
// ) -> Result<LogicalJoin, MyError> {
//     for (i, right_input) in rights.iter().enumerate() {
//         let mut join_keys = vec![];

//         for (l, r) in &mut *possible_join_keys {
//             let key_pair = find_valid_equijoin_key_pair(
//                 l,
//                 r,
//                 left_input.schema().clone(),
//                 right_input.schema().clone(),
//             )?;

//             // Save join keys
//             if let Some((valid_l, valid_r)) = key_pair {
//                 if can_hash(&valid_l.get_type(left_input.schema())?) {
//                     join_keys.push((valid_l, valid_r));
//                 }
//             }
//         }

//         if !join_keys.is_empty() {
//             all_join_keys.extend(join_keys.clone());
//             let right_input = rights.remove(i);

//             return Ok(LogicalJoin::new{
//                 left: PlanNode::from_group(left_input.into()),
//                 right: PlanNode::from_group(right_input.into()),
//                 cond: join_keys,
//                 join_type: JoinType::Inner
//             });
//         }
//     }
//     let right = rights.remove(0);

//     Ok(LogicalJoin::new{
//         left: PlanNode::from_group(left_input.into()),
//         right: PlanNode::from_group(right.into()),
//         cond: ConstantExpr::bool(true).into_expr(),
//         join_type: JoinType::Cross,
//     });
// }

fn convert_filter_cross_join_to_inner_join(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ConvertFilterCrossJoinToInnerJoinRulePicks { child, cond }: ConvertFilterCrossJoinToInnerJoinRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>>{
    let mut possible_join_keys:HashSet<(OptRelNodeRef,OptRelNodeRef)> = HashSet::new();
    let mut all_inputs = vec![];
    print!("\n\nchild:");
    print!("{}\n\n", child.to_string());
    let _parent_predicate = match child.typ {
        OptRelNodeTyp::Join(JoinType::Inner) | OptRelNodeTyp::Join(JoinType::Cross) => {
            if let Ok(false) | Err(_) = try_flatten_join_inputs(
                &child,
                &mut possible_join_keys,
                &mut all_inputs,
            ){
                return vec![];
            };

            let expr = Expr::from_rel_node(Arc::new(cond)).unwrap();
            extract_possible_join_keys(
                &expr,
                &mut possible_join_keys,
            );
            Some(expr)
        }
        _ => {
            return vec![];
        }
    };

    return vec![];

    // let mut all_join_keys = HashSet::<(Expr, Expr)>::new();
    // let mut left = all_inputs.remove(0);
    // while !all_inputs.is_empty() {
    //     left = find_inner_join(
    //         &left,
    //         &mut all_inputs,
    //         &mut possible_join_keys,
    //         &mut all_join_keys,
    //     )?;
    // }

    // left = utils::optimize_children(self, &left, config)?.unwrap_or(left);

    // if plan.schema() != left.schema() {
    //     left = LogicalPlan::Projection(Projection::new_from_schema(
    //         Arc::new(left),
    //         plan.schema().clone(),
    //     ));
    // }

    // let Some(predicate) = parent_predicate else {
    //     return Ok(Some(left));
    // };

    // // If there are no join keys then do nothing:
    // if all_join_keys.is_empty() {
    //     Filter::try_new(predicate.clone(), Arc::new(left))
    //         .map(|f| Some(LogicalPlan::Filter(f)))
    // } else {
    //     // Remove join expressions from filter:
    //     match remove_join_expressions(predicate, &all_join_keys)? {
    //         Some(filter_expr) => Filter::try_new(filter_expr, Arc::new(left))
    //             .map(|f| Some(LogicalPlan::Filter(f))),
    //         _ => Ok(Some(left)),
    //     }
    // }

}