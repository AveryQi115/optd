use std::collections::HashMap;
use std::collections::HashSet;
use std::vec;
use std::error::Error;
use std::sync::Arc;

use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use crate::plan_nodes::OptRelNodeRef;
#[allow(unused_imports)]
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, ConstantType, Expr, ExprList, JoinType, LogicalJoin, LogicalProjection, OptRelNode, OptRelNodeTyp, PlanNode
};

#[allow(unused_imports)]
use crate::properties::schema::SchemaPropertyBuilder;

pub struct ConvertFilterCrossJoinToInnerJoinRule {
    matcher: RuleMatcher<OptRelNodeTyp>,
}

const JOIN_LEFT_CHILD: usize = 0;
const JOIN_RIGHT_CHILD: usize = 1;
const FILTER_COND: usize = 2;
const JOIN_COND: usize = 3;

impl ConvertFilterCrossJoinToInnerJoinRule {
    pub fn new() -> Self {
        Self {
            matcher: RuleMatcher::MatchNode {
                typ: OptRelNodeTyp::Filter,
                children: vec![
                    RuleMatcher::MatchNode {
                        typ: OptRelNodeTyp::Join(JoinType::Cross),
                        children: vec![
                            RuleMatcher::PickOne {
                                pick_to: JOIN_LEFT_CHILD,
                                expand: false,
                            },
                            RuleMatcher::PickOne {
                                pick_to: JOIN_RIGHT_CHILD,
                                expand: false,
                            },
                            RuleMatcher::PickOne {
                                pick_to: JOIN_COND,
                                expand: true,
                            },
                        ],
                    },
                    RuleMatcher::PickOne {
                        pick_to: FILTER_COND,
                        expand: true,
                    },
                ]
            },
        }
    }
}

impl<O> Rule<OptRelNodeTyp, O> for ConvertFilterCrossJoinToInnerJoinRule 
where 
    O: Optimizer<OptRelNodeTyp>
{
    fn matcher(&self) -> &RuleMatcher<OptRelNodeTyp> {
        &self.matcher
    }

    fn apply(
        &self,
        optimizer: &O,
        mut input: HashMap<usize, RelNode<OptRelNodeTyp>>,
    ) -> Vec<RelNode<OptRelNodeTyp>> {
        let left_child = input.remove(&JOIN_LEFT_CHILD).unwrap();
        let right_child = input.remove(&JOIN_RIGHT_CHILD).unwrap();
        let filter_cond = input.remove(&FILTER_COND).unwrap();

        let mut possible_join_keys:HashSet<(OptRelNodeRef,OptRelNodeRef)> = HashSet::new();
        let mut all_inputs = vec![];
        
        flatten_join_inputs(&left_child, optimizer, &mut possible_join_keys, &mut all_inputs);
        flatten_join_inputs(&right_child, optimizer, &mut possible_join_keys, &mut all_inputs);

        return vec![];
        // let _parent_predicate = match child.typ {
        //     OptRelNodeTyp::Join(JoinType::Inner) | OptRelNodeTyp::Join(JoinType::Cross) => {
        //         if let Ok(false) | Err(_) = try_flatten_join_inputs(
        //             &child,
        //             &mut possible_join_keys,
        //             &mut all_inputs,
        //         ){
        //             return vec![];
        //         };

        //         let expr = Expr::from_rel_node(Arc::new(cond)).unwrap();
        //         extract_possible_join_keys(
        //             &expr,
        //             &mut possible_join_keys,
        //         );
        //         Some(expr)
        //     }
        //     _ => {
        //         return vec![];
        //     }
        // };

        // return vec![];
    }

    fn name(&self) -> &'static str {
        "convert_filter_cross_join_to_inner_join"
    }
}

/// flatten_join_inputs flatten recursive joins and collects inputs to all_inputs,
///     equal condition join keys to possible_join_keys.
/// When it meets a non equal condition join keys , for example, a < b, or other 
///     join types(semi, outer,...), it will return false, meaning that inputs 
///     cannot be flattened, otherwise the filter condition will be lost.
/// eg: select * from t1 join t2 on t1.a = t2.a, t3 where t2.b = t3.b will be flattened
///     and t1.a = t2.a will be collected to possible join keys, t1, t2, t3 will be
///     collected to all_inputs.
fn flatten_join_inputs<O: Optimizer<OptRelNodeTyp>>(
    input_node: &RelNode<OptRelNodeTyp>,
    _optimizer: &O,
    possible_join_keys: &mut HashSet<(OptRelNodeRef, OptRelNodeRef)>,
    all_inputs: &mut Vec<OptRelNodeRef>,
) -> bool {

    let join = match input_node.typ{
        OptRelNodeTyp::Join(_) => {
            LogicalJoin::from_rel_node(Arc::new(input_node.clone())).unwrap()
        }
        _ => {
            return false;
        }
    };

    let children = match join.join_type() {
        JoinType::Inner => {
            if !extract_possible_join_keys(&join.cond(), possible_join_keys){
                return false;
            }
            vec![join.left().clone(), join.right().clone()]
        }
        JoinType::Cross => {
            let left = join.left().clone();
            let right = join.right().clone();
            vec![left, right]
        }
        _ => {
            return false;
        }
    };

    for child in children.into_iter() {
        match child.typ() {
            OptRelNodeTyp::Join(JoinType::Inner) | OptRelNodeTyp::Join(JoinType::Cross) => {
                let child_ref = child.into_rel_node();
                if !flatten_join_inputs(&child_ref, _optimizer, possible_join_keys, all_inputs) {
                    return false;
                }
            }
            _ => {
                let child_ref = child.into_rel_node();
                all_inputs.push(child_ref);
            }
        }
    }
    true
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
        OptRelNodeTyp::BinOp(binop_type) => {
            match binop_type{
                BinOpType::Eq => {
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
                BinOpType::And => {
                    let bin_expr = BinOpExpr::from_rel_node(expr.clone().into_rel_node()).unwrap();
                    let left = bin_expr.left_child();
                    let right = bin_expr.right_child();
                    let expr_list = vec![left, right];
                    let mut invalid:bool = false;
                    (0..expr_list.len()).for_each(|i| {
                        if !extract_possible_join_keys(&expr_list[i], possible_join_keys){
                            invalid = true;
                        }
                    });
                    if invalid{
                        return false;
                    }
                }
                BinOpType::Or => {
                    let mut initial_key_sets:HashSet<(OptRelNodeRef, OptRelNodeRef)> = HashSet::new();
                    let mut invalid:bool = false;
                    let bin_expr = BinOpExpr::from_rel_node(expr.clone().into_rel_node()).unwrap();
                    let left = bin_expr.left_child();
                    let right = bin_expr.right_child();
                    let expr_list = vec![left, right];
                    (0..expr_list.len()).for_each(&mut|i| {
                        let mut key_sets:HashSet<(OptRelNodeRef,OptRelNodeRef)> = HashSet::new();
                        if !extract_possible_join_keys(&expr_list[i], &mut key_sets){
                            invalid = true;
                        }
                        intersect(&mut initial_key_sets, &key_sets);
                    });
                    if invalid{
                        return false;
                    }
                }
                _ => {
                    return false;
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