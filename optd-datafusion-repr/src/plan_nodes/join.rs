use core::fmt;
use std::fmt::Display;

use super::macros::define_plan_node;
use super::{Expr, ExprList, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner = 1,
    FullOuter,
    LeftOuter,
    RightOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

define_plan_node!(
    LogicalJoin : PlanNode,
    Join, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct PhysicalNestedLoopJoin(pub PlanNode);

define_plan_node!(
    PhysicalNestedLoopJoin : PlanNode,
    PhysicalNestedLoopJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);

#[derive(Clone, Debug)]
pub struct PhysicalHashJoin(pub PlanNode);

define_plan_node!(
    PhysicalHashJoin : PlanNode,
    PhysicalHashJoin, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, left_keys: ExprList },
        { 3, right_keys: ExprList }
    ], { join_type: JoinType }
);

impl LogicalJoin {
    pub fn map_through_join(
        index: usize,
        left_schema_size: usize,
        right_schema_size: usize,
    ) -> usize {
        assert!(index < left_schema_size + right_schema_size);
        if index < left_schema_size {
            index
        } else {
            index - left_schema_size
        }
    }
}
