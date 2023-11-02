use std::sync::Arc;

use itertools::Itertools;
use optd_core::{
    cascades::CascadesOptimizer,
    cost::{self, OptCostModel},
    plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, JoinType, LogicalFilter, LogicalJoin,
        LogicalScan, OptRelNode, OptRelNodeTyp, PlanNode,
    },
    rel_node::Value,
    rules::{
        FilterJoinPullUpRule, JoinAssocLeftRule, JoinAssocRightRule, JoinCommuteRule,
        PhysicalConversionRule,
    },
};

use tracing::Level;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_target(false)
        .init();

    let mut optimizer = CascadesOptimizer::new_with_rules(
        vec![
            Arc::new(JoinCommuteRule::new()),
            Arc::new(JoinAssocLeftRule::new()),
            Arc::new(JoinAssocRightRule::new()),
            Arc::new(FilterJoinPullUpRule::new()),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Scan)),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Join(
                JoinType::Inner,
            ))),
            Arc::new(PhysicalConversionRule::new(OptRelNodeTyp::Filter)),
        ],
        Box::new(OptCostModel::new(
            [("t1", 1000), ("t2", 100), ("t3", 10000)]
                .into_iter()
                .map(|(x, y)| (x.to_string(), y))
                .collect(),
        )),
    );

    // The plan: (filter (scan t1) #1=2) join (scan t2) join (scan t3)
    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = BinOpExpr::new(
        ColumnRefExpr::new(1).0,
        ConstantExpr::new(Value::Int(2)).0,
        BinOpType::Eq,
    );
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantExpr::new(Value::Bool(true));
    let scan3 = LogicalScan::new("t3".into());
    let join_filter = LogicalJoin::new(filter1.0, scan2.0, join_cond.clone().0, JoinType::Inner);
    let fnal = LogicalJoin::new(scan3.0, join_filter.0, join_cond.0, JoinType::Inner);
    let result = optimizer.optimize(fnal.0.into_rel_node()).unwrap();
    optimizer.dump();
    let mut result = result
        .into_iter()
        .map(|x| (x.clone(), optimizer.cost().compute_plan_node_cost(&x)))
        .collect_vec();
    result.sort_by(|(_, cost1), (_, cost2)| cost1.partial_cmp(cost2).unwrap());
    if let Some((node, _)) = result.first() {
        println!(
            "{}",
            PlanNode::from_rel_node(node.clone())
                .unwrap()
                .explain_to_string()
        );
    }
}
