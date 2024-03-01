#![allow(clippy::new_without_default)]

use std::sync::Arc;

use anyhow::Result;
use cost::{AdaptiveCostModel, RuntimeAdaptionStorage};
use optd_core::{
    optimizer::Optimizer,
    cascades::{CascadesOptimizer, GroupId, OptimizerProperties},
    heuristics::{ApplyOrder, HeuristicsOptimizer},
    rules::Rule,
};
use plan_nodes::{OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};
use properties::{
    column_ref::ColumnRefPropertyBuilder,
    schema::{Catalog, SchemaPropertyBuilder},
};
use rules::{
    ConvertFilterCrossJoinToInnerJoinRule, EliminateDuplicatedAggExprRule, EliminateDuplicatedSortExprRule, EliminateFilterRule, EliminateJoinRule, EliminateLimitRule, HashJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule, ProjectionPullUpJoin
};

pub use adaptive::PhysicalCollector;
pub use optd_core::rel_node::Value;

mod adaptive;
pub mod cost;
pub mod plan_nodes;
pub mod properties;
pub mod rules;

pub struct DatafusionOptimizer {
    hueristic_optimizer: HeuristicsOptimizer<OptRelNodeTyp>,
    optimizer: CascadesOptimizer<OptRelNodeTyp>,
    pub runtime_statistics: RuntimeAdaptionStorage,
    enable_adaptive: bool,
    enable_heuristic: bool,
}

impl DatafusionOptimizer {
    pub fn enable_adaptive(&mut self, enable: bool) {
        self.enable_adaptive = enable;
    }

    pub fn enable_heuristic(&mut self, enable: bool) {
        self.enable_heuristic = enable;
    }

    pub fn is_heuristic_enabled(&self) -> bool {
        self.enable_heuristic
    }

    pub fn optd_cascades_optimizer(&self) -> &CascadesOptimizer<OptRelNodeTyp> {
        &self.optimizer
    }

    pub fn optd_hueristic_optimizer(&self) -> &HeuristicsOptimizer<OptRelNodeTyp> {
        &self.hueristic_optimizer
    }

    pub fn optd_optimizer_mut(&mut self) -> &mut CascadesOptimizer<OptRelNodeTyp> {
        &mut self.optimizer
    }

    pub fn default_heuristic_rules() -> Vec<Arc<dyn Rule<OptRelNodeTyp, HeuristicsOptimizer<OptRelNodeTyp>>>> {
        let mut rules: Vec<Arc<dyn Rule<OptRelNodeTyp, HeuristicsOptimizer<OptRelNodeTyp>>>> = vec![];
        rules.push(Arc::new(EliminateJoinRule::new()));
        // rules.push(Arc::new(ConvertFilterCrossJoinToInnerJoinRule::new()));

        rules
    }

    pub fn default_cascades_rules() -> Vec<Arc<dyn Rule<OptRelNodeTyp, CascadesOptimizer<OptRelNodeTyp>>>> {
        let mut rules = PhysicalConversionRule::all_conversions();
        rules.push(Arc::new(HashJoinRule::new()));
        rules.push(Arc::new(JoinCommuteRule::new()));
        rules.push(Arc::new(JoinAssocRule::new()));
        rules.push(Arc::new(ProjectionPullUpJoin::new()));
        rules.push(Arc::new(EliminateJoinRule::new()));
        rules.push(Arc::new(EliminateFilterRule::new()));
        rules.push(Arc::new(EliminateLimitRule::new()));
        rules.push(Arc::new(EliminateDuplicatedSortExprRule::new()));
        rules.push(Arc::new(EliminateDuplicatedAggExprRule::new()));

        rules
    }

    /// Create an optimizer for testing purpose: adaptive disabled + partial explore (otherwise it's too slow).
    pub fn new_physical(catalog: Arc<dyn Catalog>) -> Self {
        let cascades_rules = Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();

        let cost_model = AdaptiveCostModel::new(50);
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            optimizer: CascadesOptimizer::new_with_prop(
                cascades_rules,
                Box::new(cost_model),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                    Box::new(ColumnRefPropertyBuilder::new(catalog)),
                ],
                OptimizerProperties {
                    partial_explore_iter: Some(1 << 20),
                    partial_explore_space: Some(1 << 10),
                },
            ),
            hueristic_optimizer: HeuristicsOptimizer::new_with_rules(heuristic_rules, ApplyOrder::BottomUp),
            enable_adaptive: false,
            enable_heuristic: true,
        }
    }

    /// Create an optimizer with default settings: adaptive + partial explore.
    pub fn new_physical_adaptive(catalog: Arc<dyn Catalog>) -> Self {
        let cascades_rules = Self::default_cascades_rules();
        let heuristic_rules = Self::default_heuristic_rules();
        let cost_model = AdaptiveCostModel::new(50);
        Self {
            runtime_statistics: cost_model.get_runtime_map(),
            optimizer: CascadesOptimizer::new_with_prop(
                cascades_rules,
                Box::new(cost_model),
                vec![
                    Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                    Box::new(ColumnRefPropertyBuilder::new(catalog)),
                ],
                OptimizerProperties {
                    partial_explore_iter: Some(1 << 20),
                    partial_explore_space: Some(1 << 10),
                },
            ),
            hueristic_optimizer: HeuristicsOptimizer::new_with_rules(heuristic_rules, ApplyOrder::BottomUp),
            enable_adaptive: true,
            enable_heuristic: true,
        }
    }

    /// The optimizer settings for three-join demo as a perfect optimizer.
    pub fn new_alternative_physical_for_demo(catalog: Arc<dyn Catalog>) -> Self {
        let mut cascades_rules = PhysicalConversionRule::all_conversions();
        cascades_rules.push(Arc::new(HashJoinRule::new()));
        cascades_rules.insert(0, Arc::new(JoinCommuteRule::new()));
        cascades_rules.insert(1, Arc::new(JoinAssocRule::new()));
        cascades_rules.insert(2, Arc::new(ProjectionPullUpJoin::new()));
        cascades_rules.insert(3, Arc::new(EliminateFilterRule::new()));

        let cost_model = AdaptiveCostModel::new(1000); // very large decay
        let runtime_statistics = cost_model.get_runtime_map();
        let cascades_optimizer = CascadesOptimizer::new(
            cascades_rules,
            Box::new(cost_model),
            vec![
                Box::new(SchemaPropertyBuilder::new(catalog.clone())),
                Box::new(ColumnRefPropertyBuilder::new(catalog)),
            ],
        );
        let heuristics_rules = Self::default_heuristic_rules();
        let heuristic_optimizer = HeuristicsOptimizer::new_with_rules(heuristics_rules, ApplyOrder::BottomUp);
        Self {
            hueristic_optimizer: heuristic_optimizer,
            runtime_statistics: runtime_statistics,
            optimizer: cascades_optimizer,
            enable_adaptive: true,
            enable_heuristic: true,
        }
    }

    pub fn heuristic_optimize(&mut self, root_rel: OptRelNodeRef) -> Result<OptRelNodeRef> {
        self.hueristic_optimizer.optimize(root_rel)
    }

    pub fn optimize(&mut self, root_rel: OptRelNodeRef) -> Result<(GroupId, OptRelNodeRef)> {
        if self.enable_adaptive {
            self.runtime_statistics.lock().unwrap().iter_cnt += 1;
            self.optimizer.step_clear_winner();
        } else {
            self.optimizer.step_clear();
        }

        let group_id = self.optimizer.step_optimize_rel(root_rel)?;

        let optimized_rel =
            self.optimizer
                .step_get_optimize_rel(group_id, |rel_node, group_id| {
                    if rel_node.typ.is_plan_node() && self.enable_adaptive {
                        return PhysicalCollector::new(
                            PlanNode::from_rel_node(rel_node).unwrap(),
                            group_id,
                        )
                        .into_rel_node();
                    }
                    rel_node
                })?;

        Ok((group_id, optimized_rel))
    }

    pub fn dump(&self, group_id: Option<GroupId>) {
        self.optimizer.dump(group_id)
    }
}
