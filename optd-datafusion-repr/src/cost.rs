mod adaptive_cost;
mod base_cost;
mod dummy_cost;
mod stats;

pub use adaptive_cost::{AdaptiveCostModel, RuntimeAdaptionStorage, DEFAULT_DECAY};
pub use base_cost::{
    BaseTableStats, OptCostModel, PerColumnStats, PerTableStats, COMPUTE_COST, IO_COST, ROW_COUNT,
};
pub use dummy_cost::DummyCostModel;

pub trait WithRuntimeStatistics {
    fn get_runtime_statistics(&self) -> RuntimeAdaptionStorage;
}
