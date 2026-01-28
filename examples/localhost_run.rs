use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, TaskEstimation, TaskEstimator, WorkerResolver,
    display_plan_ascii,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::TryStreamExt;
use std::error::Error;
use std::sync::Arc;
use structopt::StructOpt;
use url::Url;

mod luxdb_table;

#[derive(StructOpt)]
#[structopt(name = "run", about = "A localhost Distributed DataFusion runner")]
struct Args {
    /// The SQL query to run.
    #[structopt()]
    query: String,

    /// The ports holding Distributed DataFusion workers.
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,

    /// LuxDB segments directories (must be accessible by both driver and workers).
    ///
    /// Example: --lux-segments-dirs /path/shard_0/segments,/path/shard_1/segments,/path/shard_2/segments
    #[structopt(
        long = "lux-segments-dirs",
        use_delimiter = true,
        default_value = "/Users/admin/datafusion-distributed/generated_data/demo_db/shard_0/segments,/Users/admin/datafusion-distributed/generated_data/demo_db/shard_1/segments,/Users/admin/datafusion-distributed/generated_data/demo_db/shard_2/segments"
    )]
    lux_segments_dirs: Vec<String>,

    /// Number of scan partitions for lux_segments.
    #[structopt(long = "lux-partitions", default_value = "3")]
    lux_partitions: usize,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[derive(Debug)]
struct LuxSegmentsTaskEstimator {
    desired_tasks: usize,
}

impl TaskEstimator for LuxSegmentsTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<TaskEstimation> {
        // We can't reliably downcast across crate boundaries here (examples module), so match by name.
        if plan.children().is_empty() && plan.name() == "LuxSegmentsExec" {
            return Some(TaskEstimation::desired(self.desired_tasks));
        }
        None
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _task_count: usize,
        _cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        // Our LuxSegmentsExec already exposes multiple partitions; no rewrite needed.
        Some(Arc::clone(plan))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let localhost_resolver = LocalhostWorkerResolver {
        ports: args.cluster_ports,
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(localhost_resolver)
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_task_estimator(LuxSegmentsTaskEstimator {
            desired_tasks: args.lux_partitions,
        })
        .with_distributed_user_codec(luxdb_table::LuxSegmentsCodec)
        .build();

    let ctx = SessionContext::from(state);

    // Register LuxDB segments table on the driver side (for SQL planning).
    ctx.register_table(
        "lux_segments",
        Arc::new(luxdb_table::LuxSegmentsTableProvider::new_multi(
            args.lux_segments_dirs,
            args.lux_partitions,
        )),
    )?;

    let df = ctx.sql(&args.query).await?;
    if args.show_distributed_plan {
        let plan = df.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
    } else {
        let stream = df.execute_stream().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        let formatted = pretty_format_batches(&batches)?;
        println!("{formatted}");
    }
    Ok(())
}

#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
