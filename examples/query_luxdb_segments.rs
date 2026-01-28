use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, display_plan_ascii,
};
use luxdb::storage::engine::SegmentReader;

const SEGMENTS_DIR: &str =
    "/Users/admin/datafusion-distributed/generated_data/demo_db/shard_0/segments";

const WORKER_COUNT: usize = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_worker_resolver(datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver::new(WORKER_COUNT))
        .with_distributed_channel_resolver(datafusion_distributed::test_utils::in_memory_channel_resolver::InMemoryChannelResolver::default())
        .build();

    let ctx = SessionContext::from(state);

    let provider = Arc::new(LuxSegmentsTableProvider::new(SEGMENTS_DIR.to_string()));
    ctx.register_table("lux_segments", provider)?;

    let sql = "SELECT metric, tags, series_id, timestamp, fields \
               FROM lux_segments \
               ORDER BY timestamp \
               LIMIT 20";

    let df = ctx.sql(sql).await?;

    // 1) 打印“分布式物理计划”（如果 optimizer 能插入分布式算子，会在这里看到）
    let plan = df.create_physical_plan().await?;
    println!("{}", display_plan_ascii(plan.as_ref(), false));

    // 2) 实际执行（可选：你也可以注释掉，只看计划）
    // df.show().await?;

    Ok(())
}

#[derive(Debug)]
struct LuxSegmentsTableProvider {
    segments_dir: String,
}

impl LuxSegmentsTableProvider {
    fn new(segments_dir: String) -> Self {
        Self { segments_dir }
    }

    fn schema_ref() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("metric", DataType::Utf8, false),
            Field::new("tags", DataType::Utf8, true),
            Field::new("series_id", DataType::UInt32, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("fields", DataType::Utf8, true),
        ]))
    }
}

#[async_trait]
impl TableProvider for LuxSegmentsTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Self::schema_ref()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 关键：让 Scan 有 >=3 个 partitions，这样分布式 rule 才有机会把 partitions
        // 分发给不同 worker。
        Ok(Arc::new(LuxSegmentsExec::new(
            Self::schema_ref(),
            self.segments_dir.clone(),
            projection.cloned(),
            limit,
            WORKER_COUNT,
        )))
    }
}

#[derive(Debug, Clone)]
struct LuxSegmentsExec {
    schema: SchemaRef,
    segments_dir: String,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    partition_count: usize,
    properties: PlanProperties,
}

impl LuxSegmentsExec {
    fn new(
        schema: SchemaRef,
        segments_dir: String,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        partition_count: usize,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema,
            segments_dir,
            projection,
            limit,
            partition_count,
            properties,
        }
    }

    fn load_partition_batch(&self, partition: usize) -> anyhow::Result<RecordBatch> {
        let mut rows_metric: Vec<String> = Vec::new();
        let mut rows_tags: Vec<Option<String>> = Vec::new();
        let mut rows_series_id: Vec<u32> = Vec::new();
        let mut rows_timestamp: Vec<i64> = Vec::new();
        let mut rows_fields: Vec<Option<String>> = Vec::new();

        let mut entries: Vec<_> = std::fs::read_dir(&self.segments_dir)
            .with_context(|| format!("read_dir failed: {}", self.segments_dir))?
            .flatten()
            .collect();

        // 使分区稳定：按文件名排序
        entries.sort_by_key(|e| e.file_name());

        for (idx, entry) in entries.into_iter().enumerate() {
            if idx % self.partition_count != partition {
                continue;
            }

            let path = entry.path();
            if !path.extension().map(|e| e == "seg").unwrap_or(false) {
                continue;
            }

            let mut reader = SegmentReader::open(&path)
                .with_context(|| format!("SegmentReader::open failed: {}", path.display()))?;

            let series_blocks = reader
                .iter_series()
                .with_context(|| format!("iter_series failed: {}", path.display()))?;

            for block in series_blocks {
                let tags_json = if block.tags.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&block.tags)?)
                };

                for point in block.points {
                    rows_metric.push(block.metric.clone());
                    rows_tags.push(tags_json.clone());
                    rows_series_id.push(block.series_id);
                    rows_timestamp.push(point.timestamp);

                    let fields_json = if point.fields.is_empty() {
                        None
                    } else {
                        Some(serde_json::to_string(&point.fields)?)
                    };
                    rows_fields.push(fields_json);

                    if let Some(limit) = self.limit {
                        if rows_timestamp.len() >= limit {
                            break;
                        }
                    }
                }

                if let Some(limit) = self.limit {
                    if rows_timestamp.len() >= limit {
                        break;
                    }
                }
            }

            if let Some(limit) = self.limit {
                if rows_timestamp.len() >= limit {
                    break;
                }
            }
        }

        let mut metric_builder = StringBuilder::new();
        for v in &rows_metric {
            metric_builder.append_value(v);
        }
        let metric_arr: ArrayRef = Arc::new(metric_builder.finish());

        let mut tags_builder = StringBuilder::new();
        for v in &rows_tags {
            match v {
                Some(s) => tags_builder.append_value(s),
                None => tags_builder.append_null(),
            }
        }
        let tags_arr: ArrayRef = Arc::new(tags_builder.finish());

        let series_id_arr: ArrayRef = Arc::new(UInt32Array::from(rows_series_id));
        let ts_arr: ArrayRef = Arc::new(Int64Array::from(rows_timestamp));

        let mut fields_builder = StringBuilder::new();
        for v in &rows_fields {
            match v {
                Some(s) => fields_builder.append_value(s),
                None => fields_builder.append_null(),
            }
        }
        let fields_arr: ArrayRef = Arc::new(fields_builder.finish());

        let arrays: Vec<ArrayRef> = vec![metric_arr, tags_arr, series_id_arr, ts_arr, fields_arr];

        let projected_arrays: Vec<ArrayRef> = if let Some(ref proj) = self.projection {
            proj.iter().map(|i| arrays[*i].clone()).collect()
        } else {
            arrays
        };

        let projected_schema: SchemaRef = if let Some(ref proj) = self.projection {
            let fields: Vec<Field> = proj.iter().map(|i| self.schema.field(*i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        Ok(RecordBatch::try_new(projected_schema, projected_arrays)
            .map_err(|e| anyhow::anyhow!(e))?)
    }
}

impl DisplayAs for LuxSegmentsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LuxSegmentsExec")
    }
}

impl ExecutionPlan for LuxSegmentsExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batch = self.load_partition_batch(partition).map_err(|e| {
            DataFusionError::Execution(format!(
                "load luxdb segments failed (partition={partition}): {e}"
            ))
        })?;

        let schema = self.schema();
        let stream = Box::pin(async_stream::stream! {
            yield Ok(batch);
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn name(&self) -> &str {
        "LuxSegmentsExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}
