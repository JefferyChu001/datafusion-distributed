use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use luxdb::storage::engine::SegmentReader;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let shard_segments_dir =
        "/Users/admin/datafusion-distributed/generated_data/demo_db/shard_0/segments";

    let ctx = SessionContext::new();

    let provider = Arc::new(LuxSegmentsTableProvider::new(shard_segments_dir.to_string()));
    ctx.register_table("lux_segments", provider)?;

    let df = ctx
        .sql(
            "SELECT metric, tags, series_id, timestamp, fields \
             FROM lux_segments \
             ORDER BY timestamp \
             LIMIT 20",
        )
        .await?;

    df.show().await?;

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
        Ok(Arc::new(LuxSegmentsExec::new(
            Self::schema_ref(),
            self.segments_dir.clone(),
            projection.cloned(),
            limit,
        )))
    }
}

#[derive(Debug, Clone)]
struct LuxSegmentsExec {
    schema: SchemaRef,
    segments_dir: String,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl LuxSegmentsExec {
    fn new(
        schema: SchemaRef,
        segments_dir: String,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            schema,
            segments_dir,
            projection,
            limit,
            properties,
        }
    }

    fn load_batches(&self) -> anyhow::Result<Vec<RecordBatch>> {
        let mut rows_metric: Vec<String> = Vec::new();
        let mut rows_tags: Vec<Option<String>> = Vec::new();
        let mut rows_series_id: Vec<u32> = Vec::new();
        let mut rows_timestamp: Vec<i64> = Vec::new();
        let mut rows_fields: Vec<Option<String>> = Vec::new();

        let entries = std::fs::read_dir(&self.segments_dir)
            .with_context(|| format!("read_dir failed: {}", self.segments_dir))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path
                .extension()
                .map(|e| e == "seg")
                .unwrap_or(false)
            {
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

        let batch = RecordBatch::try_new(projected_schema, projected_arrays)
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(vec![batch])
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
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let batches = self
            .load_batches()
            .map_err(|e| DataFusionError::Execution(format!("load luxdb segments failed: {e}")))?;

        let schema = self.schema();
        let stream = Box::pin(async_stream::stream! {
            for batch in batches {
                yield Ok(batch);
            }
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
