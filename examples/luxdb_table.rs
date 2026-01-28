//! Shared module for LuxDB Segments TableProvider and ExecutionPlan.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use luxdb::storage::engine::SegmentReader;
use serde::{Deserialize, Serialize};

// Allow dead code since this is a library-like module for examples.
#[allow(dead_code)]
#[derive(Debug)]
pub struct LuxSegmentsTableProvider {
    segments_dirs: Vec<String>,
    partition_count: usize,
}

#[allow(dead_code)]
impl LuxSegmentsTableProvider {
    pub fn new(segments_dir: String, partition_count: usize) -> Self {
        Self {
            segments_dirs: vec![segments_dir],
            partition_count,
        }
    }

    pub fn new_multi(segments_dirs: Vec<String>, partition_count: usize) -> Self {
        Self {
            segments_dirs,
            partition_count,
        }
    }

    pub fn schema_ref() -> SchemaRef {
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
            self.segments_dirs.clone(),
            projection.cloned(),
            limit,
            self.partition_count,
        )))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LuxSegmentsExec {
    pub schema: SchemaRef,
    pub segments_dirs: Vec<String>,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    pub partition_count: usize,
    pub properties: PlanProperties,
}

#[allow(dead_code)]
impl LuxSegmentsExec {
    pub fn new(
        schema: SchemaRef,
        segments_dirs: Vec<String>,
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
            segments_dirs,
            projection,
            limit,
            partition_count,
            properties,
        }
    }

    fn list_all_seg_files(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        let mut all = Vec::new();
        for dir in &self.segments_dirs {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .with_context(|| format!("read_dir failed: {}", dir))?
                .flatten()
                .collect();
            entries.sort_by_key(|e| e.file_name());
            for e in entries {
                let p = e.path();
                if p.extension().map(|ext| ext == "seg").unwrap_or(false) {
                    all.push(p);
                }
            }
        }
        all.sort();
        Ok(all)
    }

    fn load_partition_batch(&self, partition: usize) -> anyhow::Result<RecordBatch> {
        let mut rows_metric: Vec<String> = Vec::new();
        let mut rows_tags: Vec<Option<String>> = Vec::new();
        let mut rows_series_id: Vec<u32> = Vec::new();
        let mut rows_timestamp: Vec<i64> = Vec::new();
        let mut rows_fields: Vec<Option<String>> = Vec::new();

        let files = self.list_all_seg_files()?;

        for (idx, path) in files.into_iter().enumerate() {
            if idx % self.partition_count != partition {
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
        write!(
            f,
            "LuxSegmentsExec(partitions={}, dirs={})",
            self.partition_count,
            self.segments_dirs.len()
        )
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

#[derive(Debug, Serialize, Deserialize)]
struct LuxSegmentsExecSerde {
    segments_dirs: Vec<String>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    partition_count: usize,
}

/// PhysicalExtensionCodec for LuxSegmentsExec so it can be sent over Flight to workers.
#[derive(Debug, Default)]
pub struct LuxSegmentsCodec;

impl PhysicalExtensionCodec for LuxSegmentsCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &datafusion::execution::TaskContext,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return Err(datafusion_proto::protobuf::proto_error(
                "LuxSegmentsExec is a leaf node and must have 0 inputs",
            ));
        }

        let decoded: LuxSegmentsExecSerde = serde_json::from_slice(buf).map_err(|e| {
            datafusion_proto::protobuf::proto_error(format!(
                "Failed to decode LuxSegmentsExec (json): {e}"
            ))
        })?;

        Ok(Arc::new(LuxSegmentsExec::new(
            LuxSegmentsTableProvider::schema_ref(),
            decoded.segments_dirs,
            decoded.projection,
            decoded.limit,
            decoded.partition_count,
        )))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let Some(node) = node.as_any().downcast_ref::<LuxSegmentsExec>() else {
            return Err(datafusion_proto::protobuf::proto_error(format!(
                "Unexpected plan {}",
                node.name()
            )));
        };

        let ser = LuxSegmentsExecSerde {
            segments_dirs: node.segments_dirs.clone(),
            projection: node.projection.clone(),
            limit: node.limit,
            partition_count: node.partition_count,
        };

        let bytes = serde_json::to_vec(&ser).map_err(|e| {
            datafusion_proto::protobuf::proto_error(format!(
                "Failed to encode LuxSegmentsExec (json): {e}"
            ))
        })?;

        buf.extend_from_slice(&bytes);
        Ok(())
    }
}