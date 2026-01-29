use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringBuilder, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use luxdb::storage::segment::SegmentReader;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesBlockRef {
    pub series_id: u32,
    pub offset: u64,
    pub size: u32,
    pub min_time: i64,
    pub max_time: i64,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrunedScanTask {
    pub seg_path: String,
    pub query_time_range: Option<(i64, i64)>,
    /// None => fallback (scan all series)
    /// Some => only read these series blocks (datablocks)
    pub blocks: Option<Vec<SeriesBlockRef>>,
    /// For plan/debugging only: whether we pruned this file by footer time range.
    pub file_time_range: Option<(i64, i64)>,
}

#[derive(Debug)]
pub struct LuxSegmentsTableProvider {
    segments_dirs: Vec<String>,
    partition_count: usize,
}

impl LuxSegmentsTableProvider {
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

    fn list_all_seg_files(&self) -> anyhow::Result<Vec<String>> {
        let mut all = Vec::new();
        for dir in &self.segments_dirs {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .with_context(|| format!("read_dir failed: {dir}"))?
                .flatten()
                .collect();
            entries.sort_by_key(|e| e.file_name());
            for e in entries {
                let p = e.path();
                if p.extension().map(|ext| ext == "seg").unwrap_or(false) {
                    all.push(p.to_string_lossy().to_string());
                }
            }
        }
        all.sort();
        Ok(all)
    }

    /// Index Applier v2:
    /// - file-level pruning by seg footer time range (if timestamp filter exists)
    /// - datablock (series block) pruning for `series_id = <literal>`
    fn apply_index_applier_v2(&self, filters: &[Expr]) -> anyhow::Result<Vec<PrunedScanTask>> {
        // Extract query time bounds
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;
        for f in filters {
            extract_timestamp_bounds(f, &mut min_ts, &mut max_ts);
        }
        let query_time_range = match (min_ts, max_ts) {
            (None, None) => None,
            (Some(a), None) => Some((a, i64::MAX)),
            (None, Some(b)) => Some((i64::MIN, b)),
            (Some(a), Some(b)) => Some((a, b)),
        };

        // Extract series_id = <literal>
        let series_id_eq = extract_series_id_eq(filters);

        let mut tasks = Vec::new();
        for seg_path in self.list_all_seg_files()? {
            // Open segment (needed for footer+index)
            let mut reader = match SegmentReader::open(&seg_path) {
                Ok(r) => r,
                Err(_) => {
                    // fail open: if can't open, keep as a full scan task
                    tasks.push(PrunedScanTask {
                        seg_path,
                        query_time_range,
                        blocks: None,
                        file_time_range: None,
                    });
                    continue;
                }
            };

            let file_tr = reader.time_range();

            // File pruning by footer time range
            if let Some((qmin, qmax)) = query_time_range {
                if file_tr.1 < qmin || file_tr.0 > qmax {
                    continue;
                }
            }

            // Datablock pruning for series_id=
            let blocks = if let Some(target_series_id) = series_id_eq {
                // Use SegmentReader's internal per-series index via query/read_series.
                // We also capture block metadata for display.
                // Unfortunately SegmentReader doesn't expose IndexEntry directly, so we approximate:
                // - read_series/query implies the series exists; but we need offset/size/count/time.
                // To get exact values, we can re-open via luxdb and read the index in SegmentReader.
                // Since SegmentReader already has index loaded internally, we can derive metadata by
                // doing a cheap query(…, qmin, qmax) decision + read_series for full block.

                // Fast existence check:
                let exists = match query_time_range {
                    Some((qmin, qmax)) => {
                        // query() checks index entry time overlap before reading the block
                        !reader.query(target_series_id, qmin, qmax)?.is_empty()
                    }
                    None => reader.read_series(target_series_id)?.is_some(),
                };

                if !exists {
                    // No such series in this seg (or no overlap), prune the file entirely
                    continue;
                }

                // We can't get offset/size/count without exposing IndexEntry from luxdb.
                // For now, we still mark it as a single datablock selection with unknown offsets.
                // This is enough to drive execution to only read this series.
                Some(vec![SeriesBlockRef {
                    series_id: target_series_id,
                    offset: 0,
                    size: 0,
                    min_time: file_tr.0,
                    max_time: file_tr.1,
                    count: 0,
                }])
            } else {
                None
            };

            tasks.push(PrunedScanTask {
                seg_path,
                query_time_range,
                blocks,
                file_time_range: Some(file_tr),
            });
        }

        Ok(tasks)
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let tasks = self
            .apply_index_applier_v2(filters)
            .map_err(|e| DataFusionError::Execution(format!("index applier failed: {e}")))?;

        Ok(Arc::new(LuxSegmentsExec::new(
            Self::schema_ref(),
            tasks,
            projection.cloned(),
            limit,
            self.partition_count,
        )))
    }
}

#[derive(Debug, Clone)]
pub struct LuxSegmentsExec {
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub tasks: Vec<PrunedScanTask>,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    pub partition_count: usize,
    pub properties: PlanProperties,
}

impl LuxSegmentsExec {
    pub fn new(
        input_schema: SchemaRef,
        tasks: Vec<PrunedScanTask>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        partition_count: usize,
    ) -> Self {
        let output_schema: SchemaRef = if let Some(ref proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|i| input_schema.field(*i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            input_schema.clone()
        };

        let eq_properties = EquivalenceProperties::new(output_schema.clone());
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input_schema,
            output_schema,
            tasks,
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

        for (idx, task) in self.tasks.iter().enumerate() {
            if idx % self.partition_count != partition {
                continue;
            }

            let mut reader = SegmentReader::open(&task.seg_path)
                .with_context(|| format!("SegmentReader::open failed: {}", task.seg_path))?;

            // If blocks is Some, only read those datablocks.
            if let Some(blocks) = &task.blocks {
                for b in blocks {
                    if let Some((qmin, qmax)) = task.query_time_range {
                        let points = reader.query(b.series_id, qmin, qmax)?;
                        // We need metric/tags/fields metadata; query returns points only.
                        // So fallback to read_series to get metric/tags, then filter points.
                        if let Some(series_block) = reader.read_series(b.series_id)? {
                            let tags_json = if series_block.tags.is_empty() {
                                None
                            } else {
                                Some(serde_json::to_string(&series_block.tags)?)
                            };
                            for p in points {
                                rows_metric.push(series_block.metric.clone());
                                rows_tags.push(tags_json.clone());
                                rows_series_id.push(series_block.series_id);
                                rows_timestamp.push(p.timestamp);
                                let fields_json = if p.fields.is_empty() {
                                    None
                                } else {
                                    Some(serde_json::to_string(&p.fields)?)
                                };
                                rows_fields.push(fields_json);
                            }
                        }
                    } else {
                        if let Some(series_block) = reader.read_series(b.series_id)? {
                            let tags_json = if series_block.tags.is_empty() {
                                None
                            } else {
                                Some(serde_json::to_string(&series_block.tags)?)
                            };
                            for p in series_block.points {
                                rows_metric.push(series_block.metric.clone());
                                rows_tags.push(tags_json.clone());
                                rows_series_id.push(series_block.series_id);
                                rows_timestamp.push(p.timestamp);
                                let fields_json = if p.fields.is_empty() {
                                    None
                                } else {
                                    Some(serde_json::to_string(&p.fields)?)
                                };
                                rows_fields.push(fields_json);
                            }
                        }
                    }
                }

                continue;
            }

            // Fallback: scan all series
            let series_blocks = reader
                .iter_series()
                .with_context(|| format!("iter_series failed: {}", task.seg_path))?;

            for block in series_blocks {
                let tags_json = if block.tags.is_empty() {
                    None
                } else {
                    Some(serde_json::to_string(&block.tags)?)
                };

                for point in block.points {
                    if let Some((qmin, qmax)) = task.query_time_range {
                        if point.timestamp < qmin || point.timestamp > qmax {
                            continue;
                        }
                    }

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

        Ok(RecordBatch::try_new(self.output_schema.clone(), projected_arrays)
            .map_err(|e| anyhow::anyhow!(e))?)
    }
}

impl DisplayAs for LuxSegmentsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let preview_n = 3usize;
        let mut items = Vec::new();
        for (i, t) in self.tasks.iter().take(preview_n).enumerate() {
            let file_name = std::path::Path::new(&t.seg_path)
                .file_name()
                .map(|v| v.to_string_lossy())
                .unwrap_or_else(|| std::borrow::Cow::Borrowed("<unknown>"));
            let tr = match t.query_time_range {
                None => "".to_string(),
                Some((a, b)) => format!(" time=[{a},{b}]"),
            };
            let blk = match &t.blocks {
                None => " series_blocks=ALL".to_string(),
                Some(v) => {
                    let ids_preview: Vec<String> = v
                        .iter()
                        .take(3)
                        .map(|b| format!("{}", b.series_id))
                        .collect();
                    let extra = if v.len() > 3 { ";..." } else { "" };
                    format!(" series_blocks={} ids=[{}{}]", v.len(), ids_preview.join(","), extra)
                }
            };
            items.push(format!("#{} {}{}{}", i, file_name, tr, blk));
        }
        let preview = if self.tasks.len() > preview_n {
            format!("{}; ...", items.join(", "))
        } else {
            items.join(", ")
        };

        write!(
            f,
            "LuxSegmentsExec(partitions={}, tasks={}, preview=[{}])",
            self.partition_count,
            self.tasks.len(),
            preview
        )
    }
}

impl ExecutionPlan for LuxSegmentsExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
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
        Ok(Statistics::new_unknown(&self.output_schema))
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
    tasks: Vec<PrunedScanTask>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    partition_count: usize,
}

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
            decoded.tasks,
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
            tasks: node.tasks.clone(),
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

fn extract_series_id_eq(filters: &[Expr]) -> Option<u32> {
    for f in filters {
        if let Some(v) = extract_series_id_eq_from_expr(f) {
            return Some(v);
        }
    }
    None
}

fn extract_series_id_eq_from_expr(expr: &Expr) -> Option<u32> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if *op == Operator::Eq {
                if let Expr::Column(c) = left.as_ref() {
                    if c.name == "series_id" {
                        if let Expr::Literal(sv, _) = right.as_ref() {
                            if let Some(v) = scalar_to_u32(sv) {
                                return Some(v);
                            }
                        }
                    }
                }
            }
            // recurse
            extract_series_id_eq_from_expr(left.as_ref())
                .or_else(|| extract_series_id_eq_from_expr(right.as_ref()))
        }
        _ => None,
    }
}

fn extract_timestamp_bounds(expr: &Expr, min_ts: &mut Option<i64>, max_ts: &mut Option<i64>) {
    match expr {
        Expr::BinaryExpr(be) => {
            if be.op == Operator::And || be.op == Operator::Or {
                extract_timestamp_bounds(be.left.as_ref(), min_ts, max_ts);
                extract_timestamp_bounds(be.right.as_ref(), min_ts, max_ts);
                return;
            }

            // Handle timestamp op literal
            if let Expr::Column(c) = be.left.as_ref() {
                if c.name == "timestamp" {
                    if let Expr::Literal(sv, _) = be.right.as_ref() {
                        if let Some(v) = scalar_to_i64(sv) {
                            match be.op {
                                Operator::Gt | Operator::GtEq => {
                                    *min_ts = Some(min_ts.map(|cur| cur.max(v)).unwrap_or(v));
                                }
                                Operator::Lt | Operator::LtEq => {
                                    *max_ts = Some(max_ts.map(|cur| cur.min(v)).unwrap_or(v));
                                }
                                Operator::Eq => {
                                    *min_ts = Some(min_ts.map(|cur| cur.max(v)).unwrap_or(v));
                                    *max_ts = Some(max_ts.map(|cur| cur.min(v)).unwrap_or(v));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            extract_timestamp_bounds(be.left.as_ref(), min_ts, max_ts);
            extract_timestamp_bounds(be.right.as_ref(), min_ts, max_ts);
        }
        Expr::Between(b) => {
            if let Expr::Column(c) = b.expr.as_ref() {
                if c.name == "timestamp" {
                    if let Expr::Literal(sv, _) = b.low.as_ref() {
                        if let Some(v) = scalar_to_i64(sv) {
                            *min_ts = Some(min_ts.map(|cur| cur.max(v)).unwrap_or(v));
                        }
                    }
                    if let Expr::Literal(sv, _) = b.high.as_ref() {
                        if let Some(v) = scalar_to_i64(sv) {
                            *max_ts = Some(max_ts.map(|cur| cur.min(v)).unwrap_or(v));
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn scalar_to_i64(sv: &datafusion::scalar::ScalarValue) -> Option<i64> {
    match sv {
        datafusion::scalar::ScalarValue::Int64(Some(v)) => Some(*v),
        datafusion::scalar::ScalarValue::Int32(Some(v)) => Some(*v as i64),
        datafusion::scalar::ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        datafusion::scalar::ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        _ => None,
    }
}

fn scalar_to_u32(sv: &datafusion::scalar::ScalarValue) -> Option<u32> {
    match sv {
        datafusion::scalar::ScalarValue::UInt32(Some(v)) => Some(*v),
        datafusion::scalar::ScalarValue::UInt64(Some(v)) => Some(*v as u32),
        datafusion::scalar::ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as u32),
        datafusion::scalar::ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as u32),
        _ => None,
    }
}
