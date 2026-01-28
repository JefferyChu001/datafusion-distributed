use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

pub mod segments;

/// High level LuxDB table provider entrypoint.
///
/// This module is intended to host production-ready LuxDB integration (not example-only).
#[derive(Debug)]
pub struct LuxSegmentsTableProvider {
    inner: segments::LuxSegmentsTableProvider,
}

impl LuxSegmentsTableProvider {
    pub fn new_multi(segments_dirs: Vec<String>, partition_count: usize) -> Self {
        Self {
            inner: segments::LuxSegmentsTableProvider::new_multi(segments_dirs, partition_count),
        }
    }

    pub fn schema_ref() -> datafusion::arrow::datatypes::SchemaRef {
        segments::LuxSegmentsTableProvider::schema_ref()
    }

    pub fn codec() -> segments::LuxSegmentsCodec {
        segments::LuxSegmentsCodec
    }
}

#[async_trait]
impl TableProvider for LuxSegmentsTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

