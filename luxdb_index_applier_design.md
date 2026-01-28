# LuxDB Index Applier: A Technical Design for Distributed Predicate Pushdown

## 1. Introduction

### 1.1. Problem Statement

The current implementation of `LuxSegmentsExec` for querying LuxDB data within `datafusion-distributed` performs a full scan of all `.seg` files within the specified directories. While it supports partitioning, it does not leverage the rich shard-level indexes (`inverted_index.dat`, `forward_index.dat`) provided by LuxDB. This leads to significant I/O inefficiency, as queries with highly selective filters (e.g., on specific tags, metrics, or time ranges) still require reading and decoding all data points, only to discard most of them at a later `FilterExec` stage.

### 1.2. Goal

This document outlines the design for an **Index Applier**. The primary goal is to implement predicate pushdown by utilizing LuxDB's indexes to dramatically reduce the amount of data read from disk. This will be achieved by:

1.  **File Pruning**: Skipping entire `.seg` files that do not contain relevant data for a given query.
2.  **Series Pruning**: Within a given `.seg` file, reading only the `SeriesBlock`s that match the query's filters.

This mechanism will be fully integrated into the `datafusion-distributed` framework, ensuring that the benefits of index-based pruning are realized in a distributed query execution environment.

## 2. Index Structure Review

Based on `luxdb-storage-format.md`, we have three levels of indexes available per shard:

-   **Inverted Index (`inverted_index.dat`)**: The primary tool for filtering. It maps `(metric, tag_key, tag_value)` to a `RoaringBitmap` of `SeriesId`s. This allows us to quickly find all series that match specific tag-based predicates.

-   **Forward Index (`forward_index.dat`)**: Maps a `SeriesId` back to its full metadata (`metric`, `tags`). This is useful for post-verification or reconstructing metadata without scanning the `.seg` files.

-   **Segment-Level Index (in `.seg` files)**: Each `.seg` file contains an index block that maps a `SeriesId` to its data's exact `offset`, `size`, and `(min_time, max_time)` range within that file. The file's footer also contains the overall `(min_time, max_time)` for all data in the segment.

## 3. Core Design: The Index Applier

The Index Applier will be a logical component whose functionality is primarily inserted into the `TableProvider::scan` method.

### 3.1. Insertion Point

The logic will reside within `luxdb_table::LuxSegmentsTableProvider::scan`. Before creating the `LuxSegmentsExec` plan, this is where we have access to the crucial inputs from DataFusion: the `filters` (`&[Expr]`), `projection`, and `limit`.

### 3.2. Data Flow and Logic

The `scan` method will be enhanced with the following step-by-step data flow:

**Step 1: Parse DataFusion Filters**

-   Iterate through the `filters: &[Expr]` array provided by DataFusion.
-   Extract actionable predicates. Initially, we will support:
    -   `metric = '...'`
    -   `tags LIKE '%"key":"value"%'` (a simple but effective starting point for the JSON string format).
    -   `timestamp BETWEEN <start> AND <end>`
    -   `series_id IN (...)`

**Step 2: Query Inverted Index**

-   For each shard, load the `inverted_index.dat` (ideally into a cached reader).
-   For each extracted `metric` or `tags` filter, query the inverted index to retrieve a `RoaringBitmap` of matching `SeriesId`s.
-   If multiple tag filters are present (connected by `AND`), perform an intersection of the resulting bitmaps.
-   The output of this step is a final `RoaringBitmap` of candidate `SeriesId`s that satisfy all tag/metric predicates.

**Step 3: Prune Segment Files and Series**

-   If no relevant filters are present, proceed with a full scan as before.
-   If there are filters, generate a precise list of scan tasks. This involves iterating through all `.seg` files in the shard(s) and applying two levels of pruning:
    1.  **Time-based File Pruning**: Use the `(min_time, max_time)` from each `.seg` file's footer. If a file's time range does not overlap with the query's `timestamp` filter, the entire file is skipped.
    2.  **Series-based Pruning**: For the remaining files, consult the file's internal index. Create a final list of `SeriesId`s to read from this file by intersecting the candidate `SeriesId`s (from Step 2) with the series actually present in this segment file.

**Step 4: Generate Pruned Scan Tasks**

-   The output of the previous steps is a pruned set of tasks, which can be represented as `Vec<PrunedScanTask>`.
-   A `PrunedScanTask` would be a struct like:

    ```rust
    struct PrunedScanTask {
        file_path: PathBuf, // Which .seg file to read
        series_to_read: Option<RoaringBitmap>, // None means read all, Some(...) means read only these series
    }
    ```

### 3.3. Modified `LuxSegmentsExec`

The `LuxSegmentsExec` plan will be modified to accept and execute these pruned tasks.

-   The `new` method will accept the `Vec<PrunedScanTask>`.
-   Its internal partitioning logic will now be based on distributing these tasks, not just files.
-   The `execute` method (or `load_partition_batch`) will receive a subset of these tasks. For each task, it will:
    -   Open the specified `.seg` file.
    -   If `series_to_read` is `Some(bitmap)`, it will use the segment's internal index to find the offset and size for each `SeriesId` in the bitmap, read only those `SeriesBlock`s, and decode them.
    -   If `series_to_read` is `None`, it will perform a full scan of that file (fallback behavior).

## 4. Distributed Execution Strategy

This design integrates seamlessly with the `datafusion-distributed` model.

1.  **Driver-Side Logic**: The **driver** node, executing `TableProvider::scan`, will be responsible for all index lookups and task generation. It loads the `inverted_index.dat` for each shard, applies the filters, and creates the global list of `PrunedScanTask`s.

2.  **Task Serialization (`PhysicalExtensionCodec`)**: The `LuxSegmentsExec` plan needs to be sent to workers. Its `PhysicalExtensionCodec` (`LuxSegmentsCodec`) will be modified. Instead of serializing just directory paths, it will now serialize the `Vec<PrunedScanTask>`.

    ```rust
    // In luxdb_table.rs
    #[derive(Debug, Serialize, Deserialize)]
    struct LuxSegmentsExecSerde {
        tasks: Vec<PrunedScanTask>, // Changed from segments_dirs
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        // partition_count is implicit in the task distribution
    }
    ```

3.  **Worker-Side Logic**: Each **worker** receives a `DistributedExec` stage containing the `LuxSegmentsExec`. The worker decodes the plan using the same `LuxSegmentsCodec`. The `PartitionIsolatorExec` (or a similar mechanism) ensures that each worker's instance of `LuxSegmentsExec` only executes a subset of the total `PrunedScanTask`s assigned to it by the driver.

4.  **Data Locality**: This model assumes that all workers have access to the shared storage where the shard data resides (e.g., via NFS, S3, or because they are on the same machine). The file paths in `PrunedScanTask` must be accessible from the workers.

## 5. Implementation Plan

1.  **Enhance `luxdb_table.rs`**: 
    -   Modify `LuxSegmentsExec` to store and operate on a list of `PrunedScanTask`s instead of just directories.
    -   Update `LuxSegmentsExec::execute` to handle the pruned task logic.
    -   Update `LuxSegmentsCodec` to serialize/deserialize the `PrunedScanTask` list.

2.  **Implement Index Loading in `luxdb` Crate**: Ensure the `luxdb` crate exposes robust and efficient readers for `inverted_index.dat` and `forward_index.dat` that can be used by the applier.

3.  **Implement the Applier Logic in `TableProvider::scan`**:
    -   Add the filter parsing logic.
    -   Add the index querying and bitmap manipulation logic.
    -   Add the file and series pruning logic to generate the `PrunedScanTask` list.

4.  **Update Driver and Worker Examples**:
    -   Ensure `localhost_run.rs` (driver) correctly passes filters down to the `scan` method.
    -   Ensure `localhost_worker.rs` has all necessary dependencies to decode and execute the enhanced `LuxSegmentsExec`.

5.  **Testing**: Add tests to verify that:
    -   Queries with selective filters result in a smaller number of tasks or tasks with specific series IDs.
    -   The query results remain correct after pruning is applied.
    -   Performance improves significantly for selective queries.

By following this design, the `datafusion-distributed` project can fully leverage LuxDB's indexing capabilities, transforming it from a simple distributed full-scan engine into a powerful, index-accelerated distributed time-series query platform.

