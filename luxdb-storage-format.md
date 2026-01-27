# LuxDB 数据文件与索引文件存储格式（基于当前实现）

本文档根据以下两处资料整理：

- `luxdb/README.md`
- `luxdb/src/storage/index/README.md`

并结合代码实现（尤其是 `src/storage/engine/segment.rs`、`src/storage/tsdb/shard.rs`、`src/storage/index/{forward,inverted}.rs`）对“真实落盘格式”做了补全说明。

---

## 1. 文件与目录组织

在单机模式下，LuxDB 的存储目录通常类似：

```text
<data_dir>/
└── standalone/
    └── <db_name>/
        ├── shard_0/
        │   ├── forward_index.dat
        │   ├── inverted_index.dat
        │   └── segments/
        │       ├── 0_0.seg
        │       ├── 0_1.seg
        │       └── ...
        └── shard_1/
            ├── forward_index.dat
            ├── inverted_index.dat
            └── segments/
                └── ...
```

- `data_dir` 可通过配置覆盖（例如环境变量 `LUXDB_STORAGE__DATA_DIR`）。
- 每个 shard 一个目录：`shard_<id>/`。
- 索引文件与 segment 文件同属 shard 范围：
  - 正排索引：`forward_index.dat`
  - 倒排索引：`inverted_index.dat`
  - Segment 文件目录：`segments/`

### Segment 文件命名

代码里 `SegmentManager::new_writer_with_level(level)` 的命名格式为：

- `"{level}_{id}.seg"`

其中：

- `level`：LSM 层级（默认 `0`）
- `id`：单调递增的文件编号

> README 里示例 `0000000000000000.seg` 更像是“早期/示例命名”，以代码实现为准。

---

## 2. 数据文件：Segment（`.seg`）存储格式

### 2.1 概览

README 中给出的 segment 结构示意：

```text
┌─────────────────────────────────────────────────────────┐
│  Header (Fixed Size)                                    │
├─────────────────────────────────────────────────────────┤
│  Data Block                                             │
│  ┌─────────────────────────────────────────────────────┐│
│  │ SeriesBlock[n]                                      ││
│  │ ┌─────────────┬───────────────────────────────────┐ ││
│  │ │ Series ID   │ Data Points                       │ ││
│  │ │ (u32)       │ ┌─────────────┬─────────────────┐ │ ││
│  │ │             │ │ Timestamp   │ Fields          │ │ ││
│  │ │             │ │ (i64)       │ [(name, value)] │ │ ││
│  │ │             │ └─────────────┴─────────────────┘ │ ││
│  │ └─────────────┴───────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────┤
│  Index Block                                            │
│  ┌─────────────────────────────────────────────────────┐│
│  │ Index Entries                                       ││
│  │ ┌─────────────┬─────────────┬─────────────────────┐ ││
│  │ │ Series ID   │ Offset      │ Time Range & Count  │ ││
│  │ │ (u32)       │ (u64)       │ (i64, i64, u32)     │ ││
│  │ └─────────────┴─────────────┴─────────────────────┘ ││
│  └─────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────┤
│  Footer + Footer Size                                   │
└─────────────────────────────────────────────────────────┘
```

在当前实现中：

- **Header 固定 16 字节**（`HEADER_SIZE = 16`）
- 数据块由多个 **压缩后的 SeriesBlock** 顺序拼接
- Index Block 是 `Vec<IndexEntry>` 的 bincode 编码
- Footer 是 `SegmentFooter` 的 bincode 编码
- 文件最后 4 字节是 `footer_size (u32 little-endian)`，用于从文件尾定位 footer

### 2.2 Header

结构体（bincode 编码，随后 padding 到 16 字节）：

- `magic: u32`：固定为 `0x4C555844`（ASCII "LUXD"）
- `version: u32`：当前为 `1`
- `created_at: i64`：纳秒时间戳

写入流程：

1. `SegmentHeader` bincode 编码写入
2. 用 0 填充到 16 字节

### 2.3 Data Block：CompressedSeriesBlock 顺序拼接

每个 Series 写入时会先将 `SeriesBlock` 压缩为 `CompressedSeriesBlock`，再做 bincode 编码写入文件。

#### 2.3.1 原始 SeriesBlock（逻辑结构）

- `series_id: SeriesId`（实际类型来自 `protocol`，通常为 `u32`）
- `metric: String`
- `tags: Vec<(String, String)>`
- `points: Vec<DataPoint>`

`DataPoint`：

- `timestamp: i64`
- `fields: Vec<(String, f64)>`

#### 2.3.2 CompressedSeriesBlock（实际落盘）

落盘时写入的压缩结构：

- `series_id`
- `metric`
- `tags`
- `field_names: Vec<String>`：从第一个点提取字段名
- `point_count: usize`
- `timestamps_compressed: Vec<i64>`：时间戳的 Delta-of-delta 编码结果
- `fields_compressed: Vec<Vec<u8>>`：每个字段独立 Gorilla XOR 压缩后的字节串

> 注意：实现假设同一 series 的所有点拥有一致的字段顺序；否则会在压缩时用 `get(idx)` 缺失补 0。

### 2.4 Index Block：Vec<IndexEntry>

写完所有 series 数据后，会写一个索引块：

`IndexEntry` 字段：

- `series_id: SeriesId`
- `offset: u64`：该 series 数据块在文件中的起始偏移
- `size: u32`：该 series 数据块字节长度
- `min_time: i64`
- `max_time: i64`
- `count: u32`：点数

索引块写入位置：

- `index_offset = data_offset`（写完 data block 后的位置）

### 2.5 Footer + Footer Size

Footer（`SegmentFooter`）字段：

- `index_offset: u64`
- `index_size: u32`
- `data_offset: u64`（固定为 `HEADER_SIZE`）
- `data_size: u64`（`index_offset - HEADER_SIZE`）
- `series_count: u32`
- `point_count: u64`
- `min_time: i64`
- `max_time: i64`

写入顺序：

1. 写入 `footer_data`（bincode 编码）
2. 写入 `footer_size: u32`（little-endian）

读取时：

- 先从文件末尾读 4 字节得到 `footer_size`
- 再向前 seek 读取 footer
- 再根据 footer 中的 `index_offset/index_size` 读取 index block

---

## 3. 索引文件存储

LuxDB 在 shard 级别有两类索引：

- **正排索引（Forward Index）**：`SeriesId -> (metric, tags)`
- **倒排索引（Inverted Index）**：`metric + tag_key + tag_value -> RoaringBitmap(series_id)`

### 3.1 查询路径（逻辑）

`src/storage/index/README.md` 描述的典型流程：

1. 倒排索引按 tag 过滤得到候选 series 集合
2. 多个 tag 条件做集合交集（RoaringBitmap AND）
3. 用正排索引取回 series 的完整元信息

---

## 4. 正排索引文件：`forward_index.dat`

### 4.1 内存结构（逻辑）

- `DashMap<SeriesId, SeriesInfo>`
- `SeriesInfo { metric: String, tags: Vec<(String,String)> }`

### 4.2 持久化格式（实际落盘）

当前实现的 `ForwardIndex::save_to_disk()` 使用 **Index V1 Flusher** 写文件：

- 通过 `ForwardIndexFlusher` 逐条写入：
  - `prepare_key(series_id)`
  - `write_series_info(series_id, metric, tags)`
  - 最后 `commit()` / `close()`

因此 `forward_index.dat` 的“权威格式”由 `src/storage/index/v1/*` 定义。

> 兼容性说明：虽然 `ForwardIndex` 仍保留了 bincode 方式的 `load_from_disk_legacy()`，但 `save_to_disk()` 已经切换到了 v1 flusher。也就是说：**新写出的 forward_index.dat 更偏向 v1 格式**。

---

## 5. 倒排索引文件：`inverted_index.dat`

### 5.1 内存结构（逻辑）

主索引：

```text
MetricName
  -> TagKey
    -> TagValue
      -> RoaringBitmap(SeriesId)
```

反向索引：

- `SeriesId -> Vec<(TagKey, TagValue)>`

### 5.2 持久化格式（实际落盘）

当前实现的 `InvertedIndex::save_to_disk()` 使用 **Index V1 Flusher**：

- 遍历 `metric/tag_key/tag_value` 三层结构
- 对每个 (metric, tag_key, tag_value) 生成一个 `key: u32`
  - 代码里是一个简单 rolling hash（`*31 + byte`），仅用于示例/当前实现
- 写入对应的 `series_ids` 列表

同样地：`inverted_index.dat` 的“权威格式”在 `src/storage/index/v1/*`。

> README 中 `engine_decode` 的输出示例里有：`[Entry] 键: 2174380853`，对应这里生成的 `u32 key`。

---

## 6. 数据生成与落盘时机（Shard Flush）

以 `src/storage/tsdb/shard.rs` 的 flush 流程为准：

1. 从 MemTable 枚举所有 `series_ids`
2. 用 forward index 拿到每个 series 的 `metric/tags`
3. 从 MemTable 取出该 series 的所有点，组装成 `SeriesBlock`
4. 通过 `SegmentWriter` 依次写入
5. `writer.finish()` 写 index block + footer
6. 清空 MemTable
7. 将索引落盘：
   - `inverted_index.save_to_disk()` -> `inverted_index.dat`
   - `forward_index.save_to_disk()` -> `forward_index.dat`

---

## 7. 压缩算法说明

来自仓库 README（并在 segment 实现中实际使用）：

- **时间戳**：Delta-of-delta（`DeltaEncoder`）
- **字段值**：Gorilla XOR（`GorillaEncoder`）

---

## 8. 小结

- **Segment 文件（`.seg`）是数据的主要持久化载体**，内部自带 per-series 索引（offset/size/time range）。
- **Shard 级别索引文件**分为正排 `forward_index.dat` 和倒排 `inverted_index.dat`。
- 索引文件当前写入走 `index/v1` flusher（格式以 `src/storage/index/v1` 为准），并保留一定 legacy bincode 读取逻辑作为过渡。


