# CockroachDB Debug MCP Server

This MCP server provides tools for analyzing CockroachDB metrics and time series data from debug artifacts.

## Available Tools

### `search-metrics`
Search CockroachDB's 2000+ metrics by keywords to discover relevant metrics.

**Parameters:**
- `query` (required): Space-separated keywords (e.g., "cpu", "changefeed latency", "gc ttl")
- `limit` (optional): Max results to return (default/max: 50)

**Search behavior:**
- All keywords must match (AND logic)
- Case-insensitive
- Searches across: metric name, description, category, type, unit, usage guidance

**Response includes:**
- `total_found`: Total matches before limit
- `returned`: Number of results returned
- `metrics`: Array of matching metrics with:
  - `name`: Metric identifier for use with `query-tsdump`
  - `category`: Layer/Category (e.g., "SYSTEM/CPU")
  - `type`: GAUGE, COUNTER, or HISTOGRAM
  - `unit`: BYTES, SECONDS, COUNT, etc.
  - `description`: What the metric measures
  - `essential`: Whether this is a key metric for monitoring
  - `how_to_use`: Guidance on interpreting the metric (when available)

**Example:**
```json
{
  "name": "search-metrics",
  "arguments": {
    "query": "changefeed error",
    "limit": 10
  }
}
```

**Important:**
- **ALWAYS use `search-metrics` first** - Never make up or guess metric names
- Use the exact `name` from search results when calling `query-tsdump`
- Metric names are complex (e.g., "cr.node.sys.cpu.host.combined.percent-normalized")

**Tips:**
- Use specific terms: "gc ttl" finds better results than just "ttl"
- Essential metrics (`"essential": true`) are the most important for monitoring

---

### `query-tsdump`
Extract time series data for one or more metrics from a tsdump file, with automatic statistics and anomaly detection.

**Parameters:**
- `file` (required): Path to tsdump file (e.g., "./debug/tsdump.gob")
- `metrics` (recommended): Array of metric names to retrieve in a single scan
- `metric` (optional): Single metric name (for backward compatibility)

**Single metric response:**
- `metric_name`: The queried metric
- `source`: Source node/component ID
- `data_points`: Array of `[timestamp_nanos, value]` pairs
- `stats`: Statistical summary
  - `count`, `min`, `max`, `mean`, `std_dev`
  - `p50`, `p90`, `p95`, `p99`: Percentiles
  - `start_time`, `end_time`, `duration_seconds`: Time range
- `anomalies`: Detected patterns
  - `outliers`: Values >3σ from mean
  - `spikes`: >50% change between consecutive points
  - `flatline`: Variance <1% (coefficient of variation)
  - `gaps`: Time gaps >2x median interval

**Multiple metrics response:**
- `requested`: Number of metrics requested
- `found`: Number of metrics found
- `metrics`: Map of metric_name → metric data (same structure as single metric response)
- `not_found`: Array of metric names not found (if any)

**Examples:**

Single metric (backward compatible):
```json
{
  "name": "query-tsdump",
  "arguments": {
    "file": "./debug/tsdump.gob",
    "metric": "sys.cpu.host.combined.percent-normalized"
  }
}
```

Multiple metrics (recommended):
```json
{
  "name": "query-tsdump",
  "arguments": {
    "file": "./debug/tsdump.gob",
    "metrics": [
      "sys.cpu.host.combined.percent-normalized",
      "sys.gc.pause.ns",
      "livebytes"
    ]
  }
}
```

**Performance note:** Tsdump files can be 40-50GB. This tool scans the file once to find all requested metrics. **Always use the `metrics` array parameter when querying multiple metrics** to avoid multiple slow scans.

## Required Workflow

**You MUST follow this workflow - do not skip steps:**

1. **Identify potential root causes**: Think about what could cause the reported issue
   - **DO NOT search directly for the symptom** (e.g., don't search "latency" if user reports latency spike)
   - **DO search for underlying causes** (e.g., search "cpu", "memory", "disk io", "gc", "contention")
   - Examples of root cause thinking:
     - Latency spike → search "cpu", "memory pressure", "gc pause", "disk io", "network", "contention"
     - Query errors → search "memory", "timeout", "connection pool", "statement execution"
     - Replication lag → search "raft", "snapshot", "range split", "lease transfer"
     - High resource usage → search "goroutine", "file descriptor", "compaction", "admission control"

2. **Search for causal metrics**: Use `search-metrics` with root cause keywords
   - Search broadly at first (e.g., "cpu", "memory", "disk")
   - Then narrow down based on results (e.g., "gc", "compaction", "admission")
   - **Never skip this step** - metric names are not intuitive

3. **Query multiple metrics at once**: Call `query-tsdump` with ALL relevant metrics from search
   - **Use the `metrics` array parameter** with multiple metric names
   - This scans the file ONCE instead of multiple times
   - Copy metric names exactly as returned from search
   - Do not modify, abbreviate, or guess metric names

4. **Analyze data**: Review the response from `query-tsdump`
   - Check `stats` for overall behavior (min/max/percentiles)
   - Check `anomalies` for unusual patterns (spikes, outliers, gaps)
   - Correlate timing of anomalies with the reported issue
   - Outliers indicate unusual values
   - Spikes show sudden changes
   - Gaps may indicate collection issues or downtime

This MCP server focuses on time series analysis. Other artifacts will be supported in future versions.
