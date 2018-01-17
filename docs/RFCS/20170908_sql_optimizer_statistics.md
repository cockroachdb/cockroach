- Feature Name: SQL Optimizer Statistics
- Status: in-progress
- Start Date: 2017-09-08
- Authors: Peter Mattis, Radu Berinde
- RFC PR: [#18399](https:////github.com/cockroachdb/cockroach/pull/18399)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC describes the motivations and mechanisms for collecting
statistics for use in powering SQL optimization decisions. The
potential performance impact is very large as the decisions an
optimizer makes can provide orders of magnitude speed-ups to queries.

# Motivation

Modern SQL optimizers seek the lowest cost for a query. The cost for a
query is usually related to time, but is usually not directly
expressed in units of time. For example, the cost might be in units of
disk seeks or RPCs or some unnamed unit related to I/O. A cost model
estimates the cost for a particular query plan and the optimizer seeks
to find the lowest cost plan from among many alternatives. The input
to the cost model are the query plan and statistics about the table
data that can guide selection between alternatives.

One example of where statistics guide query optimization is in join
ordering. Consider the natural join:

  `SELECT * FROM a NATURAL JOIN b`

In the absence of other opportunities, this might be implemented as a
hash join. With a hash join, we want to load the smaller set of rows
(either from `a` or `b`) into the hash table and then query that table
while looping through the larger set of rows. How do we know whether
`a` or `b` is larger? We keep statistics about the cardinality of `a`
and `b`.

Simple table cardinality is sufficient for the above query but fails
in other queries. Consider:

  `SELECT * FROM a JOIN b ON a.x = b.x WHERE a.y > 10`

Table statistics might indicate that `a` contains 10x more data than
`b`, but the predicate `a.y > 10` is filtering a chunk of the
table. What we care about is whether the result of the scan of `a`
after filtering returns more rows than the scan of `b`. This can be
accomplished by making a determination of the selectivity of the
predicate `a.y > 10` and then multiplying that selectivity by the
cardinality of `a`. The common technique for estimating selectivity is
to collect a histogram on `a.y` (prior to running the query).

A final example is the query:

  `SELECT * FROM system.rangelog WHERE rangeID = ? ORDER BY timestamp DESC LIMIT 100`

Currently, `system.rangelog` does not have an index on `rangeID`, but
does have an index (the primary key) on `timestamp`. This query is
planned so that it sequentially walks through the ranges in descending
timestamp order until it fills up the limit. If there are a large
number of ranges in the system, the selectivity of `rangeID = ?` will
be low and we can make an estimation of how many ranges we'll need to
query in order to find 100 rows. Rather than walking through the
ranges sequentially, we can query batches of ranges concurrently where
the size of batches is calculated from the expected number of matches
per range.

These examples are simple and clear. The academic literature is
littered with additional details. How do you estimate the selectivity
of multiple conjunctive or disjunctive predicates? How do you estimate
the number of rows output from various operators such as joins and
aggregations? Some of these questions are beyond the scope of this RFC
which is focused on the collection of basic statistics.

# Terminology

* Selectivity: The number of values that pass a predicate divided by
  the total number of values. The selectivity multiplied by the
  cardinality can be used to compute the total number of values after
  applying the predicate to set of values. The selectivity is
  interesting independent of the number of values because
  selectivities for multiple predicates can be combined.

* Histogram: Histograms are the classic data structure for computing
  selectivity. One kind that is frequently used is the equi-depth
  histogram, where each bucket contains approximately (or exactly) the
  same number of values. More complex histograms exist that aim to
  provide more accuracy in various data distributions, e.g. "max-diff"
  histograms. A simple one-pass algorithm exists for constructing an
  equi-depth histogram for ordered values. A histogram for unordered
  values can be constructed by first sampling the data (e.g. using
  reservoir sampling), sorting it and then using the one-pass
  algorithm on the sorted data.

* Cardinality: the number of distinct values (on a single column, or
  on a group of columns).

* Sketch: Used for cardinality estimation. Algorithms such as
  [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
  use a hash of the value to estimate the number of distinct values.
  Sketches can be merged allowing for parallel computation. For
  example, the sketches for distinct values on different ranges can be
  combined into a single sketch for an entire table.

# Design considerations

* Statistics need to be available on every node performing query
  optimization (i.e. every node receiving SQL queries).

* Statistics collection needs to be low overhead but not necessarily
  the lowest overhead. Statistics do not need to be updated in real
  time.

* Statistics collection should be decoupled from consumption. A simple
  initial implementation of statistics collection should not preclude
  more complex collection in the future.

* The desired statistics to collect are:
    - a histogram for a given column
    - a cardinality for a given set of columns

* The count of distinct values for a column or group of columns can be
  computed using a sketch algorithm such as
  [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Such
  sketches can be merged making it feasible to collect a sketch at the
  range-level and combine the sketches to create a table-level sketch.

* Optimizing `AS OF SYSTEM TIME` queries implies being able to
  retrieve statistics at a point in time. The statistics do not need
  to be perfectly accurate, but should be relatively close. On the
  other hand, historical queries are relatively rare right now.

# Design overview

This RFC focuses on the infrastructure and does not set a policy for
what stats to collect or when to collect them; these are discussed
under future work.

A single node per table will act as the central coordinator for
gathering table-level stats. This node sets up a DistSQL operation
that involves reading the primary index and sampling rows (as well as
computing sketches). While performed at a specific timestamp, the
table read operations explicitly skip the timestamp cache in order to
avoid disturbing foreground traffic.

[TBD: which node acts at the stat coordinator for a table? Presumably
there is some sort of lease based on the table ID. Can possibly reuse
or copy whatever mechanism is used for schema changes.]

A new system table provides storage for stats:
`system.table_statistics`. This table contains the count of distinct
values, NULL values and the histogram buckets for a column/index.

During query optimization, the stats for a table are retrieved from a
per-node stats cache. The cache is populated on demand from the
`system.table_statistics` table and refreshed periodically.

Pseudo-stats are used whenever actual statistics are unavailable. For
example, the selectivity of a less-then or greater-than predicate is
estimated as 1/3. The academic literature provides additional
heuristics to use when stats are unavailable.

An Admin UI endpoint is provided for visualizing the statistics for a
table.

## Detailed design

### `system.table_statistics`

The table-level statistics are stored in a new
`system.table_statistics` table. This table contains a row per
attribute or group of attributes that we maintain stats for. The
schema is:

```
CREATE TABLE system.table_statistics (
  table_id INT,
  statistic_id INT,
  column_ids []INT,
  created_at TIMESTAMP,
  row_count INT,
  cardinality INT,
  null_values INT,
  histogram BYTES,
  PRIMARY KEY (table_id, statistic_id)
)
```

Each table has zero or more rows in the `table_statistics` table
corresponding to the histograms maintained for the table.

* `table_id` is the ID of the table
* `statistic_id` is an ID that identifies each particular
  statistic for a table.
* `column_ids` stores the IDs of the columns for which the statistic
  is generated.
* `created_at` is the time at which the statistic was generated.
* `row_count` is the total number of rows in the table.
* `cardinality` is the estimated cardinality (on all columns).
* `null_values` is the number of rows that have a NULL on any
  of the columns in `column_ids`; these rows don't contribute
  to the cardinality.
* `histogram` is optional and can only be set if there is a single
  column; it encodes a proto defined as:

```
message HistogramData {
  message Bucket {
    // The estimated number of rows that are equal to upper_bound.
    optional int64 eq_rows;

    // The estimated number of rows in the bucket (excluding those
    // that are equal to upper_bound). Splitting the count into two
    // makes the histogram effectively equivalent to a histogram with
    // twice as many buckets, with every other bucket containing a
    // single value. This might be particularly advantageous if the
    // histogram algorithm makes sure the top "heavy hitters" (most
    // frequent elements) are bucket boundaries (similar of a
    // compressed histogram).
    optional int64 range_rows;

    // The upper boundary of the bucket. The column values for the
    // upper bound are encoded using the same ordered encoding used
    // for index keys.
    optional bytes upper_bound;
  }

  // Histogram buckets. Note that NULL values are excluded from the
  // histogram.
  repeated Bucket buckets;
}
```

[TBD: using a proto for the histogram data is convenient but makes
ad-hoc querying of the stats data more difficult. We could store the
buckets in a series of parallel SQL arrays. Or we could store them in
a separate table.]

### Estimating selectivity

Given a predicate such as `a > 1` and a histogram on `a`, how do we
compute the selectivity of the predicate? For range predicates such as
`>` and `<`, we determine which bucket the value lies in, interpolate
within the bucket to estimate the fraction of the bucket values that
match the predicate (assuming a uniform data distribution) and perform
a straightforward calculation to determine the fraction of values of
the entire histogram the predicate will allow through.

An equality predicate deserves special mention as it is common and
somewhat more challenging to estimate the selectivity for. The count
of distinct values in a bucket is assumed to have a uniform
distribution across the bucket. For example, let's consider a
histogram on `a` where the bucket containing the value `1` has a
`count` of `100` and a `distinct_count` of `2`. This means there were
`100` rows in the table with values for `a` that fell in the bucket,
but only `2` distinct values of `a`. We compute the selectivity of `a
= 1` as `100 / 2 == 50 rows` (an additional division by the table
cardinality can provide a fraction). In general, the selectivity for
equality is `bucket_count / bucket_distinct_count`.

### Collecting statistics

Consider the table:

```
CREATE TABLE test (
  k INT PRIMARY KEY,
  v STRING,
  INDEX (v, k)
)
```

At a high-level, we will maintain statistics about each column and
each tuple of columns composing an index. For the above table, that
translates into statistics about `k`, `v` and the tuple `(v, k)`. The
per-column statistics allow estimation of the selectivity of a
predicate on that column. Similarly, the per-index statistics allow
estimation of the selectivity of a predicate on the columns in the
index. Note that in general there are vastly more possible
permutations and combinations of columns that we don't maintain
statistics on than indexes. The intuition behind collecting statistics
on indexes is that the existence of the index is a hint that the table
will be queried in such a way that the columns in the index will be
used. 

The collection of statistics for a table span will be provided by a
`Sampler` processor. This processor receives rows from a TableReader
and:
 - calculates sketches for estimating the density vector, and
 - selects a random sample of rows of a given size; it does this by
   generating a random number for each row and choosing the rows with
   the top K values. The random values are returned with each row,
   allowing multiple sample sets to be combined in a single set.

```
enum SketchType {
  HLL_PLUS_PLUS_v1 = 0
}

// SamplerSpec is the specification of a "sampler" processor which
// returns a sample (random subset) of the input columns and computes
// cardinality estimation sketches on sets of columns.
//
// The sampler is configured with a sample size and sets of columns
// for the sketches. It produces one row with global statistics, one
// row with sketch information for each sketch plus at most
// sample_size sampled rows.
//
// The internal schema of the processor is formed of two column
// groups:
//   1. sampled row columns:
//       - columns that map 1-1 to the columns in the input (same
//         schema as the input).
//       - an INT column with the random value associated with the row
//         (this is necessary for combining sample sets).
//   2. sketch columns:
//       - an INT column indicating the sketch index
//         (0 to len(sketches) - 1).
//       - an INT column indicating the number of rows processed
//       - an INT column indicating the number of NULL values
//         on the first column of the sketch.
//       - a BYTES column with the binary sketch data (format
//         dependent on the sketch type).
// Rows have NULLs on either all the sampled row columns or on all the
// sketch columns.
message SamplerSpec {
  optional uint32 sample_size;

  message SketchSpec {
    optional SketchType sketch_type;

    // Each value is an index identifying a column in the input stream.
    repeated uint32 columns;
  }
  repeated SketchSpec sketches;
}
```

A different `SampleAggregator` processor aggregates the results from
multiple Samplers and generates the histogram and other statistics
data. The processor is configured to populate the relevant rows in
`system.table_statistics`.

```
message SampleAggregator {
  optional sqlbase.TableDescriptor table;

  // The processor merges reservoir sample sets into a single
  // sample set of this size. This must match the sample size
  // used for each Sampler.
  optional uint32 sample_size;

  message SketchSpec {
    optional SketchType sketch_type;

    // Each value is a sqlbase.ColumnID.
    repeated uint32 column_ids;

    // If set, we generate a histogram for the first column in the sketch.
    optional bool generate_histogram;
  }
  repeated SketchSpec sketches;

  // The i-th value indicates the column ID of the i-th sampled row
  // column.
  repeated uint32 sampled_column_ids;

  // Indicates the columns for which we want to generate histograms.
  // Must be a subset of the sampled column IDs.
  repeated uint32 histogram_column_ids;
}
```

The SampleAggregator combines the samples into a single sample set (by
choosing the rows with the top K random values across all the sets);
the histogram is constructed from this sample set. In terms of
implementation, both the Sampler and SampleAggregator can use the same
method of obtaining the top-K rows. Some efficient methods:
 - maintain the top K rows in a heap. Updating the heap is a
   logarithmic operation, but it only needs to happen when we find a
   new top element (for the initial sampling, we have only O(KlogK)
   operations).
 - maintain the top 2K (or 1.5K) rows; when the set is full, select
   the Kth element (which takes O(K)) and trim the set down to K.

This RFC is not proposing a specific algorithm for constructing the
histogram. The literature indicates that a very good choice is the
Max-Diff histogram with the "area" diff metric; this has a fairly
simple construction algorithm. We may implement a simpler (e.g.
equi-depth histogram) as a first iteration. Note that how we generate
the histogram does not affect the histogram data format, so this can
be changed arbitrarily.

[TBD: How many buckets to use for the histogram? SQL Server uses at
most 200].

Good choices for the sketch algorithm are
[HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
and its improved version
[HLL++](http://research.google.com/pubs/pub40671.html). There are a
few go implementations for both on github with permissive licenses we
can use. The infrastructure allows adding better sketch algorithms later. 

A DistSQL plan for this operation will look like this:
![Sample plan](images/stats-distsql-plan.png?raw=true "Logical Plan")

### Stats cache

To cache statistics, we emply two caches:
  1. *statistics cache*: at the level of table and stores information
     for all statistics available for each table. The information
     excludes the histogram data.
  2. *histogram* cache: each entry is a `(table_id, statistic_id)`
     pair and stores the full statistic information, including the
     histogram data.
 
The statistics cache is populated whenever we need the statistics for
a table. It gets populated by reading the relevant rows from
`table_statistics`; whenever this happens, we can choose to add the
histograms to the histogram cache as well (since we're scanning that
data anyway). The statistics cache can hold a large number of tables,
as the metadata it stores is fairly small.
 
The histogram cache is more limited because histograms are expected to
be 5-10KB in size. Whenever a histogram is required, the cache is
populated by reading the one row from `table_statistics`.
 
TBD: how do we update the statistics cache when a new statistic is
available? One option is to expire entries from the statistics
periodically to force a rescan; another option is to gossip a key
whenever a new table statistic is available.

The API for accessing the cache mirrors the two levels of caching:
there will be a function to obtain information (metadata) for all
statistics of a table, and a function to obtain the full information
for a given statistic. We can also have a function that returns all
statistics that include certain columns (which can potentially be more
efficient than retrieving each histogram separately).

## Drawbacks

* There are number of items left for Future Work. It is possible some
  of these should be addressed sooner.

* Are there any additional simplifications that could be made for the
  initial implementation which wouldn't constrain Future Work?

## Rationale and Alternatives

* Range-level statistics could be computed by a new KV operation.
  While this may yield some performance benefits, it would require
  pushing down more knowledge of row encoding, as well as the sketch
  algorithms themselves. The Sampler processor model is quite a bit
  cleaner in terms of separating the implementation.

* Range-level statistics could be computed by a new storage-level
  `StatsQueue` that periodically refreshes the stats for a
  range. Pacing of the queue could be driven by the number of
  adds/deletes to the range since the last refresh. We'd want the
  range-level stats to generate histograms because back of the
  envelope calculations indicate that storing samples for
  cluster-level aggregation would be too large. Storing histograms at
  the range-level might also be too large as we'd have one histogram
  per column which could get expensive for rows with lots of columns.

  - How to combine range-level histograms into cluster-level
    histograms?

  - Is it problematic for the range-level stats for different ranges of
    a table to be gathered at different times? How does that affect
    later cluster-level aggregation?

  - We split ranges at table boundaries, but not at index boundaries. So
    a range may have to provide more than 1 set of statistics. The
    range-level statistics collection will probably need to know about
    the format of keys to identify table/index boundaries.

* Range-level statistics could be gathered during RocksDB
  compactions. The upside of doing so is that the I/O is essentially
  free. The downside is that we'd need to perform the range-level
  statistics gathering in C++. This decision seems independent of much
  of the rest of the machinery. Nothing precludes us from gathering
  range-level statistics during RocksDB compactions in the future.

* The centralized stats table provides a new failure mode. If the
  stats table is unavailable we would proceed without the stats but
  doing so might make a query run orders of magnitude slower. An
  alternative is to store a table's stats next to the table data
  (e.g. in a separate range). We can then require the stats are
  present for complex queries (e.g. queries for which the best plan,
  without stats, has very high cost); this is similar to the existing
  failure mode where if you lose some ranges from a table, you might
  not be able to run queries on that table. The concern about the new
  failure mode is not new to the stats table: most of the system
  metadata tables have the same concern. Storing the stats for a table
  near the table data could be done using a special key
  (e.g. `/Table/TableID/MaxIndexID`). This would help the
  backup/restore scenario as well.

* There are other ways to obtain and combine reservoir sets, e.g.
  [this blog post](https://ballsandbins.wordpress.com/2014/04/13/distributedparallel-reservoir-sampling/)
  Note though that we would still need to pass the sampling rate with
  each row; otherwise the SampleAggregator can't tell which rows are
  coming from which set. The "top K random numbers" idea is cleaner.

## Future work


* Determining when and what stats to collect. We will probably start
  with an explicit command for calculating statistics. Later we can
  work on automatic stat generation, and automatically choosing which
  statistics to gather. Columns that form indexes are a good signal
  that we may see constraints on those columns, so we can start by
  choosing those columns (or column groups).

* We don't need stats for every column in a table. For example,
  consider the "second line" of an address, the "address complement".
  This is the kind of column that mostly gets printed, and seldom gets
  queried. Determining which columns to collect stats on is a tricky
  problem. Perhaps there should be feedback from query execution to
  indicate which columns and groups of columns are being used in
  predicates. Perhaps there should be DBA control to indicate that
  stats on certain columns should not be collected.

* For columns that are already sorted (i.e. the first column in an
  index), we can build the histogram directly instead of using a
  sample. Doing so can generate a more accurate histogram. We're
  leaving this to future work as it can be considered purely an
  optimization.

* Provide support for historical stats in order to better optimize `AS
  OF SYSTEM TIME` queries. The relatively rarity of such queries is
  motivating not supporting them initially and to just use the current
  stats.

* While executing a query we might discover that the expectations we
  inferred from the statistics are inaccurate. At a minimum, a
  significant discrepancy should trigger a stats refresh. The actual
  data retrieved by the query could also be used to update the
  statistics, though this complicates normal query execution. See also
  [self-tuning
  histograms](https://ashraf.aboulnaga.me/pubs/sigmod99sthist.pdf).

* If the stats haven't been updated in a long time and we happen to be
  doing a full table-range scan for some query, we might as well
  recalculate the stats too in the same pass.

* Implement a strategy for detecting "heavy hitters" (a.k.a. the N
  most frequent values). The literature says this can be done by
  performing a second pass of sampling. The first pass performs a
  normal reservoir sample. The N most frequent values are very likely
  to appear in this sample, so sort the sample by frequency and
  perform a second pass where we compute the exact counts for the N
  most frequent values in the first pass sample. The "heavy hitters"
  would be subtracted from the histogram and maintained in a separate
  list. Note that a second pass over the table data would also allow a
  much better estimation of the distinct values per histogram
  bucket. The downside is that we'd be performing a second pass over
  the table which would likely double the cost of stats
  collection. Perhaps we can find a query driven means of determining
  whether this second pass is worthwhile.

* For interleaved tables, we can optimize the stats work so that we
  collect the stats for both the parent and interleaved table at the
  same time. It is possible that this will not fall into future work
  but be required in the initial implementation in order to support
  interleaved tables.

## Unresolved questions

* How is the central coordinator selected? Should there be a different
  coordinator per-table? How is a request to analyze a table directed
  to the central coordinator?

* Should stats be segregated by partition? Doing so could help if we
  expect significant partition-specific data distributions. Would
  doing so make cross-partition queries more difficult to optimize?
  See the [table partitioning](20170208_sql_partitioning.md)
  RFC. Segregating stats by partition might make repartitioning more
  difficult. Or perhaps it makes stats inaccurate for a period of time
  after repartitioning. If partitions do not have a partition ID (one
  of the proposals) we'll need to find some way to identify the
  partition for stats storage.

* What if a column has very large values? Perhaps we want to adjust
  the number of buckets depending on that.
