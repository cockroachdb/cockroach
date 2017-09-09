- Feature Name: SQL Optimizer Statistics
- Status: draft
- Start Date: 2017-09-08
- Authors: Peter Mattis
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

  `SELECT * FROM a JOIN b`

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

* Sketch: Used in cardinality estimation. Algorithms such as
  [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
  use a hash of the value to estimate the number of distinct
  values. Sketches can be merged allowing for parallel
  computation. For example, the sketches for distinct values on
  different ranges can be combined into a single sketch for an entire
  table.

* Histogram: Equi-depth (a.k.a. equi-height) histograms are the
  classic data structure for computing selectivity. More complex
  histograms exist that aim to provide more accuracy in various data
  distributions. In an equi-depth histogram, each bucket contains
  approximately (or exactly) the same number of values. A simple
  one-pass algorithm exists for constructing an equi-depth histogram
  for ordered values. A histogram for unordered values can be
  constructing by first sampling the data (e.g. using reservoir
  sampling), sorting it and then using the one-pass algorithm on the
  sorted data.

* Selectivity: The number of values that pass a predicate divided by
  the total number of values. The selectivity multiplied by the
  cardinality can be used to compute the total number of values after
  applying the predicate to set of values. The selectivity is
  interesting independent of the number of values because selectivies
  for multiple predicates can be combined.

# Design considerations

* Statistics need to be available on every node performing query
  optimization (i.e. every node receiving SQL queries).

* Statistics collection needs to be low overhead but not necessarily
  the lowest overhead. Statistics do not need to be updated in real
  time.

* Statistics collection should be decoupled from consumption. A simple
  initial implementation of statistics collection should not preclude
  more complex collection in the future.

* The desired statistics to collect are the number of distinct values
  for a given column or index, a per-column histogram over the
  column's values, and a per-index histogram of the indexes values
  (i.e. a tuple of column values).

* The count of distinct values for a column or index can be computed
  using a sketch algorithm such as
  [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Such
  sketches can be merged making it feasible to collect a sketch at the
  range-level and combine the sketches to create a table-level sketch.

* Optimizing `AS OF SYSTEM TIME` queries implies being able to
  retrieve statistics at a point in time. The statistics do not need
  to be perfectly accurate, but should be relatively close. On the
  other hand, historical queries are relatively rare right now.

# Design overview

A single node per table will act as the central coordinator for
gathering table-level stats. The stats coordinator periodically
refreshes the stats by sending an operation to every range of the
primary index. While performed at a specific timestamp, this operation
explicitly skips the timestamp cache in order to avoid disturbing
foreground traffic. The stats refresh interval will be on the order of
hours to days, depending on how expensive the stats refresh turns out
to be in practice. An `ANALYZE <table>` statement is provided which
will direct the stats coordinator for the table to refresh the
stats. (NB: triggering stats refreshes based on queries or table
modifications is left as future work). [TBD: which node acts at the
stat coordinator for a table? Presumably there is some sort of lease
based on the table ID. Can possibly reuse or copy whatever mechanism
is used for schema changes.]

The range-level stats request specifies the columns and indexes (tuple
of columns) to gather stats for. For each column and index it will
compute a sketch and gather a random sample of data using reservoir
sampling. The size of the samples can be adjusted by the central
coordinator so that the total sample sizes are reasonable even in very
large tables. The range-level operation will also return an exact
count of the number of NULL values for each column.

The central coordinator will gather the range-level sketches and
samples and compute a table-level estimate of the per-column and
per-index distinct values. The samples will be used to compute
per-column and per-index histograms.

A new system table provides storage for stats:
`system.table_histograms`. This table contains the count of distinct
values, NULL values and the histogram buckets for a column/index.

During query optimization, the stats for a table are retrieved from a
per-node stats cache. The cache is populated on demand from the
`system.table_histograms` table and refreshed periodically.

Pseudo-stats are used whenever actual statistics are unavailable. For
example, the selectivity of a less-then or greater-than predicate is
estimated as 1/3. The academic literature provides additional
heuristics to use when stats are unavailable.

An Admin UI endpoint is provided for visualizing the statistics for a
table.

## Detailed design

### `system.table_histograms`

The table-level statistics are stored in a new
`system.table_histograms` table. This table contains a row per
attribute or group of attributes that we maintain stats for. The
schema is:

```
CREATE TABLE system.table_histograms (
  table_id INT,
  histogram_id INT,
  is_index BOOL,
  created_at TIMESTAMP,
  distinct_count INT,
  null_count INT,
  data BYTES,
  PRIMARY KEY (table_id, histogram_id, is_index)
)
```

Each table has zero or more rows in the `table_histograms` table
corresponding to the histograms maintained for the table.

* `histogram_id` is either the ID of the column or the index upon
  which the histogram was built.

* `is_index` indicates whether the histogram was built for an index or
  an individual column and separates the column ID and index ID
  namespaces so that both IDs can be used for the `histogram_id`
  field. (If, in the future, we want to maintain a histogram on a
  tuple of non-indexed columns we can add an `IndexDescriptor` which
  does not materialize the keys and values for the index).

* `created_at` is the time at which the histogram was generated.

* `distinct_count` is an estimate of the number of distinct values

* `null_count` is the count of the number of `NULL` values for the
  histogram. This is used by queries that need to know the selectivity
  of an `IS NULL` or `IS NOT NULL` predicate.

* `data` is a proto defined as:

```
message HistogramData {
  message Bucket {
    // The total count of non-NULL values in the bucket.
    optional int64 count;
    // The count of distinct non-NULL values in the bucket.
    optional int64 distinct_count;
    // The upper boundary of the bucket. The column values for the
    // upper bound are encoded using the same ordered code used for index
    // keys.
    optional bytes upper_bound;
  }
  repeated Bucket buckets;
}
```

[TBD: using a proto for the histogram data is convenient but makes
ad-hoc querying of the stats data more difficult. We could store the
buckets in a series of parallel SQL arrays. Or we could store them in
a separate table.]

Note that the cardinality of the table (i.e. the number of rows) is
the `distinct_count` for the primary key index.

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

Range-level collection of statistics will be provided by a `Sample`
operation. Since the range does not know about the associated table
schema, `SampleRequest` will indicate the tuples of columns for which
to gather random samples:

```
message SampleRequest {
  message Tuple {
    // Indicates whether the tuple of column values is unique. If true
    // the count of distinct elements will be precise instead of an
    // estimate.
    optional bool unique;
    repeated int32 ids;
  }

  // The start and end keys over which to gather the samples.
  optional Span header;
  // The timestamp at which to gather stats.
  optional hlc.Timestamp timestamp;
  // The size of each sample to return.
  optional int32 sample_size;
  // The tuples of values to gather sketches and samples for. An
  // individual column is requested by specifying a single ID in a tuple.
  message Tuple tuples;
}

message Sketch {
  // TBD: the contents are dependent on the sketch algorithm used.
}

message TupleData {
  // Total count of the number of elements seen.
  optional int64 total_count;
  // Count of the number of NULL elements seen.
  optional int64 null_count;
  // Count of the number of samples.
  optional int64 sample_count;
  // Encoded samples containing sample_count tuples. The samples are
  // encoded using the key encoding routines.
  optional bytes samples; 
  // Sketch of the distinct values.
  optional Sketch sketch;
}

message SampleResponse {
  // The per-tuple data. The response tuples parallel the requested
  // tuples.
  message TupleData tuples;
}
```

Processing of a `SampleRequest` is performed by iterating over the
key-value pairs as of the request timestamp using `MVCCIterate`. The
key-value iteration needs to be aware of of column families in order
to avoid double-counting columns in keys.  For each tuple specified in
the request a `SampleBuilder` is created and the desired values are
supplied to it. A `SampleBuilder` implements reservoir sampling,
maintains a count of the NULL values and a HyperLogLog sketch.

In order to generate table-level stats, the central coordinator
constructs a DistSQL plan. The leaf-level of the plan are
`SampleRequests` sent to every range containing primary index keys for
the desired table. The results from the `Sample` operations are
processed in a `SampleAggregator` which combines the range-level
samples and sketches. Depending on the number of ranges in the table,
multiple levels of `SampleAggregators` can be planned to reduce the
number of incoming streams to a reasonable size. A final
`StatsGenerator` stage takes takes the aggregated lower-level samples
and sketches and produces a `TableStats` containing one or more
`TableHistograms`.

A `SampleAggregator` combines sketches and samples. The incoming
sample sets have each element sampled with probability `K/N` where `K`
is the sample size and `N` is the total number of values
considered. In order to merge the two sets of samples, we need to find
a common sampling rate and adjust the sets to that common rate. See
the appendix for details.

Constructing an equi-depth histogram from the table-level sample data
is trivial: we sort the sample data and then step through it so that
each bucket contains the same number of samples. For example, if the
table-level sample data contains 10,000 samples and we want a
histogram containing 200 buckets, the boundaries for the buckets are
chosen from every 50th sample. The literature indicates that ~1000
samples provide a sufficient size for constructing a histogram. Once
the equi-depth histogram is working, a MaxDiff histogram will be
explored as easy follow-on work. [TBD: How many buckets to use for the
histogram?]

### Stats cache

TODO: Describe the API by which the optimizer will request stats from
the cache and how caching will be performed. We want to cache at the
granularity of a histogram because very wide tables will have lots of
histograms most of which the optimizer will not need to use. A
histogram is expected to be ~5-10KB in size.

## Drawbacks

* There are number of items left for Future Work. It is possible some
  of these should be addressed sooner.

* Are there any additional simplifications that could be made for the
  initial implementation which wouldn't constrain Future Work?

## Rationale and Alternatives

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

## Future work

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

* Which sketch algorithm should be used? HyperLogLog? HLL++? An
  earlier variant such as LogLog or the original FM-Sketch algorithm?
  [MTS sketches](https://arxiv.org/pdf/1611.01853.pdf)?

* How is the central coordinator selected? Should there be a different
  coordinator per-table? How is a request to analyze a table directed
  to the central coordinator?

* A DistSQL expert needs to help flesh out the DistSQL specifics and
  sanity check the proposed usage.

* Should stats be segregated by partition? Doing so could help if we
  expect significant partition-specific data distributions. Would
  doing so make cross-partition queries more difficult to optimize?
  See the [table partitioning](20170208_sql_partitioning.md)
  RFC. Segregating stats by partition might make repartitioning more
  difficult. Or perhaps it makes stats inaccurate for a period of time
  after repartitioning. If partitions do not have a partition ID (one
  of the proposals) we'll need to find some way to identify the
  partition for stats storage.

## Appendix (merging reservoir sample sets)

We use a weighted random selection to merge two sets of samples, `R`
and `S` that have sampling rates of `K/M` and `K/N` respectively to
produce a new set `T` with rate `K/(M+N)`:

```
p := float64(M) / float64(M+N)
for i := 0; i < K; i++ {
  if rand.Float64() <= p {
    // choose a random item from R and add to T
  } else {
    // choose a random item from S and add to T
  }
}
return T
```

See this blog post on [distributed parallel reservoir
sampling](https://ballsandbins.wordpress.com/2014/04/13/distributedparallel-reservoir-sampling/)
for more details.  The items in `R` started with a sampling rate of
`K/M`. After the resampling they have a rate of `(K/M) * M/(M+N) =
K/(M+N)`. Similar math can show that items in `S` end up with the same
probability of being in the final set.

An alternative to this merging approach is to reframe the sampling
problem as finding the first K elements from a random
permutation. Without generating the permutation we can accomplish this
by assigning each element a random number. At the range-level we
return the K elements with the smallest random numbers along with the
random numbers themselves. Merging then involves taking two sets and
returning the K elements with the smallest assigned numbers. This is
conceptually quite a bit more straightforward. The downside is that we
need to pass along the random numbers and use a heap or similar
structure to find the smallest K elements while sampling and merging.
