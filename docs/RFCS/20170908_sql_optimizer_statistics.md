- Feature Name: SQL Optimizer Statistics
- Status: draft
- Start Date: 2017-09-08
- Authors: Peter Mattis
- RFC PR: (PR # after acceptance of initial draft)
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
  other hand, historical queries are relatively rare right now. [TBD:
  Should we ignore providing stats for historical queries and use the
  current stats accepting the inaccuracy?]

# Design overview

A single node acts as the central coordinator for gathering
table-level stats. For each table, it periodically refreshes the stats
by sending an operation to every range of the primary index. While
performed at a specific timestamp, this operation explicitly skips the
timestamp cache in order to avoid disturbing foreground traffic.

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

Two new system tables provide storage for stats: `system.table_stats`
and `system.table_histograms`. The `system.table_stats` table contains
the count of distinct values and NULL values for a column/index. The
`system.table_histograms` table contains the histogram buckets for a
column/index.

During query optimization, the stats for a table are retrieved from a
per-node stats cache. The cache is populated on demand from the
`system` stats tables and refreshed periodically.

Pseudo-stats are used whenever actual statistics are unavailable. For
example, the selectivity of a less-then or greater-than predicate is
estimated as 1/3. The academic literature provides additional
heuristics to use when stats are unavailable.

An `ANALYZE <table>` statement is provided which will direct the
central stats coordinator to refresh the stats for the specified
table.

An Admin UI endpoint is provided for visualizing the statistics for a
table.

## Detailed design

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

  // The ID of the range to gather samples from.
  optional RangeID range_id;
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
`StatsGenerator` stage takes takes the aggregates lower-level samples
and sketches and produces a `TableStats` containing one or more
`TableHistograms`.

A `SampleAggregator` combines sketches and samples. The incoming
sample sets have each element sampled with probability K/N where K is
the sample size and N is the total number of values
considered. Combining two sets of samples involves adjusting the
sampling probabilities of one of the sets to match the other,
concatenating the sets and optionally downsampling to a smaller size.

Constructing an equi-depth histogram from the table-level sample data
is trivial: we sort the sample data and then step through it so that
each bucket contains the same number of samples. For example, if the
table-level sample data contains 10,000 samples and we want a
histogram containing 200 buckets, the boundaries for the buckets are
chosen from every 50th sample. [TBD: How many buckets and how many
samples to use? Seems like these numbers should be related to the
table size.]

## Drawbacks

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

## Unresolved questions

* Which sketch algorithm should be used? HyperLogLog? HLL++? An
  earlier variant such as LogLog or the original FM-Sketch algorithm?
  [MTS sketches](https://arxiv.org/pdf/1611.01853.pdf)?

* While executing a query we might discover that the expectations we
  inferred from the statistics are inaccurate. Should we use the
  actual data retrieved by the query be used to update the statistics?
  Should a significant discrepancy trigger a refresh of the
  statistics? See also self-tuning histograms.

* How is the central coordinator selected? Should there be a different
  coordinator per-table? How is a request to analyze a table directed
  to the central coordinator?

* Does the usage of DistSQL by the coordinator imply that
  `SampleRequest` should be a member of `BatchRequest`?

* A DistSQL expert needs to help flesh out the DistSQL specifics and
  sanity check the proposed usage.

* Should stats be segregated by partition? Doing so could help if we
  expect significant partition-specific data distributions. Would
  doing so make cross-partition queries more difficult to optimize?

* Should we try to stats work for interleaved tables are tables
  connected by a foreign key? For a foreign key, the stats on one
  column can be propagated to the other. We certainly should be using
  such constraints during optimization, but the duplication of stats
  work doesn't seem worthwhile to try and optimize away.

* Work out in detail how sample sets with different sampling rates
  will be combined.
