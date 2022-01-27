- Feature Name: Incremental Statistics Collection
- Status: draft
- Start Date: 2022-01-26
- Authors: Rebecca Taft, Marcus Gartner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #64570

# Summary

This proposal suggests a method for collecting incremental statistics by taking
advantage of indexes. Instead of scanning the full table using the primary
index, we will scan a portion of the table using either a primary or secondary
index. This partial scan can then be used to update statistics on the key
columns of the index.

We are interested in supporting incremental stats collection to prevent
statistics from becoming very stale between full stats refreshes and thus
causing bad plans. This is an issue we’ve seen frequently with some of our
customers, so we would like to reduce the problems they are seeing due to stale
statistics.

Our plan is to periodically scan the maximum and minimum values in every index.
Using the stats from the last refresh as a guide, for each indexed column, we
can start from the last maximum and scan up, and start from the last minimum and
scan down. This will ensure that the extreme values are up to date.

For non-extreme values, we can take a cue from the user in order to determine
which portion of an index to scan: which part of the index are they using for
their queries? If data is indexed by timestamp and they are primarily scanning
rows with the largest timestamps, we can use the timestamp index to collect
stats for these rows.

We expect this project to significantly improve the experience for users who
currently suffer from problems with stale statistics.

# Motivation

Currently, we only support collecting table statistics on the entire table, and
statistics are only automatically refreshed when ~20% of rows have changed. This
is problematic for very large tables where only a portion of the table is
regularly updated or queried. As stats become more stale, there is greater
likelihood that the optimizer will not choose optimal query plans.

For example, if only the rows most recently inserted into a table are regularly
queried, stats on these rows will often be stale. This also gets worse over
time: As the table increases in size, the 20% trigger for automatic refreshes
will happen less and less frequently, and therefore stats on the recent rows
will become more and more stale.

# Technical design

Similar to how full statistics are collected, incremental statistics can be
collected both manually and automatically. We will describe manual collection
first, since automatic collection will use the same infrastructure. Then we will
describe how to automatically determine when to collect incremental stats and
what portion of the table to scan.

We plan to implement this feature in two phases. In the first phase, we will
only collect stats on the extreme values of all indexes. In the second phase, we
will support collecting stats on specific ranges of an index.

## Manual creation

Similar to how users can trigger collection of full statistics using
`CREATE STATISTICS` or `ANALYZE`, users should be able to trigger collection of
incremental statistics on a portion of the table they intend to query.

If stats on the full table do not yet exist, an attempt to collect incremental
stats should return an error.

### Syntax

The syntax for incremental stats collection should be as close as possible to
what is already used for full collection. Similar to full collection, users can
choose to collect stats on a specific column or set of columns, or just use the
default of collecting stats on multiple columns as determined by the database.
Unlike full collection, however, the options for incremental collection will be
restricted based on which indexes are available. In particular, if the user
specifies specific column(s), an index must exist with a prefix matching those
columns.

For example:

```sql
CREATE STATISTICS my_stat ON a FROM t WITH OPTIONS INCREMENTAL
```

This specifies that the database should use an index on a to collect statistics
on the extreme values of a. It will use the previous maximum and minimum values
of a from the last stats refresh to perform range scans of the top and bottom of
the index in order to collect stats on any new extreme values. If no index on a
exists or if stats were not previously collected on a, this statement will
return an error.

Note that for hash indexes or partitioned indexes, we may want to consider
scanning the extreme values of each hash bucket and each partition.

Users can also collect incremental stats without specifying columns. To collect
incremental stats on the extreme values of all indexes, users can run:

```sql
CREATE STATISTICS my_stat FROM t WITH OPTIONS INCREMENTAL
```

In this case, the database will automatically collect incremental stats on all
column prefixes of the index(es).

In phase 2, users will also be able to specify an explicit range (exact syntax
subject to change):

```sql
CREATE STATISTICS my_stat ON a FROM t
WITH OPTIONS INCREMENTAL GREATER THAN 1 AND LESS THAN 10
```

In order to allow arbitrary ranges, we may want to support a `CONSTRAINT` option
as well, which will take a string argument that will be parsed into a constraint
that can then be used to constrain an index scan.

If users want to specify a range without specifying columns, they must specify
an index. For example:

```sql
CREATE STATISTICS my_stat FROM t@a_b_idx
WITH OPTIONS INCREMENTAL GREATER THAN 1 AND LESS THAN 10
```

Similar to the example above without specified columns, the database will
automatically collect incremental stats on all column prefixes of the index.

### Implementation

Much of the existing infrastructure for collecting full statistics can be reused
for incremental statistics. The `sampler` and `sampleAggregator` DistSQL
processors can be reused without changes. The tablereaders that feed into these
processors will simply perform constrained index scans instead of full table
scans.

The change to the existing logic will come when writing the new statistic in the
database. Instead of directly inserting the statistic as we do for full stats,
we will update an existing row in `system.table_statistics`. To do this, we will
select the most recent stat on the given column(s) (if no such stat exists, we
will return an error). Next, we will update the statistic with the new
incremental info (details on this below), and update the corresponding row in
system.table_statistics. To indicate that this statistic includes an incremental
update, we should add a new timestamp column to `system.table_statistics` called
`updatedAt` (currently there is only one timestamp column, `createdAt`).

How exactly to update the existing stat depends on whether or not a histogram
exists. Currently, all single column stats include a histogram, while all
multi-column stats do not.

#### With Histogram

If both the existing and incremental stats include a histogram, we can update
the stat by splicing the histogram from the incremental stat into the histogram
from the existing stat, and then updating the top level row counts and distinct
counts accordingly. For example, consider the following existing and incremental
stats on column b:

Existing:

```
{
    "columns": ["b"],
    "created_at": "2018-01-01 1:00:00.00000+00:00",
    "row_count": 1000,
    "distinct_count": 120,
    "histo_col_type": "date",
    "histo_buckets": [
        {"num_eq": 0, "num_range": 0, "distinct_range": 0, "upper_bound": "2018-06-30"},
        {"num_eq": 10, "num_range": 90, "distinct_range": 29, "upper_bound": "2018-07-31"},
        {"num_eq": 20, "num_range": 180, "distinct_range": 29, "upper_bound": "2018-08-31"},
        {"num_eq": 30, "num_range": 270, "distinct_range": 29, "upper_bound": "2018-09-30"},
        {"num_eq": 40, "num_range": 360, "distinct_range": 29, "upper_bound": "2018-10-31"}
    ]
},
```

Incremental greater than `2018-08-31` and less than `2018-10-01`:
```
{
    "columns": ["b"],
    "created_at": "2018-01-02 1:00:00.00000+00:00",
    "row_count": 340,
    "distinct_count": 28,
    "histo_col_type": "date",
    "histo_buckets": [
        {"num_eq": 0, "num_range": 0, "distinct_range": 0, "upper_bound": "2018-08-31"},
        {"num_eq": 40, "num_range": 300, "distinct_range": 27, "upper_bound": "2018-09-30"}
    ]
},
```

These will be combined into a new stat:
```
{
    "columns": ["b"],
    "created_at": "2018-01-01 1:00:00.00000+00:00",
    "updated_at": "2018-01-02 1:00:00.00000+00:00",
    "row_count": 1040,
    "distinct_count": 118,
    "histo_col_type": "date",
    "histo_buckets": [
        {"num_eq": 0, "num_range": 0, "distinct_range": 0, "upper_bound": "2018-06-30"},
        {"num_eq": 10, "num_range": 90, "distinct_range": 29, "upper_bound": "2018-07-31"},
        {"num_eq": 20, "num_range": 180, "distinct_range": 29, "upper_bound": "2018-08-31"},
        {"num_eq": 40, "num_range": 300, "distinct_range": 27, "upper_bound": "2018-09-30"},
        {"num_eq": 40, "num_range": 360, "distinct_range": 29, "upper_bound": "2018-10-31"}
    ]
},
```

Notice that the bucket with upper bound `2018-09-30` from the original histogram
has been replaced with the same bucket from the new histogram. The original
bucket had 300 rows and 30 distinct values (including the upper bound), while
the new bucket has 340 rows and only 28 distinct values. Since the values
represented by this bucket are disjoint from all others in the histogram, we can
simply subtract the old counts from the totals and add the new counts. This
results in 1040 rows and 118 distinct values.

Things get a bit more complicated if the bucket boundaries don’t line up
perfectly, but we can still do reasonably well by assuming that values are
uniformly distributed across the bucket, and adjusting accordingly.
Alternatively, we require that users provide a specific value (or values) rather
than a range for incremental stats, and determine the range to scan from the
boundaries of the existing histogram bucket that the value belongs to. This
would ensure that existing and new bucket boundaries always line up.

In case a range is not specified by the user and therefore the incremental scan
starts from the previous upper and lower bounds (phase 1 of our implementation),
we can simply prepend and append new buckets to the original histogram. The new
buckets should have depths as close as possible to the depths of the original
histogram's buckets. To prevent the number of buckets from growing indefinitely,
we may want to combine new buckets with the min and max original buckets once
the number of buckets reaches some limit.

#### Without Histogram

When there is no histogram, our options for updating the stats are limited. The
best we can do in most cases is compare the previous values to the incremental
values, and if the previous values are smaller, we can replace them with the
incremental values. For example, if the previous values for the full table were
`row_count = 100` and `distinct_count = 10`, and the incremental values are
`row_count = 200` and `distinct_count = 20`, we can just update the counts to
match the incremental values. This is safe since we know that the new counts for
the full table will be at least as large as the counts from the subset of the
table scanned to build incremental stats. If the incremental counts are smaller
than the previous counts, we should leave the previous counts unchanged.

In case a range is not specified by the user and therefore the incremental scan
starts from the previous upper and lower bounds (phase 1 of our implementation),
we can do better: We can directly add the counts, since we know that we are not
scanning any values that existed before.

## Automatic creation

Automatic refreshes of incremental stats can be performed in two different ways.
The first is very similar to how we currently perform automatic refreshes of
full stats and therefore will be easier to implement, while the second is more
complex but may be more beneficial in some cases.

### Refresh based on rows modified

Just like we trigger a full refresh of a table when approximately 20% of rows
have changed, we can trigger a partial refresh when a smaller percentage of rows
have changed. This will be equivalent to running the following manual stats
request (see Syntax section above):

```sql
CREATE STATISTICS __auto__ FROM t WITH OPTIONS INCREMENTAL
```

We'll introduce new cluster settings that allow users to control the frequency
of incremental stats collection, which mimic existing settings for full table
automatic stats collection:

* `sql.stats.automatic_incremental_collection.enabled`
  * Defaults to `true`
* `sql.stats.automatic_incremental_collection.fraction_stale_rows`
  * Defaults to `0.05`
* `sql.stats.automatic_incremental_collection.min_stale_rows`
  * Defaults to `500`

### Refresh based on queries with stale stats

When users run queries, we have the ability to determine whether the stats for a
particular scan are stale. This is because each scan includes an “estimated row
count” from the optimizer, and during execution we count the actual number of
rows produced by the scan. If these two values differ by more than a certain
amount, we can conclude that the stats for the scanned range of rows are stale.
To make this determination of staleness, we’ll likely want to use the q-error
calculation, described
[here](https://github.com/cockroachdb/cockroach/blob/6ca58470efca1e72bf6cec450ec9f386ce0ffd3e/pkg/sql/opt/testutils/opttester/stats_tester.go#L315-L325).

If a query does in fact have a scan with stale stats, we should call a new
method on stats.Refresher called NotifyStaleRange. Similar to NotifyMutation,
NotifyStaleRange will add a message to a non-blocking channel so that
stats.Refresher can process the request asynchronously. The message will include
the index as well as the range of data that was scanned. This can then be used
to construct an incremental stats request including the specific range on the
specific index that was scanned. It will be equivalent to running the following
manual stats request (see Syntax section above):

```sql
CREATE STATISTICS __auto__ FROM t@idx WITH OPTIONS INCREMENTAL CONSTRAINT <idx_constraint>
```

## Drawbacks

Possible drawbacks include:
  - Added complexity to the automatic statistics refreshing code.
  - Additional index scans required for incremental stats can increase contention.

# Alternatives

## Extrapolating min and max buckets

As a partial solution to the problem of stale stats, we could take advantage of
the fact that we store 4-5 historical stats for every column. We could use these
historical stats to build a simple regression model (or a more complex model) to
predict how the stats have changed since they were last collected. Predictions
are only be possible for column types where a rate of change can be determined
between two values, such as `DATE`, `TIME[TZ]`, `TIMESTAMP[TZ]`, `INT[2|4|8]`
, `FLOAT[4|8]`, and `DECIMAL`. This prediction would not be stored on disk, but
instead would be calculated on the fly, either inside the stats cache or the
statisticsBuilder. Because this solution is complementary to the incremental
collection proposed in this RFC and it's relatively simpler, we're already
planning to implement it.

## Mutation Sampling

Another alternative is sampling a small percentage of INSERTs, UPDATEs, and
DELETEs and updating statistics based on the values changed. As a simple
example, we could sample 1% of INSERTs and collect newly inserted values.
Periodically the row count and histogram statistics in the stats table could be
updated from the values collected.

It's possible that this solution would produce less stale stats than incremental
stats collection, depending on how often the collected stats are flushed to the
stats table. Performance of mutations that are chosen to be sampled would likely
suffer from some additional overhead to collect new values.

Another possible problem with this approach is that it would not allow us to
keep accurate distinct counts. However, we can probably assume that the
percentage of distinct values doesn’t change much between full refreshes,  so as
long as we adjust the distinct counts to maintain the same percentage of
distinct values during mutation sampling (or just maintain a %, rather than
counts), then this shouldn’t be too much of an issue.

Oracle has a flavor of mutation sampling called [Real-Time
Statistics](https://oracle-base.com/articles/19c/real-time-statistics-19c).
They've implemented this by inserting a stats collection operator, `OPTIMIZER
STATISTICS GATHERING`, into the query plan of mutation statements:

```
--------------------------------------------------------------------------------
| Id | Operation                        | Name | Rows  | Cost (%CPU)| Time     |
--------------------------------------------------------------------------------
|  0 | INSERT STATEMENT                 |      |       |     2 (100)|          |
|  1 |  LOAD TABLE CONVENTIONAL         | TAB1 |       |            |          |
|  2 |   OPTIMIZER STATISTICS GATHERING |      |     1 |     2   (0)| 00:00:01 |
|  3 |    CONNECT BY WITHOUT FILTERING  |      |       |            |          |
|  4 |     FAST DUAL                    |      |     1 |     2   (0)| 00:00:01 |
--------------------------------------------------------------------------------
```

## Alternatives from the literature

There is a large body of literature on maintaining accurate statistics. A few of
the most relevant are:

1. Gibbons, Phillip B., Yossi Matias, and Viswanath Poosala. "Fast incremental
   maintenance of approximate histograms." In _VLDB_, vol. 97, pp. 466-475. 1997.

The solution described in this paper enables keeping histograms quite fresh and
up-to-date, but it requires maintaining a backing sample (similar to a partial
index but with a random sample of the table maintained with reservoir sampling).
We don’t support maintenance of such backing samples today, so this solution
would require a lot more effort to implement than the solutions proposed in this
RFC.

2. Gibbons, Phillip B., and Yossi Matias. "New sampling-based summary statistics
   for improving approximate query answers." In _Proceedings of the 1998 ACM
   SIGMOD international conference on Management of data_, pp. 331-342. 1998.

This paper is by the same authors as above, and builds on the prior work that
uses a backing sample. This paper adds two additional types of summary
statistics that are kept fresh and up to date: concise samples and counting
samples. We do not currently maintain either type of statistic, so this paper is
less relevant, although may be worth considering in the future.

3. Cormode, Graham, and Shan Muthukrishnan. "What's hot and what's not: tracking
   most frequent items dynamically." _ACM Transactions on Database Systems
   (TODS)_ 30, no. 1 (2005): 249-278.

This is a solution for keeping track of “hot” values that appear many times in a
table, and keeping the list of hot items up to date. Tracking hot items is not
something that we do today, although we’d like to do it in the future. This
paper may be useful at that point.

4. Aboulnaga, Ashraf, and Surajit Chaudhuri. "Self-tuning histograms: Building
   histograms without looking at data." _ACM SIGMOD Record_ 28, no. 2 (1999):
   181-192.

This approach is somewhat similar to our idea of triggering automatic stats
collection based on queries that have scans with stale stats. But instead of
triggering another scan (as we propose), it uses the actual row count from the
query execution to update the histogram directly. This is something we might
also consider during our phase 2 implementation, although it would be less
accurate than the proposed approach of triggering a separate scan for
incremental stats collection. However, it does have the benefit of requiring no
additional scan overhead for incremental stats collection.

5. Gilbert, Anna C., Sudipto Guha, Piotr Indyk, Yannis Kotidis, Sivaramakrishnan
   Muthukrishnan, and Martin J. Strauss. "Fast, small-space algorithms for
   approximate histogram maintenance." In _Proceedings of the thirty-fourth
   annual ACM symposium on Theory of computing_, pp. 389-398. 2002.

This paper proposes using a sketch to maintain a histogram based on a stream of
values. It requires inserting each new value into the sketch, which would likely
be prohibitive since it would require reconciling updates from distributed
nodes, and would add overhead to each mutation query. It’s possible we could use
this with random sampling, similar to the mutation sampling approach described
above.

6. Thaper, Nitin, Sudipto Guha, Piotr Indyk, and Nick Koudas. "Dynamic
   multidimensional histograms." In _Proceedings of the 2002 ACM SIGMOD
   international conference on Management of data_, pp. 428-439. 2002.

This paper has many of the same authors as the previous, and also proposes using
a sketch to maintain histograms over data streams. The difference here is that
they are maintaining multi-dimensional histograms rather than single-dimensional
histograms. This is probably more complexity than we need right now as we only
maintain single-dimensional histograms.

7. Donjerkovic, Donko, Yannis Ioannidis, and Raghu Ramakrishnan. Dynamic
   histograms: Capturing evolving data sets. University of Wisconsin-Madison
   Department of Computer Sciences, 1999.

This approach also requires sampling of mutation queries to keep histograms
up-to-date, but it claims to work in a shared-nothing environment. It maintains
these dynamic histograms in memory. It may be worth investigating more if we
decide to go the route of mutation sampling.

# Unresolved questions

When implementing automatic incremental stats, we’ll need to decide whether to
allow incremental refreshes to run concurrently with each other as well as with
full refreshes. Currently, we only allow one full automatic stats refresh to run
at a time across the entire cluster, since each full refresh can be quite
resource intensive. Incremental refreshes will likely be much less resource
intensive, and therefore running multiple refreshes at a time may not cause
issues. We don’t want to waste work, however, so if two different nodes
simultaneously request an incremental refresh on the same range of the same
table, we should probably cancel one of the requests.

In order to distinguish full automatic stats jobs from partial automatic stats
jobs, we may want to consider creating either a new job type or naming the stats
something other than `__auto__`.

If refreshing based on stale stats, we may have many similar requests for
overlapping ranges. Should we try to coalesce all these ranges into one? If we
implement both types of automatic refreshes, should one type of refresh be
prioritized over another?
