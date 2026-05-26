- Feature Name: Index-Based Partial Statistics Collection
- Status: in-progress
- Start Date: 2022-01-26
- Authors: Rebecca Taft, Marcus Gartner
- RFC PR: https://github.com/cockroachdb/cockroach/pull/75625
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/64570

# Summary

This proposal suggests a method for collecting partial statistics by taking
advantage of indexes. Instead of scanning the full table using the primary
index, we will scan a portion of the table using either a primary or secondary
index. This partial scan can then be used to update statistics on the key
columns of the index.

We are interested in supporting partial stats collection to prevent
statistics from becoming very stale between full stats refreshes and thus
causing bad plans. This is an issue we’ve seen frequently with some of our
customers, so we would like to reduce the problems they are seeing due to stale
statistics.

Our plan is to periodically scan the maximum and minimum values in every index.
Using the stats from the last refresh as a guide, for each indexed column, we
can start from the last maximum and scan up, and start from the last minimum and
scan down. This will ensure that the extreme values are up to date. This is
especially important for timestamp indexes, since many workloads update and scan
rows with the most recent timestamps, and we want to make sure the stats for these
rows are fresh.

For non-extreme values, we can take a cue from the user in order to determine
which portion of an index to scan: which part of the index are they using for
their queries? We should keep stats fresh for that part of the index.

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

Similar to how full statistics are collected, partial statistics can be
collected both manually and automatically. We will describe manual collection
first, since automatic collection will use the same infrastructure. Then we will
describe how to automatically determine when to collect partial stats and
what portion of the table to scan.

We plan to implement this feature in two phases. In the first phase, we will
only collect stats on the extreme values of all indexes. In the second phase, we
will support collecting stats on specific ranges of an index.

## Manual creation

Similar to how users can trigger collection of full statistics using
`CREATE STATISTICS` or `ANALYZE`, users should be able to trigger collection of
partial statistics on a portion of the table they intend to query.

If stats on the full table do not yet exist, an attempt to collect partial
stats should return an error.

### Syntax

The syntax for partial stats collection should be as close as possible to
what is already used for full collection. Similar to full collection, users can
choose to collect stats on a specific column or set of columns, or just use the
default of collecting stats on multiple columns as determined by the database.
Unlike full collection, however, the options for partial collection will be
restricted based on which indexes are available. In particular, if the user
specifies specific column(s), an index must exist with a prefix matching those
columns.

For example (exact syntax subject to change):

```sql
CREATE STATISTICS my_stat ON a FROM t USING EXTREMES
```

This specifies that the database should use an index on `a` to collect statistics
on the extreme values of `a`. It will use the previous maximum and minimum values
of `a` from the last stats refresh to perform range scans of the top and bottom of
the index in order to collect stats on any new extreme values. If no index on `a`
exists or if stats were not previously collected on `a`, this statement will
return an error.

Note that for hash indexes or partitioned indexes, we may want to consider
scanning the extreme values of each hash bucket and each partition.

Users can also collect partial stats without specifying columns. To collect
partial stats on the extreme values of all indexes, users can run:

```sql
CREATE STATISTICS my_stat FROM t USING EXTREMES
```

In this case, the database will automatically collect partial stats on all
column prefixes of the index(es).

In phase 2, users will also be able to specify an explicit range:

```sql
CREATE STATISTICS my_stat ON a FROM t
WHERE a > 1 AND a < 10
```

If users want to specify a range without specifying columns, they must specify
an index. For example:

```sql
CREATE STATISTICS my_stat FROM t@a_b_idx
WHERE a > 1 AND a < 10
```

Partial statistics options should still work with current create statistics options.
For example, with the `THROTTLING` option:
```sql
CREATE STATISTICS my_stat FROM t WITH OPTIONS THROTTLING 0.3 USING EXTREMES
```

Or with the `AS OF SYSTEM TIME` option:
```sql
CREATE STATISTICS my_stat FROM t AS OF SYSTEM TIME '2015-02-01'
WHERE a > 1 AND a < 10
```

The partial statistics options can be included as the part of the production rule
that contains all the create statistics options and hence the list of options
can be specified in any order, with `WITH OPTIONS` optionally preceding the
list.

Similar to the example above without specified columns, the database will
automatically collect partial stats on all column prefixes of the index.

To start, we will only allow predicates that constrain either the first index
column or the first column after any hash or partition columns. This restriction
is needed to ensure that we can accurately update the histogram for the constrained
column.

### Implementation

Much of the existing infrastructure for collecting full statistics can be reused
for partial statistics. The `sampler` and `sampleAggregator` DistSQL
processors can be reused without changes. The `tableReader`s that feed into these
processors will simply perform constrained index scans instead of full table
scans.

The change to the existing logic will come when writing the new statistic in the
database and updating the stats cache. When inserting the new statistic into
`system.table_statistics`, we will need to explicitly mark it as partial and 
indicate its domain. This will require adding a new column to `system.table_statistics` 
called `partialPredicate` which will contain a predicate for a partial statistic, and 
`NULL` for a full table statistic. When updating the stats cache, we will need to 
combine the most recent full statistic for the given table with all subsequent partial 
statistics (details on this below).

How exactly to combine the full and partial stats depends on whether or not a
histogram exists. Currently, all single column stats include a histogram, while
all multi-column stats do not.

#### With Histogram

If both the existing and partial stats include a histogram, we can update
the stat by splicing the histogram from the partial stat into the histogram
from the existing stat, and then updating the top level row counts and distinct
counts accordingly. For example, consider the following existing and partial
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

Partial greater than `2018-08-31` and less than `2018-10-01`:
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
uniformly distributed across the bucket, and adjust accordingly.
Alternatively, we could adjust the requested range for the scan by extending it
in both directions to existing bucket boundaries. This would ensure that
existing and new bucket boundaries always line up.

In the process of updating the histogram, if any buckets become too large, we
may want to split them to match the size of the original histogram's buckets.
To prevent the number of buckets from growing indefinitely, we would need to stop
splitting if the total number of buckets exceeds some limit.

In case a range is not specified by the user and therefore the partial scan
starts from the previous upper and lower bounds (phase 1 of our implementation),
we can simply prepend and append new buckets to the original histogram. The new
buckets should have depths as close as possible to the depths of the original
histogram's buckets. If the number of buckets reaches the aforementioned limit,
we may want to combine new buckets with the min and max original buckets.

For phase 1, we could also consider going a step further than just starting the
scan from the last minimum/maximum seen: we should also find the current minimum/
maximum, and if it's greater/less than the previous, we should delete the old
buckets accordingly.

#### Without Histogram

When there is no histogram, our options for updating the stats are limited. The
best we can do in most cases is compare the previous values to the partial
values, and if the previous values are smaller, we can replace them with the
partial values. For example, if the previous values for the full table were
`row_count = 100` and `distinct_count = 10`, and the partial values are
`row_count = 200` and `distinct_count = 20`, we can just update the counts to
match the partial values. This is safe since we know that the new counts for
the full table will be at least as large as the counts from the subset of the
table scanned to build partial stats. If the partial counts are smaller
than the previous counts, we should leave the previous counts unchanged.

In case a range is not specified by the user and therefore the partial scan
starts from the previous upper and lower bounds (phase 1 of our implementation),
we can do better: We can directly add the counts, since we know that we are not
scanning any values that existed before.

In either case, we can take advantage of the fact that the partial stats are
limited to a specific range by creating a two-bucket histogram covering the
range with the partial values (even though the original statistic had no
histogram). If a query only accesses the range covered by the partial stats, we
can directly use the partial values from the histogram.

## Automatic creation

Automatic refreshes of partial stats can be performed in two different ways.
The first is very similar to how we currently perform automatic refreshes of
full stats and therefore will be easier to implement, while the second is more
complex but may be more beneficial in some cases.

### Refresh based on rows modified

Just like we trigger a full refresh of a table when approximately 20% of rows
have changed, we can trigger a partial refresh when a smaller percentage of rows
have changed. This will be equivalent to running the following manual stats
request (see Syntax section above):

```sql
CREATE STATISTICS __auto__ FROM t AT EXTREMES
```

Similar to automatic full refreshes, we will also want to use options `AS OF
SYSTEM TIME` and `THROTTLING` to ensure minimal interference with foreground
traffic.

We'll introduce new cluster settings that allow users to control the frequency
of partial stats collection, which mimic existing settings for full table
automatic stats collection:

* `sql.stats.automatic_partial_collection.enabled`
  * Defaults to `true`
* `sql.stats.automatic_partial_collection.fraction_stale_rows`
  * Defaults to `0.05`
* `sql.stats.automatic_partial_collection.min_stale_rows`
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
to construct a partial stats request including the specific range on the
specific index that was scanned. It will be equivalent to running the following
manual stats request (see Syntax section above):

```sql
CREATE STATISTICS __auto__ FROM t@idx WHERE <predicate>
```

## Drawbacks

Possible drawbacks include:
  - Added complexity to the automatic statistics refreshing code.
  - Additional index scans required for partial stats can increase contention.

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
statisticsBuilder. Because this solution is complementary to the partial
collection proposed in this RFC and it's relatively simpler, we're already
planning to implement it.

## Mutation Sampling

Another alternative is sampling a small percentage of INSERTs, UPDATEs, and
DELETEs and updating statistics based on the values changed. As a simple
example, we could sample 1% of INSERTs and collect newly inserted values.
Periodically the row count and histogram statistics in the stats table could be
updated from the values collected.

It's possible that this solution would produce less stale stats than partial
stats collection, depending on how often the collected stats are flushed to the
stats table. Performance of mutations that are chosen to be sampled would likely
suffer from some additional overhead to collect new values.

Another possible problem with this approach is that it would not allow us to
keep accurate distinct counts. However, we can probably assume that the
percentage of distinct values doesn’t change much between full refreshes, so as
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

## MVCC Time-Bound Iteration

Instead of directly sampling mutations, we can periodically scan the newest values
in a table as follows:

1. Open a time-bound iterator between `(last_stats_time, now-30s]` on the primary
   index.
2. Iterate while throttling.
3. Sample values.
4. For each deleted or updated value sampled by the iterator, retrieve the previous
   value of that key using a separate non-time-bound iterator (to know which value
   to store as a negative sample).
5. Merge back in with the rest of the stats somehow (possibly by using an approach
   from one of the papers below for maintaining up-to-date stats over streaming data).

By only collecting a slice of stats for a particular window of MVCC time (i.e.
the new versions), we can use a time-bound MVCC iterator. This allows us to
avoid scanning most files in each range's LSM. As a first-order approximation
for short enough time windows, we've seen that this can reduce the cost of
iteration by around two orders of magnitude, making it sufficiently cheap to run
in an incremental manner.

Note that step 3 above is similar to the approach used in #69191. It does reduce
the performance benefit of the time-bound iterator, but in the vast majority of
cases, this effect should be marginal. Even with the second iterator, this
approach still performs `O(num_new_versions)` work, as opposed to the
`O(num_all_versions)` work that a full stats computation performs.

Both this approach and the Mutation Sampling approach above are promising
because they have the potential to keep stats much more up-to-date than the
partial stats approach proposed in this RFC. However, they are also more complex
to implement and represent a significant departure from our current approach to
stats collection. Additionally, live stats can lead to query plans changing
frequently without warning, which can be difficult to debug and help customers
understand. Still, we will likely implement one of these two approaches in the
future, perhaps as an enterprise or cloud-only feature.

## CDC

Similar to the time-bound MVCC iterator, we could use CDC to maintain up-to-date
incremental statistics outside of the database, and periodically inject the new
stats back into the database. This approach has many of the same benefits and
challenges of the previous two approaches.

## Alternatives from the literature

There is a large body of literature on maintaining accurate statistics. A few of
the most relevant are:

1. [Gibbons, Phillip B., Yossi Matias, and Viswanath Poosala. "Fast incremental
   maintenance of approximate histograms." In _VLDB_, vol. 97, pp. 466-475. 1997.](
   http://www.mathcs.emory.edu/~cheung/Courses/584/Syllabus/papers/Histogram/Gibbons-fast-incr-histogram.pdf)

The solution described in this paper enables keeping histograms quite fresh and
up-to-date, but it requires maintaining a backing sample (similar to a partial
index but with a random sample of the table maintained with reservoir sampling).
We don’t support maintenance of such backing samples today, so this solution
would require a lot more effort to implement than the solutions proposed in this
RFC.

2. [Gibbons, Phillip B., and Yossi Matias. "New sampling-based summary statistics
   for improving approximate query answers." In _Proceedings of the 1998 ACM
   SIGMOD international conference on Management of data_, pp. 331-342. 1998.](
   https://www.researchgate.net/profile/Yossi-Matias/publication/2634326_New_Sampling-Based_Summary_Statistics_for_Improving_Approximate_Query_Answers/links/0fcfd50baafa968854000000/New-Sampling-Based-Summary-Statistics-for-Improving-Approximate-Query-Answers.pdf)

This paper is by the same authors as above, and builds on the prior work that
uses a backing sample. This paper adds two additional types of summary
statistics that are kept fresh and up to date: concise samples and counting
samples. We do not currently maintain either type of statistic, so this paper is
less relevant, although may be worth considering in the future.

3. [Cormode, Graham, and Shan Muthukrishnan. "What's hot and what's not: tracking
   most frequent items dynamically." _ACM Transactions on Database Systems
   (TODS)_ 30, no. 1 (2005): 249-278.](
   http://www.mathcs.emory.edu/~cheung/Courses/584/Syllabus/papers/Histogram/2005-Cormode-Histogram.pdf)

This is a solution for keeping track of “hot” values that appear many times in a
table, and keeping the list of hot items up to date. Tracking hot items is not
something that we do today, although we’d like to do it in the future. This
paper may be useful at that point.

4. [Aboulnaga, Ashraf, and Surajit Chaudhuri. "Self-tuning histograms: Building
   histograms without looking at data." _ACM SIGMOD Record_ 28, no. 2 (1999):
   181-192.](
   https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.39.4864&rep=rep1&type=pdf)

This approach is somewhat similar to our idea of triggering automatic stats
collection based on queries that have scans with stale stats. But instead of
triggering another scan (as we propose), it uses the actual row count from the
query execution to update the histogram directly. This is something we might
also consider during our phase 2 implementation, although it would be less
accurate than the proposed approach of triggering a separate scan for
partial stats collection. However, it does have the benefit of requiring no
additional scan overhead for incremental stats collection.

5. [Gilbert, Anna C., Sudipto Guha, Piotr Indyk, Yannis Kotidis, Sivaramakrishnan
   Muthukrishnan, and Martin J. Strauss. "Fast, small-space algorithms for
   approximate histogram maintenance." In _Proceedings of the thirty-fourth
   annual ACM symposium on Theory of computing_, pp. 389-398. 2002.](
   http://perso.ens-lyon.fr/pierre.borgnat/MASTER2/gilbert_ggikms_stoc2002.pdf)

This paper proposes using a sketch to maintain a histogram based on a stream of
values. It requires inserting each new value into the sketch, which would likely
be prohibitive since it would require reconciling updates from distributed
nodes, and would add overhead to each mutation query. It’s possible we could use
this with random sampling, similar to the mutation sampling approach described
above.

6. [Thaper, Nitin, Sudipto Guha, Piotr Indyk, and Nick Koudas. "Dynamic
   multidimensional histograms." In _Proceedings of the 2002 ACM SIGMOD
   international conference on Management of data_, pp. 428-439. 2002.](
   https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.132.2078&rep=rep1&type=pdf)

This paper has many of the same authors as the previous, and also proposes using
a sketch to maintain histograms over data streams. The difference here is that
they are maintaining multi-dimensional histograms rather than single-dimensional
histograms. This is probably more complexity than we need right now as we only
maintain single-dimensional histograms.

7. [Donjerkovic, Donko, Yannis Ioannidis, and Raghu Ramakrishnan. Dynamic
   histograms: Capturing evolving data sets. University of Wisconsin-Madison
   Department of Computer Sciences, 1999.](
   https://minds.wisconsin.edu/bitstream/handle/1793/60206/TR1396.pdf?sequence=1)

This approach also requires sampling of mutation queries to keep histograms
up-to-date, but it claims to work in a shared-nothing environment. It maintains
these dynamic histograms in memory. It may be worth investigating more if we
decide to go the route of mutation sampling.

# Unresolved questions

When implementing automatic partial stats, we’ll need to decide whether to
allow partial refreshes to run concurrently with each other as well as with
full refreshes. Currently, we only allow one full automatic stats refresh to run
at a time across the entire cluster, since each full refresh can be quite
resource intensive. Partial refreshes will likely be much less resource
intensive, and therefore running multiple refreshes at a time may not cause
issues. We don’t want to waste work, however, so if two different nodes
simultaneously request a partial refresh on the same range of the same
table, we should probably cancel one of the requests. In any case, we need to be
careful to avoid significantly increasing traffic to the jobs table.

In order to distinguish full automatic stats jobs from partial automatic stats
jobs, we may want to consider creating either a new job type or naming the stats
something other than `__auto__`.

If refreshing based on stale stats, we may have many similar requests for
overlapping ranges. Should we try to coalesce all these ranges into one? If we
implement both types of automatic refreshes, should one type of refresh be
prioritized over another?
