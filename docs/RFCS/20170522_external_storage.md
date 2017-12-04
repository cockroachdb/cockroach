- Feature Name: External Storage for DistSQL processors
- Status: completed
- Start Date: 2017-05-11
- Authors: Arjun Narayan and Alfonso Subiotto Marques
- RFC PR: [#16069](https://github.com/cockroachdb/cockroach/pull/16069)
- Cockroach Issue: [#15206](https://github.com/cockroachdb/cockroach/issues/15206)

# Summary
Add support for DistSQL processors to use external storage in addition
to memory to store intermediate data when processing large DistSQL
queries.

# Motivation and Background
Currently, DistSQL processors have a hard limit on the amount of
memory they can use before they get OOM-killed. This limits the size
of certain queries, particularly when efficient secondary indices do
not exist, so some amount of computation must be done holding a large
amount of intermediate state at a processor. For instance, a JOIN on
two very large tables, where there is no ordering on either table's
JOIN predicate columns, must either

* Use a HashJoiner, which stores one table in a single large
  hashtable, and stream the other table, looking up for matches on the
  hashtable, emitting rows when appropriate.

* Sort both tables by the JOIN predicate columns, and then stream them
  to a MergeJoiner.

Either of these two solutions runs out of memory at some query size,
and requires external storage to process larger queries.

# Scope
This problem was first encountered in running TPC-H queries on
moderately sized scale factors (5 and above). It was exacerbated by
the fact that DistSQL currently does not plan MergeJoins and resorts
to HashJoins in all cases. TPC-H itself should not be bottlenecked on
the lack of external storage, as relevant secondary indices exist for
all queries that we are aware of, such that optimal planning keeps
tuples flowing smoothly through the dataflow graph without requiring
any processor to accumulate a disproportional number of
tuples. However, users might still want to run the occasional
analytics query without indexes, and we should support that use case.

At a minimum, external storage should provide enough swap space that
it stops all DistSQL processors from being OOM-killed on queries on
the TPC-H scalefactor 300 dataset. Whatever storage format is chosen,
this should be compatible with all DistSQL processors.

# Related Work

None of this is new. Existing databases use both paths for query
execution, and typically use a cost-based query planner to decide
_which_ of the two paths to use. So for us, it's more a case of
prioritization and beginning with an implementation that is
extensible. External sorting is typically the first implemented in
OLTP databases, since it comes in handy for building new indices, but
we have a different pathway for executing those. On-disk storage for
query execution only comes into play for OLAP-style queries,

For example, in the case of running a JOIN:

1. They have implementations for the GRACE hash join, as well as the
   external sorter + merge join.

2. They have extensive microbenchmarks for the underlying costs of
   each operation (e.g. total time cost of writing a tuple into a
   serialized stream, or the expected time cost of doing a random
   insert into a sorted on-disk BTree). They have extensive table
   statistics.

3. At query planning time, with the table statistics, the database can
   compute the cost of using either plan (i.e. estimate the number of
   passes the GRACE hash join would take and multiply it out with the
   average cost of writing each tuple, etc.) and choose the better
   one.

4. We don't have (2), and thus we don't have a cost-based query
   planner capable of doing (3). Right now, our implementations only
   do purely logical query planning (i.e. do only those wins that are
   asymptotic wins), so the RocksDB path is along those lines as well.

# Detailed requirements
Examining all DistSQL processors, we have the following data
structures used:

| Processor name  | On Disk Data structure|
|-----------------|----------:|
| NoopCore        | n/a       |
| TableReader     | n/a       |
| JoinReader      | n/a       |
| Sorter          | Sorted Map|
| Aggregator      | Map       |
| Distinct        | Map       |
| MergeJoiner     | n/a       |
| HashJoiner      | Map       |
| Values          | n/a       |
| AlgebraicSetOp  | Map       |

Essentially, we have two data types to store: a sorted map and a hash
map. The processors that use a hash map (distinct, hashjoin, and
algebraic set op) do internally benefit from keys being sorted, but
when they are sorted, the emit rows eagerly, and don't need to build
expensive in-memory state! They thus only accumulate state when keys
are *not* sorted. Thus, sorting is in direct opposition to
accumulating state large enough for external storage.

# Potential solutions

## Option 1: On-disk sort-only
The first and simplest solution, is to create an on-disk method
exclusively for the Sorter processor. All other processors use less
memory when the input data is appropriately sorted, so the planner
would leverage the on-disk capable Sorter to sort data upstream.

The simplest on-disk sorter is to use an external Unix `sort` process,
or some other on-disk-capable sorter, feeding it rows, and reading
back the sorted stream of tuples. More seriously, we could use an
external sort implementation that's more battle tested, or even roll
our own native Go implementation (e.g. see
Postgresql's
[external sort](https://github.com/postgres/postgres/blob/master/src/backend/utils/sort/tuplesort.c)).

The biggest downside of only supporting external sorting is that we
might have to do multiple re-sorts for a complex query plan (for
instance, if a query plan has multiple sequential joins on multiple
different join conditions, that would require a re-join after the
first join and before the second), since each processor's ordering
requirements must be treated as a stricter precondition than it
currently is (at least until we have sophisticated table statistics
and join planning that can take that into account). This is a
quick-and-dirty fix.

## Option 2: Use temporary flat files to page out partial streams.
For processors that require an unsorted map, we could use temporary
files to page out partial output streams, a la GRACE hash joins,
merging them back later. Walking through two examples for how flat
files would be used by processors:

### Example 1: HashJoin with on-disk streams
Consider the following algorithm for
processing a HashJoin:

1. Read n rows from the right input stream, where n is the upper limit
   on the number of rows we can hold in-memory.

2. Read all rows from the left input stream, and where we see a match,
   serialize the row and emit it to a flat file.

3. Read the next n rows from the right input stream, and read all rows
   from the start from the left input stream, and emit it to a second
   flat file (note that DistSQL does not currently allow for rereading
   streams, so that capability would have to be added as well to make
   this work).

4. Repeat until we exhaust the right input stream.

5. Read all the flat files back, merging them in (if we need to
   preserve some ordering), and emitting tuples.

### Example 2: MergeSort with on-disk streams
1. Read n rows from the input stream, inserting it into an in-memory
   heap. Flush the heap to disk.

2. Repeat until the input stream is exhausted.

3. Merge all the files.

### Downsides:
The first downside is in the runtime: GRACE hash joins are not
particularly pretty in worst case algorithmic analysis, and this is
evident in that the hash join reads complete streams multiple times.

The second downside that we need to build a new processor that can
provide this buffering service. However, this is mitigated by the fact
that a BufferedStream processor would reuse the same code, since all
it does it write a stream to disk, and reread it as many times as
necessary.

Finally, this requires a bunch of processor rewriting work: each
processor would have to be rewritten to deal with multiple passes, and
this also makes the physical planner's job more critical: runtimes can
get pathological if we have to resort to paging out streams, so
getting ordering requirements right becomes even more important.

While writing serialized streams to disk has the advantage of having
no write amplification, it has very high read amplification since
streams reread multiple times. Unfortunately, GRACE hash joins can
become very expensive for very large datasets, where the number of
passes over the data exceeds `log(n)`. Thus, this is not the most
scalable implementation. In combination with a specialized sorter
node, which does recover the `n log(n)` complexity, however, and some
syntax for explicitly hinting when to sort data, we can recover
reasonable performance at scale.


## Option 3: Use RocksDB for storing sorted maps
For processors that require (or could benefit from) a sorted map,
RocksDB offers the obvious straightforward on-disk representation. A
way to elegantly augment existing processors is for them to use a new
map library, which stores a map in-memory, but flushes it to a
temporary RocksDB keyspace when it reaches some threshold. Instead of
then using a second in-memory map and having to merge streams on the
return path, the processors continue using the on-disk map, inserting
directly into RocksDB, and performing a single scan at the end.

This version also gives us random access reads and writes, which makes
using external storage for aggregations where there are a large number
of buckets simple: we simply use the KV interface to store
aggregations by bucket. As a bonus, when we finally read off the
aggregations, they will be in sorted order.

Do note that if configured correctly, RocksDB recovers the algorithm
in Option 2 Example 2: if we insert sequential sorted `WriteBatches`,
and turn compaction off. This leaves `cgo` overhead as the only
remaining downside of this path.

# Proposal

We propose to use RocksDB for external storage, using a dedicated
additional RocksDB instance solely for DistSQL processors. This is the
most general implementation (in terms of scaling to very complex
queries), is completely opaque to the user, requires no special syntax
at large scales, and requires lower implementation work in modifying
processors one-by-one (their internal maps would be changed to use an
opaque KV library that switches from in-memory maps to RocksDB at a
configurable threshold). The internal knowledge base for tuning and
working with RocksDB is available at CockroachDB, so this reduces
future implementation risk as well.

## Benchmarking overhead

As a sanity check to ensure that this isn't wildly off-base, we will
first benchmark using RocksDB with minimal compaction versus a
standard external sort library, and make sure it isn't egregious.

We need to isolate the following costs:

1. External sort vs RocksDB: We will sort a billion rows using a
   standard external sort, and insert (in random order) and scan it
   from RocksDB.
2. RocksDB vs Pure-go: We will also try, as a stretch goal, to measure
   the RocksDB overhead against a pure-Go implementation, such
   as [Badger](https://github.com/dgraph-io/badger).

## RocksDB Implementation Concerns
The primary concern in utilizing disk is contention with the existing
transactional RocksDB storage engine for Disk IO. We want to allow for
the use of an explicit flag to denote the temporary storage location
to be used, if the node is deployed with multiple disks.

The second concern is that the existing RocksDB instance is tuned to
be as efficient as possible for reads, and thus trades off much higher
write amplification (via aggressive compactions) in exchange for much
lower read amplification. This is not the same tradeoff that we
want.

Finally, RocksDB is very complex, and for this use case we don't need
the majority of its database features, so it's potentially
overkill. We have to be careful that the overhead is not too high.

These concerns lead us to a few rules for implementing the RocksDB
options:

* Use a separate RocksDB instance explicitly for DistSQL processors.

* By default this instance uses a `tmp_storage` location under the
  first store's directory, overridable by a command line flag. It is
  deleted and recreated from scratch whenever the node starts.

* Disable write-ahead logging (`write_option.disableWAL`) to lower
  write amplification.

* Processors carve out temporary keyspaces in this RocksDB instance.

* Use range deletion tombstones for rapidly deleting a processor's
  entire keyspace when it is done.

* Experiment with compaction settings (including the extreme of
  turning it off completely), since, fundamentally, we need to write
  data once and read it back once.

* This is an ephemeral RocksDB instance, and some cloud providers
  provide non-persistent disks. We want to eventually support
  configuring nodes to use non-persistent disks for this use
  case.

* `storage/engine` currently is tailored almost exclusively for use by
  the `storage` package. This implementation should reuse as much of
  the existing `engine` code as possible, modifying (and generalizing
  the package) where appropriate. Eventually, other users who also
  need a non-persistent node-local key-value store can use it as well.

## Potential optimizations: planning subsequent processors with additional orderings

Using RocksDB to store on-disk data will result in on-disk data being
sorted by key. Thus, for example, if a HashJoin evicts to disk, we can
recover an extra ordering that was not expected in
planning. Currently, there is no way for a DistSQL stream to indicate
that tuples are being delivered with additional orderings not
specified in the physical plan, but if we could send this information
in the producer metadata, downstream processors could potentially take
advantage of the additional sorting.
