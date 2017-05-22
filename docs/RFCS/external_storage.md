- Feature Name: External Storage for DistSQL processors
- Status: draft
- Start Date: 2017-05-11
- Authors: Arjun Narayan and Alfonso Subiotto Marques
- RFC PR: #16069
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
it stops all DistSQL processors from ever being OOM-killed. Whatever
storage format is chosen, this should be compatible with all DistSQL
processors.

# Detailed requirements
Examining all DistSQL processors, we have the following data
structures used:

| Processor name  | External storage required?  | On Disk Data structure
|-----------------|-----------------------------|----------:|
| NoopCore        | No, used for joining streams| -         |
| TableReader     | No                          | -         |
| JoinReader      | No                          | -         |
| Sorter          | Yes                         |Sorted Map |
| Aggregator      | Yes                         |    Map    |
| Distinct        | Yes                         |    Map    |
| MergeJoiner     | No                          | --        |
| HashJoiner      | Yes                         |    Map    |
| Values          | No                          | --        |
| AlgebraicSetOp  | Yes                         |    Map    |

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
or some other on-disk-capable sorter, feeding it rows.

The biggest downside is that we might have to do multiple re-sorts for
a complex query plan, since each processor's ordering requirements
must be treated as a stricter precondition than it currently is. This
is a quick-and-dirty fix.

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
   flat file.

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

## Option 3: Use RocksDB for storing sorted maps
For processors that require (or could benefit from) a sorted map,
RocksDB offers the obvious straightforward on-disk representation. A
way to elegantly augment existing processors is for them to use a new
map library, which stores a map in-memory, but flushes it to a
temporary RocksDB keyspace when it reaches some threshold. Instead of
then using a second in-memory map and having to merge streams on the
return path, the processors continue using the on-disk map, inserting
directly into RocksDB, and performing a single scan at the end.

# Proposal

We propose to use RocksDB for external storage, using a dedicated
additional RocksDB instance solely for DistSQL processors. This is the
most scalable implementation, is completely opaque to the user,
requires no special syntax at large scales, and requires lower
implementation work in modifying processors. The internal knowledge
base for tuning and working with RocksDB is very high among
CockroachDB developers, so this reduces future implementation risk as
well.

## Implementation Concerns
We want to make sure that processors clean up any data that they
use. Thus, beyond processors explicitly deleting temporary files, they
should use a clearly demarcated `tmp` namespace in the case of a
non-graceful processor exit, which can be periodically cleaned up.

### Concerns for using RocksDB
The primary concern in utilizing disk is contention with the existing
transactional RocksDB storage engine for Disk IO. We want to allow for
the use of an explicit flag to denote the temporary storage location
to be used, if the node is deployed with multiple disks.

The second concern is that the existing RocksDB instance is tuned to
be as efficient as possible for reads, and thus trades off much higher
write amplification (via aggressive compactions) in exchange for much
lower read amplification. This is not the same tradeoff that we
want.

These two concerns lead us to a clear implementation choice for the
RocksDB path: use a separate RocksDB instance explicitly for DistSQL
processors to use, which by default uses a `tmp_storage` location
under the first store's directory, overridable by a command line
flag. Processors carve out a temporary keyspace in this RocksDB
instance, and clean it up upon completion.

Computing the total amount of disk space used by a single processor is
complicated in RocksDB as it is not clear what the write amplification
is for each processor's keyspace. Whereas with serialized streams, it
would be straightforward to account for the disk space used by a
specific processor, it appears that using RocksDB would only allow us
to know the total amount of disk space used by _all_ processors in
aggregate. This might make debugging more complicated (especially in
the event of running out of disk space), although it might be solvable
with some clever accounting.

### Concerns for using serialized streams
Serialized streams have the advantage of having no write
amplification, but they have very high read amplification. Thus, they
cost relatively lower IO overhead. Unfortunately, GRACE hash joins can
become very expensive for very large datasets, where the number of
passes over the data exceeds `log(n)`. Thus, this is not the most
scalable implementation. In combination with a specialized sorter
node, which does recover the `n log(n)` complexity, however, and some
syntax for explicitly hinting when to sort data, we can recover
reasonable performance at scale.

## Potential optimizations: planning subsequent processors with additional orderings
Using RocksDB to store on-disk data will result in on-disk data being
sorted by key. Thus, for example, if a HashJoin evicts to disk, we can
recover an extra ordering that was not expected in
planning. Currently, there is no way for a DistSQL stream to indicate
that tuples are being delivered with additional orderings not
specified in the physical plan, but if we could send this information
in the producer metadata, downstream processors could potentially take
advantage of the additional sorting.
