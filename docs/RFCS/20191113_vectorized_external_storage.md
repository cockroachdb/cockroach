- Feature Name: Vectorized External Storage
- Status: draft
- Start Date: 2019-11-13
- Authors: Alfonso Subiotto Marqués and Yahor Yuzefovich
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#3703](https://github.com/cockroachdb/cockroach/issues/37303)

# Summary

Add the ability for vectorized operators to store intermediate results
to disk when these are too large to fit in main memory by adding an
on-disk queue abstraction. Without this ability, queries that use these
vectorized operators will encounter an out of memory error and not be
able to complete. Examples of these operators are sorts and joins (among
others). Adding support for vectorized external storage will allow the
full vectorized execution engine to be turned on by default, unlocking
performance gains for analytical queries that use these buffering operators.

Note that CockroachDB already supports external
storage in its row-oriented execution engine (covered
[here](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170522_external_storage.md)),
this RFC covers external storage for the vectorized execution engine.

# Motivation

The vectorized execution engine offers some
significant performance gains for analytical queries
(https://www.cockroachlabs.com/blog/how-we-built-a-vectorized-execution-engine/).
However, at the time of writing (19.2.0), only queries that use a constant
amount of memory are run through the vectorized execution engine by
default. This excludes a large family of queries that use operators such as sorts
or joins because their use could lead to an out of memory error. Adding external
storage would allow these vectorized operators to be turned on by default,
which will result in performance improvements for analytical workloads.

# Guide-level explanation

The full list of components that need external storage are:

| Name | Reason | Solution |
|---|---|---|
| Hash router | Buffering data for blocked output | On-disk queue |
| Merge joiner | Buffering equality groups | On-disk queue |
| Hash joiner | Buffering the build table | GRACE hash join |
| Hash aggregator | Too many groups to fit in memory | GRACE hash join |
| Sorter | Buffering data to sort | External merge sort |
| Unordered distinct | Seen table too big to fit in memory | GRACE hash join |
| Window functions | Buffering window function results | Mix of external merge sort and GRACE hash join |

All of these components will use one or more of the
following: On-disk queue data structure [External merge sort
algorithm](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort)
[GRACE hash join
algorithm](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join)
Similarly to the row execution engine [RFC on external
storage](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170522_external_storage.md#detailed-requirements),
the algorithms to implement are limited to an external sort and external hash join.
However, this
proposal differs from the original external storage RFC in that instead of using a
sorted KV storage engine as a map and delegating
on-disk sorting to it, the vectorized engine will use disk only to store data and
use external-storage aware operators to perform external merge
sorts and GRACE hash joins. This ends up being a lot more performant
(refer to the [Alternatives](#rationale-and-alternatives) section).

The proposal in this RFC is to introduce an on-disk queue data structure
backed by flat files where batches of columnar data are serialized
using the [Arrow IPC format](https://arrow.apache.org/docs/ipc.html).
This queue can be used directly by most operators that need to buffer
unlimited data in FIFO order. There will also be an additional abstraction
that will give the caller the option to use separate queues as distinct
partitions. This last abstraction will be used to flush and re-read
partitions in the case of a GRACE hash join or external merge sort.

The impact on the end user will be improved performance for large
analytical queries, as queries that would previously not run through
the vectorized execution engine will do so after the implementation of
external storage. There will be no other user-facing changes, the change
proposed in this RFC is purely focused on existing feature improvement.

# Reference-level explanation

Given that external merge sorts and GRACE hash joins are well
established algorithms (external sorts more so, refer to [this
issue](https://github.com/cockroachdb/cockroach/issues/24582) for
an explanation of a GRACE hash join), this RFC will focus on the
backend infrastructure that will allow these algorithms to spill
to disk by focusing on describing the design of a single queue.

## Detailed design

https://github.com/asubiotto/cockroach/tree/prototype is a
prototype of the proposed on-disk queue backed by flat files. A
Pebble-backed alternative was also considered in that prototype.
The choice of storage method is examined in more detail in the
[Rationale and Alternatives section](#rationale-and-alternatives).

The requirements for the storage method are that: It must provide encryption
if required. It must provide the capability for write throttling. Heavy
use of a temporary store should not block writes to the primary store.
Temporary files should be cleaned up on startup in case of a crash.

Using the same filesystem `vfs.FS` abstraction as Pebble does will
allow us to write an on-disk queue backed by flat files with the
first two of these requirements. The `vfs.FS` will be the same as the
temporary storage engine, so we will piggy-back off of the existing
temporary file management code. The interface is as simple as:

```
type Queue interface {
    Enqueue(coldata.Batch) error
    Dequeue() (coldata.Batch, error)
    Close() error }
```

The flat file `Queue` implementation will have knobs to configure
the in-memory buffer size to amortize write overhead, and the maximum
size of a file, after which it will roll over to a new file. Different
configurations will be benchmarked to find a good default value.

Thankfully, serialization of these `coldata.Batch`es is already
implemented in the `colserde` package using the [Arrow IPC
format](https://arrow.apache.org/docs/ipc.html). These serialized bytes will
be buffered until a flush is required, at which point they are written to a
file with an accompanying file footer. The start offset of these written bytes
as well as the number of bytes written will be stored for when the batches
should be dequeued. A current limitation is that bytes cannot be deserialized
at a finer granularity than they are written at, although we are planning
on fixing this (https://github.com/cockroachdb/cockroach/issues/42045).

The aforementioned abstraction of partitions as distinct queues can be naively
implemented by instantiating as many queues as partitions necessary. However,
this could mean many small files, as a queue must have at least one file.
For example, if an operator sets the maximum work memory of a sort to `4MB`
and attempts to sort `1GB` worth of data, this would result in 250 `4MB`
files. It’s unclear whether this would be undesirable or not, but given that
we know the size of a partition in a sort, we could also easily have several
partitions in a single, larger, file. This could also reduce the number of
open file descriptors at a single time. In the case of a GRACE hash join,
final partition sizes are unknown. However, we could set the maximum size of
a partition at the start and recursively partition if that size is exceeded.

## Rationale and Alternatives

A queue backed by Pebble was also considered as an alternative. The
configuration options for this queue are the write buffer size and
the maximum value size. Batches are enqueued up to the maximum value
size and written to a buffered writer with an ordinal as the key.

The benchmark used to test the performance of both queue implementations is
a simple write-everything then read-everything benchmark run on a personal
laptop. This simulates the general case of having a single goroutine flush data
which it then rereads and operates on. Before benchmarking each implementation
with a varying buffer size, the data size to store was fixed at `512MiB`
and each implementation was independently benchmarked to find the optimal
maximum file size in the case of flat files, and value size in the case of
pebble, while keeping the write buffer size constant at 0. The options used
for Pebble are the same ones the Pebble temporary storage instance uses (see
https://github.com/cockroachdb/cockroach/blob/09b391536f72349c3951ce6b75c5231b58933b07/pkg/storage/engine/temp_engine.go#L117).

![Find](images/vectorized_external_storage1.png?raw=true "Find")

Given these graphs, a fixed file size of `512MiB` was chosen for the flat queue
implementation and a `512KiB` maximum value size for the Pebble implementation.
Each queue was then benchmarked with a varying write buffer size:

![Compare](images/vectorized_external_storage2.png?raw=true "Compare")

The buffer size doesn’t seem to affect the Pebble benchmark. This is likely
because Pebble is not writing to the WAL and using `64MiB` memtables
which cache the writes/reads anyway. The performance instability starting
at `4MiB` is slightly unexpected, and an issue has been filed for the
spike at `32MiB` (https://github.com/cockroachdb/pebble/issues/404).

Overall, we see an 80% improvement by using flat files
instead of Pebble as an on-disk queue implementation.

Initially, the downside of choosing flat files over Pebble was that
there wasn’t a clear understanding of how the requirements outlined
in [Detailed Design](#detailed-design) would be satisfied when using
flat files. However, since the `vfs.FS` implementation that the
Pebble temporary engine uses can be reused, there is no upside to
using pebble barring perhaps simplicity in implementing partitions,
which is outweighed by the performance benefit of using flat files.


