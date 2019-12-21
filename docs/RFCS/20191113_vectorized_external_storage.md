- Feature Name: Vectorized External Storage
- Status: draft
- Start Date: 2019-11-13
- Authors: Alfonso Subiotto Marqués and Yahor Yuzefovich
- RFC PR: [#42546](https://github.com/cockroachdb/cockroach/pull/42546)
- Cockroach Issue: [#37303](https://github.com/cockroachdb/cockroach/issues/37303)

# Summary

Add the ability for vectorized operators to store intermediate results
to disk when these are too large to fit in main memory by adding an
on-disk queue abstraction. Without this ability, queries that use these
vectorized operators will encounter an out of memory error and will not be
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
following:
- On-disk queue data structure
- [External merge sort
algorithm](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort)
- [GRACE hash join
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

The requirements for the storage method are that:
- It must provide encryption
if required.
- It must provide the capability for write throttling. Heavy use
of a temporary store should not block writes to the primary store.
- Temporary files should be cleaned up on startup in case of a crash.

Using the same filesystem `vfs.FS` abstraction as Pebble does will
allow us to write an on-disk queue backed by flat files with the
first two of these requirements. The `vfs.FS` will be the same as the
temporary storage engine, so we will piggy-back off of the existing
temporary file management code. The interface is as simple as:

```
type Queue interface {
    Enqueue(coldata.Batch) error
    Dequeue() (coldata.Batch, error)
    Close() error
}
```

The flat file `Queue` implementation will have knobs to configure
the in-memory buffer size to amortize write overhead, and the maximum
size of a file, after which it will roll over to a new file. Different
configurations will be benchmarked to find a good default value.

Note that the maximum in-use disk space will be limited by a disk monitor
similarly to what is currently done with external storage in the row execution
engine. As bytes are written to disk, these are accounted for by a
`mon.BytesMonitor`, which additionally provides observability in to how much
space is being used at a given time as well as how much disk space was used
in a given query through `EXPLAIN ANALYZE`.

The implementation will create unique files using the following naming scheme:
 ```
<FlowID>_<QueueID>_<FileID>
 ```
A `Queue` will be created with a unique integer `ID` and keep a counter
for the number of files created, which it will use as a suffix for
the file names to maintain uniqueness within the `Queue`. Prefixing
the filenames with the `FlowID` UUID (which uniquely describes a
sub-plan of a distributed query running on a given node), will allow
the `Flow` to perform any file cleanup once the query completes (in
case of orphaned files when `recover`ing from possible `panic`s).

Thankfully, serialization of `coldata.Batch`es is already
implemented in the `colserde` package using the [Arrow IPC
format](https://arrow.apache.org/docs/ipc.html). These serialized bytes will
be buffered until a flush is required, at which point they are written to a
file with an accompanying file footer. The start offset of these written bytes
as well as the number of bytes written will be stored for when the batches
should be dequeued. A current limitation is that bytes cannot be deserialized
at a finer granularity than they are written at, although we are planning
on fixing this (https://github.com/cockroachdb/cockroach/issues/42045).
Snappy compression is used to minimize file size.

The aforementioned abstraction of partitions as distinct queues can be trivially
implemented by instantiating as many queues as partitions necessary. This could
mean many small files, as a queue must have at least one file. The problem with
this design is that there might be many open file descriptors at once if the
user of the queue needs to read a little data from each partition, in the case
of an external merge sort, for example. The number of open file descriptors
is limited on some file systems, so this could lead to crashes. However, this
problem can be mitigated by having a maximum number of open file descriptors
scoped to a temporary storage engine as well as per queue. The implementation
of partitions would keep as many file descriptors open as allowed, closing the
least recently used file when a new partition must be read from.

## Rationale and Alternatives

A queue backed by Pebble was also considered as an alternative. The
configuration options for this queue are the write buffer size and
the maximum value size. Batches are enqueued up to the maximum value
size and written to a buffered writer with an ordinal as the key.

The benchmark used to test the performance of both queue implementations is
a simple write-everything then read-everything benchmark run on a linux gceworker.
This simulates the general case of having a single goroutine flush data
which it then rereads and operates on. The flat file `Queue` implementation
syncs files every `512KiB`, similarly to Pebble. Note that both implementations
use snappy compression.

Before benchmarking each implementation with a varying buffer size,
the data size to store was fixed at `512MiB` and each implementation was
independently benchmarked to find the optimal maximum file size in the
case of flat files, and value size in the case of pebble, while keeping
the write buffer size constant at 0. The options used for Pebble are
the same ones the Pebble temporary storage instance uses (see
https://github.com/cockroachdb/cockroach/blob/09b391536f72349c3951ce6b75c5231b58933b07/pkg/storage/engine/temp_engine.go#L117).

![Find](images/vectorized_external_storage1.png?raw=true "Find")

Given these graphs, a fixed file size of `32MiB` was chosen for the flat queue
implementation and a `128KiB` maximum value size for the Pebble implementation.

![Compare](images/vectorized_external_storage2.png?raw=true "Compare")

Overall, we see a ~50% improvement by using flat files
instead of Pebble as an on-disk queue implementation in the best case.

Initially, the downside of choosing flat files over Pebble was that
there wasn’t a clear understanding of how the requirements outlined
in [Detailed Design](#detailed-design) would be satisfied when using
flat files. However, since the `vfs.FS` implementation that the
Pebble temporary engine uses can be reused, there is no upside to
using pebble barring perhaps implementation simplicity since details
like compression and file descriptor management are taken care of.
