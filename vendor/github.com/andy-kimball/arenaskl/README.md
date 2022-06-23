# arenaskl

Fast, lock-free, arena-based Skiplist implementation in Go that supports iteration
in both directions.

## Advantages
Arenaskl offers several advantages over other skiplist implementations:

* High performance that linearly scales with the number of cores. This is
  achieved by allocating from a fixed-size arena and by avoiding locks.
* Iterators that can be allocated on the stack and easily cloned by value.
* Simple-to-use and low overhead model for detecting and handling race conditions
  with other threads.
* Support for iterating in reverse (i.e. previous links). 

## Limitations
The advantages come at a cost that prevents arenaskl from being a general-purpose
skiplist implementation:

* The size of the arena sets a hard upper bound on the combined size of skiplist
  nodes, keys, and values. This limit includes even the size of deleted nodes,
  keys, and values.
* Deleted nodes are not removed from the list, and are instead tagged with
  tombstone markers. This means that iteration times are proportional to the
  total number of nodes, rather than the number of live nodes.

## Pedigree

This code is based on the skiplist found in Badger, a Go-based KV store:

https://github.com/dgraph-io/badger/tree/master/skl

The skiplist in Badger is itself based on a C++ skiplist built for
Facebook's RocksDB:

https://github.com/facebook/rocksdb/tree/master/memtable

## Benchmarks

The benchmarks consist of a mix of reads and writes executed in parallel. The
fraction of reads is indicated in the run name: "frac_X" indicates a run where
X percent of the operations are reads.

The results are much better than `skiplist` and `slist`.

```
BenchmarkReadWrite/frac_0-8           5000000	       490 ns/op
BenchmarkReadWrite/frac_10-8          5000000	       479 ns/op
BenchmarkReadWrite/frac_20-8          5000000	       448 ns/op
BenchmarkReadWrite/frac_30-8          5000000	       440 ns/op
BenchmarkReadWrite/frac_40-8          5000000	       424 ns/op
BenchmarkReadWrite/frac_50-8          5000000	       384 ns/op
BenchmarkReadWrite/frac_60-8          5000000	       361 ns/op
BenchmarkReadWrite/frac_70-8          5000000	       315 ns/op
BenchmarkReadWrite/frac_80-8         10000000	       306 ns/op
BenchmarkReadWrite/frac_90-8         10000000	       267 ns/op
BenchmarkReadWrite/frac_100-8       100000000	       25.2 ns/op
```

And even better than a simple map with read-write lock:

```
BenchmarkReadWriteMap/frac_0-8        2000000	       691 ns/op
BenchmarkReadWriteMap/frac_10-8       3000000	       566 ns/op
BenchmarkReadWriteMap/frac_20-8       3000000	       562 ns/op
BenchmarkReadWriteMap/frac_30-8       3000000	       560 ns/op
BenchmarkReadWriteMap/frac_40-8       3000000	       519 ns/op
BenchmarkReadWriteMap/frac_50-8       3000000	       436 ns/op
BenchmarkReadWriteMap/frac_60-8       5000000	       484 ns/op
BenchmarkReadWriteMap/frac_70-8       5000000	       399 ns/op
BenchmarkReadWriteMap/frac_80-8       5000000	       400 ns/op
BenchmarkReadWriteMap/frac_90-8       5000000	       319 ns/op
BenchmarkReadWriteMap/frac_100-8     30000000	        43.6 ns/op
```
