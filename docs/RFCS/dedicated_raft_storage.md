- Feature Name: Dedicated storage engine for Raft
- Status: draft
- Start Date: 2017-05-25
- Authors: Irfan Sharif
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue(s):
  [#7807](https://github.com/cockroachdb/cockroach/issues/7807),
  [#15245](https://github.com/cockroachdb/cockroach/issues/15245)

# Summary

At the time of writing each
[`Replica`](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/replica.go#L214)
is backed by a single instance of RocksDB
([`Store.engine`](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/store.go#L391))
which is used to store all modifications to the underlying state machine in
_addition_ to storing all consensus state. This RFC proposes the separation of
the two, outlines the motivations for doing so and alternatives considered.

# Motivation

Raft's RPCs typically require the recipient to persist information to stable
storage before responding. This 'persistent state' is comprised of the latest
term the server has seen, the candidate voted for in the current term (if any),
and the raft log entries themselves<sup>[1]</sup>. Modifications to any of the above are
synchronously updated on stable storage before responding to RPCs.

In our usage of RocksDB, data is only persisted when explicitly issuing a write
with [`sync =
true`](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/engine/db.cc#L1828).
Internally this also persists previously unsynchronized writes<sup>[2]</sup>.

Let's consider a sequential write-only workload on a single node cluster. The
internals of the Raft/RocksDB+Storage interface can be simplified to the following:
  - Convert the write command into a Raft proposal and
    [submit](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/replica.go#L2811)
    the proposal to the underlying raft group
  - 'Downstream' of raft we
    [persist](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/replica.go#L3120)
    the newly generated log entry corresponding to the command
  - We record the modifications to the underlying state machine but [_do
    not_](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/replica.go#L4208)
    persist this synchronously

One can see that for the `n+1-th` write, upon persisting the corresponding raft
log entry, we also end up persisting the state machine modifications from the
`n-th` write. It is worth mentioning here that asynchronous writes are often
more than a thousand times as fast as synchronous
writes<sup>[3]</sup>. Given our current usage of the same RocksDB instance for both the
underlying state machine _and_ the consensus state we effectively forego (for
this particular workload at least) the performance gain to be had in not
persisting state machine modifications. For `n` writes we have `n`
unsynchronized and `n` synchronized writes where for `n-1` of them, we also
flush `n-1` earlier unsynchronized writes to disk (bigger payload than the
alternative below).

By having a dedicated storage engine for Raft's persistent state we can address
this specific sub-optimality. By isolating the two workloads
(synchronized and unsynchronized writes) into separately running storage
engines such that synchronized writes no longer flush to disk previously unsynchronized ones,
for `n` writes we can have `n` unsynchronized and `n` synchronized writes.

# Benchmarks

As a sanity check we ran some initial benchmarks that gives us a rough idea of
the performance gain to expect from this change. We benchmarked the sequential
write-only workload as described in the section above and did so at the
`pkg/storage{,/engine}` layers. What follows is redacted, simplified code of
the original benchmarks and the results demonstrating the speedups.

To simulate our current implementation (`n+1-th` synchronized write persists
the `n-th` unsynchronized write) we alternate between synchronized and
unsynchronized writes `b.N` times in `BenchmarkBatchCommitAlternating`.

```go
// pkg/storage/engine/bench_rocksdb_test.go

func BenchmarkBatchCommitAlternating(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ..., 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            for i := 0; i < b.N; i++ {
                {
                    // ...
                    batch := eng.NewWriteOnlyBatch()
                    MVCCBlindPut(batch, key, value)

                    // Representative of persisting a raft log entry.
                    batch.Commit(true)
                }
                {
                    // ...
                    batch := eng.NewWriteOnlyBatch()
                    MVCCBlindPut(batch, key, value)

                    // Representative of an unsynchronized state machine write.
                    batch.Commit(false)
                }
            }
        })
    }
}
```

To simulate the proposed workload (`n` synchronized and `n` unsynchronized
writes, independent of one another) we simply issue `b.N` synchronized writes
followed by `b.N` unsynchronized ones in `BenchmarkBatchCommitSequential`. To
see why this is equivalent consider a sequence of alternating/interleaved
synchronized and unsynchronized writes where synchronized writes do not persist
the previous unsynchronized writes. If `S` is the time taken for a synchronized
write and `U` is the time taken for an unsynchronized one, `S + U + S + U + ... + S + U == S + S + ... + S + U + U + ... + U`.

```go
func BenchmarkBatchCommitSequential(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ..., 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            for i := 0; i < b.N; i++ {
                // ...
                batch := eng.NewWriteOnlyBatch()
                MVCCBlindPut(batch, key, value)
                batch.Commit(true)
            }

            for i := 0; i < b.N; i++ {
                // ...
                batch := eng.NewWriteOnlyBatch()
                MVCCBlindPut(batch, key, value)
                batch.Commit(false)
            }
        })
    }
}
```

```sh
~ benchstat perf-engine-alternating.txt perf-engine-sequential.txt
  name                      old time/op    new time/op     delta
  BatchCommit/vs=1024-4       75.9µs ±11%     68.1µs ± 2%   -10.38%  (p=0.000 n=10+10)
  BatchCommit/vs=4096-4        130µs ± 4%       95µs ± 3%   -27.20%  (p=0.000 n=8+9)
  BatchCommit/vs=16384-4       372µs ±20%      184µs ± 5%   -50.66%  (p=0.000 n=10+10)
  BatchCommit/vs=65536-4      1.12ms ±20%     0.61ms ± 3%   -46.02%  (p=0.000 n=10+9)
  BatchCommit/vs=262144-4     3.49ms ± 3%     2.76ms ± 3%   -20.98%  (p=0.000 n=8+10)
  BatchCommit/vs=1048576-4    11.6ms ±14%      8.0ms ± 5%   -30.87%  (p=0.000 n=10+10)

  name                      old speed      new speed       delta
  BatchCommit/vs=1024-4     13.5MB/s ±10%   15.0MB/s ± 2%   +11.30%  (p=0.000 n=10+10)
  BatchCommit/vs=4096-4     31.5MB/s ± 4%   43.3MB/s ± 3%   +37.31%  (p=0.000 n=8+9)
  BatchCommit/vs=16384-4    44.4MB/s ±18%   89.2MB/s ± 5%  +100.92%  (p=0.000 n=10+10)
  BatchCommit/vs=65536-4    58.8MB/s ±17%  108.0MB/s ± 3%   +83.76%  (p=0.000 n=10+9)
  BatchCommit/vs=262144-4   75.1MB/s ± 3%   95.1MB/s ± 4%   +26.53%  (p=0.000 n=8+10)
  BatchCommit/vs=1048576-4  91.0MB/s ±15%  131.0MB/s ± 5%   +43.95%  (p=0.000 n=10+10)
```

Similarly we ran the equivalent benchmarks at the `pkg/storage` layer:

```go
// pkg/storage/replica_raftstorage_test.go

func BenchmarkReplicaRaftStorageAlternating(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ... , 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            rep := tc.store.GetReplica(rangeID)
            rep.redirectOnOrAcquireLease()

            defer settings.TestingSetBool(&syncRaftLog, true)()

            for i := 0; i < b.N; i++ {
                // ...
                client.SendWrappedWith(rep, putArgs(key, value))
            }
        })
    }
}
```

```go
// NOTE: syncApplyCmd is set to true to synchronize command applications (state
// machine changes) to persistent storage. Changes to pkg/storage/replica.go
// not shown here.
func BenchmarkReplicaRaftStorageSequential(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ... , 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            rep := tc.store.GetReplica(rangeID)
            rep.redirectOnOrAcquireLease()

            defer settings.TestingSetBool(&syncRaftLog, false)()
            defer settings.TestingSetBool(&syncApplyCmd, false)()

            for i := 0; i < b.N/2; i++ {
                // ...
                client.SendWrappedWith(rep, putArgs(key, value))
            }

            defer settings.TestingSetBool(&syncRaftLog, true)()
            defer settings.TestingSetBool(&syncApplyCmd, true)()

            for i := b.N/2; i < b.N; i++ {
                // ...
                client.SendWrappedWith(rep, putArgs(key, value))
            }
        })
    }
}
```

```sh
~ benchstat perf-storage-alternating.txt perf-storage-sequential.txt
  name                             old time/op  new time/op  delta
  ReplicaRaftStorage/vs=1024-4      297µs ± 9%   268µs ± 3%   -9.73%  (p=0.000 n=10+10)
  ReplicaRaftStorage/vs=4096-4      511µs ±10%   402µs ± 1%  -21.29%  (p=0.000 n=9+10)
  ReplicaRaftStorage/vs=16384-4    2.16ms ± 2%  1.39ms ± 4%  -35.70%  (p=0.000 n=10+10)
  ReplicaRaftStorage/vs=65536-4    3.60ms ± 3%  3.49ms ± 4%   -3.17%  (p=0.003 n=10+9)
  ReplicaRaftStorage/vs=262144-4   10.3ms ± 7%  10.2ms ± 3%     ~     (p=0.393 n=10+10)
  ReplicaRaftStorage/vs=1048576-4  40.3ms ± 7%  40.8ms ± 3%     ~     (p=0.481 n=10+10)
```

# Detailed design

We propose introducing a second RocksDB instance to store all raft consensus
data. This RocksDB instance will be specific to a given store (similar to our
existing setup) and will be addressable via a new member variable on `type
Store`, namely `Store.raftEngine` (the existing `Store.engine` will stay as
is). This instance will consequently manage the raft log entries for all the
replicas that belong to that store. This will be stored in a subdirectory
`raft` under our existing RocksDB storage directory.

## Implementation strategy

We will phase this in bottom-up by first instantiating the new RocksDB instance
with reasonable default configurations (see [Unresolved
questions](#unresolved-questions) below) at the `pkg/storage` layer (as opposed
to using the user level store specifications specified via the `--stores` flag
in the `cockroach start` command). The points at which raft data is written out
to our existing RocksDB instance, we will additionally write them out to our
new instance. Following this at any point where raft data is read, we will
_also_ read from the new instance and compare. At this point we can wean off
all Raft specific reads/writes and log truncations from the old instance and
have them serviced by the new.

Until the [migration story](#migration-strategy) is hashed out it's
worthwhile structuring this transition behind an environment variable that
would determine _which_ instance all Raft specific reads, writes and log
truncations are serviced from. This same mechanism could also be used to
collect 'before and after' performance numbers (at the very least as a sanity
check).</br>
We expect individual writes going through the system to speed up (on average we
can assume for every synchronized raft log entry write currently we're flushing
out a previously unsynchronized state machine transition). We expect read
performance to stay relatively unchanged.

**NB**: There's a subtle edge case to be wary of with respect to raft log
truncations, before truncating the raft log we need to ensure that the
application of the truncated entries have actually been persisted.</br>
**NB**: RocksDB by default uses faster `fdatasync()` to sync files. We'll need to
use `fsync()` instead in filesystems like ext3 where you can lose files after a
reboot, this can be done so by setting `Options::use_fsync` to `true` (for the
`WriteBatch`).

Following this the `storage.NewStore` API will be amended to take in two
storage engines instead (the new engine is to be used as dedicated raft
storage). This API change propagates through any testing code that
bootstraps/initializes a node in some way, shape, or form. At the time or
writing the tests affected are spread across `pkg/{kv,storage,server,ts}`.

## Migration strategy

TBD. Some things to take note of:
- The Raft log is tightly coupled to our consistency checker, this will be
  something to be wary of in order to transition safely.
- How do we actually start using this RocksDB instance? One thought is that only
  new nodes would use a separate RocksDB instance for the Raft log. Another
  is to add a store-level migration.

## TODO
- Investigate 'Support for Multiple Embedded Databases in the same
process'<sup>[4]</sup>

# Drawbacks

It is not immediately obvious how much disk space should be allocated for the
Raft specific RocksDB engine. This is not something we had to concern ourselves
with earlier given we were using a single RocksDB instance for both the state
machine and the consensus data (we could simply configure the single
RocksDB instance to use all storage available). It is not unusual to have
thousands of ranges per store and assuming a maximum raft log size of of ~ 4
MiB
([`raftLogMaxSize`](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/raft_log_queue.go#L54)),
we could be looking at 10s of GiB disk space reserved for Raft alone.

Things break when RocksDB runs out of disk-space and this will occur should we
under-provision the Raft specific RocksDB instance. We can over-provision but
this would take away space that could otherwise be used for the state machine
instead. </br>
NOTE: If both running instances have dedicated disk spaces, we need to surface
additional usage statistics to the admin UI.

This might be a non-issue if RocksDB allows for multiple embedded databases to
operate off the same disk space (See [TODO](#todo)).

# Alternatives

An alternative considered was rolling out our own WAL implementation optimized
for the Raft log usage patterns. Possible reasons for doing so:
- A native implementation in Go would avoid the CGo overhead we incur crossing
  the Go/C++ boundary
- SanDisk published a paper<sup>[5]</sup> (a shorter slideshow can be found
  [here](https://www.usenix.org/sites/default/files/conference/protected-files/inflow14_slides_yang.pdf))
  discussing the downsides of layering log systems on one another. Summary:
  - Increased write pressure - each layer/log has its own metadata
  - Fragmented logs - 'upper level' logs writes sequentially but the 'lower
    level' logs gets mixed workloads, most likely to be random, destroying
    sequentiality
  - Unaligned segment sizes - garbage collection in 'upper level' log segments
    can result in data invalidation across multiple 'lower level' log segments

Considering how any given store could have thousands of replicas, an approach
with each replica maintaining it's own separate file for it's WAL was a
non-starter. What we would really need is something that resembled a
multi-access, shared WAL (by multi-access here we mean there are multiple
logical append points in the log and each accessor is able to operate only it's
own logical section).

Consider what would be the most common operations:
- Accessing a given replica's raft log sequentially
- Prefix truncation of a given replica's raft log

A good first approximation would be allocating contiguous chunks of disk space in
sequence, each chunk assigned to a given accessor. Should an accessor run out
of allocated space, it seeks the next available chunk and adds metadata linking
across the two (think linked lists). Though this would enable fast sequential
log access, log prefix truncations are slightly trickier. Do we truncate at
chunk sized boundaries or truncate at user specified points and thus causing
fragmentation?
Perusing through open sourced implementations of WALs and some literature on
the subject, multi-access WALs tend to support _at most_ 10 accessors, let alone
thousands. Retro-fitting this for our use case (a single store can have 1000s
of replicas), we'd have to opt-in for a 'sharded store' approach where
appropriately sized sets of replicas share an instance of a multi-access WAL.

Taking into account all of the above, it was deemed that the implementation
overhead and the additional introduced complexity (higher level organization
with sharded stores) was not worth what could _possibly_ be a tiny performance
increase. We suspect a tuned RocksDB instance would be hard to beat unless we
GC aggressively, not to mention it's battle tested. The internal knowledge
base for tuning and working with RocksDB is available at CockroachDB, so this
reduces future implementation risk as well.</br>
NOTE: At this time we have not explored potentially using
[dgraph-io/badger](https://github.com/dgraph-io/badger) instead.

Some Raft WAL implementations explored were the
[etcd/wal](https://github.com/coreos/etcd/tree/master/wal) implementation and
[hashicorp/raft](https://github.com/hashicorp/raft)'s LMDB
[implementation](https://github.com/hashicorp/raft-mdb). As stated above the
complexity comes about in managing logs for 1000s of replicas on the same
store.

# Unresolved questions

- What percentage of a given store should be dedicated to
  the Raft specific RocksDB instance? This should not be a user-level
  configuration. See [Drawbacks](#drawbacks).
- What measures, if any, should to be taken if the Raft specific RocksDB
  instance runs out of disk space?
- We currently share a block cache across the multiple running RocksDB
  instances across stores in a node. Would a similar structure be beneficial
  here? Do we use the same block cache or have another dedicated one as well?
- RocksDB parameters/tuning for the Raft specific instance.
- The migration story. See [Migration story](#migration-story).

# Future work

Intel has demonstrated impressive performance increases by putting the raft log
in non-volatile memory instead of disk (for etcd/raft)<sup>[6]</sup>.
Given we're proposing a separate storage engine for the Raft log, in the
presence of more suitable hardware medium it should be easy enough to configure
the Raft specific RocksDB instance/multi-access WAL implementation to run on
it.

# Footnotes
\[1\]: https://raft.github.io/raft.pdf </br>
\[2\]: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ </br>
\[3\]: https://github.com/facebook/rocksdb/wiki/Basic-Operations#asynchronous-writes </br>
\[4\]: https://github.com/facebook/rocksdb/wiki/RocksDB-Basics#support-for-multiple-embedded-databases-in-the-same-process </br>
\[5\]: https://www.usenix.org/system/files/conference/inflow14/inflow14-yang.pdf </br>
\[6\]: http://thenewstack.io/intel-gives-the-etcd-key-value-store-a-needed-boost/


[1]: https://raft.github.io/raft.pdf
[2]: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
[3]: https://github.com/facebook/rocksdb/wiki/Basic-Operations#asynchronous-writes
[4]: https://github.com/facebook/rocksdb/wiki/RocksDB-Basics#support-for-multiple-embedded-databases-in-the-same-process
[5]: https://www.usenix.org/system/files/conference/inflow14/inflow14-yang.pdf
[6]: http://thenewstack.io/intel-gives-the-etcd-key-value-store-a-needed-boost/
