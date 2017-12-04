- Feature Name: Dedicated storage engine for Raft
- Status: postponed
- Start Date: 2017-05-25
- Authors: Irfan Sharif
- RFC PR: [#16361](https://github.com/cockroachdb/cockroach/pull/16361)
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
and the raft log entries themselves<sup>[1]</sup>. Modifications to any of the
above are [synchronously
updated](https://github.com/cockroachdb/cockroach/pull/15366) on stable storage
before responding to RPCs.

In our usage of RocksDB, data is only persisted when explicitly issuing a write
with [`sync =
true`](https://github.com/cockroachdb/cockroach/blob/ea3b2c499/pkg/storage/engine/db.cc#L1828).
Internally this also persists previously unsynchronized writes<sup>[2]</sup>.

Let's consider a sequential write-only workload on a single node cluster. The
internals of the Raft/RocksDB+Storage interface can be simplified to the
following:
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
more than a thousand times as fast as synchronous writes<sup>[3]</sup>. Given
our current usage of the same RocksDB instance for both the underlying state
machine _and_ the consensus state we effectively forego (for this particular
workload at least) the performance gain to be had in not persisting state
machine modifications. For `n` writes we have `n` unsynchronized and `n`
synchronized writes where for `n-1` of them, we also flush `n-1` earlier
unsynchronized writes to disk.

By having a dedicated storage engine for Raft's persistent state we can address
this specific sub-optimality. By isolating the two workloads (synchronized and
unsynchronized writes) into separately running storage engines such that
synchronized writes no longer flush to disk previously unsynchronized ones, for
`n` writes we can have `n` unsynchronized and `n` synchronized writes (smaller
payload than the alternative above).

# Benchmarks

As a sanity check we ran some initial benchmarks that gives us a rough idea of
the performance gain to expect from this change. We benchmarked the sequential
write-only workload as described in the section above and did so at the
`pkg/storage{,/engine}` layers. What follows is redacted, simplified code of
the original benchmarks and the results demonstrating the speedups.

To simulate our current implementation (`n+1-th` synchronized write persists
the `n-th` unsynchronized write) we alternate between synchronized and
unsynchronized writes `b.N` times in `BenchmarkBatchCommitSharedEngine`.

```go
// pkg/storage/engine/bench_rocksdb_test.go

func BenchmarkBatchCommitSharedEngine(b *testing.B) {
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

To simulate the propose workload (`n` synchronized and `n` unsynchronized
writes, independent of one another) we simply issue `b.N` synchronized and
unsynchronized writes to a separate RocksDB instances in
`BenchmarkBatchCommitDedicatedEngines`.

```go
func BenchmarkBatchCommitDedicatedEngines(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ..., 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            for i := 0; i < b.N; i++ {
                {
                    // ...
                    batch := engA.NewWriteOnlyBatch()
                    MVCCBlindPut(batch, key, value)

                    // Representative of persisting a raft log entry.
                    batch.Commit(true)
                }
                {
                    // ...
                    batch := engB.NewWriteOnlyBatch()
                    MVCCBlindPut(batch, key, value)

                    // Representative of an unsynchronized state machine write.
                    batch.Commit(false)
                }
            }
        })
    }
}
```

```sh
~ benchstat perf-shared-engine.txt perf-dedicated-engine.txt
  name                      old time/op    new time/op     delta
  BatchCommit/vs=1024-4       75.4µs ± 4%     70.2µs ± 2%   -6.87%  (p=0.000 n=19+17)
  BatchCommit/vs=4096-4        117µs ± 5%      106µs ± 7%   -9.76%  (p=0.000 n=20+20)
  BatchCommit/vs=16384-4       325µs ± 7%      209µs ± 5%  -35.55%  (p=0.000 n=20+18)
  BatchCommit/vs=65536-4      1.05ms ±10%     1.08ms ±20%     ~     (p=0.718 n=20+20)
  BatchCommit/vs=262144-4     3.52ms ± 6%     2.81ms ± 7%  -20.30%  (p=0.000 n=17+18)
  BatchCommit/vs=1048576-4    11.2ms ±18%      7.8ms ± 5%  -30.56%  (p=0.000 n=20+20)

  name                      old speed      new speed       delta
  BatchCommit/vs=1024-4     13.6MB/s ± 4%   14.6MB/s ± 2%   +7.34%  (p=0.000 n=19+17)
  BatchCommit/vs=4096-4     34.9MB/s ± 5%   38.7MB/s ± 7%  +10.88%  (p=0.000 n=20+20)
  BatchCommit/vs=16384-4    50.5MB/s ± 8%   78.4MB/s ± 5%  +55.04%  (p=0.000 n=20+18)
  BatchCommit/vs=65536-4    62.6MB/s ± 9%   61.1MB/s ±17%     ~     (p=0.718 n=20+20)
  BatchCommit/vs=262144-4   74.5MB/s ± 5%   93.5MB/s ± 7%  +25.43%  (p=0.000 n=17+18)
  BatchCommit/vs=1048576-4  94.8MB/s ±16%  135.2MB/s ± 5%  +42.57%  (p=0.000 n=20+20)
```

NOTE: 64 KiB workloads don't exhibit the same performance increase as compared
to other workloads, this is unexpected and needs to be investigated further.
See [drawbacks](#drawbacks) for more discussion on this.

Similarly we ran the equivalent benchmarks at the `pkg/storage` layer:

```go
// pkg/storage/replica_raftstorage_test.go

func BenchmarkReplicaRaftStorageSameEngine(b *testing.B) {
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

To simulate the proposed workload (`n` synchronized and `n` unsynchronized
writes, independent of one another) we simply issue `b.N` synchronized writes
followed by `b.N` unsynchronized ones in
`BenchmarkReplicaRaftStorageDedicatedEngine`. To see why this is equivalent
consider a sequence of alternating/interleaved synchronized and unsynchronized
writes where synchronized writes do not persist the previous unsynchronized
writes. If `S` is the time taken for a synchronized write and `U` is the time
taken for an unsynchronized one,
`S + U + S + U + ...  S + U == S + S + ... + S + U + U + ... + U`.

```go
// NOTE: syncApplyCmd is set to true to synchronize command applications (state
// machine changes) to persistent storage. Changes to pkg/storage/replica.go
// not shown here.
func BenchmarkReplicaRaftStorageDedicatedEngine(b *testing.B) {
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
At the time of writing the keys that would need to be written to the new engine
are the log keys and `HardState`<sup>[4](#column-families)</sup>.

## Implementation strategy

We will phase this in bottom-up by first instantiating the new RocksDB instance
with reasonable default configurations (see [unresolved
questions](#unresolved-questions) below) at the `pkg/storage` layer (as opposed
to using the user level store specifications specified via the `--stores` flag
in the `cockroach start` command). The points at which raft data is written out
to our existing RocksDB instance, we will additionally write them out to our
new instance. Following this at any point where raft data is read, we will
_also_ read from the new instance and compare. At this point we can wean off
all Raft specific reads/writes and log truncations from the old instance and
have them serviced by the new.

Until the [migration story](#migration-strategy) is hashed out it's worthwhile
structuring this transition behind an environment variable that would determine
_which_ instance all Raft specific reads, writes and log truncations are
serviced from. This same mechanism could also be used to collect 'before and
after' performance numbers (at the very least as a sanity check).
We expect individual writes going through the system to speed up (on average we
can assume for every synchronized raft log entry write currently we're flushing
out a previously unsynchronized state machine transition). We expect read
performance to stay relatively unchanged.

**NB**: There's a subtle edge case to be wary of with respect to raft log
truncations, before truncating the raft log we need to ensure that the
application of the truncated entries have actually been persisted, i.e. for
`Put(k,v)` the primary RocksDB engine must have synced `(k,v)` before
truncating that `Put` operation from the Raft log.
Given we expect the `ReplicaState` to be stored in the first engine let's
consider the case where we've truncated a set of log entries and the
corresponding `TruncatedState`, to be stored on the first engine, is _not_
synchronized to disk. If the node crashes at this point it will fail to load
the `TruncatedState` and has no way to bridge the gap between the last
persisted `ReplicaState` and the oldest entry in the truncated Raft log.</br>
To this end whenever we truncate, we need to _first_ sync the primary RocksDB
instance. Given RocksDB periodically flushes in-memory writes to disk, if we
can detect that the application of the entries to be truncated have _already_
been persisted, we can avoid this step. See [future work](#future-work) for a
possible extension to this.
We should note that this will be the only time we _explicitly_ sync the primary
instance to disk, the performance blips that arise due to this will be
interesting to study.

Following this the `storage.NewStore` API will be amended to take in two
storage engines instead (the new engine is to be used as dedicated raft
storage). This API change propagates through any testing code that
bootstraps/initializes a node in some way, shape, or form. At the time or
writing the tests affected are spread across `pkg/{kv,storage,server,ts}`.

## Migration strategy

A general migration process, in addition to solving the problem for existing
clusters, could be used to move the raft RocksDB instance from one location to
another (such as to the non-volatile memory discussed below).
How do we actually start using this RocksDB instance? One thought is that
only new nodes would use a separate RocksDB instance for the Raft log.
An offline migration alternative that would work for existing clusters and
rolling restarts could be the following:
- We detect that we're at the new version with the changes that move the
  consensus state to a separate RocksDB instance, and we have existing
  consensus data stored in the old
- We copy over _all_ consensus data (for all replicas on that given store) from
  the old to the new and delete it in the old
- As the node is up and running all Raft specific reads/writes are directed to
  the new
We should note that we don't have precedent for an offline, store-level
migration at this time.

An online approach that could enable live migrations moving consensus state
from one RocksDB instance to another would be the following:
- for a given replica we begin writing out all consensus state to _both_ RocksDB
  instances, the instance the consensus state is being migrated to and the
  instance it's being migrated from (at this point we still only exclusively
  read from the old instance)
- at the next log truncation point we set a flag such that
    - all subsequent Raft specific reads are directed to the new instance
    - all subsequent Raft specific writes are _only_ directed to the new instance
- we delete existing consensus state (pertaining to the given replica) from the
  old instance. This already happens (to some degree) in normal operation given we're
  truncating the log

The implementation of the latter strategy is out of the scope for this RFC, the
offline store-level migration alternative should suffice.

## TODO
- Investigate 'Support for Multiple Embedded Databases in the same
  process'<sup>[5]</sup>

# Drawbacks

None that are immediately obvious. We'll have to pay close attention to how the
separate RocksDB instances interact with one another, the performance
implications can be non-obvious and subtle given the sharing of hardware
resources (disk and/or OS buffers).
To demonstrate this consider the following two versions of a benchmark, only
difference being that in one we have a single loop body and write out `b.N` synced
and unsynced writes (interleaved) versus two loop bodies with `b.N` synced and
unsynced writes each:

```go
func BenchmarkBatchCommitInterleaved(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, ..., 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            for i := 0; i < b.N; i++ {
                // ...
                batchA := engA.NewWriteOnlyBatch()
                MVCCBlindPut(batchA, key, value)
                batchA.Commit(true)

                // ...
                batchB := engB.NewWriteOnlyBatch()
                MVCCBlindPut(batchB, key, value)
                batchB.Commit(false)
            }
        })
    }
}

func BenchmarkBatchCommitSequential(b *testing.B) {
    for _, valueSize := range []int{1 << 10, 1 << 12, 1 << 20} {
        b.Run(fmt.Sprintf("vs=%d", valueSize), func(b *testing.B) {
            // ...
            for i := 0; i < b.N; i++ {
                // ...
                batchA := engA.NewWriteOnlyBatch()
                MVCCBlindPut(batchA, key, value)
                batchA.Commit(true)
            }

            for i := 0; i < b.N; i++ {
                // ...
                batchB := engB.NewWriteOnlyBatch()
                MVCCBlindPut(batchB, key, value)
                batchB.Commit(false)
            }
        })
    }
}
```

Here are the performance differences, especially stark for 64KiB workloads:
```sh
~ benchstat perf-interleaved.txt perf-sequential.txt
  name                      old time/op    new time/op     delta
  BatchCommit/vs=1024-4       70.1µs ± 2%     68.6µs ± 5%   -2.15%  (p=0.021 n=8+10)
  BatchCommit/vs=4096-4        102µs ± 1%       97µs ± 7%   -4.10%  (p=0.013 n=9+10)
  BatchCommit/vs=16384-4       207µs ± 5%      188µs ± 4%   -9.46%  (p=0.000 n=9+9)
  BatchCommit/vs=65536-4      1.07ms ±12%     0.62ms ± 9%  -41.90%  (p=0.000 n=8+9)
  BatchCommit/vs=262144-4     2.90ms ± 8%     2.70ms ± 4%   -6.68%  (p=0.000 n=9+10)
  BatchCommit/vs=1048576-4    8.06ms ± 9%     7.90ms ± 5%     ~     (p=0.631 n=10+10)

  name                      old speed      new speed       delta
  BatchCommit/vs=1024-4     14.6MB/s ± 2%   14.9MB/s ± 4%   +2.22%  (p=0.021 n=8+10)
  BatchCommit/vs=4096-4     40.3MB/s ± 1%   42.1MB/s ± 7%   +4.37%  (p=0.013 n=9+10)
  BatchCommit/vs=16384-4    78.6MB/s ± 5%   87.4MB/s ± 3%  +11.09%  (p=0.000 n=10+9)
  BatchCommit/vs=65536-4    61.6MB/s ±13%  105.5MB/s ± 8%  +71.32%  (p=0.000 n=8+9)
  BatchCommit/vs=262144-4   90.6MB/s ± 7%   97.1MB/s ± 4%   +7.13%  (p=0.000 n=9+10)
  BatchCommit/vs=1048576-4   130MB/s ± 8%    133MB/s ± 5%     ~     (p=0.631 n=10+10)
```

Clearly the separately running instances are not as isolated as expected.

# Alternatives

An alternative considered was rolling out our own WAL implementation optimized
for the Raft log usage patterns. Possible reasons for doing so:
- A native implementation in Go would avoid the CGo overhead we incur crossing
  the Go/C++ boundary
- SanDisk published a paper<sup>[6]</sup> (a shorter slideshow can be found
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

A good first approximation would be allocating contiguous chunks of disk space
in sequence, each chunk assigned to a given accessor. Should an accessor run
out of allocated space, it seeks the next available chunk and adds metadata
linking across the two (think linked lists). Though this would enable fast
sequential log access, log prefix truncations are slightly trickier. Do we
truncate at chunk sized boundaries or truncate at user specified points and
thus causing fragmentation?
Perusing through open sourced implementations of WALs and some literature on
the subject, multi-access WALs tend to support _at most_ 10 accessors, let
alone thousands. Retro-fitting this for our use case (a single store can have
1000s of replicas), we'd have to opt-in for a 'sharded store' approach where
appropriately sized sets of replicas share an instance of a multi-access WAL.

Taking into account all of the above, it was deemed that the implementation
overhead and the additional introduced complexity (higher level organization
with sharded stores) was not worth what could _possibly_ be a tiny performance
increase. We suspect a tuned RocksDB instance would be hard to beat unless we
GC aggressively, not to mention it's battle tested. The internal knowledge base
for tuning and working with RocksDB is available at CockroachDB, so this
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

- RocksDB parameters/tuning for the Raft specific instance.
- We currently share a block cache across the multiple running RocksDB
  instances across stores in a node. Would a similar structure be beneficial
  here? Do we use the same block cache or have another dedicated one as well?

# Future work

Intel has demonstrated impressive performance increases by putting the raft log
in non-volatile memory instead of disk (for etcd/raft)<sup>[7]</sup>. Given
we're proposing a separate storage engine for the Raft log, in the presence of
more suitable hardware medium it should be easy enough to configure the Raft
specific RocksDB instance/multi-access WAL implementation to run on it. Even
without the presence of specialized hardware it might be desirable to configure
the Raft and regular RocksDB instances to use different disks.

RocksDB periodically flushes in-memory writes to disk, if we can detect which
writes have been persisted and use _that_ information to truncate the
corresponding raft log entries, we can avoid the (costly) explicit syncing of
the primary RocksDB instance. This is out of the scope for this RFC.

As an aside, [@tschottdorf](https://github.com/tschottdorf):
> we should refactor the way `evalTruncateLog` works. It currently
> takes writes all the way through the proposer-evaluated KV machinery, and at
> least from the graphs it looks that that's enough traffic to impair Raft
> throughput alone. We could lower the actual ranged clear below Raft (after all,
> no migration concerns there). We would be relaxing, somewhat, the stats which
> are now authoritative and would then only become "real" once the Raft log had
> actually been purged all the way up to the TruncatedState. I think there's no
> problem with that.


# Footnotes
\[1\]: https://raft.github.io/raft.pdf </br>
\[2\]: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ </br>
\[3\]: https://github.com/facebook/rocksdb/wiki/Basic-Operations#asynchronous-writes </br>
<a name="column-families">\[4\]</a>: via
[@bdarnell](https://github.com/bdarnell) &
[@tschottdorf](https://github.com/tschottdorf):
  > We may want to consider using two column families for this, since the log
  > keys are _usually_ (log tail can be replaced after leadership change)
  > write-once and short-lived, while the hard state is overwritten frequently
  > but never goes away completely.

\[5\]: https://github.com/facebook/rocksdb/wiki/RocksDB-Basics#support-for-multiple-embedded-databases-in-the-same-process </br>
\[6\]: https://www.usenix.org/system/files/conference/inflow14/inflow14-yang.pdf </br>
\[7\]: http://thenewstack.io/intel-gives-the-etcd-key-value-store-a-needed-boost/ </br>


[1]: https://raft.github.io/raft.pdf
[2]: https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
[3]: https://github.com/facebook/rocksdb/wiki/Basic-Operations#asynchronous-writes
[5]: https://github.com/facebook/rocksdb/wiki/RocksDB-Basics#support-for-multiple-embedded-databases-in-the-same-process
[6]: https://www.usenix.org/system/files/conference/inflow14/inflow14-yang.pdf
[7]: http://thenewstack.io/intel-gives-the-etcd-key-value-store-a-needed-boost/
