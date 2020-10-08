- Feature Name: Backup & Restore
- Status: completed
- Start Date: 2016-07-13
- Authors: Daniel Harrison
- RFC PR: [#8966](https://github.com/cockroachdb/cockroach/pull/8966)
- Cockroach Issue: [#551](https://github.com/cockroachdb/cockroach/issues/551)


# Summary

Full Backup / Incremental Backup / Restore from Backup / Bulk Load


# Motivation

Any durable datastore is expected to have the ability to save a snapshot of data
and later restore from that snapshot. Even in a system that can gracefully
handle a configurable number of node failures, there are other motivations: a
general sense of security, "Oops I dropped a table", legally required data
archiving, and others.

Additionally, in an era where it's easy and useful to produce datasets in the
hundreds of gigabytes or terabyte range, CockroachDB has an opportunity for a
competitive advantage. First class support for using consistent snapshots as an
input to a bulk data pipeline (without any possibility of affecting production
traffic) as well as the ability to very quickly serve the output of them could
be a deciding factor for potential customers.


# Detailed design

## Goals and Non-Goals

- Full and incremental backups, consistent to a point-in-time
- Backup and restore are distributed and don't interrupt production traffic
- Table-level backup scheduling
- Restore to a different number of machines than were backed up
- Restore to a newer version of CockroachDB than was used for the backup.
  (Moving to an older version is also desirable when feasible.)
- Incremental backups touch minimally more data than necessary
- Blazing fast bulk load from "big data" pipelines and migration dumps
- Backup (the common operation) is kept as cheap as possible, while restore must
  be fast but is allowed to be more costly


- Desirable: Read-only access to backed up table
- Desirable: Support backup/restore of historical data (for time travel queries)


- Non-goal: [Realtime backup](#transaction-firehose) as each transaction is
  committed. This is useful, but out of scope for this rfc.
- Non-goal: A short-term hold on a snapshot of the entire database, allowing a
  [system-wide rollback](#local-snapshots). This is also potentially useful but
  out of scope.


## Data Format

A backup segments the keyspace into non-overlapping ranges and stores the data
in each in the [LevelDB sstable format]. A marshalled proto `BackupManifest`
is placed next to the data and contains the start and end hlc timestamps of the
backup, mappings between each keyrange and the filepath with that data, and
denormalized copies of the SQL descriptors. Support for common cloud storage
providers will be included, as will local files (for testing and nfs mounts).

Using sstables as the storage format paves the way to implementing
reasonably-efficient read-only queries over backups.

Restore and load both use this same format. A backup will use the current ranges
as segments, so they will be roughly range sized when restored. It's suggested
that load segements are also this size, but not required.

See [Alternatives](#overlapping-load-segments) for a discussion of how we can
remove the requirement for non-overlapping key ranges in load.

## Full Backup

An hlc timestamp is given as the end time (no later than `now - max offset` but
choosing a time further in the past, up to a few minutes, will help minimize
interference with in-progress transactions). The start time is implicitly `0,0`.
All range descriptors are scanned in a txn and each one becomes a keyrange
segment for backup. Optionally, a selected set of tables can be backed up. The
backup's progress is written to [the `system.jobs` table][jobs-rfc] and used to
continue if the coordinating node goes down.

Via `DistSender`, the leaseholder of the range containing each segment is tasked
with backing it up and grabs a snapshot. If the node doesn't have the entire
keyrange because a split has happened in the interim, the rpc is failed and the
coordinator retries with more fine-grained segments.

The RocksDB snapshot is iterated and for each key the latest MVCC entry before
the end time is streamed to the backup's configured basepath. Or, if the backup
should support time travel when restored, every MVCC entry between the start and
end time. The iteration must go through the Store at the right level for the
Store's read/push/resolve loop to take effect. When finished, the full path to
this data file and the hash of this file is returned in the rpc response.

Once each segment has been successfully completed, the backup descriptor is
written with the segment keyranges and data paths along with the SQL descriptors
(as of the end timestamp).


## Incremental Backup

Full backups are scheduled periodically. Incremental backups may be used to keep
the set of backed up data more current.

The user requests an incremental backup and gives a path to a full backup.
Previous incremental backups may also be provided, but the start and end times
must line up. The end time for the incremental backup is selected as described
above and the start time is set as the end time of the most recent of the
existing backups.

RocksDB's existing `Iterator` doesn't surface tombstones, which we'll need if a
key has been deleted since the last backup. Additionally, each sstable in
RocksDB has an explicit keyrange bound and an implicit hlc timestamp bound.
RocksDB's existing `Iterator` will use the former to select relevant sstables
during iteration, but doesn't understand the structure of our keys and so cannot
use the latter. This will create an unacceptable overhead in incremental
backups, so an `MVCCIncrementalIterator` will be added to handle this as well as
surface the tombstones.

Via `TablePropertiesCollectorFactory`, RocksDB allows a user to collect
aggregate statistics during sstable creation and store them in the resulting
file. Each keyval is presented as it is processed (along with the RocksDB
sequence number). When there are no more keyvals, a hook is called to get
metadata that is stored in the written file. This metadata is retrievable via
standard RocksDB apis. We will store the earliest and latest timestamp seen in
the metadata.

Had the `TablePropertiesCollectorFactory` proved too expensive to run on every
sstable creation, then it could have been limited to the level 0 sstables. Then,
at each compaction, an `EventListener` would have been used to merge the
earliest and latest timestamp metadata. Unfortunately, this hook is too late to
allow writing the merged timestamps to the sstable metadata, so a side store
will have to be introduced. Fortunately, the simpler approach was viable.

## Restore from Backup

The user requests a restore and gives a path to a full backup and any
incremental backups. Either all or a subset of tables are selected. A record of
the restore's progress is written and used to continue if the coordinating node
goes down.

A new SQL descriptor ID is claimed for each table and used, along with the
metadata in the `BackupManifest`, to split off new ranges in the keyspace. A
replica from each is sent an rpc via `DistSender` with the path(s) of the
corresponding segment data. The replica streams in the data, merging full and
incremental backups as appropriate, and rewrites keys with the new table ID.
These are applied in batches using the proposer evaluated kv `AddSSTable`
command. The data checksums are matched before committing. The
`system.descriptor` entry and `system.namespace` entry are written for the table
only after the rest of the work is complete to prevent users from accessing
intermediate state.

An alternate, much faster method that doesn't store the data bytes in the raft
log, and thus avoids the write amplification, is described in the [Raft SSTable
Sideloading RFC](20170601_raft_sstable_sideloading.md).

The restored table's NULL, DEFAULT, and PRIMARY KEY constraints do not need to
be checked. NOT NULL and CHECK are verified for each row as it is inserted. The
restored secondary index data is assumed to be correct and thus UNIQUE does not
need to be checked.

Any foreign key REFERENCES constraints are marked as `NOT VALID`: updates and
inserts are checked, but existing data is assumed to be correct. Then a
`VALIDATE CONSTRAINT` command can be issued, which checks the existing data in
the background and marks the constraint as `VALID` once successful. When
started, the restore can be configured to block on the completion of the
`VALIDATE CONSTRAINT` or not.

The `system.namespace` is flipped to the final name. If the restored table is
replacing a current one, the old data can be cleaned up.


## Bulk Load

Bulk load of precomputed data works similarly to restore. The user provides
files with non-overlapping ranges of kv entries. In contrast to restore, they
will not include timestamps. The files should ideally be range-sized, but the
system will handle loading big segments (which will be split after load via
the normal mechanism). Small segments will be merged into neighbors before
load, when possible.

CockroachDB will provide input and output adaptors for a few common distributed
computation frameworks. Input adaptors will be provided that read a backup,
allowing for data pipelines with no chance of affecting production. Output
adaptors will produce files in the expected format for bulk load and with a
placeholder table ID. We will also build cli subcommands that will load the
output of `./cockroach dump` and `pgdump`.

See [Alternatives](#overlapping-load-segments) for details on how we'll remove
the restrictions that the segments be non-overlapping.

## Resumability

Since large backups and restores can take on the order of hours, if not days,
automatically resuming the backup or restore when the coordinating node dies is
essential for their reliability. At present, all knowledge of backup or restore
progress is stored in memory on the coordinating node and vanishes when that
node crashes, even though that progress would be immediately usable by another
node in the cluster.

Once we can tolerate coordinator failure, two other important improvements
become trivial to implement:

  * `PAUSE` and `RESUME` commands can be exposed to operators. Providing these
    commands means minimizing the performance impact of backups and restores
    less pressing, as operators can simply pause a disruptive job while the
    cluster is busy, then resume it when the cluster has spare cycles.

  * Cloud provider service outages can be weathered without chewing up system
    resources by introducing an exponential backoff of several hours. Upon
    observing multiple cloud provider errors, the coordinator can mark the job
    as e.g. "paused until two hours from now" and clean up its goroutines,
    relying on the job daemon (see below) to resume the job.

The implementation of this feature will be similar to how schema changes
implement resumability. In short, each node will run a job daemon that
periodically adopts orphaned jobs by scanning the `system.jobs` table. At first,
only backup and restore will use this job daemon, but we'll eventually port
schema changes to use this job daemon too so that there's only one component in
the system responsible for resuming long-running jobs.

To prevent two nodes from attempting to adopt the same job, the `JobPayload`
message will be extended with a `JobLease` field:

```protobuf
message JobLease {
  optional uint32 node_id = 1 [
    (gogoproto.nullable) = false,
    (gogoproto.customname) = "NodeID",
    (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/roachpb.NodeID"
  ];
  // The epoch of the lease holder's node liveness entry.
  optional int64 epoch = 2 [(gogoproto.nullable) = false];
}

message JobPayload {
  // ...
  JobLease lease = 9;
}
```

When a job is started or adopted, the coordinating node will install a
`JobLease` with the node's liveness epoch. If the node's liveness lease expires,
so does all of its job leases. This is how [epoch-based range leases
work](20160210_range_leases.md).

To find orphaned jobs, the job daemon periodically executes

```sql
SELECT id, payload FROM system.jobs WHERE status = 'running' ORDER BY created DESC;
```

which can use the secondary index on `status, created`, then decodes the
`payload`, which contains the `JobLease`. The daemon will attempt to adopt the
first job (i.e., the oldest job) whose lease has expired by writing a new
job lease in the same transaction as the above `SELECT`. This ensures that two
nodes do not acquire the same lease.

Note that leases are a performance optimization only, as they are with schema
changes, as a race condition still exists. The node which previously held the
lease might still be performing work, having not yet realized that its lease has
expired. This is extremely unlikely, but still possible—consider a network
request that hangs for several minutes before completing, for example. Thus, it
must be safe for two nodes to perform the work of the backup or restore
simultaneously, but it need not be efficient.

Once the daemon has acquired the lease of an orphaned job, it launches a new
backup or restore coordinator that picks up where the old coordinator left off.
How progress information is persisted between coordinators depends on the type
of job.

## Restore state

The naïve implementation of restore progress would keep track of every completed
`Import`. The worst-case space requirements of this approach are unfortunate:
O(n) in the number of ranges in the restore. For a 2TB restore with
2<sup>16</sup> ranges and 16-byte split keys, this would require storing several
_megabytes_ of progress information. Storing this progress information in the
`system.jobs` table would require rewriting this several-megabyte blob on every
checkpoint, so we'd have to introduce a a separate SQL table, e.g. `CREATE TABLE
system.jobs_progress (job_id INT, startKey BYTES, endKey BYTES)`.

Instead, we'll rely on the fact that `ImportRequest`s are processed roughly in
order and keep track of the low-water mark—the maximum key before which all
`ImportRequests` have completed—in the `system.jobs` table. Whenever we update
the job progress (currently, every second or every 5%), we'll also update the
low-water mark. The logical place to store this low-water mark is the
`RestoreJobDetails` message:

```
message RestoreJobDetails {
  int64 table_id = 1;
  bytes low_water_mark = 2;
}
```

Resuming a restore, then, simply requires throwing away all `Import`s that refer
to keys beneath the low-water mark. If this scheme proves problematic, e.g.
because, with some regularity, early `Import`s get stuck and prevent the
low-water mark from rising, we can move to a more complicated scheme, like a
highly-compressed interval tree.

Importantly, because of the race condition described above, we need to be sure
that two restores can safely run concurrently. In short, this amounts to
ensuring that all interleavings of identical `Import` commands against the same
range are safe.

`Import`s currently issue `WriteBatch` commands under the hood, but will soon
issue `AddSSTable` commands instead. In both cases, these commands clear
any existing data in the key span they touch. This is not dangerous in the
current, non-resumable implementation because these commands are only issued on
key spans that are inaccessible through SQL—the table descriptor that exposes
these key spans to SQL is not installed until all `Import` commands have
completed. With resumability, however, we can no longer guarantee that all
`Import` commands have completed before writing the table descriptor.

Consider a restore that wedges just before it issues its last `WriteBatch`
command. After a few minutes, its lease expires, so another node adopts the
restore, completes the `WriteBatch` command, and exposes the table to SQL.
Suppose that the user, thankful to have their data back, immediately performs
some `INSERT`s and `UPDATE`s on the restored table. It's possible that the
original restore will unwedge and issue one last `WriteBatch` command before
realizing its lease has expired. If we're unlucky, this `WriteBatch` will wipe
out the data the user has just written.

This is a rather tricky problem to solve, since neither `WriteBatch` or
`AddSSTable` executes in the context of a transaction with commit
deadlines. One heavyweight solution is to imbue each range with a
`IsWriteBatchAllowed` flag, set the flag when the restore starts, and clear the
flag before exposing the table to SQL. Then, a wedged restore would be unable to
execute `WriteBatch` commands. This is quite a bit of overhead to prevent such
an unlikely race condition.

Instead, we propose to modify `WriteBatch` and/or `AddSSTable` to leave
existing data untouched. With these semantics, even if a lagging restore issues
a `WriteBatch`/`AddSSTable` against a range in an exposed table, it will
harmlessly overwrite the restored data with exactly the same data, while leaving
new user data alone.

## Backup state

Keeping track of backup state _requires_ O(n) space in the number of ranges in
the backup, as the coordinator must know the name of each range's SST to write
the final `BACKUP` descriptor. Since backups already require persistent remote
storage, we can use this same remote storage to maintain progress information
while the backup is in progress.

While the backup is in progress, the coordinator will write all completed
`ExportRequests` (i.e., `BackupManifest_File` messages) to a `PROGRESS` file
in the backup directory every few minutes. Streaming these messages or appending
them to an existing blob would avoid rewriting several megabytes on every
checkpoint and allow for more frequent checkpoints, but a) this would greatly
expand the `ExportStorage` interface, and b) most cloud storage providers don't
support streaming uploads or append-only blobs anyway. These cloud storage
providers, however, *are* more than than capable of handling rewriting several
megabytes of data every few minutes.

Resuming a backup, then, requires downloading the list of completed
`ExportRequest`s from the `PROGRESS` file, then proceeding with all remaining
`ExportRequest`s.

Solving the resumability race condition for backups is easy: the race condition
doesn't impact correctness. If two backups both attempt to write the `PROGRESS`
file, one will clobber the other. This is fine: it just means the `PROGRESS`
file is incomplete and a future coordinator may end up redoing more work than
necessary. (On Azure, which has actual append support, the `PROGRESS` file will
contain duplicate entries; we'll teach the coordinator to pick one at random.)
If two backups both attempt to write the `BACKUP` file, one will clobber the
other, but they're both valid `BACKUP` descriptors, so this is also fine.

# Drawbacks

- This is not realtime backup.

# Alternatives

## RocksDB WriteBatch format

The initial proposal suggested using the [RocksDB WriteBatch
format](https://github.com/facebook/rocksdb/blob/v4.9/db/write_batch.cc#L10-L30)
instead of sstables as the storage format. As mentioned above, sstables are
preferable since they allow for a reasonably-efficient implementation of
read-only queries over backups.

## Overlapping Load Segments
Distributed SQL is already building a framework for scalable computation. It
seems likely that this could be used to segment files of overlapping keyranges
(which are much easier for users to produce).

## Transaction Firehose
Several potential users have requested a stream of each transaction as it
commits, a feature also present in many competitors. If built, we could use this
to keep a log of every diff in some external system. This would be more timely
than the full & incremental proposal (though it's unclear if this is something
clients actually need in a backup system). Unfortunately, it doesn't allow for
full backups, performant restores are difficult, and there are lots of tricky
distributed correctness issues. This is something we'll build eventually, but it
will not be how we build the first backup system.

## Local Snapshots
The cluster is frozen, each node takes and holds a RocksDB snapshot, and the
cluster is unfrozen. A version upgrade or other tricky operation is run. If it's
successful, the snapshots are released. If it's not, the RocksDB snapshots are
rolled back and the cluster is restored to exactly the state it was during the
freeze. This could be combined with the transaction firehose to avoid data loss.

## Only Backup Primary Data
Secondary indexes are derivable from the primary data, so we could skip backing
them up to save space. Recomputing them on restore will probably make it too
slow.

# Unresolved questions

## UI and Scheduling
This will require a good amount of ui work, but that will be left for its own
RFC.

## Time Travelling in Restored Data
An backup option could be added to save every version of a key after the gc
lowwater mark, instead of just the latest. Users who would be interested in the
option to back up and restore time travel data would also set a very large GC
threshold for the tables where they use this feature.

# Restoring a Fraction of a Table (Options)
- We may want first-class support for restoring a segment of primary keys (or a
  secondary index) out of a table in a backup.
- The user could be instructed to bring up a new cluster and restore the whole
  table. This could be used to dump only what they wanted.
- Since sstables are used for the file format for backup, we could easily implement
  a read-only `storage.Engine` on top of the set of immutable, internally
  sorted, non-overlapping segments. This could less trivially be used to start a
  read-only version of cockroach on top of it.
- The user could use one of the provided distributed computation input adaptors
  to write a job that extracts the data they need.

[AddFile]: https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
[LevelDB sstable format]: https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats
[jobs-rfc]: 20170215_system_jobs.md
