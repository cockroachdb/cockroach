- Feature Name: Backup & Restore
- Status: in-progress
- Start Date: 2016-07-13
- Authors: Daniel Harrison
- RFC PR: [#8966](https://github.com/cockroachdb/cockroach/pull/8966)
- Cockroach Issue: [#551](https://github.com/cockroachdb/cockroach/issues/551)


# Summary

Full Backup / Incremental Backup / Restore from Backup / Bulk Ingest


# Motivation

Any durable datastore is expected to have the ability to save a snaphot of data
and later restore from that snapshot. Even in a system that can gracefully
handle a configurable number of node failues, there are other motivations: a
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
- Blazing fast bulk ingest from "big data" pipelines and migration dumps
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
in each as a file in the [RocksDB WriteBatch format](
https://github.com/facebook/rocksdb/blob/v4.9/db/write_batch.cc#L10-L30). A
marshalled proto `BackupDescriptor` is placed next to the data and contains the
start and end hlc timestamps of the backup, mappings between each keyrange and
the filepath with that data, and denormalized copies of the SQL descriptors.
Support for common cloud storage providers will be included, as will local files
(for testing and nfs mounts).

_TODO(dan): Is there any advantage of using WriteBatch over a file of proto
records?_

Restore and ingest both use this same format. A backup will use the current
ranges as segments, so they will be roughly range sized when restored. It's
suggested that ingest segements are also this size, but not required.

See [Alternatives](#sstables-and-addfile) for a discussion of RocksDB AddFile
and using sstables as the file format.

See [Alternatives](#overlapping-ingest-segments) for a discussion of how we can
remove the requirement for non-overlapping key ranges in ingest.


## Full Backup

An hlc timestamp is given as the end time (no later than `now - max offset` but
choosing a time further in the past, up to a few minutes, will help minimize
interference with in-progress transactions). The start time is implicitly `0,0`.
All range descriptors are scanned in a txn and each one becomes a keyrange
segment for backup. Optionally, a selected set of tables can be backed up. A
record of the backup's progress is written and used to continue if the
coordinating node goes down.

_TODO(dan): It seems like this this progress should be written to a
`system.jobs` table. Link to an RFC for creating new system tables._

Via `DistSender`, the leaseholder of the range containing each segment is tasked
with backing it up and grabs a snapshot. If the node doesn't have the entire
keyrange because a split has happened in the interim, the rpc is failed and the
coordinator retries with more fine-grained segments. Alternately, the backup RPC
could continue, and do a (potentially) remote scan to get the data from the
right side of the split.

The RocksDB snapshot is iterated and for each key the latest MVCC entry before
the end time is streamed to the backup's configured basepath. Or, if the backup
should support time travel when restored, every MVCC entry between the start and
end time. The iteration must either go through the Store at the right level for
the Store's read/push/resolve loop to take effect or implement such a loop
itself. A running hash of key and value bytes is kept. When finished, the full
path to this data file and the hash is returned in the rpc response.

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
backups, so a `DiffIterator` will be added to handle this as well as surface the
tombstones.

Option 1: Via `TablePropertiesCollectorFactory`, RocksDB allows a user to
collect aggregate statistics during sstable creation and store them in the
resulting file. Each keyval is presented as it is processed (along with the
RocksDB sequence number). When there are no more keyvals, a hook is called to
get metadata that is stored in the written file. This metadata is retrievable
via standard RocksDB apis. We will store the earliest and latest timestamp seen
in the metadata.

_TODO(dan): Can we note the corresponding sequence numbers and use them for
anything?_

Option 2: If Option 1 proves too expensive to run on every sstable creation,
then it will be run only for level 0 sstables. Then, at each compaction, an
`EventListener` will be used to merge the earliest and latest timestamp
metadata. Unfortunately, this hook is too late to allow writing the merged
timestamps to the sstable metadata, so a side store will have to be introduced.


## Restore from Backup

The user requests a restore and gives a path to a full backup and any
incremental backups. Either all or a subset of tables are selected. A record of
the restore's progress is written and used to continue if the coordinating node
goes down.

A new SQL descriptor ID is claimed for each table and used, along with the
metadata in the `BackupDescriptor`, to split off new ranges in the keyspace. A
replica from each is sent an rpc via `DistSender` with the path(s) of the
corresponding segment data. The replica streams in the data, merging full and
incremental backups as appropriate, and rewrites keys with the new table ID.
These are applied in batches using the proposer evaluated kv `MVCCPut` command.
The data checksums are matched before committing. The `system.descriptor` entry
is written for the table and a temporary name is given to the table in
`system.namespace`, and both are gossiped.

An alternate, much faster method that doesn't send the data bytes through raft,
and thus avoids the write amplification, is described in the upcoming [Bulk
Ingest RFC](#TODO). NB: This optimization requires empty ranges, so if
interleaved tables are being restored, they must be restored together to use it.

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


## Bulk Ingest

Bulk ingest of precomputed data works similarly to restore. The user provides
files with non-overlapping ranges of kv entries. In contrast to restore, they
will not include timestamps. The files should ideally be range-sized, but the
system will handle ingesting big segments (which will be split after ingest via
the normal mechanism). Small segments will be merged into neighbors before
ingest, when possible.

CockroachDB will provide input and output adaptors for a few common distributed
computation frameworks. Input adaptors will be provided that read a backup,
allowing for data pipelines with no chance of affecting production. Output
adaptors will produce files in the expected format for bulk ingest and with a
placeholder table ID. We will also build cli subcommands that will ingest the
output of `./cockroach dump` and `pgdump`.

See [Alternatives](#overlapping-ingest-segments) for details on how we'll remove
the restrictions that the segments be non-overlapping.

# Drawbacks

- This is not realtime backup.

# Alternatives

## SSTables and AddFile
RocksDB has an [AddFile](
https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files)
method that links a well-formed sstable directly into the LSM tree. These files
are easy to output from backup and this bypasses quite a bit of computation. It
could potentially be a huge performance win. However, there are many details to
work out, including validation of the files and keeping the `MVCCStats` updated.
It's possible that work done to support this could also be used for snapshots.

## Overlapping Ingest Segments
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
slow. Bulk ingest, however, will generate any secondary indexes.

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
- If sstables are used for the file format for backup, we could easily implement
  a read-only `storage.Engine` on top of the set of immutable, internally
  sorted, non-overlapping segments. This could less trivially be used to start a
  read-only version of cockroach on top of it.
- The user could use one of the provided distributed computation input adaptors
  to write a job that extracts the data they need.
