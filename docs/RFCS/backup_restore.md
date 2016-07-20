- Feature Name: Backup & Restore
- Status: draft
- Start Date: 2016-07-13
- Authors: Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)


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

- Full and incremental backups, consistent to a point-in-time
- Backup and restore are distributed and don't interrupt production traffic
- Table-level backup scheduling
- Restore to a different number of machines than were backed up
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

An hlc timestamp is given as the end time (likely `now` or `now - max skew`) and
the start time is implictly `0,0`. All range descriptors are scanned in a txn
and each one becomes a keyrange segment for backup. Optionally, a selected set
of tables can be backed up. A record of the backup's progress is written and
used to continue if the coordinating node goes down.

_TODO(dan): It seems like this this progress should be written to a
`system.jobs` table. Link to an RFC for creating new system tables._

Via rpc (not Raft), a replica of each segment is tasked with backing it up. The
replica waits until the end time + max skew is in the past and then flushes
RocksDB and grabs a snapshot. If the node doesn't have the entire keyrange
because a split has happened in the interim, the rpc is failed and the
coordinator retries with more fine-grained segments.

The RocksDB snapshot is iterated and MVCC entries between the start and end time
are streamed to the backup's configured basepath. A running hash of key and
value bytes is kept. When finished, the full path to this data file and the hash
is returned in the rpc response.

Once each segment has been successfully completed, the backup descriptor is
written with the segment keyranges and data paths along with the SQL descriptors
(as of the end timestamp).

_TODO(dan): Add a diagram describing incremental backups of two tables on
different schedules._

## Incremental Backup

Full backups are scheduled periodically. Incremental backups may be used to keep
the set of backed up data more current.

The user requests an incremental backup and gives a path to a full backup.
Previous incremental backups may also be provided. The descriptors are read in,
checked for consistency, and an interval tree with the most recent backup time
for each keyrange is calculated. The end time for the incremental backup is
selected as described above, but the start time for each segment is set using
lookups into this tree.

Each sstable in RocksDB has an explict keyrange bound and an implicit hlc
timestamp bound. RocksDB will use the former to select relevant sstables during
iteration, but doesn't understand the structure of our keys and so cannot use
the latter. This will create an unacceptable overhead in incremental backups, so
instead, we do our own bookkeeping of timestamps.

Option 1: Via `TablePropertiesCollectorFactory`, RocksDB allows a user to
collect aggregate statistics during sstable creation and store them in the
resulting file. Each keyval is presented as it is processed (along with the
RocksDB sequence number). When there are no more keyvals, a hook is called to
get metadata that is stored in the written file. This metadata is retrievable
via standard RocksDB apis. We will store the earliest and latest timestamp seen
in the metadata.

_TODO(dan): Can we note the cooresponding sequence numbers and use them for
anything?_

Option 2: If Option 1 proves too expensive to run on every sstable creation,
then it will be run only for level 0 sstables. Then, at each compaction, an
`EventListener` will be used to merge the earliest and latest timestamp
metadata. Unfortunately, this hook is too late to allow writing the merged
timestamps to the sstable metadata, so a side store will have to be introduced.

A custom RocksDB snapshot iterator will be written to only open sstables
that meet both the key and timestamp bounds of an incremental backup.


## Restore from Backup

The user requests an restore and gives a path to a full backup and any
incremental backups. Either all or a subset of tables are selected. A record of
the restore's progress is written and used to continue if the coordinating node
goes down.

This section describes the simple case of importing new tables, which happens
when restoring a table under a new name or restoring a deleted table. See below
[In Place Restore](#in-place-restore) for a discussion of how to expand this
mechanism to restore/ingest a table "in place". This method also supports
restoring an old copy of a table next to the current version of it and
atomically switching over.

A new SQL descriptor ID is claimed for each table and used, along with the
metadata in the `BackupDescriptor`, to split off new ranges in the keyspace. A
replica from each is sent an rpc with the path(s) of the corresponding segment
data. The replica streams in the data, merging full and incremental backups as
appropriate, and rewrites keys with the new table ID. These are applied in
batches using the proposer evaluated kv `MVCCPut` command. The data checksums
are matched before commiting. The `system.descriptor` entry is written for the
table and a temporary name is given to the table in `system.namespace`, and both
are gossiped.

An alternate, much faster method that doesn't send the data bytes through raft,
and thus avoids the write amplification, is described in the upcoming [Bulk
Ingest RFC](#TODO). NB: This optimization requires empty ranges, so if
interleaved tables are being restored, they must be restored together to use it.

Similar to Postgres, the restored table's constraints are marked as `NOT VALID`:
updates and inserts are checked, but existing data is assumed to be correct.
Then a `VALIDATE CONSTRAINT` command can be issued, which checks the existing
data in the background and marks the constraint as `VALID` once successful. When
started, the restore can be configured to block on the completion of the
`VALIDATE CONSTRAINT` or not.

The `system.namespace` is flipped to the final name. If the restored table is
replacing a current one, the old data can be cleaned up.


## In-Place Restore

Restoring in place requires data unavailability and truncates the current data,
neither of which is true for the first method.

A `Locked` flag is flipped on the `TableDescriptor` and written to kv and
gossiped. A hook in the SQL permissions code will return an error describing the
table's locked state if it is queried. Note that time travel queries can
potentially use the old descriptor and will still work.

The table data is truncated and the data imported as above.

## Bulk Ingest

Bulk ingest of precomputed data works similarly to restore. The user provides
files with non-overlapping ranges of kv entries. In contrast to restore, they
will not include timestamps. The files should ideally be range-sized, but the
system will handle ingesting big segments (which will be split after ingest via
the normal mechanism). Small segments will be merged into neighbors before
ingest, when possible.

_TODO(dan): Can we allow the user to provide the mvcc timestamp? This could be
extended to allow copying changes that are made on a bulk ingested table onto
the next bulk ingested version of that table._

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
- The user must choose between table unavailability and briefly storing it
  twice.

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
seems likely that this could be used to segment overlapping files (which are
much easier for users to produce).

## Transaction Firehose
Several potential users have requested a stream of each transaction as it
commits, a feature also present in many competitors. If built, we could use this
to keep a log of every diff in some external system. This would be more timely
than the full & incremental proposal (though it's unclear if this is  something
clients actually need in a backup system). Unfortunately, it doesn't allow for
full backups, performant restores are difficult, and there are lots of tricky
distributed correctness issues. This is something we'll build eventually, but it
will not be how we build the first backup system.

## Local Snapshots
The cluster is frozen, each node takes and holds a RocksDB snapshot, and the
cluster is unfrozen. A version upgrade or other tricky operation is run. If it's
successful, the snapshots are released. If it's not, the RocksDB snapshots are
rolled back and the cluster is restored to exactly the state it was during the
freeze. This could be combined with the transaction firehose to avoid data loss

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
lowwater mark, instead of just the latest. However, it's unclear what the
semantics should be during restore. Should backups from before the gc threshold
be ineligable for timetravel after restore or should we add complications to the
gc worker to understand restored data?

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
