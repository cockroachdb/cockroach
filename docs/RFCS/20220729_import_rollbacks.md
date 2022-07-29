* Feature Name: Import Rollbacks without MVCC Timestamps
* Status: In Progress
* Start Date: 2022-05-24
* Authors: Michael Butler, David Taylor
* Cockroach Issue: [#76722](https://github.com/cockroachdb/cockroach/issues/76722)


## Background and Motivation

Currently if IMPORT is writing data into an existing, non-empty table and fails
or is canceled mid-IMPORT, it must rollback the partial and potentially
inconsistent data it had imported by finding and deleting any key-value pairs it
had written.

These are found and deleted by scanning the table for KVs with a timestamp
greater than the time at which the IMPORT started. This method of rollback
deletes the correct data because the table is offline to all other writes during
the import and the import only writes new rows. Thus because the timestamps on
the table’s existing KVs do not change between the start of the import and the
rollback, the new KVs added by the import can be distinguished from those that
pre-existed the IMPORT by their timestamps. However, two product gaps compel us
to relax the third invariant -- that the timestamps on existing KVs must not
change during the IMPORT.

As a consequence, we need to implement import rollbacks without relying on MVCC
timestamps.


### Implementing MVCC AddSSTable in Restores of Tenants

Ideally, all data written to a cluster, including data written by all flavors of
RESTORE, _should_ be written with MVCC timestamps matching when the data was
written -- i.e. restored -- since KV and its users assume a key’s timestamp
indicates when it was last written or changed. This assumption underpins
incremental backups, rangefeeds and changefeeds, and numerous other systems.
Currently, however, System-Tenant run RESTORE of a secondary tenant does not
write the tenant's KVs with timestamps matching when it was restored, and
instead preserves the timestamps from the backup.

Preserving secondary tenant KV timestamps during RESTORE of a tenant is
unfortunate, but currently required for correctness: When the system tenant
conducts a backup of a secondary tenant, they are completely naive to what a
secondary [tenant’s key
span](https://github.com/cockroachdb/cockroach/blob/7dd1c51f6b5802e32bafd82e46747f349836592f/pkg/ccl/backupccl/backup_planning.go#L1597)
contains, meaning it could contain a tenant’s in-progress IMPORT. Consequently,
when restoring that backed up tenant, it is _forced_ to preserve the timestamps
from the backup, to allow the restored tenant to properly roll back partially
imported data, using the inspection of its original timestamps as described
above. If a restored tenant could rollback their restored, partial IMPORT
without depending on keys which preexisted the import having their original
timestamps, restoration of a tenant could write the tenant's kv's at the time at
which they were restored.


### Restoring IMPORTing Tables during Cluster, Database, and Table RESTORE

In contrast to restores of entire tenants as described above, restores and other
writes run _by_ tenants do not have the option of choosing to write keys at
their original timestamps. All of a tenant's writes, including when a system
tenant writes to its _own_ keyspace, _must_ be done with timestamps matching
when they were written, so that an observer of that tenant, such as an
incremental backup or replication stream, can detect written keys correctly.
Thus, restores of tables, databases, and clusters -- so all restores except
those of entire tenants -- are unable to write keys at a timestamp other than
when they are restored and are thus forced to sidestep the “cannot rollback a
restored partial IMPORT” issue with an even more drastic measure: by omitting
offline (e.g. importing) tables from the RESTORE entirely. In other words, an
importing table can never appear in a restored database or cluster, only in a
restored tenant due to the special-case above.

This however is a violation of the implicit contract a user expects of a cluster
level BACKUP and RESTORE i.e. that after restoring a cluster backup, made as of
system time X, the restored cluster will have all data that was committed to the
backed up cluster as of time X. Put another way, the restored cluster should be
a drop-in replacement for the backed up cluster. By omitting the committed data
from an offline table, RESTORE is violating this contract; while the table was
not available in the backed up cluster at time X, in the backed up cluster it
still existed in a form where it could become available again -- by canceling
and rolling back or by completing the IMPORT. By not restoring _any data _from
the importing table at all, including data that existed before the import, a
cluster restore could easily be accused of losing data. To avoid this, RESTORE
needs to be able to restore importing tables, even when it is constrained to
only writing at the current timestamp.

Thus, both to allow restoring tenant spans — which could contain IMPORTing
tables -- using MVCC writes and to allow restoring IMPORTing tables during
cluster restores, IMPORT rollbacks can no longer depend on the original
timestamp, assigned during the import in the backed up cluster, being the
timestamp on a restored key.


## High Level Design

To be able to roll back the keys added to a table that has been restored to a
point mid-IMPORT, even if RESTORE updated all restored key timestamps to the
time at which they were restored, the rollback must stop depending on the key
timestamps to identify keys which were added by a given IMPORT.

**Instead, some other IMPORT-identifying information must be associated with
each KV added to every index by an import job.**

Furthermore, as the amount of data in the table, including existing data _not_
added by the IMPORT, can be very large, ideally this identification should be
able to be done cheaply and as close to the deletion of the new data as possible
(i.e. it should be able to be "pushed down" into the deletion/revert operation).
To be done more cheaply, it could potentially benefit from more sophisticated
acceleration beyond that, such as via SST block-properties.

The KV API offers a ranged deletion operation to delete all keys in a span.
Adding an optional predicate to this operation would allow a caller to express
their desire to delete all keys in this span _that match this criteria_. This
would allow pushing down the filtering of KVs written by the job so long as
"written by the job" is an expressible criteria to this API.

Suppose each key-value written by an import contained metadata indicating the
import job's ID, and suppose that a cluster restore, while rewriting these KV’s
timestamps, would preserve this metadata, including the import job ID. Then,
during any import rollback, a scan of the importing table’s KVs which deletes
those keys that contain the matching job ID of the rolling back import would
correctly rollback the partial import.


## Technical Design

The proposal starts from the bottom of the stack – a modification to low-level
roachpb KV metadata–  and works up to IMPORT calls in the SQL Layer.


### KV

Consider a new field to `enginepb.MVCCValueHeader,` `clientMeta `which holds the
import job ID that created the key-value. I.e.: 
```` message ClientMeta {

int64 import_job_id = 2 [(gogoproto.customtype) = "JobID"]; 
}
````

The MVCC value writer would:

* Call `storage.MVCCPut(...enginepb.ClientMeta clientMeta), `which will contain
  an additional field the client can use to pass the client defined metadata.

* `mvccPutInternal() `will add this metadata to the `storage.MVCCValue.`

* `writer.PutMVCC() `will then call the existing `EncodeMVCCValue() `to encode
  the value and header into raw bytes for SST ingestion. (Note: IMPORT code
  willuse `writer.PutMVCC() `instead of the higher level `storage.MVCCPut().
  `See the appendix for a discussion on a potential regression in the
  writer.PutMVCC() call that the implementation would require.)

The MVCC value reader would:

* Iterate over a span of data using a `SimpleMVCCIterator, `and call the
  existing `DecodeMVCCValue() `to work with the full `storage.MVCCValue`, which
  includes `clientMeta`.

* Call a new `MVCCValue.GetImportJobId() `method to get the import job id from
  an instantiated MVCCValue

The KV Server code that reads and writes MVCC values on behalf of IMPORT will
have to learn about this new `MVCCValueHeader `field. Currently, IMPORT sends
write requests to KV via `roachpb.AddSSTableRequest, `and reads and deletes
importing data from an existing table via `roachpb.RevertRangeRequest. `As a
part of the [MVCC Bulk ops
migration](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210825_mvcc_bulk_ops.md),
the revert range call will be replaced with the new [roachpb.DeleteRangeRequest
with Time Bound Iteration](https://github.com/cockroachdb/cockroach/pull/83676)
Furthermore, to continue an IMPORT on a restored cluster, the pre-restore writes
via <code>roachpb.AddSSTableRequest</code>will need to be modified, and to
rollback the restored IMPORT, the new DeleteRange request will require an
additional <code>importJobId </code>parameter.


#### DelRange

Import Rollbacks aside, at a high level, the new DeleteRange will look quite
similar to RevertRange. Because of time bound iteration in Pebble, the iterator
can scan the LSM quickly: it skips blocks within an SST by checking the
timestamp interval in the block cache and can also skip scanning lower LSM
levels outside of the time bonds. For Import Rollbacks specifically, the KV
client passes the importing table’s key span to DeleteRange, along with the
start time of the import. DeleteRange then issues point or range tombstones to
all keys in the span above this start time.

To accommodate import rollbacks without MVCC timestamps, DeleteRange will need
to delete keys in the table’s span by filtering on the IMPORT Job ID instead.
There are three ways to implement this:

1. Naive: Pass the job ID `DeleteRange` and provide no timestamp bounds to the
   iterator. Pebble would then have to iterate over _every_ key in the rolling
   back table and could not use TBI, a clear regression. As the iterator
   surfaced keys, `DeleteRange` would then filter keys on the jobID, and issue
   point and range deletions.

2. Slightly Better: Pass the job ID _and_ timestamp bounds to `DeleteRange` :
   Suppose the client could pass correct timestamp bounds for fast scanning,
   what bounds would it pass? **Current approach**

   1. For Import Rollbacks on the same cluster the user began the IMPORT: the KV client can continue 
      using the same bounds: [import job start time, now] and the server would
      implement TBI using an `MVCCIncrementalIterator`.
   
   2. Import Rollbacks after a RESTORE: no TBI should be passed, as timestamps
      from Imported vs non imported data are now interleaved.

3. Fastest, but potentially not worth the effort: use approach 2 and teach
   Pebble about JobIDs. Suppose Pebble stored metadata on the SST or block level
   around which IMPORT jobs the keys in that SST/block came from, then Pebble
   could skip SSTs/blocks that didn’t contain that jobID.  The current plan is
   to implement and benchmark approach 2, as leveraging pebble block properties
   requires significantly more implementation work. See google doc thread on
   more commentary
   [here](https://docs.google.com/document/d/16TbkFznqbsu3mialSw6o1sxOEn1pKhhJ9HTxNdw0-WE/edit?disco=AAAAbcOzcCY).

Given these approaches, one simple approach to modifying the new
`roachpb.DeleteRangeRequest` API would be to add a general `predicateFilter
`struct, where the caller can specify the `rollback_import_id` field and the
`start_time` for TBI. All other fields could remain the same.


#### AddSSTable

When the KV Server receives an `AddSSTableRequest`, the client has already
written a fully encoded SST to memory. For IMPORT, this implies the
`import_job_id `metadata has already been encoded into each key. Nevertheless,
it’s still necessary to pass the `import_job_id `to `AddSSTableRequest` in order
to correctly run `checkSSTConflicts. `Recall that after node shutdowns or a job
pause, IMPORT may rewrite a key due to imprecise job checkpointing.
Consequently, `checkSSTConflicts `currently allows imports to rewrite a key
(i.e. a key collision) if the timestamp is _above the time_ an import started,
via the `disallowShadowingBelow `flag.` `If an import resumes on a restored
cluster; however, importing keys may have lower timestamps than existing keys,
so there isn’t a clean import start time that would guarantee checkSSTConflict
correctness. Instead, checkSSTConflicts will need to allow an IMPORT to add an
SST iff the colliding key(s) were also imported in the same job.


### KV/Bulk Package

During IMPORT ingestion on the kv client side, the `SSTBatcher` writes an in
memory SST that it periodically flushes to KV via AddSSTable requests. To write
the new `roachpb.Value.Metadata,`the job ID can be passed to 
`sstWriter.AddMVCCKey(),`and consequently `sstWriter.EncodeMVCCValue().`


### SQL Code

Currently, when the import job coordinator determines it needs to rollback a
partial import into an existing table, it calls sql.RevertTables(). Instead, it
will call a new sql/importer.DeleteImportedKVs(), which will be similar to
sql.RevertTables but instead of using RevertRange requests, it will issue the
new DeleteRange requests, described above.

## Alternatives Considered


### Add a hidden column to importing tables

IMPORT could add a hidden column, e.g. `crdb_import_source,` to the target table
and set the value to the import jobID. Import rollback is then a simple internal
executor call: `DELETE table WHERE crdb_import_source=job.id. `Rollbacks with
this approach would be slower than the approach above since this rollback would
work through the conventional sql to kv to storage path, instead of a bulk fast
path. Specifically, this hidden column would not have an index on it; therefore,
the bulk delete would require one full table scan (See
[docs](https://www.cockroachlabs.com/docs/stable/bulk-delete-data.html)).


### Make Importing Rows look like Intents / Uncommitted Rows

Suppose written values from an in-progress import looked like intents and were
committed at import completion. Committing each individual imported key is
prohibitively expensive and implementing an efficient bulk commit protocol is
hard. Further, recall that we want importing data included in backups, but
backups currently omit uncommitted KVs. It’s unclear how we could teach backup
to include uncommitted values exclusively from importing tables.


## Appendix


##### Replacing writer.Put() with writer.PutMVCC() during IMPORT ingestion

Currently, IMPORT sends keys to pebble via via `sst_writer.Put(key MVCCKey,
value []byte)`, where the value can be interpreted as
`roachpb.Value.RawBytes()`.

To implement this RFC however, we'll need to construct the MVCC value with its
new ClientMeta value, and instead call` sst_writer.PutMVCC(key storage.MVCCKey,
value storage.MVCCValue)`, introducing a potential regression. Benchmarking the
import/tpcc/warehouses=1000/nodes=4 roachtest with and without this
[diff,](https://github.com/cockroachdb/cockroach/commit/113929e3911fb2347f531bd183dbe6d12f2eed43)
reveals that this extra encoding round trip will not significantly affect
IMPORT’s run time. Over 3 trials, the control took an average 1313 seconds with
15 seconds of standard deviation, while the treatment took an average of 1345
seconds with 60 seconds of standard deviation.
