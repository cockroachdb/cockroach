- Feature Name: Migrating Bulk Operations be MVCC
- Status: accepted
- Start Date: 2021-08-02
- Authors: David Taylor, Andrew Werner, Tobias Grieger
- RFC PR: #69380
- Cockroach Issue: 
  https://github.com/cockroachdb/cockroach/issues/62585
  https://github.com/cockroachdb/cockroach/issues/62564
  

# Summary

This document describes a broad shift to make *all* SQL interactions with data
stored in the KV layer, particularly those done by bulk SQL operations like
dropping a table, restoring or importing a table, or building a new index,
operate solely by appending to the KV layer's MVCC history rather than by
mutating or destroying it as they sometimes do today. It describes at a high
level the pieces required for this shift and how they fit together to get there,
while leaving the actual detailed design specification of each piece to
subsequent separate design documents.

# Motivation

Having a preserved, append-only MVCC history for all user-data stored in a
cluster is assumed by many layers of the system, including incremental backups
or replication feeds. We currently cannot replicate a table on which bulk
operations are taking place between clusters or backup a tenant incrementally
if they may have run an index backfill because those operations mutate the 
MVCC history and make it unreliable. 

# Background 

Currently _almost_ all mutations to SQL rows are done via MVCC writes -- Puts,
Dels, etc -- that leave a historical record, which we rely on to implement
transactional behaviors and capturing changes, e.g. in incremental backups and
forthcoming cluster-to-cluster replication.

However, there are exceptions to this rule, namely when bulk deleting or
bulk-adding data, which use _non-_MVCC operations to add or remove large amounts
of data at once. 

These exceptions were originally deemed to be safe to use in these cases despite
violating assumptions made elsewhere, because we determined we could reason
externally that we knew there were no other readers or writers who were
expecting traditional MVCC to hold interacting with those key spans at that time
e.g. we could use ClearRange to delete the rows of a dropped table only after
waiting until SQL was no longer reading the dropped table span, or use
AddSSTable to add the rows of an IMPORT'ed table but before it was visible to
SQL traffic.

However, this reasoning had or later developed some flaws. For example
incremental backups of entire clusters tried to backup even tables that are
still importing or building new indexes, and cluster-to-cluster replication
wants to observe every change to every key. Thus the reasoning that unsafe,
non-mvcc operations were okay because nobody would observe them no longer holds
(and in some cases never did, leading to bugs). One example of where this goes
wrong can occur if AddSSTable rewrites history: Consider a tenant building a new
index, and their index builder that, at 3:01, adds an SSTable containing keys
dated 2:59, when it started building that index. However we the host cluster
tries to backup that tenant _incrementally_ from 3:00 to 4:00, it would naively
_exclude_ these keys, since they appear to have been added at 2:59 and it would
thus assume we previously already backed up, at 3:00pm, when in reality they
were not actually there at 3:00pm and were added since then. This would result
in missing data from incremental backups of tenants, and is why we currently
cannot backup tenants incrementally. Similar issues arise with other uses of
AddSSTable, as well as with ClearRange, and affect cluster-to-cluster
replication as well as backups, while past issues in this area have also
resulted in data loss or corruption bugs, such as when we split intents from
their rows with back-dated SSTable keys.

The underlying issue in all these is that MVCC history was changed, in an
unexpected way, by these operations. One solution could be to record these
operations separately and extend any code observing changes to look for them
too, leaving the operations themselves still non-MVCC.  For example, in the
above Backfill vs BACKUP case, we could note in some separate metadata that at
3:01, that we added keys dated to 2:59, and then force anyone looking for
changes since 3:00 to instead look back further or otherwise find these
mutations.

However a potentially more appealing solution is to make these operations
_actually MVCC _operations, and not need to continue to try to reason about
where it is okay or not okay to use less safe operations with the potential to
get such reasoning disastrously wrong. 


## Current Non-MVCC Operations

The currently used non-MVCC operations are ClearRange, RevertRange, and AddSSTable.

**ClearRange**

ClearRange destroys all MVCC revisions of all keys in a span.

When dropping a table or index, we _could_ use the MVCC operation `DelRange`
which writes an MVCC tombstone over each key in the supplied range. However that
is potentially _extremely_ expensive, in both time and disk space usage, as it
writes a new tombstone over each key it deletes in that span. Thus it could
require writing billions of new keys to drop a large table. Instead today we use
the operation ClearRange which writes a RangeDeletion tombstone to
Pebble/RocksDB, which can mark a whole span as deleted with a single write.
However these are non-MVCC deletes that just delete all keys in their range,
i.e. all MVCC-revisions of all SQL-written keys. To use this safely, we
currently wait until the TTL for the dropped span has elapsed, so we can assume
even historical SQL reads are now longer reading it or would notice its
destruction, and then clear it, but this leaves no record of what was deleted or
when, compared to a regular deletion.

**RevertRange**

RevertRange destroys all revisions newer than a target time in a span. Longer
runs of adjacent revisions of keys that all match the predicate are destroyed
using ClearRange while individual keys or shorter runs are destroyed with
individual Clears (single-key storage tombstones). Since it uses
Clear/ClearRange to destroy revisions, like ClearRange it leaves no MVCC record
of what it did.

If an IMPORT INTO fails, we run RevertRange to find and delete any keys added by
the IMPORT. It does this by iterating the span and comparing each key to the
target time and removing those which are newer, clearing individual keys and
using ClearRange on runs of matching keys. In the IMPORT case, it is always
removing a _new_ key, that had no prior mvcc revisions, where a Delete would
have the same effect. We also use RevertRange to roll a span of
cluster-to-cluster replicated keys back to a consistent target time (which
relies on Clear'ing later revisions to unearth the earlier ones).

**AddSSTable**

AddSSTable allows sending a file of key-value pairs, which already have MVCC
timestamps assigned to each key, to be written en masse, with little to no
per-key overhead. Since the keys in the SSTable have timestamps chosen when the
file is constructed, not when it is added, adding such files can "rewrite"
history i.e. put keys at historical times after the fact. Currently RESTORE'd
data keeps the timestamps that were on the keys when they were backed up, while
IMPORT picks a specific time after it starts. Index Backfills rely on the
back-dating of the keys in added files to ensure they are shadowed by any SQL
traffic that writes after the scan used to read the rows from which the index
keys are derived -- indeed, it currently writes all the keys at one time at
which it scanned the target index to avoid ever colliding with a SQL-written
intent.

# Technical Design

To make all SQL interactions with stored data, including bulk interactions, MVCC
appends, we broadly need two new major additions to the KV API: the ability to
MVCC-Delete a large range of data cheaply and the ability to cheaply ingest a
large amount of pre-generated data (i.e. an SSTable) while appearing to write it
at the current time. 

In addition to these two major building blocks, we will need a new method to find
and MVCC delete potentially large amounts of data cheaply based on its timestamp
as currently done in a non-MVCC way by RevertRange.

Additionally, the SQL layer will need to fundamentally change how it backfills 
indexes, to no longer rely on the ability to back-date writes based on stale
reads as a means of ensuring correctness.

Each of these major pieces and how they replace an existing non-MVCC operations
is discussed in more detail below. Note however that the below discussions are
*not* intended to specify the actual details of the design and implementation of
each piece: it is expected that most or all of these pieces will each have a
stand-alone RFC specifying their design and implementation. Insofar as the
sections below describe implementation details, it is only intended to
demonstrate that it is indeed a feasible component to include in this higher
level plan.

## MVCC RangeDeletion DelRange replaces ClearRange

We use ClearRange because it offers deletion of many keys with &lt;O(n) writes.
For dropping larger tables this justifies the fact that to use it, we need to do
so in a somewhat weird, round-about way of making a job that waits out the TTL
and then clears it, unlike a normal delete where we delete when we want to
delete and leave GCing at the TTL to the GC.

If instead `DelRange` could write an MVCC-RangeTombstone, that indicated a span
was deleted as of some time which was reflected during reads, this could be
avoided. A DROP table could simply DelRange the table span and let regular GC
handle cleaning it up. This would also mean that in the meantime, the data would
actually appear as deleted in KV w.r.t stats, scans, etc. For example, a backup
of a tenant span today contains their dropped but not yet GC'ed tables, because
the keys themselves are not yet deleted and are in the backed up span, even
though from SQL's point of view they are just as deleted as a deleted row, which
would not be backed up. When we restore this span, we'd dutifully restore the
keys even for the dropped table, just to turn around and notice the TTL has
elapsed and actually delete them. Even more importantly, an incremental scan of
what happened or is happening in that span will not see any ClearRanges, so if
that tenant were replicated with cluster-to-cluster replication, the replica
would never clean up GC'ed dropped tables, unless it ran its own jobs. 

A fast sub-O(n)-writing MVCC range delete has other uses too, including rolling
back spans of imported keys when reverting a partial IMPORT INTO as described
below, or even potential SQL users, like fast bulk-deletes of large spans of
rows, like DROP PARTITION or row-level TTL expirations when using date-prefixed
indexes, though these are out of scope for now.


## DelRange Replaces RevertRange

By adding a lower-bound timestamp predicate similar to RevertRange's to the
`DelRange` RPC, it could be directed to optionally delete only key matching that
predicate during its iteration. With the addition of such a predicate, DelRange
could indeed be used for rolling back IMPORT _today_ instead of RevertRange,
because IMPORT INTO only writes new keys, so a Delete of those keys is the same
as a revert to their pre-IMPORT value. 

However, to be able to actually replace RevertRange, it would likely depend on
utilizing the same optimization RevertRange uses to avoid O(n) writes on large
spans of matching keys, which is to accumulate runs of matching keys and use a
single ClearRange to clear them all, except DelRange would need to use the new
MVCC RangeDeletion tombstone to MVCC delete the span, rather then clearing it
destructively. This optimization is likely critical to performance of rolling
back large IMPORTs, which is particularly sensitive since the entire table
remains unavailable until that rollback completes. Particularly if an IMPORT
fails or is aborted due to lack of space, needing to _write_ O(n) to roll it
back may be a hard blocker. 

Additionally, DelRange with a timestamp predicate would want to use TBI just as
RevertRange does to find matching keys quickly, again as performance is
important.

With IMPORT INTO now using DelRange instead of RevertRange, it would leave a
record of what it un-imported which could be backed up or replicated, so that
backed up or replicated imported data is similarly unimported in that backup or
replica.
 
RevertRange would be left as is, using its history-destroying Clear and
ClearRange, but would now never be used by SQL or on spans that _anyone_ is
reading. Indeed, the only remaining user would be during replication ingestion,
where we could impose stronger guarantees that the keyspace in which it is doing
so is truly offline and unreadable to other users (see KV Lock/Unlock Appendix
3) than IMPORT INTO can impose (e.g. because IMPORT may be running in a database
or tenant that something else is trying to backup, replicate, etc).


## AddSSTable Writing at Current Timestamp

AddSSTable adds an SSTable that, today at least, has keys that include
timestamps that can be anything the caller chooses, including timestamps in the
past or even future, which will just be the MVCC timestamps of the keys in KV
after it is added. Typically, KV and its users assume that the MVCC timestamp on
a key is when it was last written or changed. That assumption is however
violated if AddSSTable adds keys that have very different timestamps from when
the SST was added.

This violation of the assumption that MVCC timestamps match addition time has
caused and is causing problems, primarily for incremental backups, but also
previously due to splitting intents from their rows and is now coming up in
capturing changes for cluster-to-cluster replication. For example, the issue
with incremental backups can manifest if a tenant runs a RESTORE, because
RESTORE writes its SST with keys that have MVCC timestamps taken from the backed
up rows. Say for example that a backed up row has an MVCC timestamp in February.
Ten minutes ago, a tenant RESTORE's that row into their key-space. When the host
cluster then attempts to run an incremental backup, to capture all keys that
were added or changed in the last hour since the last backup, it would **not**
backup these newly restored keys,since they claim to have been written in
February rather than in the last hour. Thus the backup of this new cluster is
missing data because that data was added by "under" its reads, aka "rewriting
history".

For the most part rewriting MVCC history below the closed timestamp or what has
been read according to the timestamp cache is already forbidden, and this is
then assumed by many parts of the system. One solution to the problems
described, when this assumption is violated primarily by AddSSTable, would be to
remove those violations. However there are both technical challenges with this
approach and potentially a question as to whether or not it is even desirable
due to the limitations on what user-visible behaviors it allows us to provide.

The most obvious user-visible implication may be that if rewriting history is
disallowed, so is _reproducing_ or copying a specific history -- e.g. you cannot
restore a specific history. Today, a RESTORE'd row looks the same -- including
its MVCC timestamp, as the backed up row. If rewriting history in the restoring
cluster were banned outright, a RESTORE'd row would need to instead need to have
its MVCC timestamp match the time at which it was restored. Similarly, if you
build a new index on a table, the keys in that new index must have MVCC
timestamps matching when the index was built, rather than the timestamps of the
rows from which those index keys were derived.

It is unclear if this is a major drawback in its own right: time-travel on
RESTORE'd data is already unsupported, and schema changes already update the
timestamps on rows to be close to when they rewrote them, so users _already_
cannot assume our internal timestamps are a reliable "last updated" by a SQL
client timestamp. Committing to no-rewriting semantics would however rule out
changing these user-visible behaviors without reworking bulk-ingestion.

However, there are two technical barriers to changing to a no-rewriting
AddSSTable. The first is a practical one: even if all users of AddSSTable
_wanted_ to write at the current time, how can they do so when building
SSTables, which involves writing keys to it that encode a timestamp, can start
_before it is added_, happens long before the time at which it is added? Second,
the current implementation of index backfills _relies_ on writing at (slightly)
historical timestamps to resolve a correctness issue introduced by its
non-transactional writes/stale reads. 

As mentioned above, a practical challenge with this is simply that SSTs take
non-zero time to construct, and thus if the timestamps on their keys are picked
as they are constructed, by the time the SST is sent in a batch, that time is
likely to be too far in the past to avoid rewriting now immutable history. 

There are a couple potential ways to resolve this: one could try to future-date
keys during construction to some specific time at which that SSTable will be
added, but this imposes a rigid timeline, that could waste time waiting or be
forced to start over if it missed its assigned time. Alternatively, or perhaps
in combination with such an approach, one could _reconstruct_ the SSTable, to
update the key timestamps once its addition-time is known. Rewriting O(n) keys
during addition (evalAddSSTable) is potentially extremely expensive however, so
instead it is proposed that the storage engine be extended to avoid this via
read-time synthesis of the correct timestamp.

### Pebble Read-Time Synthesis

To avoid the need to edit all of an SSTable's data to change the timestamp on
each key, callers can instead write their SSTs with a special placeholder
timestamp value on each key, and then during evaluation of the addition of such
a file, edit just its metadata to insert a property specifying the actual value
for those timestamps. This is then be used when _reading_ that file, to
synthesize the timestamps as if they had been written with that value, while
only asking evaluation to perform an O(1) metadata update.

One danger of this approach is any overhead this read-time rewrite adds would be
paid potentially over and over, every time these files are read, until they are
compacted, whereas an addition-time rewrite pays its cost just once. However it
appears that modeling this as a block transformation done only when a block is
loaded much like decompression, and then caching the result, mitigates this to a
large degree, with such a block-iterating-and-modifying transformation being
observed as incurring no more than 3% additional CPU overhead on a read-heavy
workload. Additionally, a compaction will materialize any keys it reads from one
of these ingested files, and thus produce a normal, no-transform-required file 
for future reads. Such a compaction may not happen soon e.g. on RESTORE-d data
that ingests directly to L6, but could be prioritized if this measured overhead 
was determined to be too large.

### Index Backfills 

Any design that includes removing support for rewriting history will require changes in how we run index backfills, or how we handle the SSTables they add.

Backfills today _rely_ on the ability to write at historical timestamps, specifically to write "under" any more recent SQL writes, to maintain correctness.

This is done because of the delay between when a backfill might read some rows, at say 2:58, from which it derives keys which it buffers up and assembles into an SSTable that it then eventually adds, at say 3:02. While it is doing that, a SQL UPDATE might come along and write a newer value to one of those rows, at say 3:00pm and will write the accordingly updated corresponding index key as well. However if the backfill's now stale index key were added _at 3:02, _it would incorrectly be overwriting the newer SQL-written value with its old value. Essentially, this is because the backfill is writing based on a read it did in the past, and not in the same transaction, and that read is now stale. To resolve this the backfill assigns a timestamp to its derived key that is at or before the time at which it read the row to derive that key. Thus when it adds that derived key, it would do so "under" (i.e. earlier than) any later SQL written index entry that might have been done as part of an update to that source row since it was read, ensuring that the newer SQL write "wins" the conflict for which write is visible.


Avoiding collisions entirely would require that once SQL is also writing to the index, any backfilling we do is done so using transactional batches. As doing so is probably much slower, we would likely do a non-transactional bulk "pre-backfill" of any new index _before_ SQL is instructed to start writing updates to it when mutating rows. During this pre-backfill, only AddSStable would be writing to the span, and would not need to worry about how to resolve collisions with other writes. After it is done, we would however need to backfill any rows that had changed while we were backfilling, i.e. we would need to do an _incremental_ backfill. The exact implementation of this could vary, including if or how it uses multiple bulk incremental passes to reduce the size of the final transactional batch pass, protected timestamps, a temporary index, etc. This approach has been suggested for other reasons several times over the last few years as it is expected to have more appealing contention behavior and reduced impact on SQL traffic compared to the current approach of bulk-adding to the same index while SQL writes updates to it, but requires significant new work to implement incremental backfills.

## Remaining Non-MVCC Operations

Cluster-to-cluster replication ingestion would like to continue to add keys with their original timestamps, so that it can later _revert_ to a specific timestamp, to remove any inconsistent suffix of the ingested keys. While this would mean these ingestions continue to rewrite history,  and thus bring the host of potential issues that doing so incurs, it is believed to be easier to prove that in this isolated case the ingestion target span is truly "offline" and not being accessed by other users unlike when restoring or importing  tables, and thus easier to prove that the unsafe operation is actually safe. For added proof of this, as with RevertRange discussed above, this invariant could be _enforced_ by KV as described in Appendix 3. 

## Alternatives Considered

### Separate MVCC and Mutation Histories

An alternative to forcing AddSSTable to always write at current time and never
rewrite history would instead be to _allow_ history to be rewritten and track
_when_ that was done.

Similar to the Read-time synthesis solution in the no-rewriting approaches
outlined above, an added SSTable would have the time at which it was added
recorded in its metadata, but unlike the above no-rewriting approach, when this
SST is read, this timestamp would _not_ be used to synthesize the MVCC timestamp
in each read key but instead those keys would retain whatever timestamp each was
assigned when the SST was constructed, while the extra addition-time timestamp
would be _appended_ to during reads to the returned KVs.

While an added SST would have only one addition-timestamp -- when it was added
-- when it is compacted with another file or files, the resulting file would
have an upper and lower bound addition time. These bounds would be stored,
similar to the mvcc ts bounds, for use by TBI. 

However, if the synthesized addition timestamp is added as a suffix or in the
values, then current mvcc reads would remain unchanged (though we would continue
to need to exercise care about not rewriting under reads could lead to
correctness violations, and our vulnerability to further mistakes in this area
would continue to require extra consideration). Only reads that specifically
were looking for what had changed since a time -- i.e. incremental scans like
those used for backups and replication -- would need to look for and use this
separate modification timestamp. While TBI would allow filtering to only files
which contained matching keys, any key in that file could match, so every
revision of a key would need to be checked, compared to today where once a
revision older than the desired span is seen, NextKey can seek over any
additional revisions.

Alternatively the addition time could be synthesized in the position of mvcc
timestamps and mvcc timestamps moved to the suffix or value position, but doing
so would require MVCC reads to potentially look past the first matching key to
see if there is a later -- in mvcc terms -- key after it. The addition of a rule
that a higher addition time implies a higher mvcc time, i.e. that history can be
rewritten but must be written older-to-newer -- could at least limit this
required look-ahead when a key has an assigned addition timestamp to just one
additional key, and only if the first seen key has a separate addition vs mvcc
timestamp.

This approach seems liable to continue to risk violating (incorrect) assumptions
about MVCC history being immutable, but maintains the ability to create, or
recreate, a specific MVCC history during RESTORE, backfills or any potential
internal operations that rewrite KVs. There are some arguments that we should
not forfeit the option to do so lightly. That said, if we banned rewriting now,
we could add support for a second timestamp/alternative history at some later
point when it became clear it was justified, though the work to make index
backfills correct without historical writes above could be wasted in that case
(though it was previously suggested for contention minimization reasons, so not
entirely wasted, and perhaps not wasted at all?).

This is explored in more detail previously in https://github.com/cockroachdb/cockroach/issues/62585.

### Index Backfills

#### Add-time Rewriting*

Alternatively, the current backfill approach that generates SSTs with keys that
may collide with SQL-written keys could use a different mechanism to resolve
those collisions in favor of the SQL-written keys, rather than back-dating its
keys. One naive solution could be to simply ask `evalAddSSTable` to iterate and
rewrite its SST, and while doing so, skip any keys that collide with an existing
key, as we can always assume any existing key would have been written by the
most recent SQL write and thus be authoritative. However this may likely impose
too high a cost on evalAddSStable to maintain bulk-writing throughput, as it
would make it an O(n) operation, including reading potentially O(n) existing,
on-disk keys.

#### Read-Time Skipping

Another option for resolving these conflicts, that is, for eliding any colliding
keys, could be to somehow mark the backfill's SST-ingested keys so that they can
be skipped during reads if, and only if, they collide with an earlier,
non-SST-ingested key. For example, one could imagine a special value of the
logical timestamp, or some bit in roachpb.Value, that indicated this "never
overwrites" property, and then code like `pebbleMVCCScanner.getAndAdvance()`
would need to do something like, in its first case, where we check
`p.curKey.Timestamp.LessEq(p.ts)` and then yield the value if it looks like it
matches, we would instead, if we see the indication that it is one of these
special keys, look at the _Next()_ key and if it is not one of these, yield it
instead. However this subtle and complex rule seems likely to be overlooked by 
any other raw readers of KV and seems fragile at best.

## Appendix 1: Maintenance Lock/Unlock KV RPC

Given how dangerous non-mvcc operations have shown to be when mixing with other
operations that still expect MVCC and transactional invariants to hold, we would
like to eliminate them entirely. However, in some limited use-cases where we can
determine they are actually safe and have no alternative, we may still need to
use them. For example, cluster-to-cluster stream ingestion wants to directly
mutate the MVCC history on the target cluster, to recreate the original logical
timestamps from the source cluster, so that it can then revert back partially
ingested data to a consistent source-cluster timestamp. 

We think it is safe for ingestion to use these unsafe methods because we assume
it is doing so only in truly "offline" keyspace, where no other operations are
not running that could observe it, similar assumptions made to use these
operations elsewhere were later violated with dire consequences. Thus we may
want to borrow a "lockout/tagout" approach from hazardous industrial settings,
where potential hazards are explicitly locked out before work near them can
begin.

One prior proposed solution to this is to require "minting" keyspace to allow
MVCC-expecting SQL traffic to use it. Prior to minting, keyspace would allow
arbitrary manipulation and rewriting directly with AddSSTable, RevertRange, and
ClearRange but reject other requests like Scans, and then once "minted" those
manipulation commands with such would fail and only regular MVCC access would be
allowed. This would work for the case of stream ingestion, however it would have
limited application to only the fully non-MVCC cases. Minted-vs-unminted does
not help with the general problem of wanting an incremental backup run mid-way
through an IMPORT or Backfill to capture what has been imported or backfilled so
far (making AddSSTable not rewrite history or tracking parallel addition history
does).

Additionally, the one-time transition from unminted to minted does not address a
separate but adjacent concern, which is delayed or duplicated application of
_non-transactional_ bulk operations, e.g. a DelRange sent as part of an IMPORT
INTO rollback arrives (or even may be sent by an orphaned processor) after the
revert has completed and the table has been brought back online to accept new
writes.

Instead, what we may want to add is a Lock/Unlock RPC that can transition spans
into and out of a "locked" maintenance state. When in the locked state, a range
would have an associated "lockID" that locked it. Any RPC, including the lock
RPC, could be sent with a lock ID, which defaults to empty, and must match the
current value of the range's LockID to be evaluated (including sending an empty
LockID if the range is not locked).

Thus we could lock a span, sending no current LockID to assert that it is now
locked with some new ID, then ingest into it with AddSSTables carrying the same
ID, then unlock it with that lockID. 

This would protect both against overlooked/accidental unsafe reading of a span
undergoing non-mvcc operations, and would solve the delayed/repeated application
problem at the same time (because a late-arriving DelRange carrying the old
LockID would be rejected by the now unlocked range).

This scheme could be extended to note the time at which the range was locked and
allow MVCC scans below that time, as long as that included only allowing the
unsafe mutations that were holding the lock to mutate only at or above the
locked time.

Such a system could help enforce that unsafe operations are used only in the
very narrow cases where we've convinced ourselves that they are actually safe,
because we would _know _other operations cannot observe them and their unsafe
behavior. Thus with most operations now "sane" w.r.t mvcc and the remaining
exceptions behind a protective cover, this could  "restore sanity" to the KV
API. 

NB: locking falls more into a "nice to have" guardrail, for a little extra
security and comfort, and not something that anything above relies on for
correctness (with the potential exception or replay/delayed application
protection where we currently may be relying on luck, though there is a proposal
to use a deadline/time-based protection scheme).
