- Feature Name: Recovery from replica corruption
- Status: in-progress
- Start Date: 2021-07-05
- Authors: Bilal Akhtar
- RFC PR: TODO
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/67568

# Summary

This RFC proposes an sstable corruption reporting path from Pebble to Cockroach,
a mechanism to handle it in pebble compactions, as well as KV code to map
reports of corruption from a store onto replicas and to delete and GC them. 

# Motivation

Audience: PMs, end-users, CockroachDB team members.

The bulk of CockroachDB data is stored on disk in SSTable files. SSTable
corruption is something that's routinely observed in the wild; see reports under
issue #57934. Upon investigation, almost all of these issues have turned out
to be instances of hardware corruption as opposed to an indication of a bug
in Cockroach/Pebble.

Due to the design of Pebble's SSTable readers and iterators, it's
unlikely for a corrupt sstable to actually affect correctness in CockroachDB;
when a corrupt sstable is encountered as part of a foreground (user) read,
the Cockroach process panics and crashes. The operator is then expected to
clean out the data directory and bring the node back as a clean node.

While this is okay from a correctness perspective, it is still something that
requires manual operator intervention and can unnecessarily excessive in
disruption for the cluster (eg. entire node being taken down for a bit flip
in an sstable somewhere). We can do better; we could just delete the replicas
corresponding to those sstables (or better yet, specific blocks of data within
an sstable), and update our compaction code to better account for
compactions into corrupt files. The replica deletion would then be followed by
up-replication from other (hopefully not corrupt) replicas. This way, we can
contain the fallout of a corrupt sstable while preserving normal operation of
non-corrupt replicas on the affected node(s).

# Technical design

## Pebble parts

The FileMetadata in Pebble will be updated to include an `IsCorrupt bool` and a
`CorruptSpans` set of spans. The former will be atomically flipped to true
by the first reader encountering corruption in Pebble. The first reader to
flip it to true will also start a full-file-scan in a separate goroutine that
will find all corrupt blocks in the sstable, and add corresponding corrupt
span(s) of keys to CorruptSpans. That goroutine will call a new
`CorruptionDetected` event on Pebble's `EventListener`, once for each corrupt
span. Cockroach will override that method on the pebble EventListener and
do its share of work (see CockroachDB parts below).

Compactions will be updated to support compactions of a known corrupt file (with
`IsCorrupt == true` and a non-empty `CorruptSpans`) with a newer file from a
higher level that contains range tombstones that delete the entirety of those
`CorruptSpans`. This is necessary as Cockroach will be expected to garbage
collect replicas corresponding to reported instances of corruption, and those
GC requests will come into pebble in the form of range deletion tombstones.
Compactions of any keys other than point tombstones into a `CorruptSpan` will
continue erroring out in the background.

Pebble could choose to not delete an obsolete corrupt file in
`deleteObsoleteFiles` after the compaction is complete. Instead, it could move
it to a `corrupt` subdirectory in the store directory. This would reduce the
chances of a corrupt block/sector of disk getting re-used by Pebble further
down the line, and would effectively quarantine that file for Pebble's purposes.

Non-compaction read paths will be audited to ensure that all outbound corruption
errors are tagged with `base.ErrCorruption`, and none of them throw a panic
within Pebble.

## CockroachDB / KV parts

Every Store will instantiate a `CorruptionManager` object that contains these
fields:

```
type CorruptionManager struct {
   ds *DistSender // for instantiating a kvcoord.RangeIterator
   mu {
      syncutil.Mutex
      corruptReplicas []ReplicaID // For best-effort deduplication of calls to
                                  // rq.changeReplicas
   }
   rq *replicateQueue
}
```

This CorruptionManager will be responsible for reacting to instances of
corruption being reported by Pebble's EventListener. On an instance of corruption
being reported:

1) It would instantiate a new `kvcoord.RangeIterator` to read range descriptors
   (will this have the right consistency guarantees?) corresponding to the
   corrupt span(s), and use its own store's `GetReplicaIfExists` to get the
   `*Replica`.
2) It will grab the mutex and add these replica IDs to `corruptReplicas`, taking
   de-duplication into account. Any replica IDs that already existed in the
   slice will be dropped; some other thread is handling / has already
   handled this. The mutex will then be dropped.
3) Call `rq.processOneChange` with the replicas that were not duplicates. 
4) As part of processOneChange, the allocator will be called. The allocator
   needs to know what replicas are corrupt. A `corruptReplicas` method in
   storePool similar to `decommissioningReplicas` will look at stores'
   CorruptionManagers and return a slice of corrupt replicas. For these
   replicas, the Allocator will return `AllocateRemoveVoter` or
   `AllocatorRemoveNonVoter`. (Do we need a special type and special-cased
   handing in `Allocator.computeAction` such as what we have for
   `AllocateRemove{Dead,Decommissioning}Voter`?)
5) The replicateQueue will remove these replicas, and the GC queue will issue
   range deletion tombstones for those replicas, which will then get compacted
   into the corrupt sstables, deleting them.

## Drawbacks

One drawback is that a lot of instances of corruption being found and reported
across the cluster at the same time could put the cluster in a state
with many underreplicated or unavailable ranges. However the likelihood of this
happening would not be any higher than under the pre-21.2 status quo, where
we just crash the node upon seeing a corrupt sstable. If anything, it will be
lower than that likelihood as the entirety of the node would not be crashing.

Another drawback is in the handling of compactions into corrupt sstables; we
would only support compactions where a range deletion or a set of fragmented
range deletion tombstones delete the entirety of a corrupt span. We would
continue to error out on compactions of point keys into a corrupt sstable/block.
This could potentially cause a churn in background errors in pebble, as multiple
compactions would get kicked off, only to face an error during execution. A
mechanism could be added to detect and pre-empt these compactions early, but
it's unclear if that will be necessary.

On the same note, there's a possibility that the compaction we want to
prioritize (of the replica GC range tombstone into the corrupt sstable) could
end up being starved out by compactions of intermediate point keys into the
corrupt sstable(s). This should be unlikely as sstables with range tombstones
are already prioritized for compactions, but it's something that would need
to be closely observed.

Finally, this is not an airtight design that tries to reduce inflight writes on
corrupt replicas; it does not impose any write locks or write
stalls on corrupt replicas. It relies heavily on asynchronous cleanup to remove
away corrupt replicas and sstables. This reduces the amount of special casing
necessary to make it all work, at the expense of potentially accepting then
throwing away a bunch of writes on corrupt replicas.

## Rationale and Alternatives

One alternative is to have the store `CorruptionManager` also institute a
write stall on corrupt replicas and stop all future write operations on those
replicas. This stall could be implemented in the MVCC layer. However the
benefit of this extra handlifting is less clear.

Another alternative implementation is to support arbitraty compactions into
corrupt sstables, while maintaining `CorruptSpans` in the new file. However
this would necessitate serializing CorruptSpans into the Manifest or sstable
properties so future restarts of Pebble do not lose those.

# Unresolved questions

1) How long will it take for the average instance of corruption to go from
   first discovery in pebble to replica GC tombstone compaction?
2) Is there a better way to delete corrupt replicas than teaching the allocator
   to advise the deletion?
3) Is there a possibility that the compaction of the sstable containing
   the corruption range deletion could get starved out by intermediate
   compactions of point-writes into the corrupt file (see Drawbacks for a more
   detailed explanation)?
