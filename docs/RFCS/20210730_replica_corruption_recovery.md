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
requires manual operator intervention and can unnecessarily cause
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
by the first reader encountering corruption in Pebble. If a Pebble option,
`CrashOnCorruption` is true, then we will just panic the process at this point instead
of proceeding further.
The first reader to flip `IsCorrupt` to bool will also start a full-file-scan in
a separate goroutine that  will find all corrupt blocks in the sstable, and add
corresponding corrupt span(s) of keys to CorruptSpans. Neither the `IsCorrupt`
nor the `CorruptSpans` fields will be persisted to disk; these will only live in
memory. This is okay as the first reader encountering the corruption after a
restart can just repeat this process. The goroutine populating `CorruptSpans`
will call a new`CorruptionDetected` event on Pebble's `EventListener`, once for
each corrupt span. Cockroach will override that method on the pebble
EventListener and do its share of work (see CockroachDB parts below).

Compactions will be updated to support compactions of a known corrupt file (with
`IsCorrupt == true` and a non-empty `CorruptSpans`) with a newer file from a
higher level that contains range tombstones that delete the entirety of those
`CorruptSpans`. This is necessary as Cockroach will be expected to garbage
collect replicas corresponding to reported instances of corruption, and those
GC requests will come into pebble in the form of range deletion tombstones.
Compactions of any keys other than range tombstones into a `CorruptSpan` will
continue erroring out in the background.

To reduce the chances of starvation with a failing compaction being repeatedly
scheduled into a corrupt file, the `FileMetadata` will also store a
`HighestFailedSeqNum` field (also not persisted to disk), that will be bumped up
when a compaction into it fails. That field will be set to the highest seqnum of
chosen input files from that compaction. The compaction picker will explicitly
exclude compactions containing either an `IsCorrupt` file as a seed file, or an
output level file where `HighestFailedSeqNum` is greater than or equal to the
highest seqnum of input files. Eventually, when the range deletion deleting this
file lands in the level above it, it will get compacted into this file and will
delete the corrupt blocks entirely.

As the range tombstone deleting the corrupt span could be fragmented, we would
have to ensure the compaction doesn't error out if all of a sequence
of range tombstones delete a corrupt file/span.

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
   gossip *gossip.Gossip
   rq     *replicateQueue
}
```

For simplicity sake, we're using `ReplicaID` to denote the tuple of
`(roachpb.RangeID, roachpb.ReplicaID)` to uniquely identify a replica in a
cluster.

This CorruptionManager will be responsible for reacting to instances of
corruption being reported by Pebble's EventListener. At instantiation time,
the CorruptionManager will register a gossip callback on one of its own methods
for any updates on a new gossip key prefix for corrupt replicas. That callback
will grab the mutex and add that replica to `mu.corruptReplicas`.

On an instance of corruption being reported:

1) It would bump up a new metric gauge counting corruption events.
2) It would instantiate a new `kvcoord.RangeIterator` to read range descriptors
   (will this have the right consistency guarantees?) corresponding to the
   corrupt span(s), and use its own store's `GetReplicaIfExists` to get the
   `*Replica`.
   1) If a corrupt span overlaps with non-replicated (aka node-local keys),
      Cockroach will panic or crash with a fatal error. These keys cannot be
      recovered with replication.
   1) Range / replica IDs corresponding to corrupt spans will be logged out with
      a log message `storage corruption reported in range rXX/XX`.
3) It will grab the mutex and add these replica IDs to `corruptReplicas`, taking
   de-duplication into account. Any replica IDs that already existed in the
   slice/map will be ignored; some other thread is handling / has already
   handled this. The mutex will then be dropped.
4) Beyond this point, no further reads should be sent to corrupt replicas, and
   the HLC timestamp when corruption was detected would get recorded in the
   corruption manager (as the "corruption start timestamp").
5) This replica could be the leaseholder.
   1) Get lease status using `replica.CurrentLeaseStatus`.
      If lease status is valid and not the current node, gossip on the corrupt
      replica gossip key prefix with this `(rangeID, replicaID)`. This lease
      will no longer be in use beyond this time. As part of gossip callback,
      stores on all nodes with a replica for `rangeID` will add `replicaID` to
      `CorruptReplicas`, and will check if they have `(rangeID, replicaID)` and if they have a
      lease on it using `replica.CurrentLeaseStatus`. If true, that `CorruptionManager`
      will call `replicateQueue.processOneChange` with that replica.
   2) If current node is the leaseholder, the lease would be forcibly ended at
      the corruption start timestamp. The corrupt replica would also be gossiped
      in the same way as above. Other nodes can then request a lease 
      at any timestamp equal to or greater than that. The node that gets the
      lease would then call `replicateQueue.processOneChange`, the same as
      above.
6) As part of processOneChange, the allocator will be called. The allocator
   needs to know what replicas are corrupt. A `corruptReplicas` method in
   storePool similar to `decommissioningReplicas` will look at stores'
   CorruptionManagers and return a slice of corrupt replicas. For these
   replicas, the Allocator will return `AllocateRemoveVoter` or
   `AllocatorRemoveNonVoter`. (Do we need a special type and special-cased
   handing in `Allocator.computeAction` such as what we have for
   `AllocateRemove{Dead,Decommissioning}Voter`?)
7) The replicateQueue will remove these replicas, and the GC queue will issue
   range deletion tombstones for those replicas, which will then get compacted
   into the corrupt sstables, deleting them. `mustUseClearRange = true` must
   be propagated in `destroyRaftMuLocked`.
8) After a corrupt replica is GC'd, its gossip key can be removed and the 
   corresponding entries in `CorruptReplicas` can be removed. 

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

Another alternative implementation is to support arbitrary compactions into
corrupt sstables, while maintaining `CorruptSpans` in the new file. However
this would necessitate serializing CorruptSpans into the Manifest or sstable
properties so future restarts of Pebble do not lose those.

One alternative on the Pebble side is to not use range tombstones altogether,
as having them interact correctly with corrupt spans in compactions is going to
require a lot of updates to read-time / iterator logic. Range tombstones can be
arbitrarily truncated due to compaction output splitting, necessitating logic
to detect adjacent "bridged" range tombstones and correctly skip over
corruption errors from corrupt blocks underneath them. Also, current
`compactionIter` logic would cause us to read a corrupt block theoretically
shadowed by range tombstones, because the compactionIter wants to read all keys
and individually check if they're deleted by tombstones in the fragmenter.
This logic would also need to be special-cased around corrupt spans.

Instead of special-casing range tombstones around corrupt spans, the alternative
would be to not use range tombstones for this purpose but to have a
set of "corrupt spans" (which for simplicity could map to entire file bounds),
which would be tracked independently, and upon replica GC, Cockroach could
call into a special method, `ClearCorruptSpan` to force a deletion of the
corrupt file in one version edit, and to lay down a range tombstone over that
entire span to catch any undeleted keys in that span in other files. This
simplification would hinge heavily on the assumption that no open snapshots
would read from the corrupt span - an assumption that Cockroach would be
able to satisfy as snapshots are only opened for up-replication and a
corrupt replica would stop replicating.

# Unresolved questions

1) How long will it take for the average instance of corruption to go from
   first discovery in pebble to replica GC tombstone compaction?
2) Is there a better way to delete corrupt replicas than teaching the allocator
   to advise the deletion?
3) Is there a possibility that the compaction of the sstable containing
   the corruption range deletion could get starved out by intermediate
   compactions of point-writes into the corrupt file (see Drawbacks for a more
   detailed explanation)?
4) Should `CorruptReplicas` be added to the liveness record? This would mimic
   node decommissioning in how it'll interact with the allocator.
