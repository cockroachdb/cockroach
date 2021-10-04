- Feature Name: Incremental Index Backfills 
- Status: in-progress
- Start Date: 2021-10-04
- Authors: David Taylor, Rui Hu
- RFC PR: 71090
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/70426

# Summary

This document describes a change to the index creation process to avoid foreground SQL traffic writing to the new index while it is being backfilled via large, slow bulk writes. This is done by redirecting foreground writes for transaction run during the backfill to a separate temporary index, then copying from that temporary index to the new index after it is backfilled.

# Background

The current index backfill approach is inspired by the F1 process, and at a high level, is based on first having traffic update the new index, so new rows can be assumed to be correctly indexed, then scanning all existing rows and 'backfilling' them to the index. By following this sequence, one can assume that when the backfill is done, all existing rows are in the index, and since any new rows that were written mid-backfill were added to the index as well, all rows are now in the index. This avoids needing to block foreground traffic while the index is built, but it does mean that foreground traffic and backfill traffic are writing to the index at the same time. One downside of that fact is that backfill traffic is often optimized for throughput, rather than latency, and can thus adversely affect latency sensitive foreground traffic.

One particular way in which CockroachDB optimizes the bulk backfill is by reading a large block or blocks of rows and deriving a large buffer of index entries before batching those entries into files and sending them to the storage layer. By the time they arrive, it could be seconds, or minutes, since the read from which they were derived, and in that time the row from which they were derived could have been updated. On its own this is not a problem: the update to that row also wrote the updated value to the index. However, the backfill process now must take care not to overwrite that newer value with its older, now stale value. Today, it solves this by backdating the value it writes to be at least as old as the read from which it was derived, so that if anything updated the source row after it was read, and wrote some corresponding value to the index the same time, the backfill write to the index, which may flush later, will appear to have happened earlier than that "later" write, and thus become invisible. However, requiring this backdated write for correctness means that these backfill operations violate MVCC semantics — that is, that history, once read, is immutable, which interacts poorly with other readers, such as backups or replication.

Thus, both because of the desire to avoid backdated writes and, more generally, to avoid mixing large and bulk writes with small and ideally fast SQL transaction traffic, we want to switch to a new process for building indexes that does not require SQL transaction traffic writes to a backfilling index.


# Detailed Design

## Backfill Then Merge

When a new index is created, a second "temporary" index is also created. The temporary index steps through the usual states of delete-only, then delete-and-write-only, like new indexes do today.

A new "backfilling" state is added to the index descriptor states enumeration. Foreground SQL traffic write operations completely ignore indexes in this new state.

The new index is initially placed in the new "backfilling" state, while the temporary index steps through the delete-only and then delete-and-write-only states which new indexes step through today. When the temporary index is in delete-and-write-only on all nodes, and thus all new writes are assured to be reflected in it, the backfill of the new index, still in state "backfill", begins.

When the backfill completes, the state of the new index advances to delete-only, then delete-and-write-only. At this point, both the new and temporary index are in delete-and-write only, so ongoing SQL traffic will write to both. Thus any new writes will be reflected in the new index, and it contains all entries from rows that existed before the backfill. However, it is still missing entries for writes committed during the backfill. This is where the fact that the temporary index was added and moved to delete-and-write-only prior to the backfill reading is critical, as it _will_ have been updated with those entries.

The final step is then to merge these entries for writes that happened during the backfill from the temporary index into the new index.. This is done in transactional batches, to avoid the potential of overwriting a newer value(in the new index based on an older read in the temporary index. Because SQL traffic is still obligated to update the temporary index, any value in it (including a deletion) can still be assumed to be the correct, most up to date value at the time it is read, so reading some entries from it, and writing to the new index in a transaction is guaranteed to write the correct value — if a newer entry was written to the new index after a given batch read, it would encounter a conflict and refresh, and also then observe the newer temporary entry.  This comes at a cost however: these transactional batches of course incur much higher per-row write overhead than the bulk write path used during the backfill and thus we may wish to try to minimize the amount written by this merge step, e.g. by using iterative pre-merges before a final merge as discussed in appendix 1.

## New Index Encoding for Deletions vs MVCC

To be correct, the merge pass needs to read every index key that was changed during the prior backfill pass and thus needs to be updated. For new keys or keys with updated values, this is simple; that is their current value when the merge reads them since the most recent SQL transaction will have written that. However in a normal index, if an indexed value is removed, the index entry is deleted. The merge pass needs to be able to determine which items were deleted however to correctly remove them from the new index as well, however in a normal index, the record of these deleted items is only in the MVCC history, invisible to normal scans and subject to GC. If that GC were to run before the merge, it would be unable to correctly remove a stale entry added by the backfill.

To avoid this problem, a new "delete-preserving" index encoding is added. This differs from other index encodings in that it does not issue KV delete operations, but rather writes special "deleted" sentinel values.

The basis of this encoding is the addition of a new proto message that boxes the bytes that would otherwise appear in a given index entry, e.g.
``` 
message TempIndexKV {
  bytes value  = 1;
  bool deleted = 2;
} 
```

When a transaction would write an index entry, if that index uses this new encoding, it instead wraps the bytes it would normally write in this new message type, i.e. boxing them, and then writes the encoded box in its place. If that transaction would ordinarily have deleted an index entry, by adding a KV delete to its batch that would have written an MVCC tombstone, in this delete-preserving encoding it instead updates it, by writing an entry containing an encoded box that has set the boolean deleted flag.

Thus this new index encoding ensures that the merge pass can correctly read all keys that changed in the index during the prior backfill, including those that were "deleted".

As a special case, an empty byte string KV value, which is extreme common in Cockroach's current index encodings, can be interpreted as an empty boxed byte string in this new encoding, rather than requiring a empty box message wrapper, since it is unambiguously not a deletion marker (which would be non-empty, as it would have the field tag for the deletion bool).

## Mixed Version State
The index backfill process outlined in this document cannot be used until all nodes have been upgraded to a version that supports the new delete encoding and the new “backfilling” index state. 

In addition, since the existing index backfilling process relies on a non-MVCC compatible implementation of AddSSTable, the new [MVCC compatible implementation of AddSSTable](https://github.com/cockroachdb/cockroach/issues/70422) cannot be used until all nodes in the cluster have been upgraded to a version with the new index creation process. 

# Alternatives Considered
## Incremental Scans and Protected Timestamps

We could instead of adding a temporary index, just add the new index in the offline state and leave ongoing SQL traffic alone, i.e. not emitting any index updates during writes at all while we backfill the new index. To merge in entries that should have been added by transactions that committed during the backfill, we would instead need to scan for what changed in the primary index since the backfill started and compute and add the index entries for those rows. While this works in simple cases, it would require a) an incremental scan, using ExportRequest and more importantly b) that that scan's start time remain within the GC TTL, so all those changes, in particular deletions, remain available to the incremental scan. This would in turn imply adding a Protected Timestamp over the primary key span. With the current protected timestamps system which only implements "gc-nothing-above-this-time" semantics, this would preserve every revision, which could be significant in a high write-churn table over a long backfill. To avoid the protected timestamp falling too far behind, and thus retaining too much history and impacting foreground scan performance, we could potentially intermittently stop backfilling and do a merging catch-up scan, then move the PTS record up and resume the backfill, but this would add additional complexity and pausing and resuming often wastes some amount of progress. Additionally protected timestamps are not currently available to tenants. The temporary index using the delete-preserving encoding on the other hand retains, and only retains, exactly what we need — the latest value, including if that is the deletion of a value, for each changed key, does not affect scans at all, and works in tenants natively without PTS work, so this alternative was rejected.

# Appendix
## Appendix: Iterative Copy Passes Before Final Merge Pass

As mentioned above, the merging pass is required to use transactional batches to correctly merge with foreground traffic, which makes its per-row write cost much higher than if it could use bulk-writes like the rest of the backfill. In a long enough running backfill on a high enough write-rate table, the size of the temporary index that needs to be merged with this more expensive write path could become non-trivial.

One potential solution would be to perform iterative non-transactional merge passes before the final merge pass, of hopefully decreasing size: the amount that needs to be merged the first time is everything that accumulated during the initial backfill, while the amount that needs to be merged the next time is just how ever much accumulated during that merge. The non-final merge passes would be done while the new index is still in the backfill state, and thus not being written to by foreground traffic, making it safe to use bulk-writes. However one challenge to implementing this approach is how to find, on subsequent merge passes including the final one, only those entries written by transactions committed during the prior pass. One option might be to try to use ExportRequest rather than ScanRequest on the temporary index and include a lower time-bound, to incrementally read just those records written since the prior pass' minimum read time.

An incremental scan however would depend on that timestamp remaining readable, which would in turn require protected timestamps. Another option might be to instead allocate additional temporary indexes -- when a pass is nearing completion, add a new temporary index and step it through the usual delete and delete-and-write-only states, then once it is capturing all new traffic, move the old temporary index to backfilling, scan and merge it to the new index, then drop it, before moving on to merge the new temporary index.
