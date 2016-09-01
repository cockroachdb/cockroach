- Feature Name: segmented_storage
- Status: rejected
- Start Date: 2015-07-29
- RFC PR: [#1866](https://github.com/cockroachdb/cockroach/pull/1866)
- Cockroach Issue: [#1644](https://github.com/cockroachdb/cockroach/issues/1644)

# Rejection notes

This proposal was deemed too complex and expensive for the problem it
solves. Instead, we will drop snapshots whose application would create
a conflict in the `replicasByKey` map. This avoids the race conditions
in issue #1644, but leaves the range in an uninitialized and unusable
state. In the common case, this state will resolve quickly, and in the
uncommon case when it persists, we simply rely on the usual repair and
recovery process to move the replica to a new node.

# Summary

Partition the RocksDB keyspace into segments so that replicas created
by raft replication do not share physical storage with replicas
created by splits.

# Motivation

Currently, keys in the distributed sorted map correspond more or less
directly to keys in RocksDB. This makes splits and merges cheap
(since the bulk of the data does not need to be moved to a new
location), but it introduces ambiguity since the same RocksDB key
may be owned by different ranges at different times.

For a concrete example of the problems this can cause (discussed more
fully in
[#1644](https://github.com/cockroachdb/cockroach/issues/1644)),
consider a node `N3` which is temporarily down while a range `R1` is
split (creating `R2`). When the range comes back up, the leaders of
both `R1` and `R2` (`N1` and `N2` respectively) will try to bring it
up to date. If `R2` acts first, it will see that `N3` doesn't have any
knowledge of `R2` and so it sends a snapshot. The snapshot will replace
data in `R2`'s keyspace, which `N3`'s replica of `R1` still covers.
`N3` cannot correctly process any messages relating to `R2` until `R1`
has caught up to the point of the split.

# Detailed design

## Segment IDs

Each replica is associated with a **segment ID**. When a replica is
created in response to a raft message, it gets a newly-generated
segment ID. When a replica is created as a part of `splitTrigger`, it
shares the parent replica's segment ID. Segment IDs are unique per
store and are generated from a store-local counter. They are generally
not sent over the wire (except perhaps for debugging info); all
awareness of segment IDs is contained in the storage package.

## Key encoding

We introduce a new level of key encoding at the storage level.
For clarity, the existing `roachpb.EncodedKey` type will be renamed to
`roachpb.MVCCKey`, and the three levels of encoding will be as follows:

* `Key`: a raw key in the monolithic sorted map.
* `StorageKey`: a `Key` prefixed with a segment ID.
* `MVCCKey`: a `StorageKey` suffixed with a timestamp.

The functions in `storage/engine` will take `StorageKeys` as input and
use `MVCCKeys` internally. All code outside the `storage` package will
continue to use raw `Keys`, and even inside the `storage` package
conversion to `StorageKey` will usually be done immediately before a
call to an MVCC function.

The actual encoding will use fixed-width big-endian integers, similar
to the encoding of the timestamp in MVCCKey. Thus a fully-encoded key
is:

```
+-----------------------------------------------+
|               roachpb.MVCCKey                   |
+-----------------------+
|   roachpb.StorageKey    |
           +------------+
           | roachpb.Key  |

Segment ID | Raw key    | Wall time | Logical TS |
4 bytes    | (variable) | 8 bytes   | 4 bytes    |
```

All keys not associated with a replica (including the counter used to
generate segment IDs) will use segment ID 0.

## Splitting and snapshotting

Ranges can be created in two ways (ignoring the initial bootstrapping
of the first range): an existing range splits into a new range on the
same store, or a raft leader sends a snapshot to a store that should
have a replica of the same range but doesn't.

Each replica-creation path will need to consider whether the replica
has already been created via the other path (comparing replica IDs,
not just range IDs). In `splitTrigger`, if the replica already exists
under a different segment, then a snapshot occurred before the split.
The left-hand range should delete all data that are outside the bounds
established by the split. In the `ApplySnapshot` path, a new segment
will need to be created only if the replica has not already been
assigned a segment.

TODO(bdarnell): `ApplySnapshot` happens in the `Store`'s raft
goroutine, but raft may call other (read-only) methods on its own
goroutine. I think this is safe (raft already has to handle the data
changing out from under it in other ways), but we should double-check
that raft behaves sanely in this case.

# Drawbacks

* Adding a segment ID to every key is a non-trivial storage cost.
* Merges will require copying the entire data of at least one range to
  put them into the same segment.

# Alternatives

* An earlier version of this proposal did not reuse segment IDs on
  splits, so splits required copying the new range's data to a new
  segment (segments were also identified by a (range ID, replica ID)
  tuple instead of a separate ID).

# Unresolved questions

* Whenever a split and snapshot race, we are wasting work, since the
  snapshot will be ignored if the split completes while the snapshot
  is in flight. It's probably worthwhile to prevent or delay sending
  snapshots when an in-progress split should be able to accomplish the
  same thing more cheaply. This is less of an issue currently as new
  ranges are started in a leaderless state and so no snapshots will be
  sent until a round of elections, but we intend to kick-start
  elections in this case to minimize unavailability so we will need to
  be mindful of the cost of premature snapshots.
