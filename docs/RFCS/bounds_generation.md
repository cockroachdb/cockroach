- Feature Name: bounds_generation
- Status: rejected
- Start Date: 2015-07-29
- RFC PR: [#1867](https://github.com/cockroachdb/cockroach/pull/1867)
- Cockroach Issue: [#1644](https://github.com/cockroachdb/cockroach/issues/1644)

# Rejection notes

This RFC supplements  #1866, which was rejected.

# Summary

Add a **bounds generation number** to all range descriptors, and use
it to maintain consistency of the store's `replicasByKey` map.

# Motivation

One of the issues in
[#1644](https://github.com/cockroachdb/cockroach/issues/1644) is that
a replica may be created in response to a raft message before the
split that created the range has been processed, leading to conflicts
in the `Store.replicasByKey` map.

# Detailed design

Add a field `bounds_generation` to `RangeDescriptor`. This value is
incremented whenever the bounds of the range change. On a split, the
`bounds_generation` of each post-split range is one plus the pre-split
`bounds_generation`. On a merge, the `bounds_generation` of the
post-merge range is one plus the maximum of the two pre-merge ranges'
values.

In `Store.addRangeInternal`, when we add the range to `replicasByKey`,
we first test for an overlapping range. If we find one, we compare
bounds generations: the range with the lower bounds generation has its
effective bounds reduced to eliminate the conflict.

When this happens, the range whose bounds have been reduced will stop
receiving KV requests for the affected portion of the keyspace. It
will, however, continue to receive raft messages, which are routed by
ID instead of by key. This will allow the replica to catch up and
process the split transaction that allowed the new range with a higher bounds generation to come into existence (or the replica has been removed and will eventually be garbage-collected).

# Drawbacks

# Alternatives

# Unresolved questions
