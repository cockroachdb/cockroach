- Feature Name: range_replica_naming
- Status: completed
- Start Date: 2015-07-29
- RFC PR: [#1864](https://github.com/cockroachdb/cockroach/pull/1864)
- Cockroach Issue:

# Summary

Consistently use the word **range** to refer to a portion of the global
sorted map (i.e. an entire raft consensus group, and **replica** to
refer to a single copy of the range's data.

# Motivation

We currently use the word "range" informally to refer to both the
consensus group and the data owned by one member of the group, while
in code the `storage.Range` type refers to the latter. This was a
deliberate at the time because (at least in code) the latter is more
common than the former. However, resolving certain issues related to
replication and splits requires us to be precise about the difference,
so I propose separating the two usages to improve clarity.

# Detailed design

Rename the type `storage.Range` to `storage.Replica`, and `range_*.go`
to `replica_*.go`. Rename `RaftID` to `RangeID` (the name `RaftID` was
chosen to avoid the ambiguity inherent in our old use of "range"). A
new `Range` type may be created to house pieces of `Replica` that do
in fact belong at the level of the range (for example, the
`AdminSplit`, `AdminMerge`, and `ChangeReplicas` methods)

# Drawbacks

This reverses a previously-made decision and moves/renames a lot of code.

# Alternatives

Keep `Range` for a single replica and use `Raft` or `Group` when
naming things that relate to the whole group.

# Unresolved questions
