- Feature Name: prevent_out_of_range_access
- Status: draft
- Start Date: 2015-08-24
- RFC PR: #2100
- Cockroach Issue:  N/A

# Summary

Implement a mechanism that prevents out-of-bound access to a range.

# Motivation

`RequestHeader` has the `Key` field and the `EndKey` field. These
fields are used to specify key(s) that a command accesses. More
specifically, `Replica` uses the fields in the following locations:

- `checkCmdHeader` to make sure the replica is responsible for the specified key(s).
- `beginCmd` to enforces at most one command is running per key(s). 

In most cases, commands access keys specified in `RequestHeader`, but 
the following commands access keys outside of a specified range:

- `RangeLookup`: Scans the metakey space to find a range descriptor.
- `EndTransaction`: Calls `splitTrigger` and `mergeTrigger` copies/merges range data
such as response cache.
- `Gc`: Writes GC metadata to a `RangeGCMetadataKey`.

These commands can access keys that are outside of the range boundary.
The commands can also cause potential race issues since two commands
accessing the same key can be executed at the same time. Here are
example cases:

Scenario 1: `RangeLookup` after meta key split

Suppose that we have the following two ranges: R0 and R1:

```
    meta2a   meta2b  meta2\xff\xff   meta3   a    \xff\xff
+------+-------+-------+--------------+------+-----+
|     R0       |                R1                 |
+--------------+-----------------------------------+
```

A lookup of a range containing "a" is routed to R0, which contains
"meta2a". Then, R0 scans ["meta2a\x00", "meta3") and finds a range
descriptor of R1, which is stored at key "meta2\xff\xff" (= meta(R0
end key)). "meta2\xff\xff" belongs to R1, not R0.

Scenario 2: read during split

1. Split starts. This will split range ["a", "z") to ["a", "i") and ["i, "z").
2. Executes `EndTransaction` and starts `splitTrigger`. The replica
starts copying the timestamp cache.
3. Before `splitTrigger` completes, a `Get` request for "j" comes. It
touches the second half of the range.
4. The copy of the timestamp cache completes. Scan's update to the
timestamp cache is lost for the second half of the replica's copy.

# Detailed design

TBD.

# Drawbacks

TBD.

# Alternatives

- To address the range lookup issue: Restrict start/end key that
`Replica.RangeLookup` scans with the range's start/end key. Thsi does
not work when a lookup request continues to be sent to a wrong replica
and gets `RangeKeyMismatchError`.

- To address the split race: Enqueue admin commands to the command
queue so that they won't run concurrently with other overlapping
consist read/write commands. This approach needs to address a deadlock
that can happen when write commands issued from `AdminSpilt` overlap
with keys that are being split. One solution is to change the command
queue to allow key overlap of admin split command A and C when C is
originated from A. This requires some extra information propagation
and performance overhead.

- To address the split range: Add new fields for specifying keys that
can be potentially accessed. For example, a 'EndTransaction' command
with 'splitTrigger' specifies the start/end key of the range to be split.
The command queue additionally looks at these new fields to identify
overlapping commands.




- Do nothing.

# Unresolved questions


