- Feature Name: Cascading Zone Configs
- Status: draft
- Start Date: 2018-09-19
- Authors: Ridwan Sharif
- RFC PR: None
- Cockroach Issue: [#28901]

# Motivation

For the 2.2 release, we're updating the logic surrounding zone configs such
that the zones inherit the configuration from their parents and not just copy
them down at creation time. Currently, when a zone config is created, the parent
zone config is copied and the copy is adapted with the fields specified by the
user. However, if the parent zone config changes later, the child won't
receive the update. This behaviour is really unintuitive as demonstrated by the 
following example: 

A user creates a database with the replication factor set to `3`, and then
creates a table. The table is automatically configured to use the database
zone config by default and so also has a replication factor of `3`. Now the
user updates the replication factor of the database to `5` but this change
does not apply to the table as the change doesn't cascade down.

This is of particular interest for another reason as well: these semantics
for zone configs also apply to the `.liveness`, `.meta` and `system` zone 
configs too. These inherit from the `.default` zone config but if the default
is changed, the changes aren't applied to each of them. This leaves the cluster
vulnerable to situations where changing the `.default` zone config to
use a higher replication factor still causes the cluster to die with a smaller
number of outages because the vital system ranges were replicated by an older 
setting.

# Detailed design

## Changing the `ZoneConfig` struct

To allow for cascading zone configs, we have to be able to tell whether a field 
is in-fact explicitly set or inherited from its parent. Doing so almost certainly
means changing the `ZoneConfig` struct. 

A boolean could be added for **each** field that tells us whether the field was
inherited. This would be needed for each of the fields: `NumReplicas`, 
`RangeMaxBytes`, `RangeMinBytes`, `GC`, `Constraints` and `LeasePreferences`.
Admittedly, this isn't pretty, but allows existing zone configs to work as today
and doesn't involve changing much of the code around zone configs (more on this later).

Alternatively, since the syntax used for the zones is `proto2` we could make the
fields in question nullable. This converts the fields into pointers and allows us to
check if they have been set or not. However, this comes with a performance penalty
because of the extra heap allocations. This would also involve 
changes be made all around the codebase to treat the `ZoneConfig` fields as pointers.

## Backwards Compatibility
Both of the above are backwards compatible. Cascading Zone Configs will only
apply to clusters when the cluster version is v2.2. For clusters with older nodes, we
can enforce that any zone config being written is complete (all fields explicitly set).

## Inheritance Hierarchy

The hierarchy of `ZoneConfig` inheritance looks like:

```
.default
  |
  +-- database
  |      |
  |      +-- table
  |            |
  |            +-- index
  |                  |
  |                  +-- partition
  |
  +-- liveness
  +-- meta
  +-- timeseries
  +-- system

```

Unless explicitly set, the fields in the zone config for each zone/subzone would inherit
the value from its parent recursively. The `.default` fields are always explicitly set.

## Returning Valid `ZoneConfig`s

Before the `getZoneConfigForKey` or the `GetConfigInTxn` returns the zone and/or subzone,
we must ensure all the values in the return are valid (all fields must be set). So
after the call from each of the two, to `getZoneConfig`, the returned zone and/or 
subzones will populate its unset fields by walking up the chain of parents as defined
by the Zone hierarchy above until they find a config with the field explicitly set.

After [adding caching], we must ensure the Zone configs are fully set before cached.
This would mean the inheritance enforcement would happen before the cache is updated,
and after `hook` is called. As for `GetConfigInTxn`, the enforcement can happen before 
it returns.

The worst case chain of inheritance would be:
Partition -> Index -> Table -> Database -> Default

## Additional Considerations

If a table is a `SubzonePlaceholder`, all of it's fields must be unset except for `NumReplicas`
which is set to `0`. For incomplete zone configs (where only a proper subset of fields were 
provided by the user), the unprovided fields must be unset and not copied over when created.

# Drawbacks

# Unresolved Questions
What is the performance penalty of making the `ZoneConfig` fields nullable?
The fields become pointers, but how much does the extra heap allocations affect performance?
This needs to be measured.

# Side Note
Are we going to move away from this nested subzone structure? It seems that
if we could augment the `system.zones` table, we could do away with the nested
subzones and have a more cohesive way of dealing with this hierarchy. 
This would have to involve having the `system.zones` table keyed by
`(descriptorID, indexID, partitionName)` or something along those lines. We might just
want to wait until CRDB supports primary key changes first.

We could also have another `system.zones` table with a different name in the 2.2+
releases with the desired format and it could be kept consistent with the other table.
This is just speculative. Whether a better zones table is warranted is something I wanted
to bring up, any design or suggestions on the design of which, should be in a separate RFC.

[#28901]: https://github.com/cockroachdb/cockroach/issues/28901
[adding caching]: https://github.com/cockroachdb/cockroach/pull/30143
