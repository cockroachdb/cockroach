- Feature Name: Cascading Zone Configs
- Status: draft
- Start Date: 2018-09-19
- Authors: Ridwan Sharif
- RFC PR: [#30426]
- Cockroach Issue: [#28901]

# Summary

There is currently some unintuitive behavior in the Zone Configuration update
logic and this RFC proposes changes to correct them. When a Zone Config is 
created, it copies over all the settings unspecified by the user from the table/
database it belongs to, failing that, the default. However, this means an update
to a zone config doesn't have the change propagate to the children as they're loaded
with the original settings at creation time. The RFC change proposed makes it so that
the zones only store the details specified by the user and all other details are
calculated at request time - thus making every zone subscribed to changes that would
apply to it. 

There are some changes in the usage of the zone configs that this would cause:
Per-Replica constraints that relies on the number of replicas can only be set if the
number of replicas for the zone is also explicitly set. Similarly, the `RangeMinBytes`
and `RangeMaxBytes` would also have to be set in tandem. These changes do not have to
be visible to the user at all. For example, when a `RangeMinBytes` is specified, we
calculate the `RangeMaxBytes` and write them both to the Zone Config even though only
one was explicitly specified. The validation of the zone will then take care of
whether that change is appropriate.

# Motivation

For the 2.2 release, we're updating the logic surrounding zone configs such
that the zones inherit the configuration from their parents and not just copy
them down at creation time. Currently, when a zone config is created, the parent
zone config is copied and the copy is adapted with the fields specified by the
user. However, if the parent zone config changes later, the child won't
receive the update. This behaviour is really unintuitive as demonstrated by the 
following example: 

A user creates a database with the replication factor set to `3`, and then
creates a table. The user then configures the table to have a locality constraint
and so only specifies the `constraints` field in the zone config. What happens now
is, the unspecified values of the zone config for the table are copied over from the
parent (database in this case). Thus the table has a replication factor of `3`.
Now the user updates the replication factor of the database to `5` but this change
does not apply to the table as the change doesn't cascade down as one might
intuitively guess.

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

Now that we have [caching], we must ensure the Zone configs are fully set before cached.
This would mean the inheritance enforcement would happen before the cache is updated,
and after `hook` is called. As for `GetConfigInTxn`, the enforcement can happen before 
it returns. Because the worst case chain of inheritance (shown below) would only involve
a walk in the chain of parents of a maximum length of 2, before getting to a cached table
zone config - performance will not be a significant concern.

Additionally, we could update the cache to map from `(id, keysuffix)` to a `ZoneConfig`
and also store the subzone configs in it.

The worst case chain of inheritance would be:
```
Partition -> Index -> Table -> Database -> Default
```

## Additional Constraints on Valid Zone Configs

When validating zone configs, additional measures need to be adopted to ensure the partial 
zone configs make sense.

When creating a partial zone, or making changes to one - we must inherit all the fields
that are not specified from the hierarchy of parents and ensure the new changes are valid in
context of the other fields. 

An additional implication of cascading is, if a user changes a field on any zone
config, every child that will inherit the value **must** validate it against its own config.
The field can only be set if all children can validate the change as well. Without cascading 
validations, the cluster is vulnerable to issues like the following:

### Constraints on Replication Factor

A user creates a database and sets the replication factor to 5. The user then proceeds to set
the zone config for one of its tables with the following constraints without setting the 
replication factor explicitly:

```
Constraints: {'region=west': 2, 'region=east': 2, 'region=central': 1}
```

This change is acceptable (maybe shouldn't be?) as the inherited replication factor is 5.
However, now it must become illegal for the replication factor of the database to be set to
less than 5 (As that change would propagate to the table making its zone config invalid)

### Constraints on Range Bytes

A user creates a database with the `RangeMinBytes` set to 32MB in its zone config. Then
proceeds to define a table with a zone config that specifies `RangeMaxBytes` as 64MB.
This is a valid change. However, now it must become illegal for the `RangeMinBytes` in the
database to be increased higher than 64MB (As that change would propagate to the table 
making its zone config invalid)


## Additional Considerations

If a table is a `SubzonePlaceholder`, all of its fields must be unset except for `NumReplicas`
which is set to `0`. For incomplete zone configs (where only a proper subset of fields were 
provided by the user), the unprovided fields must be unset and not copied over when created.
We could now also completely do away with `SubzonePlaceholder`s.

Although we must populate the non-user specified fields when we want to validate a partially 
complete zone config, we write only the specified fields to the zone config.

# Drawbacks

# Unresolved Questions
What is the performance penalty of making the `ZoneConfig` fields nullable?
The fields become pointers, but how much does the extra heap allocations affect performance?
This needs to be measured.

Now that we're cascading the `ZoneConfig`s, it must be possible that a user _unsets_ the
value for a given field. This would simply mean that a parents field value would apply
from here on forth. Proposed syntax from @knz :

`ALTER ... CONFIGURE ZONE DROP field_name`

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

[#30426]: https://github.com/cockroachdb/cockroach/pull/30426
[#28901]: https://github.com/cockroachdb/cockroach/issues/28901
[caching]: https://github.com/cockroachdb/cockroach/pull/30143
