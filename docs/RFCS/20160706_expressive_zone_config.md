- Feature Name: Expressive ZoneConfig
- Status: obsolete
- Start Date: 2016-07-06
- Authors: @d4l3k
- RFC PR: [#7660](https://github.com/cockroachdb/cockroach/pull/7660)
- Cockroach Issue: [#4868](https://github.com/cockroachdb/cockroach/issues/4868)

# Summary

This document has been made partially obsolete by more recent changes to
ZoneConfig constraints. For the latest information, please see [the
docs](https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html).

# Motivation

The current ZoneConfig format has a number of drawbacks that make it
hard to specify the number of replicas and more complicated constraints.

Current ZoneConfig format:
```yaml
replicas:
- attrs: [comma-separated attribute list]
- attrs: [comma-separated attribute list]
- attrs: [comma-separated attribute list]
range_min_bytes: <size-in-bytes>
range_max_bytes: <size-in-bytes>
gc:
  ttlseconds: <time-in-seconds>
```
https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html#replicaton-zone-format

## Number of replicas

Currently, the number of replicas is controlled by adding extra `- attrs: []`
elements to the replicas array.

Comment from @bdarnell on #4866:

> I'd be in favor of introducing a num_replicas field which would be used in
> place of len(attrs). If attrs are present they would be used to constrain the
> replica selection in the way that they are today, otherwise it is as if there
> are num_replicas empty attr entries. No need to actually address the issues
> around negative or diversity constraints at this point.

## Replication Constraints

Currently it's only possible to specify positive constraints, such as a replica
must be placed on a store with an SSD or in `us-west-1`. Each node/store must
have the list of tags provided in the command line arguments to the server.
There's also no way to do wild card matches such as `us-.*`.

### Types of constraints

* Hardware
  * What type of backing store `hdd`, `ssd`, `mem`.
* Location
  * Cloud provider `gce`, `aws`
  * Data center `us-west-1`
  * Sub data center `rack-12`
  * These could also all be combined into one `gce-us-west-1-rack-12`.

### Potentially desired constraints

* Positive (must match)
  * on `ssd`
  * in `us-.*`.
* Negative (must not match)
  * not on `hdd`
  * not in `us-.*`
* Diversity (replicas distributed to minimize risk)
  * 3 replicas with no 2 replicas on the same rack unless there are less than 3
    racks
* Compound
  * in `us` or `canada`
  * not in `us` or `canada`
  * in `us` and on `ssd`
  * in `us` and not on `ssd`

# Detailed design

Note: This is one potential design. See [Alternatives](#alternatives) and
[Unresolved Questions](#unresolved-questions).

## New ZoneConfig Format
```yaml
num_replicas: <number-of-replicas>
constraints: [comma-separated constraint list]
range_min_bytes: <size-in-bytes>
range_max_bytes: <size-in-bytes>
gc:
  ttlseconds: <time-in-seconds>
```

## Constraint system

This approach would be to extend the current simple tag matching with modifiers
and the ability to do regex matches.

Comment from @petermattis on #4868:

> For example, instead of a single expression we could imagine there are a
> series of expressions. `diversity(us-.*), require(hdd), prohibit(eu-.*)` would
> provide diversity across nodes containing an attribute matching `us-.*`,
> require nodes to have the attribute `hdd` and prohibit nodes from containing
> the attribute `eu-.*`. To make this more terse we could provide short-hand
> operators: `~us-.*, +hdd, !eu.*`. We'd want some rule about how to drop
> constraints when they can't be achieved.

This would be implemented as a simple array of string tags (similar to the
current implementation) with three types of constraints.

### Positive constraints `us-.*`.

Positive constraints are sufficient for most use cases. The allocator would
take care of automatically maximizing diversity of these constraints so the
user doesn't have to worry about having duplicate replicas on the same rack
unless it's unavoidable. If these constraints can't be matched, the allocator
will fall back to the next closest match.

Since the allocator will maximize diversity, this allows us to have `or`
statements. Specifying `[ssd, mem]`, will have all replicas put on an SSD or
in-memory store since there won't be any stores that are both. Likewise,
specifying `[us-.*, ssd]` will have all replicas in the US and on SSDs.

### Required constraints `+us-.*` / Prohibited constraints `-us-.*`

Required and Prohibited constraints are useful for legal situations where you
need a certain data restriction. This may be used for cases where data has to be
local to a country, or shouldn't be in certain ones. The failure mode would be
blocking new replicas from being added while there isn't enough capacity that
matches the constraints.

### Node Locality

The suggested format for locality would be a set of keys and values that would
then be diversified across. This could look like
`--locality cloud=gce,country=us,region=west,datacenter=us-west-1,rack=12`.
Constraints would be applied like `[cloud=gce, country=us]`.

We would try to maximize diversity within each KV tag. This would make it
easier to tell which tags match each other instead of just the index in the
hierarchy list.

We would use the order the tags were defined in as a hierarchy for which levels to
prioritize hierarchy for. If the order of tags on one node doesn't match those
on others, we would warn the user. In such a case, we will use the order that the
majority of nodes have.

If we have three datacenters and each datacenter has 3 racks
`datacenter=us-{1,2,3},rack={{1,2,3},{1,2,3},{1,2,3}}` there's no way to
distinguish the primary diversity factor automatically, but we can maximize
diversity on both levels. In this situation, the best thing to do would be to
have 3 replicas
`datacenter=us-1,rack=1`, `datacenter=us-2,rack=2` and `datacenter=us-3,rack=3`.

While maximizing diversity in a mostly empty cluster is easy to do, as servers
fill up we might be in the situation of putting two replicas on the same rack or
two replicas in the same datacenter. This means that an ordering is needed to
know which tag diversity is more important.

### Examples

Prefer SSDs and do not put on in-memory stores.
```yaml
constraints: [ssd, -mem]
```

Require data to be stored in the US.
```yaml
constraints: [+country=us]
```

In-memory store, and not in aws.
```yaml
constraints: [mem, -cloud=aws]
```

## Remove per replica attributes

Remove the ability to specify per replica attributes as they're a poor stand in
for proper diversity constraints.

With a first class diversity system we can get rid of the attributes arrays.
This also allows for specifying a candidate sets that are bigger than the exact
number of replicas.

### Old format

```yaml
replicas:
- attrs: [us-1]
- attrs: [eur-2]
- attrs: [asia-3]
```

### New Format

```yaml
num_replicas: 3
constraints: [us-1, eur-2, asia-3]
```

## Constraint failure modes

By default, the allocator will try to maximize the number of constraints it
satisfies. If it can't find a perfect match it'll find the best matching store
and use that one instead.

The only exception is that for hard constraints and prohibited constraints,
replication will fail and an alert will show up in the admin UI. In the future,
we could try and migrate other ranges off of matching stores to free space and
allow for replication to continue.

## How do you notify users of constraint failures or issues?

Any issues with constraints will show up in logs and in the admin UI. While
typically a constraint issue shouldn't break the system, it could operate in a
manner that a user doesn't expect.

## Debugging

A simple tool that will allow users to specify contraints and view the matching
stores will be added to the web UI. This will allow users to vet any changes
before actually using them. In addition, it will also highlight any existing
invalid configurations such as out of order locality tags, and failing
constraints.

# Drawbacks

One drawback is that the new system prevents explicit control over the diversity
of a replica set. There's no way to say "one ssd and two hdds".

# Alternatives

# Unresolved questions

## Weak negative constraints?

Might be useful to have weak negative constraints such as `!ssd` that won't stop
allocation such as how a prohibited constraint would. There's currently not a
strong use case for it, but should be fairly easy to add later on.
