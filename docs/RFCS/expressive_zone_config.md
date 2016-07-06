- Feature Name: Expressive ZoneConfig
- Status: draft
- Start Date: 2016-07-06
- Authors: @d4l3k
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #4868


# Summary

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
https://www.cockroachlabs.com/docs/configure-replication-zones.html#replicaton-zone-format

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

### Examples

Prefer SSDs and do not put on in-memory stores.
```yaml
constraints: [ssd, -mem]
```

In-memory store replicated across different cloud providers.
```yaml
constraints: [cloud-.*, mem]
```

Require data to be stored in the US on different racks.
```yaml
constraints: [+country-us, rack-.*]
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

# Drawbacks

One drawback is that the new system prevents explicit control over the diversity
of a replica set. There's no way to say "one ssd and two hdds".

# Alternatives

# Unresolved questions

## Locality

There's two primary ways being considered for locality.

### Hierarchy tags

One option is to have a hierarchy of tags on each node in the form of
`--locality cloud-gce/country-us/region-west/datacenter-us-west-1/rack-12`.

This allows us to maximize diversity at the beginning of the hierarchy. For
instance, it's much more important to diversify across data-centers than it is
racks if we can. If the user wants more performance/lower latency, he can
manually add any part of the locality tag to the constraint list
`[datacenter-us-west-1]`. We'll then only be able to diversify across the racks.

We should also warn the users if there are ambiguous locality constraints. This
will prevent issues in the case of locality tags `us-west/rack-1` and
`us-east/rack-1` with the constraint `[rack-1]`. In most cases it they probably
meant something like `[us-west, rack-1]`.

A potential pitfall of this method, is if a user forgets to add a specific
locality field. If one node has 5 elements and the other has 6, it's unclear of
how to handle that.

### KV locality

The other suggested format for locality would be a set of keys and values that
would then be diversified across. This could look like
`--locality cloud=gce,country=us,region=west,datacenter=us-west-1,rack=12`.
Constraints would be applied like `[cloud=gce, country=us]`.

We would try to maximize diversity within each KV tag. This would make it
easier to tell which tags match each other instead of just the index in the
hierarchy list.

We would probably want to use the order as a suggestion for which levels to
prioritize hierarchy for. Or define an explicit list of tags in the ZoneConfig
for hierarchy and do a best effort for the remaining tags.

If we have three datacenters and each datacenter has 3 racks
`datacenter=us-{1,2,3},rack={{1,2,3},{1,2,3},{1,2,3}}` there's no way to
distinguish the primary diversity factor automatically, but we can maximize
diversity on both levels. In this situation, the best thing to do would be to
have 3 replicas
`datacenter=us-1,rack=1`, `datacenter=us-2,rack=2` and `datacenter=us-3,rack=3`.

If we have three datacenters and each datacenter has 3 racks
`datacenter=us-{1,2,3},rack={{1,2,3},{4,5,6},{7,8,9}}` there's no way to
distinguish the primary diversity factor automatically and we have to pick
either maximizing diversity of datacenter or rack.

## Weak negative constraints?

Might be useful to have weak negative constraints such as `!ssd` that won't stop
allocation such as how a prohibited constraint would.

## Verifying correctness of zone configurations

It could be very confusing for users to configure and debug when using a complex
constraint system. Also, potentially very slow for complex constraints with
large numbers of stores and replicas.

## Constraint failure modes

* What happens when there is no matching store for a replica?
* Should we have configurable failure modes to fall back to less strict
  constraints?
* How do you notify users of constraint failures?

