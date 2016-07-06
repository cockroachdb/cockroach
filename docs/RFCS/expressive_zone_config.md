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

# Drawbacks

# Alternatives

# Unresolved questions

## Constraint system

### Simple

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

### General expression, SQL Based

One possible implementation would be a simple DSL based on the SQL syntax for
consistency with the rest of the database.

Examples:

  * `"ssd" AND NOT "us-.*"`
  * `"us-.*" AND uniq("rack-.*")` or `"us-*" AND DISTINCT "rack-.*"`
  * `"ssd" AND NOT ("us-.*" OR "canada-.*")`

This format would require it to be heavily regex based with capture groups and
potentially cumbersome and slow.

Every server would have a list of tags such as `ssd` or `us-west-1-rack-12`.
When evaluating the constraints the string would scan over all the strings and
return the match. E.g. `"us-.*"` might evalutate to `us-west-1`. Matches are
truthy when computing constraints.

Functions:

*  `uniq` returns true or false depending on if there would be any other
   replicas with the same passed value. Assuming each store is tagged with
   `rack-<n>`, `uniq("rack-.*")` would ensure that there are no other replicas
   for that range on the same rack.

Instead of relying on functions, we could adopt closer SQL syntax and do
something like `"us-*" AND DISTINCT "rack-.*"`

## Verifying correctness of zone configurations

It could be very confusing for users to configure and debug when using a complex
constraint system. Also, potentially very slow for complex constraints with
large numbers of stores and replicas.

## Constraint failure modes

* What happens when there is no matching store for a replica?
* Should we have configurable failure modes to fall back to less strict
  constraints?
* How do you notify users of constraint failures?

## Per replica attrs

Do we want to keep the ability to specify per replica attributes?

With a general constraint system we can get rid of the attributes arrays.

Replace
```yaml
replicas:
- attrs: [us-1]
- attrs: [eur-2]
- attrs: [asia-3]
```
with `num_replicas: 3` and `DISTINCT "(us-1|eur-2|asia-3)"`.
