- Feature Name: Multi-tenant zone configs
- Status: in-progress
- Start Date: 2021-06-10
- Authors: Irfan Sharif, Arul Ajmani
- RFC PR: [#66348](https://github.com/cockroachdb/cockroach/pull/66348)
- Cockroach Issue: (one or more # from the issue tracker)


# Summary

Zone configs dictate data placement, replication factor, and GC behavior; they
power CRDB's multi-region abstractions. They're disabled for secondary tenants
due to scalability bottlenecks in how they're currently stored and
disseminated. They also prevent writes before DDL in the same transaction,
implement inheritance and key-mapping by coupling KV and SQL in an undesirable
manner (making for code that's difficult to read and write), and don't
naturally extend to a multi-tenant CRDB.

This RFC proposes a re-work of the zone configs infrastructure to enable its
use for secondary tenants, and in doing so addresses the problems above. We
introduce a distinction between SQL zone configs (attached to SQL objects,
living in the tenant keyspace) and KV zone configs (applied to an arbitrary
keyspan, living in the host tenant), re-implement inheritance by introducing
unique IDs for each zone configuration object, and notify each node of updates
through rangefeeds.


# Background

Zone configs (abbrev. zcfgs) allow us to specify KV attributes (# of replicas,
placement, GC TTL, etc.) for SQL constructs. They have useful inheritance
properties that let us ascribe specific attributes to databases, tables,
indexes or partitions, while inheriting the rest from parent constructs or a
`RANGE DEFAULT` zcfg. They're currently stored only on the host tenant in its
`system.zones` table, which in addition to a few other tables
(`system.descriptors`, `system.tenants`, etc.) form the `SystemConfigSpan`.
Whenever there's a zcfg update, we gossip the _entire_ `SystemConfigSpan`
throughout the cluster. Each store listens in on these updates and applies it
to all relevant replicas.


# Problems

The existing infrastructure (from how zcfgs are stored, how they're
distributed, and how inheritance is implemented) doesn't easily extend to
multi-tenancy and was left unimplemented (hence this RFC!). This prevents using
CRDB's multi-region abstractions for secondary tenants. The current
infrastructure has operations that are `O(descriptors)`; with multi-tenancy we
expect an order of magnitude more descriptors and as such is unsuitable for it.

zcfgs, even without multi-tenancy, have a few problems:

1. The way we disseminate zcfg updates (written to `system.zones`) is by
   gossipping the entirety of the `SystemConfigSpan` whenever anything in the
   entire span gets written to. This mechanism is as it is because we also rely
   on gossiping updates to `system.descriptors` and `system.tenants`. This is
   so that each store is able to determine the appropriate split points for
   ranges: on the host tenant's table boundaries and on tenant boundaries. The
   similar need to disseminate zcfg updates resulted in us "conveniently"
   bunching them together. But gossiping the entire `SystemConfigSpan` on every
   write to anything in the span has very real downsides:
   - It requires us to scan the entire keyspan in order to construct the
     gossip update. This is an `O(descriptors + zcfgs + tenants)` operation and
     prohibitively slow for a large enough `SystemConfigSpan` and frequent
     enough change.
   - To construct the gossip update, the leaseholder running the
     gossip-on-commit trigger needs to scan the entire `SystemConfigSpan`. For
     this reason, we've disallowed splitting the `SystemConfigSpan`.
   - To run the gossip-on-commit trigger on the leaseholder, we need to
     anchor any txn that writes to the `SystemConfigSpan` on a key belonging to
     the `SystemConfigSpan`. This prevents us from supporting txns that attempt
     to write arbitrary keys before issuing DDL statements.
   - Gossipping the entire `SystemConfigSpan` necessitates holding onto all
     the descriptors in memory (the "unit" of the gossip update is the entire
     `SystemConfigSpan`), which limits schema scalability ([#63206][63206]).

   The determination of split points and zone configs is the last remaining use
   of gossip for the `SystemConfigSpan` (we used to depend on it for [table
   leases][48159] and [cluster settings][58362], but that's no longer the
   case).
2. zcfgs don't directly capture the parent-child relations between them; we
   instead rely on them being keyed using the `{Database,Table}Descriptor`
   ID<sup id="a2">[2](#f2)</sup>. For KV to determine what the inherited
   attributes are, it reaches back into SQL to traverse this tree of
   descriptors. Inheritance for indexes and partitions (where IDs are scoped
   only to the table they belong to) are consequently implemented using
   sub-zones. Between this and the KV+SQL cyclical dependency, it makes for
   unnecessarily complex code and smells of a faulty abstraction, hindering
   future extensions to zcfgs.
3. zcfgs also don't directly capture the keyspans they're applied over. We rely
   on the same SQL descriptor+sub-zone traversal to determine what attributes
   apply over a given key range.

With multi-tenancy, we have a few more:

4. zcfgs are currently only stored on the host tenant's `system.zones`, which
   is keyed by the `{Database,Table}Descriptor` ID. These are tenant scoped,
   thus barring immediate re-use of the host tenant's `system.zones` for
   secondary tenants.
5. It should be possible for tenants to define divergent zcfgs on adjacent
   tables. This will necessitate splitting on the table boundary. For the host
   tenant, KV unconditionally splits on all table/index/partition boundaries
   (by traversing the tree of SQL descriptors accessible through the
   `SystemConfigSpan`), so it's less of a problem. We don't do this for
   secondary tenants for two reasons:
   - Doing so would introduce an order-of-magnitude more ranges in the system
     (we've also disallowed tenants from setting manual split points:
     [#54254][54254], [#65903][65903]).
   - Where would KV look to determine split points? Today we consult the
     gossipped `SystemConfigSpan`, but it does not contain the tenant's
     descriptors. Since we can't assume splits along secondary tenant table
     boundaries, we'll need to provide a mechanism for KV to determine the
     split points implied by a tenant's zcfgs.
6. The optimizer uses zcfgs for [locality-aware planning][la-planning]. Today
   it peeks into the cached gossip data to figure out which zcfgs apply to
   which descriptors. We'll need to maintain a similar cache in secondary
   tenant pods.
7. Multi-tenant zcfgs open us up to tenant-defined splits in KV. We'll have to
   tread carefully here; the usual concerns around admission control apply:
   protecting KV from malicious tenants inducing too many splits, ensuring that
   the higher resource utilization as a result of splits are somehow
   cost-attributed back to the tenant, and providing some level of
   tenant-to-tenant fairness.


# Technical Design


## Overview

We'll introduce the notion of a KV span config, distinguishing it from the zone
config we're familiar with today. Zone configs can be thought of an exclusively
SQL level construct, span configs an exclusively KV one. Each tenant's zone
configs will be stored in the tenant keyspace, as part of the tenant's SQL
descriptors directly. Inheritance between zone configs will not straddle tenant
boundaries (each tenant gets its own `RANGE DEFAULT` zone config that all
others will inherit from). KV span configs will have no notions of inheritance,
they're simply attributes defined over a keyspan.

On the host tenant we'll capture all KV span configs and maintain the spans
each one applies over. This will let us derive split points on keys with
diverging configs on either side. Each active tenant's SQL pod(s) would
asynchronously drive the convergence between its zone configs and the cluster's
span configs (pertaining to the tenant's keyspace) through an explicit KV API.
The SQL pod, being SQL aware, is well positioned to transform its zone configs
to span configs by spelling out the constituent keyspans. When SQL decides to
promote a set of zone configs to span configs, KV will perform relevant safety
checks and rate-limiting. Each KV server will establish a single range feed
over the keyspan where the span configs and the corresponding spans are stored.
Whenever there's an update, each store will queue the execution of all implied
actions (replication, splits, GC, etc).

TODO: Create a diagram for this overview?


## Zone configs vs. span configs

With multi-tenant zone configs, it's instructive to start thinking about "SQL
zone configs" (just zcfgs) and "KV span configs" (abbrev. scfgs) as two
distinct things.

1. A "SQL zone config" is something _proposed_ by a tenant that is not directly
   acted upon by KV; they correspond to the tenant's "desired state" of tenant
   data in the cluster. It's a tenant scoped mapping between the tenant's SQL
   objects and the corresponding attributes. Updating a zcfg does not guarantee
   the update will be applied immediately, if at all (see the discussion around
   [declared vs. applied zone configs](#declared-vs-applied-zone-configs)).
   Only zcfgs need to capture inheritance semantics, it's what lets users apply
   a zcfg on a given SQL object (say, a table) and override specific attributes
   on child SQL objects (say, a partition). We want to be able to later change
   a parent attribute, which if left unset on the child SQL object, should
   automatically apply to it.
2. A "KV span config" by contrast is one that _actually_ influences KV
   behavior. It refers to arbitrary keyspans and is agnostic to tenant
   boundaries or SQL objects (though it ends up capturing the keyspans implied
   by SQL zone configs -- we can think of them as a subset of the latter).

This is not a distinction that exists currently. In the host tenant today,
whenever a zcfg is persisted (to `system.zones`), that's immediately considered
by KV -- all KV nodes hear about the update through gossip and start acting on
it. Given its coupling to SQL descriptors, KV ends up also (unwillingly)
adopting the same inheritance complexity found in SQL. This needn't be the
case. Introducing a level of indirection will help us prevent tenants from
directly inducing KV actions (replication, splits, GC). Finally, it'll help us
carve out a natural place to cost operations and enforce per-tenant zcfg
limits.

If a tenant (including the host) were to successfully `ALTER TABLE ...
CONFIGURE ZONE`, they would be persisting a zcfg. The persisted zcfg is later
considered by KV, and if it were to be applied, would be persisted as a scfg.
KV servers will only care about scfgs, and on hearing about changes to them,
will execute whatever changes (replication, gc, splits) are implied. A periodic
reconciliation loop will be responsible for promoting zcfgs to scfgs. More on
this later.


## Storing SQL zone configs

The optimizer needs access to zcfgs to generate locality optimized plans. Today
it uses the gossiped `SystemConfigSpan` data to access the descriptor's
(potentially stale) zcfg. But as described in [Problems](#problems) above, use
of gossip has proven to be pretty untenable. Also with multi-tenancy, SQL pods
are not part of the gossip network. We _could_ cache zcfgs in the catalog
layer, and associate them with descriptors. We propose an alternative: doing
away with `system.zones` and storing zcfgs as part of the descriptor itself.

Every database/table descriptor will include an optional `zcfg` field which
will only be populated if the zcfg has been explicitly set on it by the user.
For example, table descriptors will change as follows:

```proto
message TableDescriptor {
 // ...
 optional ZoneConfig zcfg = 51;
}
```

This has the added benefit of letting us simplify how we implement zcfg
inheritance. As described [above](#problems), zcfgs don't fully capture the
parent-child relations between them. This information is instead derived by
traversing the tree of SQL descriptors. The code complexity is worsened by
index and partition descriptors which don't have a unique ID associated with
them, preventing us from storing their zcfgs in `system.zones`. For them we've
introduced the notion of "sub-zones" (zone configs nested under other zone
configs); for an index descriptor, its zone config is stored as a sub-zone in
the parent table's zone config. Storing zcfgs in descriptors will allow us to
get rid of the concept of subzones, an index/partition's zcfg can be stored in
its own descriptor (not in the critical path for this RFC).

This change minimally affects the various zcfg operations; inheritance
semantics will be left unchanged -- we're simply removing a layer of
indirection through `system.zones`. It will however make descriptors slightly
larger, though this is simply reclaiming space we were already using in
`system.zones`. Because the constraints that can be defined as part of a zcfg
can be an arbitrarily long string, we'll want to enforce a limit.

For distinguished zcfgs (`RANGE DEFAULT`, `RANGE LIVENESS`, ...), now that
we're storing them in descriptors directly, we'll want to synthesize special
descriptors also stored in `system.descriptor`. Conveniently, they already have
[pseudo][pseudo-ids] descriptor IDs allocated for them. We'll ensure that all
instances where we deal with the set of descriptors work with these
placeholders. This change will let us deprecate and then later delete
`system.zones` (see [Migration](#migration) below).


## Storing KV zone configs

Separate from zcfgs are scfgs, the translation between the two will be
discussed [below](#reconciliation-between-zone-and-span-configs). All scfgs
will be stored on the host tenant under a (new) `system.span_configurations`
table:

```sql
CREATE TABLE system.span_configurations (
    start_key     BYTES NOT NULL PRIMARY KEY,
    end_key       BYTES NOT NULL,
    config        BYTES
);
```

This is a "flat" structure. There's no notion of scfgs inheriting from one
another, obviating a need for IDs. The spans in `system.span_configurations`
are non-overlapping. Adjacent spans in the table will either have diverging
scfgs, or will belong to different tenants. This schema gives us a convenient
way to determine what attributes apply to a given key/key-range, and also helps
us answer what the set of split points are: all `start_key`s.

Removing inheritance at the KV level does mean that changing the zcfg of a
parent SQL descriptor would potentially incur writes proportional to the number
of child descriptors. We think that's fine, they're infrequent enough
operations that we should be biased towards propagating the dependency to all
descendant descriptors. It simplifies KV's data structures, and inheritance
semantics are contained only within the layer (SQL) that already has to reason
about it.

## Reconciliation between zone and span configs

In multi-tenant CRDB, each active tenant has one or more SQL pods talking to
the shared KV cluster. Every tenant will have a single "leaseholder" pod
responsible for asynchronously<sup id="a3">[3](#f3)</sup> reconciling the
tenant's zcfgs with the cluster's scfgs pertaining to the tenant's keyspan.
We can author a custom leasing mechanism similar to [sqlliveness][50377] for
mutual exclusion, or make use of the jobs infrastructure. All pods will be able
to see who the current leaseholder is, the duration of its lease, and in the
event of leaseholder failure, attempt to acquire a fresh one.

When acquiring the lease, the pod will first establish a rangefeed over
`system.descriptors`. Following a catch-up scan and then following every
rangefeed update, it will attempt to reconcile the tenant's zcfgs with the
cluster's relevant scfgs. Generating scfgs from a set of zcfgs entails creating
a list of non-overlapping spans and the corresponding set of attributes to
apply to each one. To do so, we'll recurse through the set of SQL descriptors
(within `system.descriptors`), capture their keyspans, and materialize a scfg
for each one by applying inherited attributes following parent links on the
descriptor. Put together, KV will no longer need to understand how SQL objects
are encoded or need to bother with inheritance between scfgs.

We'll post-process this list of non-overlapping spans-to-scfgs to coalesce
adjacent spans with the same effective scfg. This will reduce the number of
splits induced in KV while still conforming to the stated zcfgs. It's worth
noting that this is not how things work today in the host tenant -- we
unconditionally split on all table/index/partition boundaries even if both
sides of the split have the same materialized zcfg. We could skip this
post-processing step for the host tenant to preserve existing behavior, though
perhaps it's a [desirable improvement][66063].

The reconciliation loop will only update mismatched entries in scfgs through a
streaming RPC, with KV maintaining a txn on the other end. The streaming RPC
also helps us with pagination, both when reading the set of extant scfgs, and
when issuing deltas. When a pod first acquires a lease, it will construct the
desired state of `system.span_configurations` as described above. It will then
compare it against the actual state stored in KV and issue updates/deletes as
required. Since this "diffing" between a tenant's zcfgs and scfgs happens in
the SQL pod, we'll introduce the following RPCs to roachpb.Internal.

```proto
message SpanConfigEntry {
	optional Span span = 1;
	optional KVZoneConfig kvZoneConfig = 2;
};

message GetSpanConfigsRequest {
	optional int64 tenant_id = 1;
};

message GetSpanConfigsResponse {
	repeated SpanConfigEntry span_configs = 1;
};

message UpdateSpanConfigsRequest {
	repeated SpanConfigEntry span_configs_to_upsert = 2;
	repeated SpanConfigEntry span_configs_to_delete = 3;
};
```

To guard against old leaseholders being able to overwrite scfgs with a stale
view of zcfgs, we'll have the pods increment a counter on a tenant-scoped key
after acquiring the lease. We'll send along this expected counter value as part
of the streaming RPC, and KV will only accept the deltas if the counter is
equal to what was provided. If the pod finds that the counter was incremented
from underneath it, it must be the case that another pod acquired the lease
from underneath it (we'll abandon the reconciliation attempt).

This SQL pod driven reconciliation loop makes it possible that zcfgs for
inactive tenants are not acted upon. We think that's fine, especially given
that there's a distinction between a zcfg being persisted/configured, and it
being [applied](#declared-vs-applied-zone-configs). Also, see alternatives
below.


## Disseminating and applying KV zone configs

KV servers want to hear about updates to scfgs in order to queue up the actions
implied by said updates. Each server will establish a rangefeed on the host
tenant's `system.span_configurations` table and use it maintain an in-memory
data structure with the following interface:

```go
type SpanConfig interface {
  GetConfigFor(key roachpb.Key) roachpb.SpanConfig
  GetSplitsBetween(start, end roachpb.Key) []roachpb.Key
}
```

Consider a naive implementation: for every `<span, scfg>` update, we'll insert
into a sorted tree keyed on `span.start_key`. We could improve the memory
footprint by de-duping away identical scfgs and referring to them by hash. Each
store could only consider the updates for the keyspans it cares about (by
looking at the set of tenants whose replicas we contain, or look at replica
keyspans directly). If something was found to be missing in the cache, we could
fall back to reading from the host tenant's `system.span_configurations`. The
store could also periodically persist this cache (along with the resolved
timestamp); on restarts it would then be able to continue where it left off.


## Dealing with missing KV zone configs

It's possible for a KV server to request the span configs for a key where that
key is not yet declared in the server's known set of scfgs. The spans captured
`system.span_configurations` are only the "concrete" ones, for known
table/index/partition descriptors. Seeing as how we're not implementing
inheritance, there's no "parent" scfg defined at database level to fall back
on. Consider the data for a new table, where that table's scfg has not yet
reached the store containing its replicas. This could happen through a myriad
of reasons: the duration between successive attempts by a SQL pod to update its
scfgs, latency between a scfg being persisted and a KV server finding out about
it through the rangefeed, etc. To address this, we'll introduce a global,
static scfg to fall back on when nothing more specific is found. Previously the
fallback was the parent SQL object's zone config, but that's no longer
possible. It's worth noting that previously, because we were disseminating
zcfgs through gossip, it was possible for a new table's replicas to have
applied to them the "stale" zcfgs of the parent database. If the "global"
aspect of this fallback scfg proves to be undesirable, we can later make this
per-tenant.


## Enforcement of per-tenant limits

Supporting zcfgs for secondary tenants necessarily implies supporting
tenant-defined range splits. To both protect KV and to ensure that we can
fairly cost tenants based on resource usage (tenants with the same workload but
with differing number of tenant-defined splits will stress KV differently),
we'll want to maintain a counter for the number of splits implied by a tenant's
set of scfgs.

```sql
CREATE TABLE system.spans_configurations_per_tenant (
    tenant_id               INT,
    num_span_configurations INT,
)
```

When a tenant attempts to promote their current set of zcfgs to scfgs, we'll
transactionally consult this table for enforcement and update this table if
accepted. We'll start off simple, with a fixed maximum allowed number of
tenant-defined splits. If the proposed set of scfgs implies a number of splits
greater than the limit, we'll reject it outright (and not try any best-effort
convergence or similar). If we want to allow more fine-grained control over
specific tenant limits, we can consult limits set in another table writable
only by the host tenant.


## Declared vs. applied zone config

With the introduction of per-tenant zcfg limits, it's possible for a tenant's
proposed set of zcfgs to never be considered by KV. Because the reconciliation
between zcfgs and scfgs happens asynchronously, it poses difficult questions
around how exactly we'd surface this information to the user. How's a tenant to
determine that they've run into split limits, and need to update the
schema/zcfgs accordingly?

We'll note that a form of this problem already exists -- it's possible today to
declare zcfgs that may never be fully applied given the cluster's current
configuration. There's also a duration between when a zcfg is declared and when
it has fully been applied. There already exists a distinction between a zcfg
being simply "declared" (persisted to `system.zones`) and it being fully
"applied" (all replicas conform to declared zcfgs). The only API we have today
to surface this information are our conformance reports. As we start thinking
more about compliance, we'll want to develop APIs that capture whether or not a
set of zcfgs have been fully applied, or to wait until a zcfg has.

In the short term, to surface possible errors, we'll make use of in-memory
virtual tables that returns the last set of errors seen by the reconciliation
loop. Going forward we could make the reconciliation loop more stateful, and
have it write out errors to some internal table for introspection.


## Access to distinguished zone configs

CRDB has few distinguished zone configs for special CRDB-internal keyspans
`RANGE {META, LIVENESS, SYSTEM, TIMESERIES}`. These will only live on the host
tenant; secondary tenants will not be able to read/write to them. Each tenant
will still be able to set zone configs for their own `DATABASE system`, which
only apply to the tenant's own system tables.


## Introspection

CRDB has facilities to inspect declared zone configs (`SHOW ZONE CONFIGURATIONS
FOR ...`). We also have facilities to observe [conformance][conf-reports] to
the declared zone configs. They'll be left unharmed and can provide the same
functionality for tenants. Some of this might bleed over into future work
though.


## Migration

The complexity here is primarily around the host tenant's existing use of
zcfgs. Secondary tenants can't currently use zcfgs so there's nothing to
migrate -- when running the new version SQL pod, talking to an already migrated
KV cluster, they'll simply start using the new zcfg representation and scfgs.
As part of migrating the host tenant to start using the new zcfgs
infrastructure, we'll need to migrate KV to start using the new (rangefeed
driven) scfgs instead.

For the new `system.span_configurations` table, we'll create it through a
startup migration. For the rest, we'll use the [long-running migrations][48843]
framework. We'll first introduce a cluster version `v21.1-A`, which when rolled
into, will disallow setting new zone configs<sup id="a1">[1](#f1)</sup> (we'll
later re-allow it after `v21.1-B`). We'll then recurse through all descriptors,
and for each one copy over the corresponding zfcg from `system.zones` into it.
If/when tackling subzones, we'll do the same by instantiating zcfgs (from the
parent descriptor's subzones) for all index/partition descriptors. 

We'll then run the reconciliation loop for the host tenant, to instantiate its
set of scfgs within `system.span_configurations`. Next we'll fan out to each
store in the cluster, prompting it to establish a rangefeed over the table.
Each store will (re-)apply all scfgs, as observed through the rangefeed, and
hence forth discard updates received through gossip. It can also stop
gossipping updates when the `SystemConfigSpan` is written to. Finally, we'll
roll the cluster over to `v21.1-B`, re-allowing users to declare zcfgs. Going
forward we'll be using the new zcfgs and scfgs exclusively.


## Backup/restore considerations

Currently, we only record zcfgs (`system.zones`) when performing a full cluster
backup. Database/table backups don't include zcfgs. Instead they inherit zcfgs
from the cluster and the database (in case of tables) they're restored into.
It's clarifying to think of zcfgs as an attribute of the cluster, applied to a
specific SQL descriptor, not as an attribute of the descriptor itself. These
framing helps reason about restoring tables/databases into clusters with
different physical topologies without having to think of configuration
mismatches -- we want to preserve this framing.

If zcfgs are being moved into descriptors, we'll want to ensure that we clear
them when performing a table/database restore, and retain them when performing
full cluster restores. We also want to preserve backwards compatibility with
full cluster backups with pre-migration state (specifically, `system.zones`).
[#58611][58611] describes the general solution, but in its absence we will
introduce ad-hoc migration code in the restore path to move zcfgs into the
descriptor.


## Breakdown of work

The work for zcfgs and scfgs can take place (mostly) independently; scfgs is
comparatively shorter. We'll implement a first pass that simply uses the
existing zcfgs representation (where zcfgs are stored in `system.zones`) to
generate the span-based "flat" scfgs. The only thing we'll need then is the
reconciliation loop in the sql pods, leasing between sql pods, establishing
rangefeeds over `system.span_configurations` and maintaining the cached state
on each store.  This limited scope should de-risk implementing zcfgs for
secondary tenants.
- We can start with reconciliation loop using the existing `system.zones`,
  txn-ally reading from it. It decouples the zcfgs work from scfgs. For the
  optimizer, can maintain a simple cache of descs-to-zcfgs.
- We can start checking in code without meaningfully wiring up the new
  scfgs infrastructure to anything. We'll use it only if the cluster was
  initialized with a test-only feature-flag, or only as part of our testing
  harnesses, sidestepping all migration concerns until the end. If using the
  init time feature flag, other nodes can learn about it as part of the join
  RPC.
- On the KV side we'll:
  - Introduce `system.span_configurations` on the host tenant
  - Implement the RPC server for the RPCs above.
  - Allow access to the RPCs through `kv.Connector`
  - Implement per-tenant limits
  - Establish the rangefeed over `system.span_configurations` on each store.
  - Maintain per-store in-memory cache with rangefeed updates
  - Apply rangefeed scfg updates to relevant replicas
  - Implement zcfg relevant bits of the migration 
- On the SQL side we'll:
  - Implement generating scfgs using the existing (`system.zones`) zcfgs
    representation
  - Implement leasing between sql pods (right now we only have one pod per
    tenant)
  - Implement collapsing of adjacent scfgs
  - Implement logic to construct `UpdateSpanConfigurations` request
    - Could start with a "whole-sale update" RPC, and only later
      implement client-side "diffing" and pagination for more targeted updates
  - Store zcfgs in descriptors directly
  - Switch reconciliation job to use zcfgs stored in descriptors directly,
    instead of `system.zones`
  - Implement scfg relevant bits of the migration
  - Implement clearing zone configurations on table/database restores
  - Implement migration for restoring an older version full cluster backup


## Drawbacks

There are a lot of moving parts, though most of it feels necessary.


## Rationale and Alternatives

- We could store limits and a tenant's scfgs in the tenant keyspace. We
  could use either gossip to disseminate updates to all these keys or
  rangefeeds established over T tenant spans. This spreads out "KV's internal
  per-tenant state" in each tenants own keyspace, instead of it being all in
  one place. Establishing T rangefeeds per store feels excessive, and we're not
  gaining much with spreading out scfgs across so many keyspans. They can be
  reconstructed from zcfgs, which already are in the tenant keyspace. Use of
  gossip is also made complicated due to its lack of ordering guarantees. If we
  individually gossip each zcfg entry, stores might react to them out of order,
  and be in unspecified intermediate states while doing so. Also gossip updates
  are executed as part of the commit trigger; untimely crashes make it possible
  to avoid gossipping the update altogether. To circumvent the ordering
  limitations and acting on indeterminate intermediate states, we could gossip
  the entire set of tenant's zcfgs all at once. We could also gossip just a
  notification, and have each store react to by reading from kv directly. The
  possibility of dropped updates would however necessitate periodic polling
  from each store, and for each store to read from T keyspans to reconstruct
  in-memory state of zcfgs upon restarts. It feels much easier to simply use
  rangefeeds instead.
- We could define a separate keyspace in the host tenant to map from spans to
  scfg ID, to be able to apply the same scfg to multiple spans. This comes from
  the observation that most scfgs will be identical, both within a tenant's
  keyspace and across. Referring to scfgs through ID or hash would reduce the
  size of `system.span_configurations`. 
- For the reconciliation loop, if we didn't want the SQL pod to drive it, the
  system tenant/some process within KV could peek into each tenant's keyspace
  in order to apply its zcfgs. The breaching of tenant boundaries feels like a
  pattern we'd want to discourage.

## Future work

- We can't currently define [zcfgs on schema objects][57832]. We could now, if
  we're storing zcfgs in descriptors directly.
- For sequences, we might want them to be in the same ranges as the tables
  they're most frequently used in tandem with. Now that we're not
  unconditionally splitting on table boundaries, we could be smarter about
  this.
  - For the host tenant we also unconditionally split around the sequence
    boundary. Given they're just MVCC counters, that feels excessive. If we're
    coalescing adjacent spans based only on scfgs, we could coalesce multiple
    sequences into the same range.
- We could support defining manual splits on table boundaries for secondary
  tenants ([#65903][65903])
- We could avoid unconditionally splitting on table boundaries on the host
  tenant ([#66063][66063])
- See discussion around [declared vs. applied
  zcfgs](#declared-vs-applied-zone-configs). We want to provide
  APIs to observe whether or not a declared zcfg has been applied, or if
  applicable, determine what's preventing it from being applied (due to the
  number of splits? due to something else?) We can't guarantee that a declared
  zcfg has been instantaneously conformed to, but we can provide the guarantee
  that once a zcfg has been fully applied, we won't execute placement decisions
  running counter to that zcfg. That feels like a useful primitive to reason
  about as we start designing for data-domiciling patterns. We can still be
  under-replicated should a node die, but we'll never place a replica where
  we're not supposed to. 
- Ensure replication and conformance reports work for secondary tenants. We
  don't have distinguished, per-tenant spans today, but we might in the
  future. They'll be inaccessible for tenants, probably we won't want tenants
  to be able to declare zcfgs over them. How should the host tenant declare
  zcfgs over them? When creating a new tenant, we'd install zcfgs over the
  tenant's distinguished keyspans that will likely only be writable by the host
  tenant. If we want tenants to declare zcfgs over this distinguished data, we
  can expose thin APIs that will let them do just that.
- Ensure we cost replication-related I/O, as induced by zcfgs.


## Unresolved questions

- Should we disallow secondary setting zone configs directly? Leave it only
  settable by our multi-region abstractions? It's not clear that we're looking
  to encourage using the raw zone configs as a primitive for users. Unlocking
  all of it right away makes any future backwards compatibility/migration story
  more difficult. On the other hand, our MR abstractions don't have the full
  expressivity of zcfgs.
- For the limits, we didn't bother with granularly tracking each tenant's
  size-of-scfgs, or size of spans, opting instead for a coarse number-of-splits
  measure. The thinking here was that it's the split count that would be the
  bottleneck, not really the size of each tenant's scfgs. Is that fine, or do
  we still want to track things more granularly?
- I'm not sure how admission control will work, but do we need to safeguard
  the host tenant's scfg range from getting hammered by the set of all sql
  pods. This is somewhat mitigated by having the sql pods only asynchronously
  propose updates, but do we still want some sort of admission control? To
  reject the reconcile RPCs at the outset before doing any processing?
- With the asynchronous reconciliation between zcfgs and scfgs and
  per-tenant limits on the number of splits, it's possible for a proposed set
  of zcfgs to be rejected by KV. How should this information be surfaced?  We
  want the tenant to understand why they're perma-non-conformant in order for
  them to reconfigure their zcfgs to imply fewer splits, or to reach out to us
  to up their limit. It's unclear what the UX here will be, and how much we'll
  address as part of the MVP.
- Locality aware planning would read the zone configuration off the
  descriptor, but this state may not have been accepted by KV and may not
  conform to the actual state of the cluster. Is this okay, considering we
  don't expect this situation to arise often?

---

<a name="f1">[1]</a>: The [long running migrations][48843] infrastructure
provides the guarantee that intermediate cluster versions (`v21.2-4`,
`v21.2-5`, ...) will only be migrated into once every node in the cluster is
running the new version binary (in our examples that's `v21.2`). Part of
providing this guarantee entails disallowing older version binaries from
joining the cluster. The migration described here will be attached to one of
these intermediate cluster versions. Considering older version SQL pods running
`v21.1`, they don't allow setting zone configs because it was not supported in
that release. `v21.2` SQL pods will only be able to configure zone configs once
the underlying cluster has been upgraded to `v21.2` (having migrated
everything). <sup>[ret](#a1)</sup>

<a name="f2">[2]</a>: Distinguished zcfgs for the meta and liveness ranges have
[pseudo descriptor IDs][pseudo-ids] allocated for them. <sup>[ret](#a2)</sup>

<a name="f3">[3]</a>: We don't want to have the KV zone configs and SQL zone
configs be written to as part of the same txn. Because of where they're stored
(one in the tenant keyspace, one in the host tenant), using the same txn would
mean a txn straddling tenant boundaries. Given that we're writing zcfgs and
scfgs in separate txns, it's hard to do anything better than reconciling
them asynchronously. Say we want to commit zcfgs first, scfgs second, returning
to client only if both are successful ("synchronously"):
(a) if scfgs commit was unsuccessful, we'd want to revert zcfgs. But what if
    the client disconnects? Or the pod shuts down? Now we're back to a committed
    zcfg without the corresponding scfgs. 
(b) if we don’t commit zcfgs first, commiting scfgs instead, what happens if
    the client disconnects/pod shuts down after scfgs have been committed?
    Now we have a scfg from an uncommitted zcfg.
We'd need to do some sort of two-phase commit, persisting zcfgs and scfgs as
"staged" writes and then "commit" them only after both have gone through (i.e.
the number of splits was valid). Sounds complicated. <sup>[ret](#a3)</sup>

[48159]: https://github.com/cockroachdb/cockroach/pull/48159
[48843]: https://github.com/cockroachdb/cockroach/pull/48843
[50377]: https://github.com/cockroachdb/cockroach/pull/50377
[57832]: https://github.com/cockroachdb/cockroach/pull/57832
[58362]: https://github.com/cockroachdb/cockroach/pull/58362
[54254]: https://github.com/cockroachdb/cockroach/issues/54254
[63206]: https://github.com/cockroachdb/cockroach/issues/63206
[65903]: https://github.com/cockroachdb/cockroach/issues/65903
[66063]: https://github.com/cockroachdb/cockroach/issues/66063
[58611]: https://github.com/cockroachdb/cockroach/issues/58611
[la-planning]: https://github.com/cockroachdb/cockroach/blob/692fa83ce377c86cf1b6f865a7583a383c458ce2/pkg/sql/opt_catalog.go#L455-L466
[pseudo-ids]: https://github.com/cockroachdb/cockroach/blob/ce1c68397db8ebc222ed201fef1f9ca92485ddcd/pkg/keys/constants.go#L379-L385
[conf-reports]: https://www.cockroachlabs.com/docs/v21.1/query-replication-reports.html
