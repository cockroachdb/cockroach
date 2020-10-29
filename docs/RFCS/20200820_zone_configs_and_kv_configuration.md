- Feature Name: Decoupled KV Configuration
- Status: draft
- Start Date: 2020-08-20
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Zone Configuration configures KV-level policy for SQL-level objects. The
implementation of zone configuration hinders SQL semantics, causes excessive
overhead, adds implementation complexity, and does not naturally extend to
tenants. This document sets forth a pathway to address these issues by
decoupling the configuration of the key-value store from the set of objects
which might want to configure them.

# Motivation

There are several distinct motivations for this document.

* `SetSystemConfigTrigger` is a bummer for SQL semantics.
    * Cannot perform a DDL after a write in a transaction.
* The `SystemConfigTrigger` is very expensive in the face of frequent changes.
* Caching all of the descriptors in RAM is problematic for schema scalability.
* Propagating all of the descriptors whenever any of them changes is bad.
* The tight coupling of the kvserver to sql-level concepts indicates a problem
in abstraction.
   * Perhaps stands in the way of future extensions to zone configuration we'd
     like to make.
* Tenants complicate the story.
   * We have no way to set sub-tenant zones or split points.
   * This may be the most pressing motivation.
* Code to inspect zone configs is bespoke and awful.
   * GC Job
   * Constraint reports
   * SHOW ZONES
   * Within KV

# Guide-level explanation

## Zone Configuration Today

CockroachDB allows users fine-grained control over certain KV-level
configuraiton related to splitting and merging, GC, and replication decisions.

The [`Expressive Zone Config RFC`](./20160706_expressive_zone_config.md)
contains background on this topic. The key point is that zone configurations
provide configuration which applies almost exclusively at the **kv** layer (
the optimizer sometimes looks at constraints).

The basic idea of the existing infrastructure is that we associate zone configs
with descriptors. The descriptors and the zone configurations are stored in
system tables which reside inside of the system config span. The system config
span is a hard-coded keyspan which contains special system tables. The important
tables are `system.settings`, `system.zones`, `system.descriptor`, and
`system.tenants`. When transactions make changes to tables in this span, they
ensure that the their transaction is anchored on a special key. Then, these
transactions attach a special trigger to their `EndTxnRequest` which will
ensure that when the transaction commits, the system config span is gossipped.

That trigger only actually gossips the data if there are no uncommitted intents
in the span. We added a special trigger when removing an intent to trigger
gossip and ensure progress to deal with cases where an earlier transaction was
open when a trigger event committed.

The protobuf defining a zone config can be found [here][zone config proto].

One critical feature of zone configs today is that they have powerful
inheritence properties. We allow indexes to inherit fields from their tables
and tables to inherit fields from their databases (glaringly we don't do
anything with schemas,
[#57832](https://github.com/cockroachdb/cockroach/issues/57832)). We'll need
to capture that property, otherwise updates will get very expensive. 

### Costs of gossipping the SystemConfigSpan

The trigger leads to `MaybeGossipSystemConfig` being called on the leaseholder
for the system config span. This replica acquires a snapshot of the store and
proceeds to buffer the entire span in memory and then publishes the data as an
encoded slice of kvs.

* Split queue and merge queue are slow, but we rate-limited them so it's less of
  a pressing problem. (TODO(ajwerner): link issue)
* Generally scanning all of the data when any of it changes can be problematic
  if the system config span is large.

## Tenants

In Cockroach 20.2, we introduced the concept of a tenant keyspace. A tenant is
a logical cluster embedded inside of another "system" tenant. All of the data
for a tenant (other than meta), live inside underneath the tenant prefix.

Currently, all of the data underneath tenant prefixes are governed by a single
zone configuration record in `system.zones`. A synthetic descriptor ID has been
allocated to configure that one zone. This means that individual tenants cannot
carry a zone config, let alone sub-zones for elements in a tenant's schema.

## Basic Approach

The basic approach is to leverage rangefeeds to build materialized views of
the relevant information on all of the hosts. One argument against this
approach is that rangefeeds can have a period of time between when a
row is published and when it is determined that the client has a valid snapshot
of the data.

This is an area of active development in CockroachDB as we work to evolve the
closed timestamp subsystem in the context of
[Non-blocking Transactions](https://github.com/cockroachdb/cockroach/pull/52745).

The idea for each of these is that we can leverage rangefeeds plus some either
careful handling to get a value for each range and to make sure that relevant
data and to ensure that the lack of a value be properly handled. The idea is
that we're going to have a centralized repository of data for the entire cluster
and the configuration of its entire keyspace and we'll expose authenticated
APIs to each tenant to update their relevant entries.

This document assumes that while we don't think each store in a large multi-tenant cluster can necessarily hold all of the zone configuration data for the entire
key span on disk, each node should be able to handle receiving all updates. Note
that right now we assume that all nodes can handle the much less compactly
represented configuration for the entire cluster in RAM and can handle
receiving a full copy of this any time any entry changes; we're making a much
more conservative set of assumptions here. For the first pass, we might just
want to cache locally on each store a fully materialized view.

# Reference-level explanation

The end goal is motivated from different 3 different directions:

* Avoid needing to gossip the entire descriptor span to increase schema scale.
* Avoid needing to set the gossip trigger to allow writes prior to DDLs in
  transactions.
* Allow tenants to control some KV level configuration.

Each of these goals could likely be achieved independently by more targeted
mitigation than what is proposed here. However, such solutions would not
rectify the more fundamental issue that gossip is a bad choice for data
which is already committed to the database.

## Detailed design

### High-level pieces

 * Cluster Zone Configuration State
 * KV Zone Configuration API
 * SQL-Level Zone Configuration Reconciliation
 * KV Server Configuration Watcher

## KV Zone Config API

In the multi-tenant world, we've added RPC's to the [`Internal` Service
](https://github.com/cockroachdb/cockroach/blob/597e4a8c487e3c23d64885563d608a692b59055c/pkg/roachpb/api.proto#L2219). This
document proposes adding some more:

```protobuf

// SpanConfig represents an entry for a span of data that points to 
// a configuration.
message SpanConfig {
  Span span = 1;
  config_id int64 = 2;
}

// ZoneConfigEntry configures a set of data and potentially inherits from a
// parent.
message ZoneConfigEntry {
  id     int64 = 1;
  parent int64 = 2;
  depth  int64 = 3;
  config zonepb.ZoneConfig 
}

message GetZoneConfigRequest {
  int64 tenant_id

  // The response will contain all the SpanConfigs covered by the requested
  // spans and all of the requested ZoneConfigEntry values they refer to as
  // well as any ZoneConfigEntry values requests specifically of any of their
  // parents recursively.
  repeated Span spans;
  repeated int64 configs;

  // Is this too complicated? Should we just return them all?
}

message GetZoneConfigResponse {
  int64 tenant_id = 1;
  
  repeated SpanConfig spans = 2;
  repeated ZoneConfigEntry configs = 3;
}

message UpdateZoneConfigRequest {
  int64                          tenant_id = 1;
  repeated SpanConfig      spans_to_delete = 2;
  repeated int64           zones_to_delete = 3;
  repeated ZoneConfigEntry zones_to_upsert = 4;
  repeated SpanConfig      spans_to_upsert = 5;

  // This may be too complicated, perhaps just whole-sale update
  // though that makes me a tad sad. 
}
```

On some level, this API assumes that we can store all of the zone config
information for a given tenant in RAM on the tenant node. I do not think that
this is so crazy. Likely, even with some crazy partitioning and indexing scheme
this is 10s of bytes per partition, tops. There's an open question about
compacting the representation further by de-duplicating shared zone config
values. I think the zone configs in this representation fitting in ram is sane
because we don't have the tools to think about millions of ranges and the idea
of millions of ranges per tenant feels hilarious. 

The idea as proposed in this document is for the KV server to then run the SQL
logic to update these tables. This is a bit weird. Another option is to have
the SQL servers run the library code and figure out how to state the relevant
security policy for the portions of these tables. 

One primary concern is the threat model around abuse. My guess is that bugs in
the KV server are just as likely as bugs in the SQL server so it's really about
a compromised SQL pod being able to cause trouble. 

See the discussion below about not even putting this data into the system
keyspace.

One really important note here is that if we're going to give IDs to each
subzone then we need to construct tenant-unique IDs for each index and index
partition. This seems like a really good idea regardless for indexes. For
partitions it is somewhat less clear. It also starts to run us afoul of the
issues related to the 32 bit IDs in postgres and it being hard to know when it
is safe to use an ID again.

### Cluster Zone Configuration State

We're going to store the data for the zone configuration is two system tables
in the system tenant. These two tables are: `system.zone_configurations` and
`system.span_configurations`.

```sql
CREATE TABLE zone_configs (
  tenant_id INT,
  id        INT,
  parent_id INT,
  depth     INT, -- may want to limit, the query is going to be funny. 
  config    BYTES, -- encoded proto
  PRIMARY   KEY (tenant_id, id);
  FOREIGN   KEY (tenant_id, parent_id) REFERENCES zone_configs(tenant_id, id);
);

CREATE TABLE span_configs (
    start_key     BYTES NOT NULL,
    end_key       BYTES NOT NULL,
    tenant_id     INT NOT NULL,
    config_id     INT NOT NULL,
    FOREIGN KEY   (tenant_id, config_id) REFERENCES zone_configs(tenant_id, id) ON DELETE RESTRICT,
    PRIMARY KEY   (start_key)
);
```

#### Limits

We may be worried that one tenant can just create an absolutely crazy number
of spans because we can, in principle, if they ask us to, split at arbitrary
points. It's like then to add some limits that we update transactionally when
mucking with tenants. We've done this in the past and the experience has been
so-so. Picking limits is hard but not having them is bad. Let's also add
something like this:

```sql
CREATE TABLE zone_config_meta (
  tenant_id INT,
  num_spans INT,
  span_bytes INT,
  num_configs INT,
  config_bytes INT,
)
```

We can update this every time we update the zone configs.

##  SQL-Level Zone Configuration Reconciliation

The basic idea is that we'd pick a node in the cluster to scan the schema and
see what zone configs it implies and then reconcile. If we're going to make
these rich APIs for controlling this state then the KV server should deal with
running its own transactions. 

There's a lot left undefined here and a lot of UX to be worked out.

## KV Server Configuration Watcher

The idea is that each node will establish a rangefeed over the two tables.
It will also keep an on-disk cache of some of the state. The main idea is that
it'll maintain a cache for the configs for all tenants for which it has ranges.

TODO(ajwerner): fill in this sketch some more.

## Drawbacks

It's not simple.

## Rationale and Alternatives

### Attach zone configurations to range state

Problems:
 * writing becomes an O(ranges) proposition.
 * indicating split points is awkward.
 * merge semantics are awkward.

### Put the zone configs within the tenant, teach the KV server to go read them

One thing that this current proposal does is that it ensures that each store
always has some view of the system config for the entire keyspace. This is
potentially expensive. We may limit the number of bytes each tenant gets to, say
1MiB. This is probably plenty but it does mean that if we have 1M tenant then
we'll have 1TiB of system config data. That would be *way* too much. However,
probably each node won't have that many tenants, and there are probably good 
reasons to pack tenants into nodes (see copysets). Maybe it'd be fine to have
a rangefeed per tenant and just generally eschew the whole centralization
argument. 

It's very plausible that this is actually the way to go.

## Unresolved questions

* Is this too tightly coupled to tenants?
  * Tenants are a big motivating problem here, but what if next week, we decide
    we want to add yet another layer of virtualization where we virtualize a
    multi-tenant cluster underneath some other new concept?
     * Should we make up new terminology for the namespacing?
     * Seems like it's over-complicating this all, let's let somebody deal
       with that refactor in another 5-7 years.
* Given we're not putting this logic into the tenant keyspace, how do we make
  sure tenants don't go crazy?
* How long will it take from the commit of a value in the KV store to a closed
  snapshot in a materialized view?
   * Right the threshold is seconds. Is that too long?
   * We're talking about much shorter thresholds for some ranges.
* Is it okay to use a view that is not a snapshot of the data?
   * Probably a great thing.
* Inheritence seems important, how do we reason about it? Did I get it right?
* Do we want to limit what fields or values that a tenant can set?
   * In the past, there's been some talk of only permitting a fixed menu
     of zone configurations. That wouldn't be a huge departure from this
     proposal: it'd mostly involve ripping out the tenant provide config
     values.
   * There's UX questions to be filled in here about how to make that all work.
* Is it worth a third layer of indirection? Given we're getting rid of subzones
  (thank the lord), the zone config struct is very likely to end up being
  repeated many many times at the leaf of the graph in the cases where representing
  it compactly matters the most. This is true even across tenants! What if in
  the inheritence scheme, we end up pointing to zone config values by hash or
  something like that? 
* How do we deal with getting unique IDs for subzones?
  * Are we going to have problems if we run out of 32 bits?
* How do protected timestamps fit in here? Should they have a parallel scheme?
  * Sort of feels like it. 