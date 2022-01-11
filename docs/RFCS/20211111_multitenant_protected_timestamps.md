- Feature Name: Multi-tenant Protected Timestamps Support
- Status: draft
- Start Date: 2021-11-11
- Authors: Arul Ajmani, Aditya Maru
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #73727


# Summary

The protected timestamp subsystem (PTS) provides a mechanism to prevent data from getting garbage 
collected. In its current form, the system writes records to the protected timestamp system table 
which is consulted by the GC queue when making garbage collection decisions for a span. The PTS 
subsystem does not work for secondary tenants because GC runs on the host tenant, and as such, only 
consults the host tenant's PTS table.

This RFC proposes to rework the [PTS subsystem](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20191009_gc_protected_timestamps.md)
to enable its use in secondary tenants. This allows its current consumers (backup, cdc) to leverage 
protected timestamps in multi-tenancy as they do today in dedicated clusters, thus bringing us closer
to feature parity. We also propose expressing PTS records using schema concepts instead of static 
spans, to unlock new backup functionality for both the host tenant and secondary tenants.

The rework of the PTS subsystem proposed here builds on top of the newly introduced 
[span configuration infrastructure](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210610_tenant_zone_configs.md) 
as the transport layer. The RFC proposes how the existing span configuration infrastructure can be 
tweaked to enable the host tenant to lay protected timestamps for secondary tenants without writing 
keys into the tenant’s keyspace.

## Table of contents

- [Motivation](#motivation)
- [Technical Design](#technical-design)
  - [Overview](#overview)
  - [Semantics Of Protecting A Timestam](#semantics-of-protecting-a-timestamp)
  - [Detailed Design](#detailed-design)
    - [Data Model](#data-model)
    - [SQL Changes](#sql-changes)
      - [PTS Record](#pts-record)
      - [PTS Storage](#pts-storage)
      - [SQLWatcher](#sqlwatcher)
      - [SQLTranslator](#sqltranslator)
      - [Reconciler](#reconciler)
    - [KV Changes](#kv-changes)
      - [KVAccessor](#kvaccessor)
      - [SystemSpanConfigStore](#systemspanconfigstore)
      - [KVSubscriber](#kvsubscriber)
    - [Migration](#migration)
    - [Limits](#limits)
    - [Differences from the Protected Timestamp Subsystem V1](#differences-from-the-protected-timestamp-subsystem-v1)
      - [Verification](#verification)
  - [Future Work](#future-work)
    - [Exclusion](#exclusion)
  - [Rationale and Alternatives](rationale-and-alternatives)
  - [Storing protected timestamp records in system.protected_ts_records vs. on zone configurations](#storing-protected-timestamp-records-in-systemprotected_ts_records-vs-on-zone-configurations)
  - [Building a fully generalized hierrarchy for span configurations](#building-a-fully-generalized-hierrarchy-for-span-configurations)


# Motivation

The primary consumers of the protected timestamp subsystem today are scheduled and non-scheduled 
backups, and CDC. These long running operations lay protected timestamps to ensure that the keys 
they operate on are not GC-ed while the job is executing. Unfortunately, because of the lack of 
protected timestamp support for secondary tenants, these operations are rather brittle when run from 
secondary tenants. This requires the operator to either run with a high enough GC TTL or accept 
transient failures, neither of which is ideal. Building feature parity for protected timestamps 
between the host and secondary tenants allows secondary tenants to circumvent this and take 
advantage of many of the same benefits that motivated the original protected timestamps subsystem.

In addition to building feature parity, the new protected timestamp subsystem aims to express PTS 
records in terms of schema objects instead of static keyspans.This allows users of the subsystem to 
lay protected timestamp on (say) databases and have them take effect on all tables inside the database 
(including those created in the future!). This is quite powerful as it allows us to 
[decouple scheduled backups with revision history from the GC TTL] (https://github.com/cockroachdb/cockroach/issues/67282) 
by [chaining PTS records](https://github.com/cockroachdb/cockroach/pull/68446). The proposal here 
enables this for database schedules as well, thus allowing us to drop down the long standing cluster
wide default GC TTL set to 25 hours.

The introduction of hierarchy in KV for span configurations is necessitated by the desire for the host 
tenant to lay protected timestamps on one or all secondary tenants’ keyspace. This is required when 
the host tenant performs full tenant or full cluster backups, respectively. We also want to allow a 
“fast path” for secondary tenants to set protected timestamps on their entire keyspace with no write
amplification.

# Technical Design
## Overview

The design here builds on top of the original [protected timestamp subsystem](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20191009_gc_protected_timestamps.md). 
The detailed design that follows below proposes changes to the transport of PTS records and the 
interaction points for GC. The mechanism by which the old subsystem ensured protection semantics is 
much the same. Refer to the original RFC for it.

We aim to use the span configuration infrastructure, introduced to enable multi-tenant zone 
configurations (see [rfc](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20210610_tenant_zone_configs.md) 
and [existing interfaces](https://github.com/cockroachdb/cockroach/blob/master/pkg/spanconfig/spanconfig.go)),
as the transport layer for shipping protected timestamp information from secondary tenant’s keyspace into the host tenant.

The span configuration infrastructure allows secondary tenants to store config information in their 
keyspace which is asynchronously reconciled with the state stored in the host tenant using RPCs on 
the `Connector`. KV then uses its view of the reconciled state to make various split, merge, and 
rebalancing decisions. In line with this SQL/KV separation, the design below includes two orthogonal
proposals, each with its own set of alternatives, to make protected timestamps work with multi-tenancy:
- SQL changes to store protected timestamp state and reconcile it with KV.
- KV changes to the span configuration data-model to support a hierarchy and integration with the GC queue.

Following the description of the design we call out some differences between v1 and v2 of the PTS 
subsystem, future work in this area, and alternatives considered.


## Semantics of Protecting a Timestamp

The new PTS subsystem will operate on schema objects instead of a list of static spans. Schema 
objects form the catalog hierarchy (“cluster”->database->schema->table->index->partition). Leaf 
nodes in this hierarchy, such as tables, indexes, and partitions, are associated with actual spans 
written in KV. Non-leaf nodes, such as “cluster”, database, and schema, are simply used for 
namespacing.


Protected timestamps are no longer a flat structure with this change; instead, laying a protected 
timestamp on any non-leaf node in the catalog hierarchy will cascade to all leaves in its subtree. 
Any future leaves in the subtree will also be protected until the record is removed.

The original RFC provided an invariant in terms of the transaction that wrote the PTS record. The 
asynchronous nature of span configuration transport necessitates a change to the invariant, 
expressed in terms of the transaction that writes the span configuration to KV. Specifically, the 
new invariant is:

```
If the transaction writing the span configuration (with the protected timestamp record) commits at 
timestamp t, and none of the data in the spans corresponding to the span configuration has expired 
at t, then the span will be protected until the record is removed from the configuration. 
```

The new invariant is slightly unsatisfying for users of the subsystem because there can be an 
arbitrary amount of time between writing the PTS record and span configuration reconciliation. 
Even still, the transaction writing the span configuration is only causally related to the actions 
of the `Reconciler`. But, similar to v1 of the subsystem, we still attempt to capture that replicas 
may have an arbitrarily stale view of their span configurations (and thus their GC TTL, protectedtTS 
state) when performing GC.

Lastly, it's worth calling out that even though KVs view of protected timestamp state for a tenant 
may be arbitrarily stale, it *will* be a consistent view that was valid at some timestamp in the 
past.

## Detailed Design


### Data Model

Protected timestamp information will be stored on `SpanConfigs` by minimally changing the proto as follows:

```protobuf
// SpanConfig holds the configuration that applies to a given keyspan.
message SpanConfig {
  option (gogoproto.equal) = true;
 
  ...
 
  // ProtectedTimestamps is a list of timestamps which are protected from being
  // GC-ed.
  repeated util.hlc.Timestamp protected_timestamps = 10  [(gogoproto.nullable) = false];
}
```

We will also carve out three forms of special keys that the host tenant and secondary tenants can 
use to write protected timestamp metadata. The spans corresponding to these special keys will be 
known as `SystemSpans` and their associated configurations will be called `SystemSpanConfigs`. 
These special keys will be of the form:

- `system/scfg-pts/host/all`: Written to by the host tenant to lay a protected timestamp over its 
entire keyspace (including all secondary tenants).
- `system/scfg-pts/host/ten/<ten_id>`: Written to by the host tenant to lay a protected timestamp 
over the entire keyspace of a tenant with the given tenant ID.
- `system/scfg-pts/ten/<ten_id>`: Written to by the host tenant on behalf of a secondary tenant 
- that wishes to lay a protected timestamp on its entire keyspan.

`SystemSpans` and their associated `SystemSpanConfigs`  will be stored in the 
`system.span_configurations` table. `SystemSpanConfigs` will only include protected timestamp 
information and will form a fixed hierarchy for protected timestamp state – the protected timestamp 
state for a span is what is stored on its span configuration combined with the protected timestamp 
state stored in the (at most) 3 `SystemSpanConfigs` that apply to the span (constructed using the 
tenant ID of the tenant that the span belongs to). We’ll add the following protos:

```protobuf
message SystemSpanConfig {
  option (gogoproto.equal) = true;

  // ProtectedTimestamps is a list of timestamps which are protected from being
  // GC-ed.
  repeated util.hlc.Timestamp protected_timestamps = 1  [(gogoproto.nullable) = false];
}

// SystemSpanConfigEntry is a SystemSpan and its corresponding SystemSpanConfig.
message SystemSpanConfigEntry {
  Span systemSpan = 1 [(gogoproto.nullable) = false];

  // SystemSpanConfig is the config that applies to the system span. Only the
  // protected timestamp attribute may be set on the config.
  SystemSpanConfig systemSpanconfig = 2 [(gogoproto.nullable) = false];
}
```

### SQL Changes

This includes changes we will make to the PTS record itself, interaction points users of the subsystem 
leverage, and reconciliation of PTS metadata. Much of this is motivated by the desire to express 
protection in terms of schema objects as opposed to static spans.

#### PTS Record
To begin with we describe the changes to the protected timestamp record proto. Notably, the record 
switches from protecting spans to protecting a `target` that represents one of cluster, tenants or 
schema objects (databases/tables).

```protobuf
// Record corresponds to a protected timestamp.
message Record {
 ...
 repeated roachpb.Span deprecated_spans = 7;
 // Target holds information about what this Record protects. The Record can
 // either protect the entire cluster, a subset of tenants, or individual
 // schema objects (database and table).
 Target target = 8;
}

// Target is the format of the message encoded in the target column of the
// system.protectedts_records table.
message Target {
  message SchemaObjectsTarget {
    // IDs are the descriptor IDs of the schema objects being protected by this
    // Record. This field will only contain database and table IDs.
    repeated uint32 ids = 1 [(gogoproto.customname) = "IDs",
      (gogoproto.casttype) = "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb.ID"];
  }

  message TenantsTarget {
    // IDs correspond to the tenant keyspaces being protected by this Record.
    repeated roachpb.TenantID ids = 1 [(gogoproto.customname) = "IDs",
      (gogoproto.nullable) = false];
  }

  message ClusterTarget {
    // ClusterTarget indicates that all SQL state in the cluster is being
    // protected by this Record. This includes all user defined schema objects,
    // as well as system tables used to configure the cluster. In a system
    // tenant this target will also protect all secondary tenant keyspaces that
    // exist in it.
    //
    // Today, this target is only used by cluster backups.
  }

  oneof union {
    SchemaObjectsTarget schema_objects = 1;
    TenantsTarget tenants = 2;
    ClusterTarget cluster = 3;
  }
}
```

Consumers of the subsystem can create records to interact with the `protectedts.Storage` interface 
below. Consumers are expected to hold on to the UUID of the PTS record for subsequent interactions 
with `protectedts.Storage`.


#### PTS Storage

Users of the protected timestamp subsystem will continue to interact with the subsystem using the 
`Storage` interface. It will minimally change to the following:

```go
type Storage interface {
  // Protect will durably create a protected timestamp record ...
  Protect(context.Context, *kv.Txn, *ptpb.Record) error
  // Release allows spans of the schema object(s) which were previously protected   
  // to now be garbage collected.
  //
  // ...
  Release(context.Context, *kv.Txn, id uuid.UUID) error
  // UpdateTimestamp updates the timestamp protected by the record with the
  // specified UUID. The updated timestamp must be greater than the previous  
  // timestamp protected by the record. 
  UpdateTimestamp(ctx context.Context, txn *kv.Txn, id uuid.UUID, timestamp   
  hlc.Timestamp) error
}
```

Internally, we will continue to store the PTS records in the `system.protected_ts_records` table. 
We will add a bytes `target` column alongside the existing `spans` column, to store the `ptpb.Target`
protobuf which will describe what object the record is protecting.

Protecting and releasing a record will work much the same way as it does today by inserting and 
deleting records from the system table.

#### SQLWatcher

The SQLWatcher will maintain a rangefeed over `system.protected_ts_records` in addition to the 
rangefeeds it maintains over `system.zones` and `system.descriptors`. The interface changes to the 
following:

```go
// ProtectedTSUpdate captures the target of a protectedTS record that has been
// updated.
type ProtectedTSUpdate struct {
  target ptpb.Target
}

// SQLWatcher watches for events on system.zones, system.descriptors, and
// system.protected_ts_records.
type SQLWatcher interface {
  // WatchForSQLUpdates watches for updates to zones, descriptors, and the
  // protectedTS state starting at the given timestamp (exclusive), informing
  // callers periodically using the given handler and a checkpoint timestamp.
  //
  // ...
  WatchForSQLUpdates(
    ctx context.Context,
    startTS hlc.Timestamp,
    handler func(
      ctx context.Context,
      descUpdates []DescriptorUpdate,
      protectedTSUpdates []ProtectedTSUpdate,
      checkpointTS hlc.Timestamp,
    ) error,
  ) error
}
```

Similar to today, `WatchForSQLUpdates` will be used to perform incremental reconciliation, including
that of the protectedTS state with the introduction of `protectedTSUpdates`.

#### SQLTranslator

The `SQLTranslator` will continue to be responsible for translating SQL state to Span Configurations.
It will assume the responsibility to translate protected TS information stored in 
`system.protected_ts_records` to `SpanConfigs` as well.

We first introduce the `SystemPTSRecordTableReader` interface which will be used by both the 
`SQLTranslator` and the `Reconciler` to construct `SpanConfig` and `SystemSpanConfigs` respectively.
Its implementation will be transaction scoped.

```go
// SystemPTSRecordTableReader is a transaction scoped in-memory representation
// of the system.protected_ts_records table.
type SystemPTSRecordTableReader interface {
  // GetProtectedTimestampsForCluster returns all the protected timestamps that
  // apply to the entire cluster's keyspace.
  GetProtectedTimestampsForCluster() []hlc.Timestamp
  // GetProtectedTimestampsForTenant returns all the protected timestamps that
  // apply to the entire tenant's keyspace with the given ID. Only the host
  // tenant is allowed to view another tenant's protected timestamps.
  GetProtectedTimestampsForTenant(tenantID roachpb.TenantID) []hlc.Timestamp
  // GetProtectedTimestampsForSchemaObject returns all the protected timestamps
  // that apply to the descID's keyspan.
  GetProtectedTimestampsForSchemaObject(descID descpb.ID) []hlc.Timestamp
}
```

As the `SystemPTSRecordTableReader` is transaction scoped and we want to use the same transaction to 
construct `SystemSpanConfigs` and `SpanConfigs` when doing an incremental reconciliation, the 
`SQLTranslator` interface changes as follows:

```go
// SQLTranslator translates SQL descriptors and their corresponding zone
// configuration / PTS state to constituent spans and span configurations.
//
// ...
type SQLTranslator interface {
  // Translate generates the span configuration state given a list of
  // {descriptor, named zone} IDs ...
  Translate(
    ctx context.Context,
    ids descpb.IDs,
    txn *kv.Txn,
    descsCol *descs.Collection,
    ptsTableReader SystemPTSRecordTableReader,
  ) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error)

  // FullTranslate translates the entire SQL state to the span configuration
  // state (including system span configurations). The timestamp at which such a
  // translation is valid is also returned.
  FullTranslate(ctx context.Context) (
    []roachpb.SpanConfigEntry, []roachpb.SystemSpanConfigEntry, hlc.Timestamp, error,
  )
}
```


#### Reconciler

The `Reconciler` is responsible for reconciling SQL implied span configuration state with what is 
actually stored in KV. It does so by performing a full reconciliation (if required) and incremental 
reconciliation subsequently. This happens by stitching the `SQLTranslator`, `SQLWatcher`, `Store`, 
and `KVAccessor` interfaces together.

Full reconciliation does not change much with the introduction of `SystemSpanConfigs` -- much of the
heavy lifting will be continue to be done by the `FullTranslate` method of the `SQLTranslator`. In 
addition to using the `Store` interface to find diffs between intended and actual state, we will use
the `SystemSpanConfigStore` (see KV changes below) to find diffs for `SystemSpanConfigs`.

When reconciling incrementally, the `Reconciler` will sniff out `ProtectedTSUpdates` fed to it from 
the `SQLWatcher` that apply to the entire cluster or a specific tenant. These will be converted to 
`SystemSpanConfigs` using the `SystemPTSRecordTableReader` interface. IDs corresponding to 
descriptor updates will continue to be fed to the `SQLTranslator` along with the same 
`SystemPTSRecordTableReader` that was used to generate `SystemSpanConfigs`. By using the same 
transaction in the `SQLTranslator` that was used to intialize the `SystemPTSRecordTableReader` we 
will ensure that the protected timestamp state update to KV is represents a consistent view from the
tenant's perspective.

### KV Changes

This includes the changes we will make to introduce a hierarchy using `SystemSpanConfigs` and all the
interaction points with the GC queue that will replace the `protectedts.Cache` from v1 of the subsystem.

#### KVAccessor

We will modify the existing RPCs on the `KVAccessor` to account for `SystemSpanConfigs` as follows:

```go
// GetSpanConfigsRequest is used to fetch the span configurations over the
// specified keyspans. It also returns the system span configurations that have
// been set by the tenant.
type GetSpanConfigsRequest struct {
  // Spans to request the configurations for. The spans listed here are not
  // allowed to overlap with one another.
  Spans []Span `protobuf:"bytes,1,rep,name=spans,proto3" json:"spans"`
}

// GetSpanConfigsResponse lists out the span configurations that overlap with
// the requested spans and the system span configurations that have been set
// by the tenant.
type GetSpanConfigsResponse struct {
  ...
  // SystemSpanConfigEntries captures the system span configurations that have
  // been set by the tenant.
  SystemSpanConfigEntries []SystemSpanConfigEntry `protobuf:"bytes,2,rep,name=system_span_config_entries,json=systemSpanConfigEntries,proto3" json:"system_span_config_entries"`
}

// UpdateSpanConfigsRequest is used to update span configurations and system
// span configurations.
// 
// ...
type UpdateSpanConfigsRequest struct {
  ...
  // SystemSpanConfigsToDelete captures the system spans to delete configs for.
  SystemSpanConfigsToDelete []Span `protobuf:"bytes,3,rep,name=system_span_configs_to_delete,json=systemSpanConfigsToDelete,proto3" json:"system_span_configs_to_delete"`
  // SystemSpanConfigsToUpsert captures the system span configurations we want to
  // upsert with.
  SystemSpanConfigsToUpsert []SystemSpanConfigEntry `protobuf:"bytes,4,rep,name=system_span_configs_to_upsert,json=systemSpanConfigsToUpsert,proto3" json:"system_span_configs_to_upsert"`
}
```

The `SystemSpanConfig` related fields in `UpdateSpanConfigsRequest` will serve as a "fast path" for 
secondary tenants to reconcile protected timestamps laid on their entire keyspace without updating 
`SpanConfig`s for all their ranges. The host tenant can use these fields to reconcile protected 
timestamps laid on specific or all secondary tenants' keyspace(s).

The `SystemSpanconfig` related fields in the `GetSpanConfigsResponse` will be used during full 
reconciliation to issue diffs between the SQL intended protectedTS state and KVs understanding of it.

We choose to modify the existing RPCs instead of adding new ones to specifically deal with 
`SystemSpanConfigs` as doing so allows us to read/write both `SystemSpanConfigs` and `SpanConfigs` 
in the same transaction. This allows us to ensure that KV always sees a consistent view of the 
protected timestamp state (which is a combination of fields on `SpanConfigs` and `SystemSpanConfigs`).


#### SystemSpanConfigStore

The `SystemSpanConfigStore` will be an in-memory representation of all `SystemSpanConfig`s and will 
be populated by the `KVSubscriber`. It will be consulted by the `KVSubscriber` to synthesize 
protected timestamp information when queried for a span's `SpanConfig`.

The `Reconciler` will also maintain the `SystemSpanConfigStore` so that updates via the KVAccessor 
can be diffed against the KV state during partial/full reconciliation.

```go
// SystemSpanConfigStore is a datastructure that stores SystemSpans and their
// corresponding SystemSpanConfigurations.
type SystemSpanConfigStore interface {
  // Apply applies a batch of unique updates and returns (i)
  // returns (i) the existing SystemSpans corresponding to SystemSpanConfigs
  // that were deleted, and (ii) the entries that were added.
  Apply(ctx context.Context, updates ...roachpb.SystemSpanConfigEntry) (
    deleted []roachpb.Span, added []roachpb.SystemSpanConfigEntry,
  )
  // Combine takes the SpanConfig that applies to a given span with any
  // SystemSpanConfigs that may apply to the span and returns the result.
  Combine(ctx context.Context, entry roachpb.SpanConfigEntry) roachpb.SpanConfig
}
```

#### KVSubscriber

The `spanconfig.KVSubscriber` maintains a rangefeed over `system.span_configurations`, buffers 
rangefeed events received, flushes them at consistent snapshots, and applies the changes to an 
in-memory representation of the span configuration state. With the introduction of `SystemSpanConfigs`
and the `SystemSpanConfigStore`, the KVSubscriber will not apply all updates to the `spanconfig.Store`.
It will instead sniff out `SystemSpan` updates and apply them to the `SystemSpanConfigStore` instead.

The `spanconfig.KVSubscriber` allows the `kvserver.Store` to register handlers which are invoked with
a keyspan whenever the span’s config changes. If the `SystemSpan` that applies to every range of a 
particular tenant changes we will invoke all registered handlers with the tenant span. Similarly, if
the `SystemSpan` that applies to the entire keyspace changes, we will invoke all handlers with the 
`EverythingSpan`.

The `KVSubscriber` will maintain a notion of freshness by keeping track of the frontier timestamp of
the rangefeed on `system.span_configurations`. This determination will be used to make GC decisions 
similar to how v1 of the subsystem used the freshness of the `protectedts.Cache`. The new 
KVSubscriber interface changes to the following:
```go
// KVSubscriber presents a consistent snapshot of a StoreReader and 
// ProtectedTSReader interface that's incrementally maintained with changes made to 
// the global span configurations state (system.span_configurations).
//
// ...
type KVSubscriber interface {
  StoreReader
  ProtectedTSReader
  Subscribe(func(updated roachpb.Span))
}
```

We will introduce the `ProtectedTSReader` interface to allow the GC queue to query protected 
timestamp information from the `KVSubscriber`. The `ProtectedTSReader` interface will also be useful
for the migration of the subsystem as we will store protectedTS information in both 
`system.protected_ts_records` and `SpanConfigurations` in 22.1 (more details in the migration 
section below). We can use it to synthesize protectedTS information by taking the minimum timestamp 
protected in the `KVSubscriber` and the `protectedts.Cache` for a span. In a future release (22.2), 
the `KVSubscriber` will become the only implementer of this interface and we can get rid of the `protectedts.Cache` entirely.

```go
// ProtectedTSReader is the read-only portion for querying protected 
// timestamp information. It doubles up as an adaptor interface for
// protectedts.Cache.
type ProtectedTSReader interface {
  // GetEarliestRecordAboveThreshold examines all protection timestamps that apply to 
  // the given keyspan and returns the least timestamp that is above the supplied  
  // threshold.
  GetEarliestProtectionTimestampAboveThreshold(ctx context.Context, sp roachpb.Span,    
    threshold hlc.Timestamp) (protectionTimestamp hlc.Timestamp, asOf hlc.Timestamp)
}
```


### Migration

The following migrations are needed to switch over to the new protected timestamp subsystem in the 
order specified below.

1) Turn on the span configuration infrastructure and enable reconciliation for tenants.
2) Alter the `system.protected_ts_records` table to add a bytes `target` column, that stores the 
encoded protocol buffer describing what a record protects. [1]


Once the above migration runs, we can stop persisting the `spans` of any new `ptpb.Record` in the 
`system.protected_ts_records` table. This way all calls to `Protect` after the migration has run 
will only populate the `target` column . To ensure jobs in a mixed version state do not fail, for 
the entirety of the 22.1 release, we will consult both the old subsystem and the new subsystem to 
make GC decisions – we’ll simply use the minimum timestamp protected on a PTS record in the 
`protectedts.Cache` or `SpanConfiguration` for a given span. The GC queue will interface with a 
`ProtectedTSReader` which will make this determination.

[1] Although protected timestamps did not work for secondary tenants prior to the changes proposed
in this RFC, the `system.protected_ts_records` table does exist in their keyspace. It is simply 
never read from or written to by internal consumers.

### Limits

The previous version of the PTS subsystem enforced limits by tracking usage in 
`system.protected_ts_meta`. This table was consulted before a PTS record was written. The new 
protected timestamp subsystem will continue to enforce the bytes limit that caps the total byte size
of PTS records written to the `system.protected_ts_records` table. We will however, no longer use
the limit on the maximum number of spans that can be protected, since PTS records no longer operate 
on spans but on schema objects. In addition to this we will want to enforce limits when reconciling 
through the `KVAccessor`. The exact semantics of limit checking are TBD but much of the work here 
will tack on to [what we do for span configurations](https://github.com/cockroachdb/cockroach/issues/70555).

### Differences from the Protected Timestamp Subsystem V1

#### Verification

v1 of the protected timestamp subsystem provided “verification” semantics to validate the protected 
timestamp record. If verification succeeded then the consumer could rely on all the data being
present and protected as of (and after) the timestamp the record was protecting until the record was
removed. This was particularly important for IMPORT INTO to rollback to a pre-import state in case 
of job failure. Import no longer relies on protected timestamps for reasons outlined 
[here](https://github.com/cockroachdb/cockroach/pull/61004).

In this RFC we propose to not provide verification semantics given the complexity that comes with 
supporting this in a multi-tenant model. The consumers of the protected timestamp subsystem, i.e.
backup and CDC will both fail non-destructively in case the “best effort” protection fails to protect
all data in the schema object from GC, which is not expected to be the common case.

For backup, the ExportRequest for the replica that attempts to read below the GC threshold will fail,
and the backup will need to be retried.

For CDC, we first note that changefeeds currently only verify the first protected timestamp created
for the initial scan. Protected timestamps created at schema change boundaries and during pause
requests are not verified. Thus, many of the issues discussed below are issues in the current system
as well.

In the case of initial scans, changefeeds protect a timestamp close to the current timestamp. Thus, 
a user would have to have a very low gc.ttl to see a verification failure in the common case. It is possible that  such a failure could be seen if a changefeed job fails before the initial scan and the job system does not retry it for a considerable amount of time.

In the case of schema change boundaries, we are typically protecting a timestamp close to the current
timestamp. If the feed is far enough behind that it is at risk of a verification failure, then this
feed was already in danger of failing irrecoverably.

In the case of `protect_data_from_gc_on_pause`, lack of verification could result in a paused
changefeed that the user assumes they can resume that they cannot, in fact, resume. While unfortunate,
such a changefeed was likely close to failure since its highwater mark must have been close to the 
gc.ttl. We note that future work to regularly install protected timestamps throughout the changefeed's
run would further reduce this risk.

The tradeoff here is that we give up a mode of failing jobs fast. In practice, we do not anticipate
this to be a problem since CDC mostly protects at `time.Now()` and with chaining of protected 
timestamps across backups, backup will also follow a similar pattern in the future. Thus, we should 
rarely be running close to the GCThreshold when writing the protected timestamp record.


## Future Work

### Exclusion

Today, we have a few system tables that run with a low TTL. Users too have tables that have 
high-churn, ephemeral data for which they would like to maintain low TTLs. Jobs are long running 
operations and so a schema object such as a cluster or database can be protected for hours before 
the job completes. This will prevent GC of revisions putting stress on other parts of the system. 
There should be a mechanism to exclude such ephemeral schema objects from protection, when protecting
a parent schema object. This is outside the scope of this RFC but will be explored in this one 
[pager]( https://docs.google.com/document/d/1RmNcGnjL0e9_zL4wiBQFgqIeS5H9--O3EyZkME87y8c/edit?usp=sharing).


## Rationale and Alternatives

### Storing protected timestamp records in `system.protected_ts_records` vs. on zone configurations


The proposal above suggests continuing to store protected timestamps in a separate system table. An 
alternate approach that was considered was to store the record on the zone configuration associated 
with the schema object that it is protecting. This was motivated by the desire to express PTS records
as a function of schema object IDs instead of static spans.

Storing records on zone configs was attractive since the SQLTranslator has logic to hydrate a zone 
configuration by walking up its hierarchy, and subsequently generating its span configuration. We 
could reuse much of this hydration logic to aggregate the PTS records written to the zone configs as
it provides us with an implicit `schema object ID -> PTS` record mapping. A PTS record on the entire
cluster (during cluster backups) could be written to `RANGE DEFAULT`, and everything would work out 
of the box! The main deterrent to this approach was that it went against the nature of fields stored
in the zone configuration. While all other fields in the zone configuration are user specified, the 
modification of the protected timestamp field would be limited to the internal subsystem. Continuing
to write records to the system table also means that much of the `protectedts.Storage` logic remains
unchanged which inturn simplifies the migration from the old subsystem to the new one.

### Building a fully generalized hierrarchy for span configurations

The proposal above suggests building a fixed form hierarchy for span configurations by reserving 
special keyspans. This allows us to write and combine protected timestamp information that applies to
all ranges of a particular tenant (or all tenants). Span configurations, as reconciled from zone 
configurations by tenants, continue to be flat.

Alternatively, we could eschew this flat structure by introducing a notion of inheritance for span 
configurations similar to zone configurations and shipping the zone configuration hierarchy into KV 
instead of flattening it out. We would do so by introducing a layer of indirection between the 
`span -> SpanConfig` mapping using a `ConfigID`. We would change `system.span_configurations` to 
store the `Span->ConfigID` mapping and introduce a new system table (`system.configurations`) to 
persist the `ConfigID->Config` mapping. The `KVSubscriber` could then maintain a rangefeed on both 
these tables to watch for changes to the `SpanConfig` state and materialize them into an in-memory
datastructure that can be consulted by the various KV queues.

To allow the host tenant to lay protected timestamps on a secondary tenant or all secondary tenants 
without stradling tenant boundaries, the motivation behind the fixed form hierarchy the RFC proposes,
we would require all secondary tenant reconciled span configurations to be parented under a 
predetermined static ID (eg. tenant ID). Only the host tenant would be allowed to write span 
configurations to this static ID and it would be in-turn parented under a “root” span configuration 
that would apply to all tenants.

A fully generalized span configuration hierarchy would eschew some of the write amplification issues
the current span configuration infrastructure succumbs to. For example, a zone configuration change 
on a database decomposes to span configuration updates for all tables (+ spans of their 
partitions/indexes) under the database; with a fully generalized scheme this would be a single write.

The fully generalized solution was explored in detail during an early iteration of this RFC, but it
was discarded in favor of the more targeted fixed form hierarchy. This was to reduce the surface area
of change to the newly introduced span configuration infrastructure which is yet to be hardened. 
The fully generalized scheme is also more involved and would require a complex batching scheme when 
reconciling the entire SQL state with KV. 
