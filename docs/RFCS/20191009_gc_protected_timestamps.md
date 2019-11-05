- Feature Name: GC Protected Timestamps
- Status: draft
- Start Date: 2019-10-09
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Schema changes, backups, and imports can require that old versions of data are
not garbage collected in order to complete (or rollback) successfully. Today's
default GC TTL is set very high in an attempt to avoid cases where
GC would be problematic or potentially catastrophic. In order to reasonably
shorten the default GC TTL we need to provide a mechanism for ongoing
jobs to prevent relevant spans from being garbage collected.

# Motivation

CockroachDB internally uses
[MVCC](https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html#mvcc)
to store key-value pairs. Old values need to be removed eventually.
Today's default GC TTL is 25 hours. This value comes from an
assumption about common backup strategies; we assume that customers frequently
run a nightly incremental backup from the previous nightly backup and then
run a full backup once a week. With this assumption we need to ensure that at
least 24 hours of history remains and will continue to remain until the
incremental backup of the previous day completes. Even this strategy can be
at risk if the backup takes more than an hour. The duration of the backup is
only partly in the control of the database and can be limited somewhat by the
sink receiving the data.

For workloads which update rows frequently throughout a single day this GC
policy can have huge costs for query execution 
([#17229](https://github.com/cockroachdb/cockroach/issues/17229)).

One particularly pernicious interaction with garbage collection relates to the
use of `IMPORT INTO`. These jobs can fail or be canceled. In
order to properly revert any changes they have created, these jobs use a
mechanism called revert range to move the ranges back to their previous state.
In order to perform a revert-range operation the data prior to the beginning of
the schema change must be retained.

In order to perform an incremental backup, the value as of the last backup must
be retained in order to know what to include.

Long-running OLAP transactions also merit consideration. Imagine a transaction
is kept open for a long period of time, today if it attempts to read at a
timestamp which has been garbage collected, the client will receive an error.


```
root@127.59.67.119:44643/defaultdb> SELECT * FROM foo AS OF SYSTEM TIME '-1m';
pq: batch timestamp 1572380392.231464923,0 must be after replica GC threshold 1572380448.708590964,0
```

This last use case is generally not a problem with day-long TTLs but may be
more of a problem with TTLs on the order of minutes. This may become a yet
further problem when work on admission control makes it feasible to run
low-priority, long-running OLAP queries concurrently with bursty OLTP-workloads
but can expose the OLAP queries to large amount of delay. This use case is not
overly considered in this RFC but we do not want to preclude it by choosing
solutions that are unlikely to be reasonably efficient if run once for every
minutes-long query.

This RFC works to enable a shorter default GC interval by implementing a 
system to allow for fine-grained prevention of GC for key spans at or above
specific timestamps in order to ensure safety of operations which rely on
historical data.

# Guide-level explanation

This RFC introduces a `protectedts` subsystem which provides primitives on top
of which long running jobs can prevent relevant data from being garbage
collected. This state is then plumbed in to the `storage` layer's data GC
process to provide the specified semantics. We'll then detail how internal KV
clients such as implementers of jobs can use the transactional API to safely
protect data needed for long-running jobs. In the rest of this RFC, the term
"client" designates an internal component of CockroachDB, such as the job
subsystem or the SQL executor.

## Basic terminology

  * GC TTL: A zone configuration's configured `gc.ttlseconds`. Each range has
    one zone config which applies to it (this isn't true but let's assume it is
    as scenarios when it is not are transient). Replicas hold in memory a copy 
    of their zone config. When any zone configs change, replicas are
    asynchronously notified via gossip.

  * GC Threshold: When a range runs garbage collection it sets, in the
    replicated range state, a timestamp below which no reads may be performed.
    This timestamp is called the GC Threshold. The GC threshold additionally 
    permits GC of rows. Due to this restriction, the GC protocol occurs in two
    steps, first setting the GC threshold with a GCRequest with no keys and then
    with a second GCRequest at the same threshold which contains the data to
    actually be removed.

  * Expiration: a row (henceforth a single MVCC version of a key) is said to
    expire if its MVCC timestamp is older than the leaseholder's current view of
    `Now()` less the leaseholder's current view of its GC TTL and there exists a
    newer row with the same key. An "expired" row  may still be read so long as
    its timestamp remains above the GC Threshold. Conceptually the expired time
    refers to the latest time which could possibly be set as the GC Threshold.
    This term does not use any new concepts but is being defined here to
    describe semantic properties later.

  * Protected Timestamp Record: the basic unit in the `protectedts` subsystem is
    a `Record`. A record, identified by a unique ID, associates an HLC timestamp
    with a set of spans which, while it exists, prevents any range which
    contains data in those spans from garbage collecting data at that timestamp.
    Creating protected timestamp records is best-effort and mere record creation
    does not ensure protection. Records created in a transaction which commits
    prior to any data expiring will be guarantee protection. 

  * Protection Mode: each record has a protection mode. There are two 
    protection modes, `PROTECT_AT` and `PROTECT_AFTER`. `PROTECT_AT` ensures
    only that values which are live at the specified timestamp are protected
    whereby `PROTECT_AFTER` ensures that all values live at or after the
    timestamp are protected (note that `PROTECT_AFTER` protects a superset of
    the data which would be protected by `PROTECT_AT` and protections marked as
    `PROTECT_AT` will be treated the same in the initial implementation). Each
    protected timestamp may contain metadata to identify the record (what
    exactly this metadata looks like is an open question). There are a number of
    challenges which stand in the way of properly implementing `PROTECT_AT`, not
    least of which would be the need to modify the replicated range state when
    increasing the GC Threshold above such a timestamp. These challenges are
    outlined in the Future Work section below.

## High Level Overview

In this section we'll go through a high level overview of the basic operations
and describe how they are made safe, describing the changes to the Replica's GC
in the process, and then we'll talk about how we anticipate the API will be
integrated.

The primary consumer of the protected timestamp subsystem will be implementors
of jobs (backups, schema changes, imports). These jobs would like to protect the
data on which they need to operate up front, when they create the job. In that
setting, this RFC anticipates that the users will write the protection record in
the same transaction which creates the jobs. The protection record will be
stored in a system table as an encoded protocol buffer which declares the
timestamps being protected, the relevant spans, and some metadata which might
help a background reconciliation job to delete orphaned protection records. The
mere existence of a record does not itself provide any protection; the
protection semantics are best-effort. However, if the timestamp being protected
is not already very close to expiring, the protection will succeed. The exact
semantics are outlined below. A synchronous verification can be performed after
the record is created for clients which need stronger guarantees.

The bulk of the heavy lifting of this proposal fall on the GC protocol in the
storage layer. In this proposal, leaseholders will need to consult the global
protected timestamp state in order to prove that it is safe to move the GC
threshold. In practice this leads to a relatively lightweight protocol (no
`O(ranges)` write operations, and even then only `O(ranges)` operations to
verify) which still provides useful guarantees. The cost of consulting the
global state of the subsystem is mitigated by caching and polling.

### The GC Process

GC TTLs are defined in 
[Zone Configurations](https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html).
A leaseholder is free is set the GC threshold to any older than the current
timestamp less its current view of its GC TTL. Unfortunately we have no claims
on the staleness of a Replica's current view of its GC TTL, though in practice
it is generally not more than a few seconds stale. Furthermore, we may not have
access to the history of possible zone configurations for a given range.

The GC Threshold is a part of a range's state which determines the lower bound
of timestamps for requests. The GC threshold is updated during the GC
process. Today the GC process proceeds by calculating the new GC threshold
based on the leaseholder's view of the present time less its view of the
GC TTL. This value is then proposed and written into the range state with
a GC request. The GC process then proceeds to compute all values which can be
removed below the specified threshold and issues a second GC request with the
same GC threshold.

GC occurs based on a heuristic which estimates how much garbage would be
collected if GC were to run. Adapting this heuristic to closed timestamps is an
open issue in this RFC. The naive approach, which is to prevent all GC on ranges
which are covered by a protected timestamp record, will be initially pursued
here. See open questions at the bottom of the document.

## Semantics of protecting a timestamp

There are tradeoffs we can make about the cost of different interfaces for
performing these protections. It is useful to clients that the interface to
creating and removing protected timestamps be transactional (i.e. Records are
created and Released as a part of a transaction). Providing an interface to
create protected timestamps which does not take a transaction would make using
protected timestamps more difficult as clients of the subsystem are ultimately
responsible for ensuring that records are eventually cleaned up. It is critical
to note that while some of the semantics presented here refer to the commit
timestamp of the transaction which create protection records, there is zero
expectation or desire for those transactions to observe their commit timestamp.
The only reason commit timestamps for protection records are discussed at all is
for the purposes of verifying that a record will be respected.

A strong guarantee which the subsystem could provide (but this RFC does not) is
the following: if the transaction which writes the protection record commits
then it is guaranteed that no data in the specified spans which the record deems
protected could be garbage collected until the record is released. Instead we
provide a weaker guarantee which in practice meets the requirements and is
probably more useful as it enables best-effort protection of data which may
have expired but has not yet been deleted without requiring `O(ranges)`
operations. Verification in this proposal will need to perform `O(ranges)`
requests (but not `O(ranges)` writes). It's worth noting that no use cases
envisioned by this RFC need to perform verification directly or synchronously. 

The invariant provided by this RFC is as follows:

```
If a transaction writing a protection record commits at a timestamp t such that
none of the data in the spans of the record have expired at t then the spans
will be protected until that record is removed.
```

This language attempts to capture the idea that ranges may have an arbitrarily
stale view of their zone configuration when performing GC. To avoid the need to
reason about the propagation of changing GC TTLs due to changing zone configs
we express the correctness invariant in terms of all possible zone configs
which ranges may hold. This formulation permits "best-effort" protections of
data which may have expired but have not yet been GC'd.
Determining whether a timestamp was indeed protected can be performed lazily
when requests at that timestamp run.

This as yet unverified property might initially seem insufficient or frustrating
from a programming perspective however it likely meets the needs of this
project. All clients of the `protectedts` subsystem which will fail
non-destructively if data is not protected will encounter failures to protect at
runtime. This leaves us with the revert-range case when using `IMPORT INTO`.
In this case we can augment the AddSSTable request to ensure that if
the request succeeds then the timestamp has been successfully protected. In this
way, even if we do not successfully protect the timestamp on all ranges into
which we will be importing, we can always successfully roll back.

In order to improve testing we provide a mechanism to validate that a timestamp
indeed will be protected. The validation mechanism provides the following 
contract:

```
Once a record has been successfully verified, any data in the spans of the 
record will be protected until the record is removed.
```

### Achieving Correctness

The heavy lifting to achieve the invariants provided by the protected timestamp
subsystem fall on the Replica and its GC logic. Now let's walk through the high
level protocol for writing protected timestamps and for GC.

#### Protecting a timestamp

Every time a protected timestamp is written it must check that it will not
exceed the rows or spans limits. If the limits would be exceeded an errors is
returned (see limits below). If the limits are not exceeded, the counters in
the meta row are incremented the counts accordingly, along with the version,
then insert the row and spans are inserted.

This check and shared single row will form both a bottleneck and source of
transaction restarts for transactions which protect timestamps. The
introduction of read locks would help mitigate these restarts. Even without read
locks, we at least front-load the update so hopefully restarts will be rare.
Furthermore I suspect that most operations which create these records will be
able to auto-retry. 

The update will then write the relevant protection record. To reiterate, 
this record's existence does not in any way ensure that the timestamp is
indeed protected. The GC behavior outlined in the next section provides us
with the invariant that if the record commits before any of the data has
expired then we're guaranteed that the record will protect the data.

#### GC

GC occurs on a `Replica` when a heuristic determines that there is
enough garbage which should be collected. It is important to note that this
heuristic is guided by the TTL (which also sets a lower bound) but is often
quite a bit later than the TTL. When it is determined that a replica should 
run GC today then it generally just does it at the latest timestamp allowed
by the TTL. This RFC is going to require more careful selection of a the 
GC timestamp.

Given the above described protocol for writing protected timestamp state,
let's lay out the conditions under which a Replica can GC data.

When running GC the replica will have the following information:

  * Protected TS state
    - This is exposed to the `Replica` through an interface which exposes:
      1) MVCC timestamp at which this state was retrieved.
      2) A method to query the set of `Record`s which intersect the `Replica`'s 
        key span.
  * Zone Config
    - Assume that each range has exactly one zone config. If more than one zone
      config applies to a `Replica` we should split that range and we should not
      run GC. We could alternatively assume that the longer GC TTL will be used.
  * The current time (`hlc.Clock.Now()`).
  * The current lease.
  * A minimum protectedts state timestamp the leaseholder has promised a
    verifying client.
  
There's a large amount of complexity which arises when we think about
modifying the GC heuristics to be compatible with the idea of a protected
timestamp. We sidestep that for now by simplifying the GC logic to
not GC anything in any range which has a protected timestamp. When this document
discussed running GC at a timestamp it implies moving the GC Threshold to 
`timestamp-GC TTL` where GC TTL is due to the leaseholder's current view of the
zone config.

The following properties must be upheld in order to run GC at all.
  
1) We cannot run GC for data which has expired since our last reading of the
   protected TS state. In this way we can treat the last read of the protected
   timestamp state as `now` for the purposes of computing a score.
   
1) We cannot run GC for timestamps which are older than the lease start time.

1) We cannot run GC if the timestamp at which the protected timestamp state was
   read is not newer than the minimum protected timestamp state promised to a
   verifying client.

At this point it is safe to garbage collect up to now less the GC TTL less
the time since the protected state was read.
As you can tell, there's an important relationship between how often the 
ProtectedTS state is read and how close a Replica can get to running GC at 
exactly its TTL. 

In general, for sane rates of GC, it's probably fine to run with an interval
on the order of say, 5 minutes. This rate can be controlled by a cluster
setting `kv.protected_timestamp.poll_interval`. This setting defines a
background rate; in the face of changing zone configs, the implementation may
choose to read the state more frequently.

We additionally ensure that a GC threshold cannot be moved until the timestamp
at which the protected timestamp state was read is newer than the start time of
the lease. This policy, in conjunction with the leaseholder's implementation of
`AdminVerifyProtectedTimestamp`, enables the implementation of the verification
protocol.

## Limits

Given the plan to keep an in-memory copy of all of the protected timestamps
on all nodes, we need to be sure to bound the memory usage of that state.
In order to do that we provide cluster settings (hidden?) which 
control the maximum number of records and spans which can be protected.
In the initial prototype I suspect values like 4096 spans and 512 records is
more than enough for most use cases and will have a likely sane memory overhead. 

## Verification

After a protection record has been created, the protection can be validated.
If validation succeeds then we know that all data alive at that timestamp
(or potentially after) will not be GC'd until after the record has been
removed.

In order to do this we need to touch every range which overlaps with the
spans in questions and either determine that the data has been GC'd or
ensure that GC will not run until the relevant protection record has been
read.

To do this we augment the volatile state on the leaseholder to include a
lower-bound on the timestamp of the last read from the protected timestamp
subsystem. We also store the timestamp at which we last attempted to run a GC on
the leaseholder. Just modifying the in-memory state of the leaseholder is safe
because we also added a condition that GC can only be run if the start time of
the lease is before the last read from the protected timestamp state. In this
way we ensure that if the lease were to move to a new node that the protected
timestamp state would be read again after that lease started before running GC.
If the same node were to restart it too would satisfy the safety property as it
would have to read the protected timestamp state after restarting before running
GC.

### Cleanup and an external job scanner

Throughout the development of this RFC there have been open questions about
what sort of systems will we put in place to ensure that protected timestamps
are cleaned up. After much back and forth about heart-beating, the ultimate 
decision is that most jobs should clean up after themselves gracefully. However
in cases where jobs might be orphaned we could create a mechanism to scan both
tables (jobs and protected timestamps) and clean up seemingly orphaned protected
timestamps. To aid in this process we allow clients to associate metadata with
each protected timestamp.

The decision allows different users of protected timestamps to ensure that they
are ultimately cleaned up in a manner which is appropriate for their use case. If
a client has confidence that the lifecycle of some external state will move to
a terminal state and can always release a protected timestamp in that
transition, great. If the client would prefer to use metadata to associate 
protected timestamps to some other state and run periodic reconciliation,
that is also great.

# Reference-level explanation

In this initial implementation we focus on the idea that the primary creators of
protected timestamps are schema changes and backups. In both cases these clients generally have some decided upon timestamp and need to ensure that
that timestamp stays alive throughout the duration of the job, even if the
job is paused. For each of these cases there is a one-to-one mapping from a job
to a protected timestamp record.

This section will proceed by introducing the interface with
which job implementers create and release protected timestamps. Then we'll
introduce the interface exposed to the `storage` package for use
during the GC process. Lastly we'll walk through the interface exposed for
creating mechanisms to periodically reconcile protected timestamps with
external state. After the interfaces have been exposed, we'll walk through an
envisioned use of these interfaces to protect timestamps. Then the section will
dive into a more detailed design of the implementation.

## Interfaces

To avoid circular dependencies on, the implementation of the interfaces
exported by `protectedts` will be implemented by a sub-package.
The data format of the `protectedts` subsystem which features in its interfaces
are defined in a sub-package `pkg/storage/protectedts/ptpb`. The key value type
is the `Record`.

```protobuf
// Record corresponds to a protected timestamp.
message Record {

  // ID uniquely identifies this row.
  bytes id = 1 [(gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID", (gogoproto.nullable) = false, (gogoproto.customname) = "ID"];

  // Timestamp is the timestamp which is protected.
  util.hlc.Timestamp timestamp = 2  [(gogoproto.nullable) = false];
 
  // Mode specifies whether this record protects all values live at timestamp
  // or all values live at or after that timestamp.
  ProtectionMode mode = 3;
  
  // MetaType is used to help interpret the value in meta.
  string meta_type = 4;

  // Meta is client-provided metadata about the record.
  bytes meta = 5;

  // Spans are the spans which this record protects.
  repeated roachpb.Span spans = 6 [(gogoproto.nullable) = false];
}
```

When setting up a schema change or starting a backup, the user of the protected
timestamp subsystem will create a Record and then probably also add a reference
to the record into the job. In this way a background task can reconcile
records and jobs. The metadata of the system takes the following form:

```protobuf
// Metadata is the system metadata.
// TODO(ajwerner): consider renaming to SystemMetadata.
message Metadata {

   // Version is incremented whenever a Record is created or removed.
   uint64 version = 1;
   
   // NumRecords is the number of records which exist in the subsystem.
   uint64 num_records = 2;

   // NumSpans is the number of spans currently being protected by the
   // protectedts subsystem.
   uint64 num_spans = 3;
}
```

Thus we can view the complete `protectedts` subsystem state is encapsulated in
the following message:

```protobuf
// State is the complete system state.
message State {
  Metadata metadata = 1 [(gogoproto.nullable) = false, (gogoproto.embed) = true];
  repeated Record records = 2 [(gogoproto.nullable) = false];
}
```

The `pkg/storage/protectedts` package exports the interfaces through which
clients interact with the protected timestamp subsystem. The below `Storage`
interface will be exposed through the sql execution context.

```go
package protectedts

// Storage provides clients with a mechanism to protect, release, and inspect 
// records stored in the protected timestamp subsystem.
type Storage interface {

  // Protect will durably create a protected timestamp. Protect may succeed and 
  // yet data may be or already have been garbage collected in the spans 
  // specified by the Record. However, the protected timestamp subsystem
  // guarantees that if all possible zone configs which
  // could have applied have GC TTLs which would not have allowed data at
  // the timestamp which the passed Txn commits have been GC'd then that
  // data will not be GC'd until this Record is released.
  //
  // It is the caller's responsibility to ensure that a timestamp is ultimately
  // released. The metadata fields in the record can be used to aid in
  // background reconciliation.
  //
  // An error will be returned if the ID of the provided record already exists
  // so callers should be sure to generate new IDs when creating records.
  Protect(context.Context, *client.Txn, *ptpb.Record) error

  // Get retrieves the record at with the specified UUID as well as the MVCC
  // timestamp at which it was written.	If no corresponding record exists
  // ErrNotFound is returned.
  //
  // Get exists to work in coordination with AdminEnsureProtectedRequests 
  // requests. In order to verify that a Record will apply, the client must
  // provide both the timestamp which should be protected as well as the
  // timestamp at which the Record providing that protection was created.
  GetRecord(
    context.Context, *client.Txn, uuid.UUID,
  ) (_ *ptpb.Record, createdAt hlc.Timestamp, _ error)

  // Release allows spans which were previously protected to now be garbage
  // collected.
  //
  // If the specified UUID does not exist ErrNotFound is returned but the
  // passed txn remains safe for future use.
  Release(context.Context, *client.Txn, uuid.UUID) error

  // GetMetadata retrieves the metadata with the provided Txn.
  GetMetadata(context.Context, *client.Txn) (ptpb.Metadata, error)

  // GetState retrieves the entire state of storage with the provided Txn.
  GetState(context.Context, *client.Txn) (ptpb.State, error)
}
```

The `storage` package will interact with the `protectedts` subsystem through the
following `Tracker` interface. The tracker is implemented on top of `Storage`
and additionally encapsulated polling logic.

```
// Tracker will be used in the storage package to determine a safe
// timestamp for garbage collection.
type Tracker interface {

  // ProtectedBy calls the passed function for each record which overlaps the
  // pass Span. The return value is the MVCC timestamp at which this set of
  // records is known to be valid.
  ProtectedBy(
    context.Context, roachpb.Span, func(*ptpb.Record),
  ) (asOf hlc.Timestamp)
}
```

## Example usage

Let's walk through the case of a backup. A `BACKUP` statement is executed by the
execution engine as a mutation which creates a job. This RFC envisions that that
plan will now additionally created a `ptpb.Record` with the timestamp of the 
backup in the same transaction. It will write into that record
metadata which encodes the job's ID into the record. Additionally the job's
state will be extended to refer to the ID of the protected timestamp record. 
When the backup moves to a terminal state, it will remove the protection record.

## Detailed Design

The package structure proposed looks like the following:

```
pkg/storage/protectedts - exports symbols for consumption by clients.
pkg/storage/protectedts/ptpb - protocol buffer definitions used in interfaces.
pkg/storage/protectedts/ptprovider - implements protectedts.Store and Catalog
pkg/storage/protectedts/ptstorage - implements protectedts.Tracker on top of protectedts.Storage.
pkg/storage/protectedts/ptverify - provides business logic to verify the application state of a record.
```

### Implementation of `protectedts.Storage`

This RFC chooses to store the state in the `kv` database. An initial 
implementation was implemented on top of SQL but the lack of access to the MVCC
timestamps of rows and the accompanying complexity that created while trying to
provide invariants was deemed to not be worth it.

* There is a meta row which stores the aggregate counts and the version.
  * The meta row is transactionally updated on every write.
  * The meta row enables cheap caching at the cost of high contention; writes 
    are certain to be pushed, see the `Tracker`.
  * The meta row key is defined as a constant in the `keys` package.
  * The value is an encoded `ptpb.Metadata`.
* The records themselves are stored in a system table which has a well-known ID.
  * The `protected_timestamp_records` table is defined in the `sqlbase` package.
    It will be created on bootstrap and in a `sqlmigrations` migration.
  * The primary key of the table is a UUID. The other `bytes` column contains
    encoded `ptpb.Record` values.

This well-known ID approach is fine even as we discuss being able to backup and
restore system tables as there is no way that protected timestamp information
could be meaningfully translated to the new cluster.

### Implementation of `Tracker`

The tracker which is utilized by the `storage` package wraps a provider and
will periodically poll the state from the catalog based on a cluster setting.

The polling mechanism will initialize by reading the entire state and observing
the commit timestamp. Later attempts will begin by just retrieving the metadata
and only retrieving the full state when the version changes.

Internally the tracker could use a data structure like an interval tree to make
answering queries cheap.

In order to reduce the contention on the meta key, we could imagine instead of
having every node poll the state directly we could have a single node poll the
state and gossip the meta record and the timestamp at which it was last read.

### Changes to the GC Process in `storage`

The conditions to achieve safety were outlined in the above guide-level
description. The `storage.Store` will be augmented to hold a handle to a 
`protectedts.Tracker`. In the GC queue that tracker will be consulted to
determine whether the `Replica` in question is covered by a protected timestamp.
Additionally the lease as well as protected promise state will be consulted to
ensure the requirements to run GC are met.

The GC queue will now use the timestamp at which it last read the `protectedts`
state as "now" to provide the invariant that data which was protected prior to
its expiration cannot be GC'd.

### Implementation of verification

Verification throws a bit of a wrench into the basic implementation.
We need a lightweight mechanism to prove that a record will be seen before the
next attempt of a range to increase its GC TTL above the timestamp protected by
some record. The protocol relies on leases to avoid the need to touch every
replica of a range. We involve the lease start time in the GC protocol so that
so long as we prove that the leaseholder at verification time will not attempt
to raise the GC threshold above some timestamp will not happen without that
leaseholder updating its protectedts state up to some point and we ensure that
future leaseholders will not run GC until they see the GC threshold at at least
the start time of the lease then we can prove that the range will not attempt to
increase its gc threshold above some timestamp before reading the protectedts
state such that it will observe the protection record in question.

The mechanism by which we achieve this invariant is a new request,
`AdminVerifyProtectedTimestamp`. This will be distributed to all spans covered
by a record. 

### Ensuring that AddSSTable requests can be reverted

Today AddSSTable requests must be single requests. We'll augment that policy to
allow them to co-occur in a batch with an AdminVerifyProtectedTimestamp request.
This will allow users of AddSSTable to know that if the AddSSTable succeeds then
it can be reverted the the desired timestamp.

### Cluster Settings

The below three cluster settings control the total cost of the `protectedts`
subsystem. 

```
"kv.protectedts.max_spans"
"kv.protectedts.max_records"
"kv.protectedts.poll_interval"
```

## Drawbacks

* The polling delay may extend the life of bytes marginally, especially when the
  GC TTL is very short (seconds).

* Operations which choose to use the `protectedts` subsystem expose themselves
  to serializable restarts due to the interaction with the meta key if writing
  records becomes frequent.

* The failure of this document to extend the GC heuristic may lead to more
  data being protected than was intended. 

## Future work

### Administrative tools to display, inspect, and remove protected timestamps

Protected timestamps need to be visible to end-users somehow. Probably we need
to have a `crdb_internal` virtual table to list protected timestamps.
Additionally we would likely benefit from a mechanism to manually remove records
from a SQL session. Perhaps this mechanism should know about relationships
between records and jobs and should cancel jobs which rely on protections. 

A good observability story should be treated as a requirement.

### More exact protection of the desired time (PROTECT_AT)

For a number of different users, the PROTECT_AFTER semantics which protects all 
versions alive at and newer than some timestamp, are stronger than required.

For example imagine the following history for key `k`:

```
  5: put foo
  4: put baz
  3: --- protected ---
  2: put bar
  1: put foo
```

Imagine then that we want to run GC at time 6 at which time we have protected
`k` at time 3. In this proposal we'll only be able to GC the row `k@1` but if
we were to be more exact about the protected timestamp we'd be able to GC both
`k@1` and `k@4`.

Incremental backups which do not preserve history might benefit massively from
this optimization. Imagine a workload which regularly updates rows, perhaps
roughly every minute. For each key it would generate over 1000 rows of history.
In order to perform incremental backup we'd only need to keep 2.

The process of extending the GC logic to preserve the correct keys does not seem
too daunting. In order to implement verification, the range state will need to 
be extended to include information of any respected records using
`PROTECTED_AT` when raising the GC threshold. Some additional care will need to
be taken to deal with preservation of some delete tombstones when multiple
`PROTECTED_AT` records overlap with a range.

The largest challenge the author sees is how these protections can be reconciled
with the existing heuristics to determine whether to run GC at all. Perhaps this
is not a more difficult challenge than adapting the heuristic to `PROTECT_AFTER`
records; once you raise the GC threshold above a `PROTECT_AT` record, you can
know exactly how much data is protected due to that record (in the case where
there is exactly one). Perhaps metadata about how many bytes are protected by
records or sets of records could help here.

### More exact protection in space

Imagine that the span `/Table/53/{1-2}` is protected but a range contains
somehow `/Table/5{3-4}` then we could imagine that there's a part of the
range that is not protected. In this proposal we'll avoid processing any
GC for this range because part of its data is protected. In general this
optimization doesn't seem worthwhile because ranges generally don't cross
table boundaries. If larger ranges which contain multiple tables or indices
become the norm then we may want to rethink this decision.

It's completely unclear whether this would be useful in practice. Protection
records will generally span table or index boundaries. Ranges generally do not
span tables except when the ranges are small in which case GC is likely not too
important.

## Rationale and Alternatives

This design is nice because it is relatively decoupled and seems pretty safe.

### Storing protected timestamps in range state

A downside of this approach is that it adds a dependency to the low-level
storage package on accessing distributed storage before being able to garbage
collect old values. While this dependency is opaque it might be considered
concerning.

One alternative considered is to move the state of protection down into range
state. In this way a range would always know whether it could GC some timestamp
without consulting an external subsystem. There are however several downsides:

 * Range now becomes even more bloated.
   * Range state is interacted with transactionally and thus it incurs even more
     opportunity to race with other admin commands.
 * Splits and merges gain complexity.
 * Creating and clearing protected timestamps become transactions which write to
   all protected ranges.

Ignoring the cost of installing, clearing, and introspecting these protections,
this initial straw man does not provide any safety mechanisms to ensure that
protections expire or are removed. One could imagine that each protection
installed in the ranges carries its own identifier which refers back to a table
of protections similar to that already proposed in this RFC.
   
### Stronger semantics through coordination with zone configs

An earlier iteration of this document sought to provide a guarantee that if a
protected timestamp record commits then it must be the case that it has provided
protection. The mechanism by which this was going to be achieved was to causally
relate all zone config changes to all protected timestamp changes by making both
sets of changes write to a shared key. The problem with this approach is that 
there is not an obvious for such a transaction to ensure that no ranges
previously held a shorter GC TTL which led them to have already GC'd data of 
interest.

If we naively consulted the zone configurations to determine whether the
timestamp can be protected we'd receive a false positive. Preventing the
protection of timestamps prior to the most recent zone configuration change
which applies to the relevant spans is certainly too restrictive.
Imagine the following sketch of a scenario:

 * The GC TTL for some range is initially 1.
 * Some data becomes GC-able at time 3.
 * GC runs at time 4 and GCs some data.
 * At time 50 the zone config changes to have a GC TTL of 100.
 * At time 60 the old zone config is GC'd
 * At time 100 a client attempts to protect time 3.

With stronger semantics this protection should obviously fail. It would be
difficult to know about this higher GC Threshold without communicating with all
ranges. Communicating with all ranges is expensive and complicated. No use cases
require these stronger semantics.

### Data model

There are levels to the alternatives considered. The most simplistic model
considered was a single key which would store an encoded proto which roughly
captures all of `ptpb.State`. This was deemed too simplistic and
decidedly would not scale (though there's perhaps an interesting question
whether using column families to store the metadata and state all in one row
would be okay).

Another alternative was to keep all of the state in SQL tables. This approach
was initially pursued. The SQL-heavy data model was primarily chosen out of a
desire to utilize and familiarize myself with the data modeling which our users
will be exposed to. In this approach the meta row was implemented as a table
with a single row plus a table for records which had a table of spans 
interleaved into it. The reason this approach was ultimately rejected it that
it was difficult to access the MVCC timestamps of the records. Additionally it
felt rather heavyweight.

The approach in use here is a middle ground which provides the granularity of
the SQL approach with the lower level control and efficiency of the KV 
interface.

## Unresolved questions

- Why not heartbeating or expiration?
  
Just using an expiration is much simpler. For jobs which will fail but not cause
correctness problems in the case of undesired GC an expiration feels like a
reasonable solution. The problem with expirations is how to choose a good one
that isn't too lenient such that waiting for it to expire is too expensive but
also isn't too aggressive such that jobs are actually at risk of hitting their
expiration.

For jobs which will cause correctness problems if undesired GC occurs then
expirations on protections are almost as bad as no protections at all. At the
very least there would need to be a mechanism to update the expiration. Once
there's a mechanism to update an expiration I'm not sure how it differs from
a heartbeat.

Heartbeating is a problem for jobs which can be paused and resumed.

- Will clients need to implement reconciliation?

- How does the use of time-bound iterators interact with the efficiency of
  this solution as written and more importantly the exact protection 
  optimization?

- How do we deal with the garbage collection heuristics given the existence of
  protection records.

The GC heuristic won't be able to give us an estimate of how much data might be
recovered if we only run the GC up to the current minimum protected timestamp
(will it?). Given that we know we have a protected timestamp at time `t5` and we
have a GC threshold of `t1`, when should we run GC? 

The most naive thing to do is just not run GC if we have a protected timestamp
above our threshold.
