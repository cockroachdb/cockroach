- Feature Name: GC Protected Timestamps
- Status: draft
- Start Date: 2019-10-09
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Schema changes and backups can require that old versions of data are not
garbage collected in order to complete (or rollback) successfully. Today's
default GC threshold is set very high in an attempt to avoid cases where
GC would be problematic or potentially catastrophic. In order to reasonably
shorten the default GC threshold we need to provide a mechanism for ongoing
jobs to prevent relevant spans from being garbage collected.

# Motivation

CockroachDB internally uses
[MVCC](https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html#mvcc)
to store key-value pairs. Old values need to be removed eventually.
Today's default GC threshold is 25 hours. This threshold comes from an
assumption about common backup strategies; we assume that customers frequently.
run a nightly incremental backup from the previous incremental backup and then
run a full backup once a week. With this assumption we need to ensure that at
least 24 hours of history remains and will continue to remain until the
incremental backup of the previous day completes. Even this strategy could be
at risk if the backup takes more than an hour. The duration of the backup is
only partly in the control of the database and can be limited somewhat by the
sink receiving the data.

For workloads which update rows frequently throughout a single day this GC
policy can have huge costs for query execution 
[#17229](https://github.com/cockroachdb/cockroach/issues/17229).

One particularly pernicious interaction with garbage collection relates to long
running schema changes with backfill. These jobs can fail or be canceled. In
order to properly revert any changes they have created, these backfills use a
mechanism called revert range to move the ranges back to their previous state.
In order to perform a revert-range operation the data prior to the beginning of
the schema change must be retained.

In order to perform an incremental backup the value as of the last backup must
be retained in order to know what to include.

Long-running OLAP transactions also merit consideration. Imagine a transaction
is kept open for a long period of time, today if it attempts to read at a
timestamp which has been garbage collected, the client will receive an error.
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
of which long running jobs can safely prevent relevant data from being garbage
collected. This state is then plumbed in to the `storage` layer's data GC
process to provide the specified semantics. We'll then detail how clients can
use the transactional API to safely protect data needed for long-running jobs.

## Basic terminology

A protected timestamp is an HLC timestamp associated with a set of spans which,
while it exists, prevents any range which contains data in those spans from
garbage collecting data at that timestamp (open question about partial
coverings, say Range contains [a, c) and [a, b) is protected, will [b,c) get
collected?). There are two protection modes, `ProtectAt` and
`ProtectAfter`. `ProtectAt` ensures only that values which are live at the
specified timestamp are protected whereby `ProtectAfter` ensures that all
values live at or after the timestamp are protected (note that `ProtectAfter`
includes `ProtectAt` and protections marked as `ProtectAt` will be treated the
sam in the initial implementation).  Each protected timestamp may contain
metadata to identify the record (what exactly this metadata looks like is an
open question).

In this section we'll go through a high level overview of the basic operations
and describe how they are made safe, describing the changes to the Replica's GC
in the process, and then we'll talk about how we anticipate the API will be
integrated.

## Semantics of protecting a timestamp

Clients of the protected timestamp subsystem would like it to be the case that
if a timestamp is successfully protected then it is not possible that any data
live in any of the protected timestamp's spans at the timestamp will be garbage
collected until the protected timestamp is released.

It would be unreasonable if an attempt to protect a timestamp succeeded after
data live at that timestamp has already been GC'd. The protected timestamp
subsystem should only succeed if indeed it is impossible for the timestamp
which the client tries to protect is indeed protected. Furthermore it useful
that clients be able to protect timestamps inside of a transaction rather than
as a distinct transaction (i.e. the interface will take a `*client.Txn`).

An important element to all of this is that GC TTLs are defined in 
[Zone Configurations](https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html).
This implies a to need to couple the protected timestamp subsystem to the zone
configuration subsystem or the downstream side-effects of those zone configs,
replica state. This chooses to base its implementation on the zone config GC
TTLs.

There are different choices we could make about which timestamp are available
for protection at any given time. This proposal allows clients to protect
timestamps for a set of spans up to the GC ttl ago. Imagine that you have a 60
minute TTL on a table and you decide at minute 59 that data which was
overwritten 59 minutes ago should be saved, you have one minute to write a
transaction to save it (note I'm not proposing exposing protected timestamps to
SQL clients or anything like that). It totally might be the case that after 61
minutes all of the data is still there (it almost definitely is if the workload
is producing mountains of garbage). We probably could implement a solution that
could go protect that data but we chose not to for efficiency and
simplicity. The choice of using the GC ttl and the zone configs as opposed to
the Replica state itself is discussed below.

### Achieving Correctness

In order to achieve correctness we must only GC timestamps at which we know
that no future transactions could add a protected timestamp to.

We achieve this by having a special, single-row `system.protected_ts_meta`
table through which we'll chain causality between zone configs and protected
timestamps.

This meta row will help us implement this feature by enabling caching and size
limits. It will be observed and modified in all transactions which create
protected timestamps. We'll also observe this meta row in all transactions
which update zone configurations.

Now let's walk through the high level protocol for writing protected timestamps
and for GC.

#### Protecting a timestamp

First the code will, in the transaction, read all of the relevant zone
configurations for the spans being protected (this is not as cheap as it should
be, see TODO other RFC). It will then determine the relevant zone config with
the shortest TTL. The deadline for the transaction to be committed is the
requested timestamp plus that minimum TTL.

Every time a protected timestamp is written it must check that it will not
exceed the rows or spans limits. If the limits would be exceeded an errors is
returned (see limits below). If the limits are not exceeded, the counters in
the meta row are incremented the counts accordingly, along with the version,
then insert the row and spans are inserted.

#### GC

GC occurs on a `Replica` when a heuristic determines that there is
enough garbage which should be collected. It is important to note that this
heuristic is guided by the TTL (which sets a lower bound) but is often quite
a bit later than the TTL. When it is determined that a replica should 
run GC today then it generally just does it at the latest timestamp allowed
by the TTL. This RFC is going to require more careful selection of a the 
GC timestamp.

Given the above described protocol for writing protected timestamp state,
let's lay out the conditions under which a Replica can GC data.

When running GC the replica will have the following information:

  * Protected TS state
    - This is exposed to the `Replica` through an interface which exposes:
    1) MVCC timestamp at which this state was retrieved
    2) A method to query the set of timestamps which intersect the `Replica`'s 
       keyspace and their associated `ProtectionMode`
  * Zone Config
    - We assume that each range has exactly one zone config.
    - Sometimes a range will need to be split because it spans more than
      one zone config but we won't (don't) run GC in that case.
    - We also have with the zone config the MVCC timestamp at which the
      zone config was written.
  * The current time

1) Ensure that the zone config's MVCC timestamp is older than the read time of
   the Protected TS state, if it's not then no garbage collection can occur
   until the protected TS state has been refreshed.

1) Read the protected TS state to determine which timestamps are protected
   for this range. Any read timestamps must be protected regardless of the
   rest of the procedure.

At this point it is safe to garbage collect up to now less the GC TTL less
the time since the protected state was read (and a clock max offset).
As you can tell, there's an important relationship between how often the 
ProtectedTS state is read and how close a Replica can get to running GC at 
exactly its TTL. 

In general, for sane rates of GC, it's probably fine to run with an interval
in on the order of say, 5 minutes. This rate can be controlled by a cluster
setting `kv.protected_timestamp.refresh_interval`. This setting defines a
background rate; in the face of changing zone configs, the implementation may
choose to read the state more frequently.

## Limits

Given the plan to keep an in-memory copy of all of the protected timestamps
on all nodes, we need to be sure to bound the memory usage of that state.
In order to do that we provide cluster settings (hidden?) which 
control the maximum number of records and spans which can be protected.

In the initial prototype I suspect values like 4096 spans and 512 records is
more than enough for most use cases and will have a likely sane memory overhead. 

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
are ultimately cleaned up in a manor which is appropriate for their use case. If
a client has confidence that the lifecycle of some external state will move to
a terminal state and can always release a protected timestamp in that
transition, great. If the client would prefer to use metadata to associate 
protected timestamps to some other state and run periodic reconciliation,
also great.

# Reference-level explanation

In this initial implementation we'll say that the main creators of protected
timestamps are schema changes and backups. In both cases these clients these
clients generally have some decided upon timestamp and need to ensure that
that timestamp stays alive throughout the duration of the job, even if the
job is paused. For each of these cases there is a one-to-one mapping from a job
to a protected timestamp.

This section will proceed by introducing the interface with
which job implementers create and release protected timestamps. Then we'll
introduce the interface exposed to the `storage` layer for use
during the GC process. Lastly we'll walk through the interface exposed for
creating mechansism to periodically reconcile protected timestamps with
external state. After the interfaces have been exposed, the section will dive
into a more detailed design of their implementation.

## Interfaces

To avoid circular dependencies on SQL, the implementation of the interfaces
exported by `protectedts` will be implemented by a sub-package. Furthermore
another package `protectedtsbase` which `storage` can import will contain
basic structs and interfaces for its consumption.

### `protectedts.Store`

Jobs will interact with a `protectedts.Store`:

```go
package protectedts 

// Store provides clients with a mechanism to protect and release
// protected timestamps for a set of spans.
//
// Clients may provide a Txn object which will allow them to write the id
// of this new protection transactionally into their own state. If the caller
// provides a Txn object then it is the caller's responsibility to call
// CleanupOnError if an error is returned. If an error is returned and the
// transaction is subsequently committed, the resulting state of the system
// is undefined (perhaps this is a good place for savepoints?).
//
// It is the caller's responsibility to ensure that a timestamp is ultimately
// released.
type Store interface {

  // Protect will durably create a protected timestamp, if no error is returned
  // then no data in the specified spans at which are live at the specified
  // timestamp can be garbage collected until the this ProtectedTimestamp is
  // released.
  //
  // Protect will set the deadline on the provided transaction to ensure that
  // it commits before `ts` could possibly have been garbage collected.
  // In general this deadline will be the ts.Add(minGCTTL(spans)).
  Protect(
    ctx context.Context,
    txn *client.Txn,
    ts hlc.Timestamp,
    mode ProtectionMode,
    metaType string, meta []byte,
    spans ...roachpb.Span,
  ) (uuid.UUID, error)

  // Release allows spans which were previously protected to now be garbage
  // collected.
  //
  // If the specified UUID does not exist ErrNotFound is returned but the
  // passed txn remains safe for future use.
  Release(_ context.Context, txn *client.Txn, id uuid.UUID) error
}
```

### `protectedtsbase`

```go
package protectedts

type Record struct {
  ID        uuid.UUID
  Timestamp hlc.Timestamp
  Mode      ProtectionMode
  MetaType  string
  Meta      []byte
  Spans     []roachpb.Span
}

// Tracker will be used in the storage package to determine a safe
// timestamp for garbage collection.
type Tracker interface {
  // TODO(ajwerner): Is there anything useful to do with an error?
  // Should this just return records and a timestamp and if failed then
  // return a timestamp of 0 and log internally?
  GetProtectedTimestamps(intersecting roachpb.Span) ([]Record, hlc.Timestamp, error)
  
  // TODO(ajwerner): consider providing a method to force refreshing the
  // state as of the current time. Clients may want to call this when
  // zone configs are changing. 
}
```

### `protectedts.Catalog`

State reconciliation can be implemented by retrieving the entire state
of the protected timestamp subsystem. The complete state looks like the
following:

```go 
package protectedts

type State struct {
  Metadata
  Records []protectedtsbase.Record
}

type Metadata struct {
  Version    int
  NumRecords int
  NumSpans   int
}

type Catalog interface {
  // List returns the complete state of the protected timestamp subsystem.
  List(context.Context, *client.Txn) (*State, error)

  // GetMetadata can be used to cheaply update the timestamp at
  // which a listing was "observed". All changes to State will
  // increment the version so if a later call to GetMetadata
  // returns the same version as an earlier call to List then
  // it is guaranteed that the value returned from List at that
  // timestamp would not have changed.
  GetMetadata(context.Context, *client.Txn) (*Metadata, error)
}
```

### Additional Changes to storage

Each replica currently keeps an in-memory copy of its zone config.
This value is updated based on gossip. In the gossiped system config
we already include MVCC timestamps. In order to implement the GC protocol
the Replica needs to be aware of the MVCC timestamp at which its zone config
was written.

### Changes to Zone Configuration Updates

Zone configuration updates need to observe the protected timestamp
metadata. This is important so that when protected timestamps are written
they know that they have seen an up-to-date copy of the zone config. We do
this by relying on the single-key linearizability offered by CockroachDB.
All protected timestamp writing operations read the relevant zone configs and
write to `protected_ts_meta`. We need it to be the case that all updates that
write to zone configs read from `protected_ts_meta`. Fortunately this is easy.
We plumb a `Catalog` into the sql execution context and then call `GetMetadata`
when writing zone configs.

## Detailed Design

The package structure proposed looks like the following:

```
pkg/protectedts - exports symbols for consumption by clients
pkg/protectedts/protectedtsbase - exports symbols for consumption by storage
pkg/protectedts/provider - implements protectedts.Store and Catalog
pkg/protectedts/tracker - implements protectedtsbase.Tracker on top of Catalog
```

### Implementation of Store and Catalog

This RFC chooses to store the state in SQL system tables (in contrast to
KVs with their own ad-hoc structure). Futhermore the approach taken was to
utilize SQL for data modeling rather than to store encoded protocol buffers.

The protected timestamp subsystem add 3 system tables (though one is interleaved
in another).

The current state of the table is stored in a special table called
`system.protected_ts_meta`. 
```sql
  CREATE TABLE protected_ts_meta (
      version INT8 NOT NULL,
      rows    INT4 NOT NULL,
      spans   INT4 NOT NULL,
      FAMILY "primary" (_exists, version, rows, spans),
      CONSTRAINT check_exists CHECK (_exists)
  )
```

What doesn't show up in this is a hidden columns to ensure that this table has
one row. The hidden column is defined as:

```
_exists BOOL NOT NULL CHECK (_exists) DEFAULT (true) PRIMARY KEY
```

The other tables are defined as:

```sql 
  CREATE TABLE protected_ts_timestamps (
      id UUID NOT NULL,
      ts DECIMAL NOT NULL,
      mode INT NOT NULL,
      meta_type STRING NOT NULL,
      meta BYTES NULL,
      CONSTRAINT "primary" PRIMARY KEY (id ASC),
      FAMILY "primary" (id, ts, meta_type, meta)
  )

  CREATE TABLE protected_ts_spans (
    id UUID NOT NULL,
    key BYTES NOT NULL,
    end_key BYTES NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC, key ASC, end_key ASC),
    CONSTRAINT fk_protected_ts_timestamps FOREIGN KEY (id) REFERENCES protected_ts_timestamps(id) ON DELETE CASCADE,
    FAMILY "primary" (id, key, end_key)
  ) INTERLEAVE IN PARENT protected_ts_timestamps (id)
```

The implementation provided by `protectedts/provider` interact with this
schema using a `sqlutils.InternalExecutor`.

### Implementation of `Tracker`

The tracker which is utilized by the `storage` package wraps a provider and
will periodically poll the state from the catalog based on a cluster setting.

The polling mechanism will initialize by reading the entire state and observing
the commit timestamp. Later attempts will begin by just retrieving the metadata
and only retrieving the full state when the version changes.

Internally the tracker could use a data structure like an interval tree to make
answering queries cheap. Given the small size of the data set however there are
likely simpler solutions.

## Drawbacks

May extend the life of bytes marginally, especially when the GC TTL is very
short (seconds).

## Future work

### More exact protection of the desired time

In theory a protected timestamp does not need to protect all versions alive at
and newer than some timestamp but rather could just protect the version alive
at exactly that timestamp.

For example imagine the following history for key `k`:

```
  5: put baz
  4: put bar
  3: --- protected ---
  2: delete
  1: put foo
```

Imagine then that we want to run GC at time 6 at which time we have protected
`k` at time 3. In this proposal we'll only be able to GC the row `k@1` but if
we were to be more exact about the protected timestamp we'd be able to GC both
`k@1` and `k@4`. This is an isolated optimization.

It's not clear that all users of protected timestamps would benefit from this
optimization. In particular, if the purpose of the protection were for backup
which preserves history then this optimization were invalid. Perhaps there
is a need to extend the protection semantics to specify whether just a single
timestamp is protected or additionally all times newer than that timestamp are
also protected.

That being said, incremental backups which do not preserve history might benefit
massively from this optimization. Imagine a workload which regularly updates
rows, perhaps roughly every minute. For each key it would generate over 1000
rows of history 

### More exact protection in space

Imagine that the span `/Table/53/{1-2}` is protected but a range contains
somehow `/Table/5{3-4}` then we could imagine that there's a part of the
range that is not protected. In this proposal we'll avoid processing any
GC for this range because part of its data is protected. In general this
optimization doesn't seem worthwhile because ranges generally don't cross
table boundaries. If larger ranges which contain multiple tables or indices
become the norm then we may want to rethink this decision.

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

 * Range now becomes even more bloated
   * Range state is interacted with transactionally and thus it incurs even more
     opportunity to race with other admin commands.
 * Splits and merges gain complexity
 * Clearing protected timestamps becomes transaction which touches all protected
   ranges.

Ignoring the cost of installing, clearing, and introspecting these protections,
this initial straw man does not provide any safety mechanisms to ensure that
protections expire or are removed. One could imagine that each protection
installed in the ranges carries its own 
   
One could imagine a hybrid solution where we install both the (spans, ts) and
some cluster-unique identifier for that protection and then there might exist
some mechanism to invalidate protections. 

### Data model

There are levels to the alternatives considered. The most simplistic model
considered was a single key which would store an encoded proto which roughly
captures all of `protectedts.State`. This was deemed too simplistic and
decidedly would not scale (though there's an interesting question about using
column families to store the metadata and state all in one row).

Another alternative was to keep a meta row and a list of protected timestamps
but to further encode the protected timestamps as protos, i.e. encode
`protectedtsbase.Record` and store it in a table. This would probably be totally
fine.

The SQL-heavy data model was primarily chosen out of a desire to utilize and
familiarize myself with the data modeling which our users will be exposed to.

I can certainly see arguments against the interleaved table.

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

- Is it a bad idea to use SQL for the state as opposed to a pseudo-table like
  descriptors?

There is a small efficiency argument to be made for storing the protected
timestamp data as encoded protocol buffers as opposed to structured SQL data
that isn't particularly compelling. The better argument in favor of avoiding
SQL is architectural purity. If there were somehow a correctness bug in SQL
processing it could potentially lead to timestamps not being protected. In the
worst case this seems like it leaves us where we are today, which is to say,
not too bad.
