- Feature Name: SQL Liveness
- Status: draft
- Start Date: 2020-06-15
- Authors: Andrew Werner, Spas Bojanov
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

As Cockroach moves towards a multi-tenant world, we need a mechanism to
associate resources at the SQL level with tenant SQL processes. This
RFC introduces a registry of live SQL instances for the purposes of
tenant-scoped resource leasing and discovery.

# Motivation

Today some amount of resource leasing (jobs) is associated with
node-liveness while other resource leasing explicitly heartbeats (sql 
table leases) KV rows with references to node IDs. Both the 
node ID and node-liveness will be unavailable to SQL tenant processes.

The driving motivation for this work is the usage of node-liveness 
epochs as the basis for the best-effort locks in the jobs registry.
Jobs are a property of an individual SQL tenant rather than of a KV 
deployment.

An additional goal of this effort is to strengthen the
semantics of job leases to ensure that only the most recent successful
session which has adopted a job can update that jobs state as today
there is little that precludes concurrent resumptions of the same job
from overwriting eachother's state.

# Guide-level explanation

The design of SQL liveness is heavily inspired by cockroach's 
NodeLiveness layer, though differs in its implementation due to its 
differring constraints and implementation environemnt. SQL Liveness does
not need to worry about the bootstrapping problems involved in creating
range leases (see [Range Leases RFC](../20160210_range_leases.md)).

The goals of SQL liveness is twofold:

1) Causality propagating notion of instance liveness.

This use maps directly to the use of node liveness to drive epoch-based
range leases. We would like to utilize SQL liveness to create leases
which do not need to be independently heartbeated to remain active. This
is primarily a scalability optimization but additionally confers 
lifetime clarity on distributed resources throughout cockroach.

Over time we anticipate needing more rather than less distributed
coordination and expecting each such case to implement its own leasing
protocol is madness.

2) Discovery of SQL instances and their metadata.

This use case is secondary but likely important as we discuss exposing
DistSQL to tenants. It is probably the case that the data stored in
SQL liveness should not drive network address lookup and status alone 
but we anticipate that it will be valuable for bootstrap.

## Key Terms

 * `Instance` - An instance is a (unix) process running a SQL server. An instance is bound to a single SQL tenant. Instances have at various times been referred to as a SQL pod, or a SQL node. An instance will have an `InstanceID` which is gauranteed to be unique within the tenant cluster while the `Instance` exists. 
 * `SQLInstanceID` - A 64-bit integer ID associated with an instance. It is guaranteed that each existing `Instance` will have a unique `SQLInstanceID`. It is, however, possible that an `SQLInstanceID` will be
 recycled and used by a different `Instance` which shares no state or
 properties with a previous holder of that ID.
 * `Epoch` - An epoch is a sequence number associated with the liveness
of an `SQLInstanceID` in a tenant cluster.
 * `Session` - A session is defined as a tuple of `(SQLInstanceID, Epoch)`.
 An instance will have at most one live `Session` at a time.
 * `Record` - A record associates an `SQLInstanceID` to its `Session` 
 and additional metadata. A `Record` contains an expiration timestamp. 
 The expiration timestamp is updated periodically by the `Session` to
 maintain its liveness.
 * `Claim` - A resource which refers to a session. A claim can be viewed
as a lock which has a lifetime tied to the session. In order to ensure 
that a claim remains valid, the session must either be observed by 
transactions which utilize the claimed resource or the transaction must
commit before a known expiration timestamp for the session (perhaps by
utilizing the `*kv.Txn`'s commit deadline functionality which has serious room for improvement).

## Interface

```go

package sqlliveness

type Epoch int64

type SessionInfo struct {
    InstanceID base.SQLInstanceID
    Epoch      Epoch

    // Transactions run by this Instance which ensure that they commit before this
    // value will be assured that any resources claimed
    // under this session are known to be valid.
    //
    // See discussion in Open Questions.
    Expiration hlc.Timestamp

    // Metadata might include network addresses, locality information, constraints, etc.
    Metadata   ...
}

// Instance represents a SQL tenant server instance.
type Instance interface {
    InstanceID() base.SQLInstanceID
    Session(context.Context) (Session, error)
}

type Session interface {
    Done() <-chan struct{}

    Info() SessionInfo

    // Observe can be used to transactionally ensure that
    // the current session is not finished. If the transaction
    // commits, any resources claimed under this session
    // are known to be valid.
    //
    // See discussion in Open Questions about whether this
    // is a good idea.
    Observe(context.Context, *kv.Txn) error
}

type SessionIterator func(SessionInfo) (wantMore bool)

type Storage interface {
    Sessions(context.Context, SessionIterator) error
}
```

Above find something of a straw-man interface to the `sqlliveness`
subsystem. Key is the idea that an `Instance` may have multiple
`Session`s over its lifetime and that the sessions may end. Further
note that a transaction may `Observe` a session. This API provides
a mechanism by which a transaction can ensure that if it commits, the
observed session exists.

# Reference-level explanation

The creation and maintainance of session liveness is entrusted to
the `Instance` (TBD whether the construction of the current `Session`
should be done lazily or eagerly). Each SQL server will have a handle
to an instance. The `Instance.Session()` method will block until a 
`Session` has been durably created (straw man). When the
`Session` fails to heartbeat its record and thus extend its expiration
due to the detection of an epoch change, it will close its `Done` 
channel. Resources claimed by that session will no longer be held.
Operations which need to ensure the validity of claims for correctness

## Liveness, expirations, and epoch bumping

The `sqlliveness` subsystem maintains a set of sessions. Sessions
may be `live`, `expired`, or `done`. An `expired` session only
differs from a `live` session in that it can be moved to `done` by
another `Instance`. A `Session` may be successfully observed even if
the current expiration has already passed.

In this straw man, each instance's storage will periodically poll
all of the instances and will bump epochs for instances which are
expired. This background bumping may not be ideal for rapid resource
reclaimation. The mechanics of epoch bumping are left as an open 
question

## State

This RFC proposes that liveness session state live in a new system 
table, `system.sqlliveness`. The table will initially have the
following schema:

```sql
CREATE TABLE system.sqlliveness (
    instance_id INT8 PRIMARY KEY,
    epoch       INT8 NOT NULL,
    expiration  DECIMAL -- could use TIMESTAMPTZ, though handy to use an HLC as we can then convert it to a commit deadline
    -- various other metadata as we see fit.
    -- probably worth using column families.
);
```

### Storage implementation

For the first pass I anticipate that the `Storage` implementation just periodically polls the `system.sqlliveness` table.

In a second iteration it might maintain a cache by using a rangefeed.

Cached data is generally okay here.

## Use in `jobs`

See [#47892](https://github.com/cockroachdb/cockroach/issues/47892). 

### Some background on what's there today

Today jobs are "leased" by the `Registry` by writing a `NodeID` into a field in the job's `Payload`. This forms something of a best-effort
prevention of jobs being adopted by more than one node. The `Registry`
periodically polls the set of jobs and checks whether the `NodeID` for
a job's lease is live, if not, it considers the job as available for
adoption. Furthermore, the `Registry` periodically checks if it itself
is alive and, if not, it cancels the jobs it is currently running. It
remains wholly possible for a node running a `Registry` to become not
live for a short period of time, during which time another registry
adopts the job, and begins to run concurrently, and then, by the time
that the `Registry` goes to check whether it should cancel its jobs,
finds that it is again live and merrily keeps them running.

It's shocking the extent to which the `Registry`/`Job` subsystem does
not validate that it is, in fact, the current leaseholder node when
updating job state. I (Andrew Werner) feel relatively confident that 
this has caused bugs in the past. It is worth noting that the original RFC seemed more careful about ensuring leases remained valid but alas, today's are not such.

### Overview

The basic idea is that we'll transition to running jobs only on behalf of a
 `Session`. The `Registry` will have a handle to a `sqlliveness.Instance` and
 will hold on to its current `Session`. When the `Session` is `Done`, the
`Registry` will cancel execution of all jobs claimed under
that `Session`.

When adopting a job, a registry will do so using its 
current active session and will serialize the session into
the job record.

### Jobs table changes

The jobs table will be augmented to include two new columns:

```sql
ALTER TABLE system.jobs ADD COLUMN claim_instance_id INT8 FAMILY claim,
                        ADD COLUMN claim_epoch INT8 FAMILY claim;
```

### Adopting jobs

When looking to adopt jobs, the `Registry` can construct a
query which excludes jobs claimed by sessions thought to
currently be live (or at least not finished).

As an aside, perhaps we can expose the live sessions through
a `crdb_internal` table and delegate this checking to a join
between these two tables and in that way avoid even given a 
`sqlliveness.Storage` to the `Registry` (great idea or bad idea?).

For example:

```sql
-- Exposes access to the cached view of sqlliveness.
CREATE TABLE crdb_internal.node_sqlliveness_cache (
    sql_instance_id INT8 PRIMARY KEY,
    epoch           INT8,
    -- ...
)


SELECT *
  FROM system.jobs
       JOIN (SELECT instace, epoch FROM crdb_internal.node_sqlliveness_cache) AS t ON
            claim_instance IS NULL
            OR (instance = claim_instance AND epoch < claim_epoch)
 WHERE state IN ("<non-terminal states>",);
```

### Mixed version migration

During the mixed version time period, we'll require that jobs maintain both 
types of leases. In particular, we should filter out the set of nodes we know
to be live right now. This conditions is a bit annoying today because we cannot
evaluate the predicate over the lease field of the payload in the execution
engine (see this proposal for a protobuf type
[#47534](https://github.com/cockroachdb/cockroach/issues/47534)).

## Future use cases

### DistSQL address discovery bootstrap

We can store metadata on these sessions that have network addresses and also
maybe constraints or attributes which can help with physical planning decisions.

I suspect we'll want to augment whatever metadata we get with other information
like contained by the rpc context. To some extent this liveness info can be
combined with other signals like inbound and outbound connection state to get
a better picture of actual liveness.

### SQL leases

Today each SQL node maintains a set of leases for table 
descriptors in the `system.lease` table. The 
`lease.Manager` is responsible for heartbeating the leases
on a per-descriptor basis. In most clusters only a small
number of leases are in use at a time, say, at most 
hundreds. Today leases last for 5 minutes but refresh every minute. At 
100 leases that's just 5/3rd updated per node per second.

As user-defined types flourish and the scale at which cockroach grows, perhaps 
users might want to have 10s of
thousands of leased objects (fine, it's sort of farfetched).
At that point, the lease renewal load would be something like 165 per node per 
second.

Maybe that 5m is excessive as we anticipate running SQL
pods on pre-emptible instances. Shortening it too much is costly here.

### `pg_advisory_lock`

One postgres feature that we've discussed building are
`pg_advisory_locks`. Having a sql-level liveness construct
for the gateway node may make this easier. 

## Drawbacks

TODO(ajwerner)

## Rationale and Alternatives

### Leaving leasing to subsystems (maybe with libraries)

The main alternative is to not build a subsystem but rather to request
that each individual subsystem which needs to lease resources to find
a solution which best fits its needs. With this, perhaps, could be the
construction of libraries which are flexible and can be plugged into
subsystems with ease. For example, the existing `sqlmigration/leasemanager`
package provides an implementation of a heartbeated lease
which can be relatively generically plugged into a column family of
any sort of table.

In the end, however, the above is maintainability nightmare and potentially
exposes cockroach to scalability problems as it is unlikely
that projects whose sole purpose is anything other than leasing will
invest in a well-designed solution.

### Providing lower-level "session" primitives decoupled from instances

One closely related idea is buy into the idea that generally centralizing the
concept of heart-beated rows used to then claim resources is good but that
binding that concept to sql instance IDs is a mistake.

The basic proposal put forth here is that we want to tie the lifetime of a claim
of a resource to a "lease" that has a validity period. We can then utilize
either the timestamp validity or the properties of serializable transactions to
ensure that transactions utilizing resources can prove that if they commit then
they had a valid claim to that resource.

In this proposal, the unique identifier for this "lease" is a `Session` or
`(InstanceID, Epoch)`. In Etcd, for example, these leases are always stored
as `UUID`s and the process of keeping these leases alive is left to the client
(though the interface has a `KeepAlive` which should do the heavy lfiting), see
[here](https://github.com/etcd-io/etcd/blob/d8c8f903eee10b8391abaef7758c38b2cd393c55/clientv3/lease.go#L108-L148).
They then have a higher level construct built on top of this `Leaser` called a
[`Session`](https://godoc.org/github.com/coreos/etcd/clientv3/concurrency#Session)
 which represents, roughly, the non-metadata portions of what we propose here.

A main counter-argument against using `UUID`s for sessions is that it will make
observability more difficult for little obvious gain. The epoch-based leases in
KV have shown us that node epochs, given their monotonic nature, give state in
the cluster a sense of time and place.

The other main argument for this more general approach is that it allows
attaching different resource claims to different lifetimes. It does not seem
obvious to me how this would be a good thing within cockroach.

Perhaps one use case that I don't feel super eager to entertain is a distributed
lock which could be "resumed" in the face of gateway failure and exposed at a 
low level to clients? 

## Unresolved questions

- How does epoch bumping happen? Should it be explicit? In this straw-man it 
  happens due to periodic polling. 
- How long should expirations be?
    - Maybe like 1m? Shorter than SQL schema leases but much longer than node
      liveness.
- How should transactions dependent upon claims verify the 
validity? There are two different approaches offerred above and another below.
They seems like they all might have valid use cases. Are any strictly better?
Is there any reason to avoid doing any of them?

    1) Observing the session record
    
        - This approach is potentially expensive because you have to read from
          the sqlliveness table. Today we don't have the APIs to attach the
          logic of observing the session to some future batch; we'd need to
          send an RPC to do the observation. In a single-region cluster this is
          a good approach. For global clusters and latency sensitive operations,
          it's pretty bad.
        - This approach is correct if transactions are serializable.
        - This approach doesn't rely on any hooks into the KV.


    2) Ensuring transactions commit below the expiration.
    
        * This is the approach used by SQL schema leases today.
        * Today's use is primative: the transaction deadline is the earliest of
          the expirations seen for any table descriptor. Even if later in the
          transaction that lease gets renewed. E.g.
          ```sql
          CREATE TABLE foo (i INT PRIMARY KEY);
          INSERT INTO foo VALUES (1);
          BEGIN;
            SELECT * FROM foo;
            SELECT pg_sleep('5m'::INTERVAL::FLOAT);
            INSERT INTO foo VALUES (2);
          COMMIT;

          ERROR: restart transaction: TransactionRetryWithProtoRefreshError: TransactionRetryError: retry txn (RETRY_COMMIT_DEADLINE_EXCEEDED - txn timestamp pushed too much; deadline exceeded by 1m2.355364715s (1592448539.221650880,2 > 1592448476.866286165,0)): "sql txn" meta={id=ca3a8484 key=/Table/59/1/2/0 pri=0.00314736 epo=0 ts=1592448539.221650880,2 min=1592448236.647092821,0 seq=2} lock=true stat=PENDING rts=1592448539.221650880,2 wto=false max=1592448236.647092821,0
          SQLSTATE: 40001
          ```

        * This approach has the problem of potentially exposing tight coupling to
        the KV layer. The KV layer indepently decides when it's going to issue
        EndTransaction requests. Ideally for this approach to work best, the
        deadline would be determined as close to the sending as possible.
        * It seems reasonable to inject a closure into the KV coord to call
          as it constructs the EndTxn.

    3) (not yet discussed) Removing resources with epoch bumps

        * Approach discussed in [this blog post](https://dev.to/ajwerner/quick-and-easy-exactly-once-distributed-work-queues-using-serializable-transactions-jdp).
        * Imagine if all "claims" had a FOREIGN KEY relationship with the
          sqlliveness table with `ON DELETE SET NULL`
        * Nice because it moves the cost of validation to the rare event of
          bumping an epoch. Now you just need to read your claim which is likely
          in the data you're already updating.
        * Big bummer here is that we're talking about sticking foreign key
          relationships between system tables.
        * Also seems really bad to make marking a node as dead an expensive
          proposition.

    4) Perhaps there's a compromise here between 1) and 3) to be made here.
       Let's call this one lazy resolution.
        * This is a lot like intent resolution.
        * In order to take a resource, you need to clear its claim.
        * In order to clear a claim you have to ensure that it's session
          is done.
        * If you observe your claim in your transaction, then you know you
          hold the resource.
        * This is probably the approach that makes the most sense for the jobs
          use case (rather than 2). Perhaps 2 should not exist at all.
