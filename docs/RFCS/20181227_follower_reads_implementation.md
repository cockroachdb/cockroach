- Feature Name: Follower Reads Adoption
- Implementation Status: draft 
- Start Date: 2018-12-27
- Authors: Andrew Werner
- RFC PR: #33474
- Cockroach Issue: #16593

# Summary

Follower reads are consistent reads at historical timestamps from follower
replicas. They make the non-leader replicas in a range suitable sources for
historical reads. Historical reads include both `AS OF SYSTEM TIME` queries as
well as transactions with a read timestamp sufficiently in the past (for example
long-running analytics queries). Most of the required machinery to safely
perform these reads was implemented in the [Follower Reads
RFC](../20180603_follower_reads.md). Follower reads can greatly improve query
performance by avoiding the need to make wide area RPCs and by reducing traffic
on lease holders. This document proposes mechanisms to expose follower reads
through a new SQL function to determine a reasonable read time stamp and the
accompanying change to make that function legal in use with `AS OF SYSTEM TIME`
queries as well as tweaks to make the SQL physical planner and kv
DistSender aware that follower reads are possible. Given the intention to make
follower reads an enterprise feature, some of the complexity in this proposal
stems from the need to inject behavior from CCL code.

# Motivation

Given that cockroachdb stores multiple replicas of data, a client might expect
that it be able to serve reads from any of those replicas. In order to provide
its high level of consistency and isolation, cockroachdb currently requires that
all reads for a range go to the current lease holder. For reads against data
written sufficiently far in the past, consistency and isolation morally should
not be a concern as no concurrent writes should be possible.  There are many
queries which do not require a completely up-to-date view of the database such
as analytical workloads for report generation.  Enabling reads to be performed
within the same data center can greatly increase throughput for large reads and
greatly reduce latency for small ones. Increasing performance and lowering cost
to run large analytical queries is valuable, especially for geo-distributed
deployments.  Providing a convenient mechanism to request local rather than
lease-holder historical reads will be a compelling enterprise feature.

# Guide-level explanation

The Follower Reads RFC lays out the mechanisms to understand closed timestamps
and presents the rules which the store follows to determine whether a replica
can serve a read. This document deals with how clients can make use of this
behavior offered by the storage layer.  The work described in the Follower Reads
RFC provides the mechanism which enables a replica to determine if a query
timestamp is adequately old to allow for a follower read through the use of its
`Closed Timestamp` mechanism. That work has already enabled replicas to fulfil
read requests in the past. In order to expose this functionality to clients all
that needs to be done is to convince the SQL physical planner to direct
historical reads to local nodes and to coax the `DistSender` to send requests to
followers when appropriate.

The `Closed Timestamp` is tracked on a per-range basis which attempts to lag
behind "real" time by some target duration controlled by the cluster setting
`kv.closed_timestamp.target_duration`. As of writing this value defaults to 30
seconds but could likely be lowered to 5-10 seconds (at some threshold it may
potentially interfere with on-going transactions). The closed timestamp 
subsystem seeks to update the closed timestamp at an interval defined as a
fraction of the `target_duration` which is termed the `close_fraction`. For 
example, if the `target_duration` is 30s and the `close_fraction` is 0.2 then
the subsystem will try to keep the closed timestamp 30s behind real time and
will try to update the value every 6s. This proposal seeks to achieve follower
reads by employing stateless approximation of when a follower read is possible
by assuming that a read may be directed to a follower if it occurs at some 
multiple of the target duration which is controlled by a new
cluster setting `kv.follower_reads.target_multiple`. While this may ultimately 
lead to failure to perform reads at a follower it leads to a simple
implementation that will offer clients a tradeoff between staleness (the amount
of time behind the "present" at which `recent_timestamp()` reads occur) and the
risk of needing to  perform a leader read (which will happen seamlessly due to a
NotLeaseHolderError). The `target_multiple` offers clients a tradeoff between
 staleness and likelihood of follower read failing.

In order to ease the burden of the client determining an adequately old
timestamp for use with an `AS OF SYSTEM TIME` query, this RFC introduces a new
SQL function `recent_timestamp()` which is effectively a syntactic short-hand
for multiplying the above mentioned cluster settings then extends
`AS OF SYSTEM TIME` to allow for a non-constant expression. After this
change and the enabling of `kv.closed_timestamp.follower_reads_enabled` clients
can trivially encourage their `SELECT` statements to be directed to physically
close replicas. For example, imagine that the kv.kv table exists, the below
query would perform a read against the nearest replica:

```
SELECT * FROM kv.kv AS OF SYSTEM TIME recent_timestamp() ORDER BY k LIMIT 10;
```

The physical planning of SQL evaluation currently tries to send DistSQL
processors to be run on nodes which are currently the leaseholder for ranges of
interest. This allocation is performed via the `distsqlplan.SpanResolver` which
internally uses a `leaseHolderOracle` which provides a ReplicaDesc given a
RangeDesc according to a policy. This RFC refactors the oracle logic into its
own package and provides (via injection) a new follower read aware policy.

The `kv.DistSender` currently attempts to send all writes and reads at
consistency levels other than INCONSISTENT to the current lease holder for a
range falling back to replica closeness. This RFC adds an injectable 
`CanUseFollowerReads` function which defaults to returning `false` that the
DistSender code will consult when determining whether to locate the current 
lease holder.

# Reference-level explanation

This section will focus on the specific details of plumbing the functionality
required to expose follower reads through the codebase. Because follower reads
will be implemented as an enterprise feature the core enabling logic will live
in a CCL licensed package `pkg/ccl/followerreadsccl`. This package will then
inject the needed abstractions to provide the following five changes:

1. Introduce the `kv.follower_reads.target_multiple` cluster setting.
2. Add the `recent_timestamp()` SQL builtin.
2. Extend the SQL evaluation to support non-constant expressions with `AS OF SYSTEM TIME`.
3. Abstract the replica selection mechanism for SQL physical planning.
4. Modify DistSender logic to determine when it may safely send a read to a follower.

## Detailed Design

### The `kv.follower_reads.target_multiple` Cluster Setting

The new setting will be defined inside of `followerreadsccl` and thus will only
exist in CCL builds. The setting will be a float value greater than or equal to
one which is combined with `kv.closed_timestamp.target_duration` to determine at
which time `recent_timestamp()` should evaluate. A function in followerreadsccl 
like below will use the recent time:

```go
// recentDuration returns the duration to be used as the offset to create a
// RECENT SYSTEM TIME. The same value plus a unit of clock uncertainty, then
// should be used to determine if a query can use follower reads.
func recentDuration(st *cluster.Settings) time.Duration {
    targetMultiple := FollowerReadsTargetMultiple.Get(&st.SV)
    targetDuration := closedts.TargetDuration.Get(&st.SV)
    closeFraction := closedts.CloseFraction.Get(&st.SV)
    return -1 * time.Duration(float64(targetDuration) * 
        (1 + closeFraction * targetMultiple))
}
```

The setting represents the tradeoff between staleness of `RECENT` queries and
the chance that such queries may fail to be performed on a follower. The
initial choice of value is `3` which likely is rather conservative. Given that
the current target duration for closed timestamps is 30s and the close fraction
is .2, queries performed with `recent_timestamp()` should lag "real" time by
roughly 48s. If we can lower the target duration to 10s which would lead to a
16s real time delay.

### SQL `recent_timestamp()` builtin

A new SQL builtin `recent_timestamp()` is added to call through to an injected
function which determines a timestamp which likely can be used to perform a
follower read. This function will only be functional with the CCL version of
CRDB and an enterprise license. 

### Non-constant Expressions with `AS OF SYSTEM TIME` Queries

Prior to this change, SQL evaluation enforced that the expression passed to
`AS OF SYSTEM TIME` clauses be constant. This requirement prevented the use
of the above mentioned function. Allowing for some evaluation of non-constant
expressions enables useful functionality such as taking the max of two
timestamps allowing enforcement of a maximum staleness.

Allowing `AS OF SYSTEM TIME` clauses to accept all expressions is likely
problematic and the author looks forward to finding an appropriate mechanism
for determining whether an expression is reasonable for use in this context.

### Abstract replica selection for SQL physical planning.

The physical planning of SQL query evaluation attempts to place evaluation near
the lease holder for ranges when known, falling back to a policy which seeks to
pack requests on nearby nodes. This logic is encapsulated in an interface called
a `leaseHolderOracle` (henceforth Oracle) which is constructed based on a
policy. Today's policy is called the `binPackingLeaseHolderChoice`.  All of this
logic currently resides in the `sql/distsqlplan` package and is used by the
`SpanResolver`. A span resolver uses a `*client.Txn` to create a
`SpanResolverIterator` which iterates through ranges and provides replica
selection.

This proposal moves the Oracle logic into a new package
`sql/distsqlplan/replicaoracle` which will extend the current abstraction for
selecting a replica given a policy to additional be able to take into account
the current transaction. The package will also provide a mechanism to register
new policies which we'll see that followerreadsccl will exploit. In addition to
today's existing binPacking and random policies the new package will include a
policy which selects the closest replica.

Prior to this change the policy is used to statically construct an Oracle
which is used throughout the life of the SpanResolver. An Oracle provides a
single method:

```go
// ChoosePreferredReplica returns a choice for one range. Implementors are free to
// use the queryState param, which has info about the number of
// ranges already handled by each node for the current SQL query. The state is
// not updated with the result of this method; the caller is in charge of
// that.
//
// A RangeUnavailableError can be returned if there's no information in gossip
// about any of the nodes that might be tried.
ChoosePreferredReplica(
    context.Context, roachpb.RangeDescriptor, OracleQueryState,
) (kv.ReplicaInfo, error)
```

The change will add a layer of indirection such that rather than holding
an Oracle, the SpanResolver will hold an OracleFactory with the following
interface:

```go
// OracleFactory creates an oracle for a Txn.
type OracleFactory interface {
    // Oracle provides an Oracle to select an appropriate replica for a range.
    Oracle(*client.Txn) Oracle
}
```

For the existing policies the OracleFactory can be implemented by the same
concrete struct which implement today's Oracles by merely returning themselves
in calls to `Oracle()`. This mechanism allows different policies to be used for
different Txns, namely the use of the closest policy for historical queries and
the binPacking policy for all others. This `FollowerReadsAwarePolicy` will check
to see if the OrigTimestamp of a Txn is before now less `recentDuration` plus a
clock uncertainty duration. The `followerreadsccl` code will then register this
new policy and set it to the global var `distsqlplan.ReplicaOraclePolicy`.

### Expose DistSender For Determining Follower Read Safety

The last hurdle to exposing follower reads is that the `kv.DistSender` attempts
to send batch requests to current lease holders which may prevent reads from
going to nearby follower replicas. In order to inform the DistSender that it can
send a batch request to a follower we add a new global var in the kv package

```go
var CanSendToFollower = func(
   clusterID uuid.UUID, _ *cluster.Settings, _ *roachpb.BatchRequest,
) bool {
   return false
}
```

Which is adopted by the DistSender when it decides whether to look up a cached
lease holder in `DistSender.sendSingleRange`. The followerreadsccl package can
then inject a new implementation of this function which ensures that the batch
request is a read only transaction and then verifies that it meets the criteria
for a follower read.

## Drawbacks

There are very few drawbacks to implementing the high level idea.  Most of the
groundwork has already been laid. Any obvious downsides come from the impurity
of the injection required to realize the functionality as an enterprise feature.

## Rationale and Alternatives

### Stateful closed timestamp tracking

One potential downside of this approach is that in an edge case it may have the
potential to have a detrimentally effect cluster performance in the case of
bursty traffic and a large volume of follower reads. Imagine a situation where a
large percentage of client traffic is due to follower reads and the cluster is
heavily loaded such that all transactions are performing acceptably but if the
workload were to be shifted entirely such that all requests were forced to go to
leaseholders it would not be capable of acceptably serving the traffic. If then,
a burst of load or some other cluster event were to lead one or more replicas to
fall behind in its ability to publish closed timestamps, all traffic which was
spread over all of the replicas would begin recieving all of the load that had
been going to followers. It is possible that this concern is not realistic in
most common cases.  Furthermore it seems straightforward to mitigate by
increasing the target multiple. The problem seems worse as the replication
factor increases beyond 3 to numbers like 7 or 9. Furthermore even if the
increased load during this bursty period does not meaningfully affect OLTP
traffic, it may lead to potentially massively increased latency for queries
which in the previous regime had been fast.

A more sophisticated mechanism which statefully tracks a closed timestamps on a
per-range basis on all nodes would allow `recent_timestamp` to always evaluate
to a timestamp which is known to be closed. Such an approach may, in the common
case, be less pessimistic than this proposal's target_multiple and unlike the
optimistic approach, would be sure to always safely perform follower reads.
That being said, a stateful approach which tracks follower reads would require
nodes to track closed timestamps for all replicas at planning time and may
additionally require new mechanisms to mark as known to be safe for follower
reads. Furthermore the state tracking may be prohibitively expensive on large
clusters.

Another less invasive might be to dynamically update the target multiple by
detecting NotLeaseHolderErrors for queries which expected to hit followers.
This could mitigate the flood of consistent reads in the face of lagging closed
timestamps but would make the semantics of `recent_timestamp()` harder to 
understand and would require increased participation from the DistSender to 
provide the feedback.

## Unresolved questions

### What sort of expressions should be legal with `AS OF SYSTEM TIME`

In order to allow for a builtin to determine the time for an `AS OF SYSTEM TIME`
query, we need to lift the restriction that only constant expressions are
allowed. Removing this limitation is trivial but may have consequences the
author does not expect. The semantic properties are limited to
`RejectSpecial|RejectSubqueries` which the author believe should limit overly
expensive work in expression evaluation. Is that sufficient? Is there another
property of the expression which could or should be enforced that is weaker
than constant and permits the use of builtins but forbids access to tables or
other database state?

### Is the CCL Injection Reasonable

For the amount of logic which it actually provides, the CCL injections feels
potentially heavy. Is there an alternative way to lay out the CCL code which may
feel less egregious?

### How does this capability fit in to the SQL query planning?

There has been some discussion of the SQL optimizer taking expected latencies
(based on RTT to replicas) into account when planning a query. For historical
queries the optimizer can expect DistSender to use the local replica rather than
the lease holder. This could enable big wins for analytical queries in
geographically distributed clusters. How do we enable this level of
optimization? What work needs to be done now? What can be done later?
