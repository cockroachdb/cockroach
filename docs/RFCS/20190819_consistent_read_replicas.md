- Feature Name: Consistent Read Replicas
- Status: draft
- Start Date: 2019-08-19
- Authors: Nathan VanBenschoten
- RFC PR: TODO
- Cockroach Issue: None

# Summary

Consistent Read Replicas provide a mechanism through which follower replicas in
a Range can be used to serve reads for **non-stale** read-only and read-write
transactions.

The ability to serve reads from follower replicas is beneficial both because it
can reduce wide-area network jumps in geo-distributed deployments and because it
can serve as a form of load-balancing for concentrated read traffic. It may also
provide an avenue to reduce tail latencies in read-heavy workloads, although
such a benefit is not a focus of this RFC.

The purpose of this RFC is to introduce an approach to consistent read replicas
that I think could be implemented in CockroachDB in the medium-term future. It
takes inspiration from @andy-kimball and the AcidLib project. I'm hoping for
this to spur a discussion about potential solutions to the problem and also
generally about our ambitions in this area going forward.

The most salient design decision from this proposal is that it remains
completely separate from the Raft consensus protocol. It does not interact with
Raft, instead operating in the lease index domain. This is similar in spirit to
our approach to implementing closed timestamps.

The RFC includes three alternative approaches that each address part of these
issues, but come with their own challenges and costs. Unlike with most RFCs, I
think it's very possible that we end up preferring one of these alternatives
over the main proposal in the RFC.

# Motivation

Closed timestamps and [follower
reads](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180603_follower_reads.md)
provide a mechanism to serve *consistent stale reads* from follower replicas of
a Range without needing to interact with the leaseholder of that Range. There
are two primary reasons why a user of CockroachDB may want to use follower
reads:
1. to avoid wide-area network hops: If a follower for a Range is in the same
   region as a gateway and the leaseholder for that Range is in a separate
   region as the gateway, follower reads provides the ability to avoid an
   expensive wide-area network jump on each read. This can dramatically reduce
   the latency of these reads.
2. to distribute concentrated read traffic: Range splitting provides the ability to
   distribute heavy read and write traffic over multiple machines. However, Range
   splitting cannot be used to spread out concentrated load on individual pieces
   of data. Follower reads provides a solution to the problem of concentrated read
   traffic by allowing the followers of a Range, in addition to its leaseholder,
   to serve reads for its data.

However, this capability comes with a large asterisk. Follower reads is only
suitable for serving _**historical**_ reads from followers. They have no ability
to serve consistent reads at the current time from followers. Even with
[attempts](https://github.com/cockroachdb/cockroach/pull/39643) to reduce the
staleness of follower reads, their historical nature will always necessarily
come with large UX hurdles that limit the situations in which they can be used.

The most damaging of these hurdles is that, for all intents and purposes, follower
reads cannot be used in any read-write transaction - their use is limited to read-only
transactions. This dramatically reduces their usefulness, which has caused us to
look for other solutions to this problem, such as [duplicated indexes](https://www.cockroachlabs.com/docs/v19.1/topology-duplicate-indexes.html)
to avoid WAN hops on foreign key checks.

Another hurdle is that the staleness they permit requires buy-in, so accessing
them from SQL [requires application-level changes](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20181227_follower_reads_implementation.md).
Users need to specify an `AS OF SYSTEM TIME` clause on either their statements
or on their transaction declarations. This can be awkward to get right and
imposes a large cost on the use of follower reads. Furthermore, because a
statement is the smallest granularity that a user can buy-in to follower reads
at, there is a strong desire to support [mixed timestamp statements](https://github.com/cockroachdb/cockroach/issues/39275), which
CockroachDB does not currently support.

Because of all of these reasons, follower reads in its current form remains a
specialized feature that, while powerful, is also not usable in a lot of
important cases. It's becoming increasingly clear that to continue improving
upon our global offering, we need a solution that solves the same kinds of
problems that follower reads solves, but for all forms of transaction and
without extensive client buy-in. This RFC presents a proposal that fits within a
spectrum of possible solutions to address these shortcomings.

# Guide-level explanation

## Consistent Read Replicas

The RFC introduces the term "consistent read replica", which is a non-leaseholder
replica in a Range that can serve transactionally-consistent reads but that cannot
serve writes. Any transaction (read-only or read-write) is able to route read-only
batches to consistent read replicas.

A Range with no read replicas will behave exactly as it does now with respect to
reads and writes. A Range with one or more read replicas will be required to
perform an extra round of communication between the leaseholder and each of the
Range's read replicas before evaluating a write. In exchange for this, each of
the read replicas in the Range are able to serve consistent reads, offsetting
the reliance on a Range's single leaseholder. In effect, read replicas trade
reduced write performance for improved read performance, which may be an
appropriate trade-off for read-heavy data.

Consistent read replicas are configured using zone configs. It's likely that their
configuration will mirror that of [lease preferences](https://www.cockroachlabs.com/docs/v19.1/configure-replication-zones.html#constrain-leaseholders-to-specific-datacenters).
Zones will default to zero consistent read replicas per Range, but this can
be configured by specifying localities that consistent read replicas should be
placed. Because this proposal is still a rough draft, this is as far as the
discussion about the UX of consistent read replicas will go for now.

## Example Use Cases

### Reference Tables

It is often the case that a schema contains one or more tables composed of
immutable or rarely modified data. These tables fall into the category of "read
always". In a geo-distributed cluster where these tables are read across
geographical regions, it is highly desirable for reads of the tables to be
servable locally in all regions, not just in the single region that the tables'
leaseholders happens to land. A common example of this arises with foreign keys
on static tables.

Until now, our best solution to this problem has been [duplicated indexes](https://www.cockroachlabs.com/docs/v19.1/topology-duplicate-indexes.html).
This solution requires users to create a collection of secondary SQL indexes that
each contain every single column in the table. Users then use zone configurations
to place a leaseholder for each of these indexes in every geographical locality.
With buy-in from the SQL optimizer, foreign key checks on these tables are able
to use the local index to avoid wide-area network hops.

A better solution to this problem would be adding a consistent read replica for
the table in each geo-graphical locality. This would reduce operational burden,
reduce write amplification, and increase availability.

### Read-Heavy Tables

In addition to tables that are immutable, it is common for schemas to have some
tables that are mutable but only rarely mutated. An example of this is a user
profile table in an application for a global user base. This table may be read
frequently across the world and it expected to be updated infrequently.

# Reference-level explanation

## Detailed design

- All handling of consistent read replicas is done at the Range lease level. No
  complexity is pushed into the Raft consensus level
- Leases are extended to store a single leaseholder and 0 or more "read replicas"
- A single leaseholder remains the only replica that can propose writes through Raft
- The leaseholder and all read replicas are able to serve reads. They all maintain
  "partial" Timestamp Cache structures
- Timestamp Cache information is collected from each read replica during each write
- Latches are maintained across the leaseholder and all read replicas during each
  write

### Read Path

Read-only batch requests can be routed to the leaseholder or any of a Range's
read replicas. The read would first perform a lease check, which would be handled
as usual. The replica would check whether it is the leaseholder or one of the read
replicas in the currently active lease. If it is, then it compares the liveness epoch
in the lease with the replica's current liveness epoch, and uses the replica's current
liveness expiration as the lease expiration. This is handled exactly as it is today.

If the read replica's lease is valid then the read would go through the exact
same path that it normally does. It would acquire latches from its spanlatch
manager, it would evaluate against the local state of the storage engine, and the
it would bump its timestamp cache.

This entire read path is almost identical on the leaseholder and on read replicas.

### Write Path

The write path is where the extra complication of read replicas comes into play.
Again, the leaseholder remains the only replica that is allowed to propose writes,
so all writes are directed to it. When a write comes in, the lease is checked,
latches are acquired, and the timestamp cache is consulted, just as usual.

Then the current lease is checked for any read replicas. If there are none, the
write proceeds as normal. If there are read replicas, things get more
interesting. Before evaluating the write, the leaseholder is required to send an
RPC to each of the read replicas. This RPC would be called a `RemoteLatch`
request (name pending), and would contain the following pieces of information: 
```
message RemoteLatchRequest {
   hlc.Timestamp timestamp      = 1;
   repeated Span spans          = 2;
   int64         lease_sequence = 3;
   bytes         command_id     = 4;
}
```

When a read replica received one of these `RemoteLatch` requests, it would acquire
a collection of latches at the specified timestamp. Once all of these latches were
acquired, it would check its local timestamp cache over the specified spans and return
the maximum timestamp in its cache. It would then return the following response and
would **not remove** the latches it had acquired.
```
message RemoteLatchResponse {
   hlc.Timestamp max_timestamp    = 1;
   bool          timestamp_bumped = 2;
}
```

The read replica would then wait for one of the following two occurrences before
removing the latches it acquired with this request:
1. it observes a Raft command with the same ID pop out of Raft and apply (note that
   this is equivalent to when the leaseholder removes its latch for this request)
2. it observes a lease change that increments the lease sequence

### Lease Changes

We introduce the notion of "read replicas", which are stored on Range leases
(which might be better thought of now as Range "configurations"). Each
`roachpb.Lease` records a single leaseholder and its node liveness epoch, along
with zero or more read replicas and their node liveness epochs. The Lease
structure still maintains a monotonic sequence number, which is incremented
whenever the configuration present in the Lease changes and is included on each
Raft proposal.

We assume that any Lease with read replicas is an epoch-based lease.

### Adding a Read Replica

#### Cooperative

_Cooperative configuration changes that are allowed when the replicas affected
by the change are live and can be made aware of the adjustment. They are an
optimization. Currently, `TransferLease` requests fall into this category._

Adding a new read replica only requires synchronization with the current
leaseholder. It does not require communication with the new read replica itself.

#### Non-Cooperative

_Non-cooperative configuration changes that are required when the replicas
affected by the change are non-live or otherwise cannot be made aware of the
adjustment. Currently, `RequestLease` requests fall into this category._

Adding a new read replica without the leaseholder's approval will require
invalidating the current leaseholder's node liveness epoch and choosing a new
leaseholder. It's probably not worthwhile to support this state transition.
Adding a new read replica does not require communication with the new read
replica itself.

### Removing a Read Replica

#### Cooperative

Removing a read replica requires synchronization with the read replica to ensure
that it stops serving local reads. It then requires communicating with the
leaseholder to ensure that it stops sending the old read replica
`RemoteLatchRequest`s.

#### Non-Cooperative

Removing a read replica without the read-replicas approval requires invalidating
the current read replica's node liveness epoch. Only after this point can the
read replica be removed from the lease.

### Availability

This mechanism must be able to handle the failure of the leaseholder and the
failure of any of the consistent read replicas.

#### Leaseholder Failure

If nothing else was done with this proposal then a leaseholder failure could
leak write latches held on read replicas. There are two mechanisms we must
introduce to address this.

The first mechanism is latch invalidation. When a write latch is acquired on a
read replica, it will be bound to the lease sequence number present in the
`RemoteLatch` request. When the read replica applies a new lease (see
`leasePostApply`), it will remove all latches with old sequence numbers. This is
permissible because any write proposed under the previous sequence number will
necessarily fail with a lease mismatch error, so it cannot interact with any
future request.

The second mechanism is a periodic check for expired leases on read replicas
that contain remote write latches that are blocking reads. This parallels the
lazy lease acquisition in `redirectOnOrAcquireLease`, which attempts to acquire
a lease when no valid leaseholder exists, except it must also be able to detect
expired leases after some period of blocking on latches. Without this, a read
could enter a read replica, observe a valid lease, and then get stuck
indefinitely waiting for a write latch to be invalidated.

#### Read Replica Failure

Similarly, if nothing else was done with this proposal then a read replica
failure could stall all write traffic because all `RemoteLatch` requests would
begin to block. To handle read replica failures, a leaseholder must remove the
read replica from the current lease by acquiring a new lease sequence (see the
section on non-cooperative read replica removal) before continuing with its
proposal. This will typically involve incrementing the read replica's node
liveness epoch before removing the read replica from the lease.

## Drawbacks

The major drawbacks of this approach are that it is somewhat complex, it
introduces a latency penalty on writes to Ranges with one or more read replicas,
and it requires an explicit configuration decision.

The two "optimistic" alternatives below are able to avoid this latency penalty
and this configuration decision, but at the expense of increased transaction
retries under contention and concerns around stale reads.

## Rationale and Alternatives

There are three viable alternatives that I am aware of to a design like this.
The alternatives chose different trade-offs and impose different costs to
accomplish a similar set of goals. They each deserve full consideration, as
their inclusion in this RFC is meant to inspire conversation.

### Reader-Enforced Consistent Follower Reads

We can think of the design laid out thus far in this RFC as "writer-enforced
consistent follower reads". The responsibility of ensuring that all reads from
followers are consistent is owned by writers, who are forced to pay a cost of
consulting remote timestamp caches before proposing changes to Raft.

We can come to an alternative solution to this problem by shifting the
responsibility of enforcing consistent follower reads onto readers. Instead of
requiring that writers reach out to consistent read replicas before writing,
we instead require that readers from follower replicas consult the leaseholder
before serving reads.

An implementation of this would look something like the following:
1. read request enters follower replica
2. follower replica sends `RemoteRead` request to leaseholder replica
3. `RemoteRead` request acquires latches, grabs current lease applied index, and
   bumps timestamp cache to requests (span, timestamp). It returns a `RemoteRead`
   response, which contains the lease applied index found during evaluation
4. upon the receipt of the `RemoteRead` response, the follower replica waits until
   its lease applied index equals or exceeds the index from the response. If it is
   in the majority quorum for the Range, it should already be there
5. follower replica evaluates the read request and returns a response

This is a much simpler proposal than the primary one explored in this RFC. It does
not require distinguished "consistent read" followers. It also does not require any
interaction with closed timestamps. This is because the proposal is essentially
creating a form of follower-requested fine-grained closed timestamps.

The alternative addresses some of the goals of this RFC. For instance, it can be
used as a form of load-balancing if the leaseholder-served `RemoteRead` request
is significantly less expensive than the read itself (think large scans).
However, at least in its naive form, it does not address the biggest goal of
this proposal, which is to avoid wide-area network hops when performing reads
across a geographically-diverse cluster topology. Batching these `RemoteRead`
requests across reads might help here, but that doesn't seem any more possible
for interactive transactions than batching remote reads themselves.

Furthermore, in imposing a new cost on reads instead of writes, the alternative
goes against the general intuition that reads are commonly orders of magnitude
more frequent than writes. Because of this, it's probably not the right
general-purpose solution.

#### Benefits
- Simple
- Independent of closed timestamp duration

#### Drawbacks
- Does not avoid WAN jump to leaseholder
- Still requires interaction with leaseholder when serving reads, so not a
  perfect tool for load-balancing.
- Cost imposed on individual reads instead of individual writes

### Inconsistent Optimistic Follower Reads

_The following two alternatives are similar in that their primary focus is to
avoid cross-region communication per-statement during the evaluation of a
transaction when the leaseholders for the data accessed in the transaction are
remote but followers for that data are local. They differ primarily in their
implementation._

An idea floated by @spencerkimball was to evaluate transactions against follower
replicas inconsistently, at the transaction coordinator-chosen timestamp.
Transactions would then send the equivalent of a refresh span request to the
remote leaseholder to validate the initial optimism before committing.

The benefit here is that it imposes no extra cost on writes. Instead, the cost
is imposed once-per-transaction when validating that the inconsistent reads
observed the correct state. The other benefit is that this approach does not
require any extra manual configuration.

The downside is that this optimistic approach introduces the chance of increased
transaction retries. It also raises the question of stale reads before the read
refresh, which clients may or may not be ok with.

#### Benefits
- No extra cost for individual writes or reads
- No explicit client buy-in. Could even be a cluster setting

#### Drawbacks
- Initial reads are inconsistent, which may cause all kinds of issues for both
  our SQL layer and for client applications. This is severe enough that I don't
  think we could enable the feature by default
- Requires a round of remote read refreshing before committing transactions.
  This latency will be difficult to hide, although at least it's once per
  transaction instead of once per statement
- Optimistic, may cause transaction restarts and severely changes our approach
  to concurrency control
- May result in stale reads before refresh is performed. This again indicates
  that a causality token would be useful

### Consistent Optimistic Follower Reads

A similar alternative to Inconsistent Optimistic Follower Reads is Consistent
Optimistic Follower Reads. The idea here is to start with transaction timestamps
far enough in the past that all reads served by followers can be done below
their closed timestamp. In other words, all transactions would start at the
timestamp returned by `experimental_follower_read_timestamp()`. We might even
start with the timestamp a little further back to incorporate replication
latency so that it's more likely that any local followers we contact will
have heard of a closed timestamp high enough to serve reads at the transaction
timestamp.

In doing so, we can be sure that all followers are serving stale consistent
reads instead of fully inconsistent reads. This eliminates the problem from the
previous alternative that the initial read portion of transactions could return
invalid and potentially conflicting data, which could cause all kinds of
problems at both the CockroachDB SQL layer and in client applications. Of
course, the trade-off here is that this stale consistent timestamp might be far
in the past, resulting in even more transaction restarts due to contention and
an even larger chance of observing stale reads before the read refresh.

The idea of starting transaction timestamps at the follower read timestamp seems
significantly less wild after the observation from [this comment](https://github.com/cockroachdb/cockroach/issues/36478#issuecomment-521903404),
which implies that the closed timestamp duration could be made arbitrarily low
while still allowing us to place an upper bound on the number of restarts that
any transaction that converges on a write-set will perform. In other words, if
we continue pulling hard on that thread, then it doesn't seem improbable that
we could reduce the closed timestamp duration to O(10) ms, in which case 
the contention problems here could be less concerning. To do so, I think we'd
need to rethink a good amount of our transaction model. Specifically, I think
we'd need to stop thinking of refresh requests as special cases, and instead think
of them as a major part of the model.

As with the previous alternative, optimistic transactions would slowly increase
their timestamp as they wrote intents and would likely always require a round of
read refreshes before committing. By default, we would need to perform this
round of read refreshes before committing the transaction, though it may be
possible to perform the WAN hop for the read refreshes at the same time as
writing intents and committing if the read refreshes also left intents to
indicate their success.

Both this proposal and the previous prompt us to reconsider the idea of batching
all reads until commit-time instead of pipelining them as we go. Doing so would
avoid the synchronous gateway-leaseholder hop present even with transaction
pipelining. The synchronous gateway-leaseholder hop is also much less important
once we fully (re-)embrace the idea of optimistic concurrency where transactions
may be forced to restart due to conflicts or skew between reads and writes and
can no longer use write intents as pessimistic locks as they navigate a
transaction but before they commit. The two proposals also prompt us to
eliminate read-write request types entirely and fully separate out read requests
from blind-write requests.

If we did all of this just right then I think transactions with remote
leaseholders and local followers for all data could be served in a local
round-trip per read or write statement + 2 WAN round-trips (jump to leaseholders +
replication of intent writes for commit), assuming all optimistic reads
succeeded.

#### Benefits
- No extra cost for individual writes or reads
- No explicit client buy-in. Could even be a cluster setting

#### Drawbacks
- Only feasible if we dramatically reduced the closed timestamp duration, which
  may or may not be possible
- Requires a round of remote read refreshing before committing transactions.
  This latency will be difficult to hide, although at least it's once per
  transaction instead of once per statement
- Optimistic, may cause transaction restarts and severely changes our approach
  to concurrency control
- May result in stale reads before refresh is performed. This again indicates
  that a causality token would be useful

### Consistent Optimistic Follower Reads v2

As a further variation of the previous proposal, I think we could bake this into
our standard transaction protocol so that gateways that are local to leaseholders
fall back to exactly what they do today - contacting the leaseholder synchronously
on every read and write and eagerly laying down intents. The only difference in the
approach would come from gateways that are remote from leaseholders but local to
follower replicas.

Instead of starting with a timestamp old enough to perform follower reads like
the previous version of this proposal, transactions would start with up-to-date
timestamps, as they do today. This avoids any stale read concerns. Transactions
would then evaluate as normal. The big difference comes when a transaction tries
to read or write data whose leaseholder is remote but whose follower is local.
Instead of ever going to the leaseholder, the transaction would go to the
follower and wait for the closed timestamp to surpass its read timestamp. The
transaction could then perform a transactional read off of the follower. In a
radical world where `WAN latency >> closed timestamp duration`, we expect this
initial wait to be `~1 WAN hop` (not round-trip). So already, we're doing better
than if we went to the leaseholder to satisfy this read. Future reads could then
all also be satisfied by followers, so they would never have to wait at all.

The transaction would hold off on performing any writes to remote leaseholders
unless forced to by an overlapping read. This could go other ways as well, like
that it could pipeline them without even performing the gateway to leaseholder
hop synchronously. The important part is that these writes would not incur a
gateway to leaseholder WAN RTT on every writing statement. To do this, remote
writes would now be optimistic and our model of pessimistic locking would need
to adjust. For this to work, we'd need to either eliminate non-blind writes or
have a way at the transaction layer to break non-blind writes into composite
reads and writes and fall back to this strategy when the data these requests are
supposed to operate on is remote. This allows us to use followers to satisfy
reads and delay/pipeline writes that are only validated later.

All together, this leads to the following latency model in a topology with
cross-region replication.

```
start with orig timestamp as normal

local reads:
    L = 2 local
first non-local read:
    wait for follower read timestamp to catch up
    L = 2 local + 1 wan + follower read duration
future non-local reads:
    L = 2 local
local writes:
    eagerly write intents (helps with contention)
    L = 2 local
remote writes:
    delay/pipeline (optimistic)
    L = 0
when committing:
    if any non-local refresh non-local reads:
        L = 2 wan
        NOTE: we could eliminate this round-trip if we were willing to write
              replicated intents as markers that these refreshes succeeded.
              They could then take part in the parallel commit protocol instead.

    if any non-local writes:
        if sync cross-region replication:     L = 4 wan
        if not sync cross-region replication: L = 2 wan
    else:
        if sync cross-region replication:     L = 2 wan
        if not sync cross-region replication: L = 0 wan

read-only transactions:
  max L (no remote reads) = 0 wide-area network hops (0x   WAN RTT)
  max L (remote reads)    = 1 wide-area network hops (0.5x WAN RTT)

read-write transactions with majority quorum that requires WAN RTT (e.g. 2/5 replicas in same region):
  max L (no remote reads or writes) = 2 wide-area network hops (1x   WAN RTT)
  max L (no remote writes)          = 5 wide-area network hops (2.5x WAN RTT)
  max L (no remote reads)           = 4 wide-area network hops (2x   WAN RTT)
  max L (remote reads + writes)     = 7 wide-area network hops (3.5x WAN RTT) (** or 2.5x WAN RTT)

read-write transactions with majority quorum that does not require WAN RTT (e.g. 3/5 replicas in same region):
  max L (no remote reads or writes) = 0 wide-area network hops (0x   WAN RTT)
  max L (no remote writes)          = 3 wide-area network hops (1.5x WAN RTT)
  max L (no remote reads)           = 2 wide-area network hops (1x   WAN RTT)
  max L (remote reads + writes)     = 5 wide-area network hops (2.5x WAN RTT) (** or 1.5x WAN RTT)

*  all assuming no restarts due to contention
** see NOTE above
```

## Unresolved questions

- Which of these alternatives should we continue to explore?
- What is the right trade-off between latency imposed on reads vs. latency
  imposed on writes. The main proposal here pushes all cost onto writes. The
  alternative proposals here push varying levels of cost onto reads.
- How do we reconcile whatever proposal we decide on with our concurrency
  control mechanisms?
- How do we reconcile whatever proposal we decide on with closed timestamps and
  follower reads?
