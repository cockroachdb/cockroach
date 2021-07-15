- Feature Name: Consistent Read Replicas
- Status: draft
- Start Date: 2019-08-19
- Authors: Nathan VanBenschoten, Andrei Matei
- RFC PR: 39758
- Cockroach Issue: None

!!!
- talks about unreplicated locks. Unsuported? Redirect to leaseholder?
- talk about speculative remote latching initiated by the gw. deadlock detection. talk about range
cache updates. Understand ePaxos.
- talk about speculative replication before latching is completed. Useless if we have write pipelining?
- talk about long-lived learners and cost of latching vs cost of quorum
- talk about FK checks not needing to conflict with latches
- talk about reads running into replicated locks. Do they go queue at the leaseholder's lock table?

# Summary

Consistent Read Replicas provide a mechanism through which follower replicas in
a Range can be used to serve reads for **non-stale** read-only and read-write
transactions.

The ability to serve reads from follower replicas is beneficial both because it
can reduce wide-area network jumps in geo-distributed deployments and because it
can serve as a form of load-balancing for concentrated read traffic. It may also
provide an avenue to reduce tail latencies in read-heavy workloads by
distributing the load, although such a benefit is not a focus of this RFC.

The purpose of this RFC is to introduce an approach to consistent read replicas.
It takes inspiration from @andy-kimball and the AcidLib project.

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
   splitting cannot be used to spread out concentrated load on individual keys 
   Follower reads provides a solution to the problem of concentrated read
   traffic by allowing the followers of a Range, in addition to its leaseholder,
   to serve reads for its data.

However, this capability comes with a large asterisk. Follower reads are only
suitable for serving _**historical**_ reads from followers. They have no ability
to serve consistent reads at the current time from followers. Even with
[attempts](https://github.com/cockroachdb/cockroach/pull/39643) to reduce the
staleness of follower reads, their historical nature will always necessarily
come with large UX hurdles that limit the situations in which they can be used.

The most damaging of these hurdles is that, for all intents and purposes, follower
reads cannot be used in any read-write transaction - their use is limited to read-only
transactions. This dramatically reduces their usefulness, which has caused us to
look for other solutions to this problem, such as [duplicated indexes](https://www.cockroachlabs.com/docs/stable/topology-duplicate-indexes.html)
to avoid WAN hops on foreign key checks.

Another hurdle is that the staleness they permit requires buy-in, so accessing
them from SQL [requires application-level changes](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20181227_follower_reads_implementation.md).
Users need to specify an `AS OF SYSTEM TIME` clause on either their statements
or on their transaction declarations. This can be awkward to get right and
imposes a large cost on the use of follower reads. Furthermore, because a
statement is the smallest granularity that a user can buy-in to follower reads
at, there is a strong desire to support [mixed timestamp statements](https://github.com/cockroachdb/cockroach/issues/35712), which
CockroachDB does not currently support.

Because of all of these reasons, follower reads in its current form remains a
specialized feature that, while powerful, is also not usable in a lot of
important cases. It's becoming increasingly clear that to continue improving
upon our global offering, we need a solution that solves the same kinds of
problems that follower reads solves, but for all forms of transaction and
without extensive client buy-in. This RFC presents a proposal that fits within a
spectrum of possible solutions to address these shortcomings.

## What's wrong with the "duplicate index" pattern

CRDB's current "solution" for people wanting local consistent reads is the
[duplicated index pattern](https://www.cockroachlabs.com/docs/stable/topology-duplicate-indexes.html).
This RFC proposes replacing that pattern with capabilities built into the lower
layers of the system, on the argument that it will result in a simpler, cheaper
and more reliable system. This section discusses the current pattern (in
particular, its problems).

The duplicate indexes idea is that you create a bunch of indexes on your table,
one per region, with each index containing all the columns in the table (using
the `STORING <foo>,...` syntax). Then you create a zone config per region,
pinning the leaseholders of each respective index to a different region. Our query
optimizer is locality-aware, and so, for each query trying to access the table
in question, it selects an index collocated with the gateway in the same
locality. We're thus taking advantage of our transactional update semantics to
keep all the indexes in sync and get the desired read latencies without any help
from the KV layer.

While neat, there's some major problems with this scheme which make it a tough
proposition for many applications. Some of them are fixable with more work, at
an engineering cost that seems greater than the alternative in this RFC, others
seem more fundamental assuming no cooperation from KV.

### Problem 1: Ergonomics

So you’ve got 15 regions and 20 of these global tables. You’ve created 15*20=300
indexes on them. And now you want to take a region away, or add a new one. You
have to remember to delete or add 20 indexes. Tall order. And it gets better:
say you want to add a column to one of the tables. If, naively, you just do your
`ALTER TABLE` and add that column to the primary key, then, if you had any
`SELECT *` queries (or queries autogenerated by an ORM) which used to be served
locally from all regions, now all of a sudden all those indexes we’ve created
are useless. Your application falls off a cliff because, until you fix all the
indexes, all your queries travel to one central location. The scheme proves to
be very fragile, breaking surprisingly and spectacularly.

What would it take to improve this? Focusing just on making the column
components of each index eventually consistent, I guess we'd need a system that
automatically performs individual schema changes on all the indexes to bring
them up to date, responding to table schema changes and regions coming and
going. Tracking these schema changes, which essentially can't be allowed to
fail, seems like a big burden; we have enough trouble with sporadic
user-generated schema changes without a colossal amplification of their number
by the system. We'd also presumably need to hide these schema changes from the
user (in fact, we need to hide all these indexes too), and so a lot of parts of
the system would need to understand about regular vs hidden indexes/schema
changes. That alone seems like major unwanted pollution. And then there's the
issue of wanting stronger consistency between the indexes, not just eventual
consistency; I'm not sure how achievable that is.

### Problem 2: Fault tolerance

You’ve got 15 regions - so 15 indexes for each reference table - and you’ve used
replication settings to “place each index in a region”. How exactly do you do that?
You've got two options:
1. Set a constraint on each index saying `+region=<foo>`. The problems with this
is that when one region goes down (you’ve got 15 of them, they can’t all be
good) you lose write-availability for the table (because any writes needs to
update all the indexes). So that's no good.

2. Instead of setting a constraint which keeps all of an index’s replicas in one
region, one can let the system diversify the replicas but set a “leaseholder
preference” such that reads of the index from that region are fast, but the
replicas of the index are more diverse. You got to be careful to also constrain
one replica to the desired leaseholder region, otherwise the leaseholder
preference by itself doesn't do anything. Of course, as things currently stand,
you have to do this 15 times. This configuration is resilient to region failure,
but there's still a problem: any leaseholder going away means the lease goes out
of region - thus making reads in the now lease-less region non-local (because we
only set up one replica per region).  

So, all replicas in one region is no good, a single replica in one region is not
good, and, if the replication factor is 3, having 2 replicas in one region is
also not good because it’s equivalent to having all of them in one region. What
you have to do, I guess, is set a replication factor of at least 5 *for each
index* and then configure two replicas in the reader region (that’s 3*5=15
replicas for every index for a 3-region configuration; 10*5=50 replicas in a
10-region configuration). In contrast, with this RFC's proposal, you'd have 2
replicas per region (so 2 per region vs 5 per region). Our documentation doesn't
talk about replication factors and fault tolerance. Probably for the better,
because these issues are hard to reason about if you don't spend your days
counting quorums. This point shows the problem with doing such replication
schemes at the SQL level: we hide from the system the redundancy between the
indexes (and between the replicas of different indexes), and so then we need to
pay a lot to duplicate storage and replication work just to maintain the
independent availability of each index.

### Load-balancing implications

An envisioned use of consistent-read replicas is for scaling up reads on
seldomly-modified tables, independent of geo-distribution (e.g. even in a single
locality). With duplicate indexes it seems unclear what the read scaling story
would be. At the moment, the optimizer does not attempt to do load-balancing (it
just does locality-aware index selection). It seems like a dubious proposition
for the optimizer to do load-balancing, and it's not the optimizer doing it, I
can't clearly see how it would work. In contrast, the road from having multiple
read replicas to load-balacing between them seems more tractable.

# Guide-level explanation

## Consistent Read Replicas

The RFC introduces the term "consistent read replica", which is a non-leaseholder
replica of a Range that can serve transactionally-consistent reads but that cannot
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

Leaseholders have a critical role in ensuring the serializability of
transactions: they maintain the "timestamp cache" data structure that registers
all reads and ensures that writes cannot "change history": once a txn T1 has
read a datum, no transactions serialized before T1 can write to that record.
Being a centralized point, a leaseholder makes sure that all reads are
registered with the timestamp cache. The challenge with allowing other replicas
to perform reads is that the timestamp cache needs to essentially become a
distributed structure. As such, writers (which need to consult the tscache) need
to communicate with all the replicas that have a partial view on the tscache,
checking whether *any of them* is aware of a read at a higher timestamp than the
write's.

Consistent read replicas are configured using zone configs. It's likely that their
configuration will mirror that of [lease preferences](https://www.cockroachlabs.com/docs/v19.1/configure-replication-zones.html#constrain-leaseholders-to-specific-datacenters).
Zones will default to zero consistent read replicas per Range.

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
  write. They are released on every replica as the write applies on that replica.

### Read Path

Read-only batch requests can be routed to the leaseholder or any of a Range's
read replicas. The read would first perform a lease check, which would be handled
as usual. The replica would check whether it is the leaseholder or one of the read
replicas in the currently active lease. If it is, then it compares the liveness epoch
in the lease with the replica's current liveness epoch, and uses the replica's current
liveness expiration as the lease expiration. This is handled exactly as it is today.

If the read replica's lease is valid then the read would go through the exact
same path that it normally does: it would acquire latches from its spanlatch
manager, it would evaluate against the local state of the storage engine, and
then it would bump the local timestamp cache. If a replicated lock is !!! 

This entire read path is almost identical on the leaseholder and on read replicas.

### Write Path

#### Remote latching option

The write path is where the extra complication of read replicas comes into play.
Again, the leaseholder remains the only replica that is allowed to evaluate and
propose writes, so all writes are directed to it. When a write comes in, the
lease is checked, latches are acquired, and the timestamp cache is consulted,
just as usual.

Then the current lease is checked for any read replicas. If there are none, the
write proceeds as normal. If there are read replicas, things get more
interesting. Before evaluating the write, the leaseholder is required to send an
RPC to each of the read replicas. This RPC would be called a `RemoteLatches`
request and would contain the following pieces of information: 
```proto
message RemoteLatchesRequest {
   hlc.Timestamp timestamp      = 1;
   repeated Span spans          = 2;
   int64         lease_sequence = 3;
   // This is a unique identifier for the command. We use `CmdIDKey` in the
   // codebase for it.
   uint64         command_id     = 4;
}
```
The `command_id` field uniquely identifies the command. The remote latches will
be held until the respective command applies. Note that we can't use a command's
`MaxLeaseIndex` for this purpose, although that would be very convenient. I'm not
sure what mechanism we should use here yet and this is one of the bigger open
questions (!!!). See discussion below.

When a read replica received one of these `RemoteLatch` requests, it checks that
it's still operating under the lease indicated by the `lease_sequence`. Then, it
acquires a collection of `SpanReadWrite` latches at the specified timestamp
(waiting until the latches can be acquired - so blocking on all writes, and on
reads at higher timestamps). Once all of these latches were acquired, it would
check its local timestamp cache over the specified spans and return the maximum
timestamp found (and the id of the transaction that performed the respective
read, if known). It would then return the following response and would **not
remove** the latches it had acquired. The caller handles this response as it
handles the local tscache query: it possibly bumps the txn's write timestamp to
respect reads at lower timestamps.
```proto
message RemoteLatchResponse {
   hlc.Timestamp max_timestamp    = 1;
   // If the request was for a single span and the tscache has info on which
   // txn's read set max_timestamp for that span, the respective transaction id
   // is returned. This mimics querying a local tscache.
   UUID          txn_id           = 2;
}
```

Read replicas need to maintain the latches while the leaseholder evaluates the
request, until the respective replica applies the resulting command (if any).
Were the latch to be removed earlier, the replica would potentially serve a
higher-timestamp read which would miss to see the overlapping command. The
mechanics for releasing leases are tricky, but crucial - a leaked latch causes
read unavailability at the level of a read replica. So, the question is how does
a replica know when the command associated with a latch has applied. And,
separately, what do we do when the leaseholder has acquired latches for requests
that don't end up resulting in any command being proposed (because evaluation
fails, or backtracks above latching for `WriteIntentErrors`).  

The first idea is to put a command identifier in the latching request, and then release the latches 
on every read replica as that read replica applies that command. The difficulty is that
replicas generally cannot keep exact track of what commands applied - when a snapshot is
received, the snapshot naturally has lost track of what commands went
into it. Replicas know what log index is committed, what log index they have
applied, and also what `LeaseAppliedIndex` they have applied, but none of these
seem to help us as they're all assigned too late for commands. Both log
positions and `LeaseAppliedIndexes` are assigned at proposal time, whereas
the remote latches need to be acquired before evaluation. The reason why the
`LeaseAppliedIndex` is assigned late is because, at the level of a range, we want
to evaluate multiple non-overlapping requests in parallel, and propose them in
the order in which they finish evaluation. The `LeaseAppliedIndex` dictates
application order, and so also proposal order, and so we need to assign it as
late as possible if we are to allow evaluation parallelism.  
I see the following options for how to ensure a limited lifetime for remote latches:

1. We include latch information in snapshots and we make sure that only the
leaseholder creates snapshots for read replicas. The latch information
represents the latches that a read replica is supposed to have at the applied
log position corresponding to the snapshot. The replica receiving the snapshot
would drop any latches that it holds which are not in the snapshot, reasoning
that it must have been logically released by one of the commands in the
snapshot.
Generally, it's the leader that creates snapshots, but the leader doesn't have
the remote latch information we need; only the leaseholder does (i.e. even the
leader might be out of sync with the leaseholder truth because there might have
been a snapshot in the leader's history before it became leader). So we'd need
to somehow make sure that only leaseholders can provide snapshots for read
replicas. 

2. We assign a latching sequence to commands, separate from the `MaxLeaseIndex`.
This sequence would be assigned as commands perform latching (so, before
evaluation). Since we continue to allow proposals out of order with this latch
sequence, the leaseholder will, at any point in time, hold latches for some of
the commands in the window `[minLatchSeq, curLatchSeq)`, where `minLatchSeq`
would represent the oldest command that performed latching and hasn't applied
yet. Information about this window could be attached to all proposals, and
replicas could drop latches that have fallen out of the window. And/or the
`minLatchSeq` could be made part of the range state, and then it would be also
available inside snapshots. I guess we also need a mechanism to make sure that
this window info is periodically broadcast to replicas, even when there's no
Raft proposals otherwise, to capture cases where proposals were dropped
(unbeknownst to replicas).

3. We assign `LeaseAppliedIndexes` early, at latching time, and we don't allow
out-of-order proposals any more (out of order w.r.t. the `LeaseAppliedIndex`).
With this scheme, when applying a command with a given LAI, a replica drops any
latches corresponding to that LAI and lower ones. We'd do this only on ranges
with read replicas. This would limit the range's write throughput, particularly
when faced with heterogeneous write requests that take different amounts of time
to evaluate, but since these ranges are not supposed to have a lot of writes,
maybe it's OK. It'd be the easiest thing to implement. What makes this scheme
attractive is that a `LeaseAppliedIndex` would precisely identify which latches
need to be held after applying that command. It's clear that this works with
snapshots since snapshots include a LAI as part of the range state.

4. We introduce another mechanism, separate from Raft, by which a replica can
ask the leaseholder about the set of latches at a particular
LAI. The leaseholder would reply with the LAI corresponding to
the last applied command, and the set of latches it currently holds. The replica
would wait to apply that `LAI` and then it would drop any latches not in the
leaseholder's set. It seems that this communication with the leaseholder needs to
be synchronized with any other `RemoteLatchesRequest` from the leaseholder.

5. Use Raft to acquire the remote latches, and use Raft to explicitly release
them. For ranges with read replicas, we'd make the `SpanReadWriteLatches` part
of the range state explicitly, by writing them in range-local keys. Acquiring a
latch would be done by writing to such a key, and dropping a latch would be done
by deleting a key. This approach would recognize the fact that these latches are
replicated range state indeed, and Raft is how we keep range state coherent.
There is, of course, an impedance mismatch wich Raft - acquiring the latches
needs to be done by an exact set of replicas, not any majority quorum of voters.
So, before a latch can be used, we need to find out that it was applied by all
the read replicas. I think this kind of replica state tracking could be built on
top of, or even within, Raft. The command releasing a latch doesn't need any
special tracking; it can be committed and applied with a normal quorum.

!!! talk about evaluation errors and how we release leases, and also
WriteIntentError

When a new least is applied, a read replica can drop all of its latches since
latches are lease-scoped.

The local latches acquired by the leaseholder are also released when the Raft
command applies on the leaseholder, as they normally are.

#### Application-time latching

Another option is to use the locks implicitly acquired when an intent is
replicated instead of the remote latches. This option muddies the waters between
request evaluation and command application (it arguably goes kinda backwards
w.r.t "proposer-evaluated KV" project), but it's very attractive because getting
rid of remote latches simplifies things greatly. The key observation here is
that the result of request evaluation doesn't depend too much on the request's
timestamp when we know all the overlapping future writes (but not all the
overlapping future reads) - the timestamp cache only affects the timestamp of
the resulting intent, but as far as the request's results are concerned (e.g.
for a `CPut` or `DelRange`) it doesn't matter. This means that we can evaluate
the request "speculatively" without waiting for the remote latches. The idea is
to replicate the intent at a timestamp that's possibly too low (and thus will
need to be updated at commit time) and use a replication-driven mechanism for
finding out the correct timestamp to use.

Let's briefly discuss latches again before spelling things out. The point of the
remote latches in the previous scheme was twofold:
1. To block new conflicting readers at timestamps higher than the write.
2. To return a timestamp cache value - a timestamp higher than any previous
overlapping read.
3. To provide synchronization on read replicas between Raft command application
and overlapping reads.

The blocking part doesn't need to be achieved by latching; it can also be
achieved by locking. In particular, replicating an intent blocks all reads at
higher timestamps once the intent has been applied on the respective replica.
For the reading of the tscache part, one thing to note is that within a
transaction it's ultimately the coordinator that needs to find out about the
lowest value that the transaction can commit at, not any particular leaseholder
for any of the ranges involved in the txn. So we could have the coordinator
communicate with all the read replicas to find out the tscache values, without
that information going through any leaseholder. We'll also deal with the
synchronization aspect more explicitly.

The scheme would be the following:
- On leaseholders:
    - Leaseholders evaluate write requests locally as they always have. The only
    change is that they make sure to return to the coordinator the lease under
    which the request was evaluated since the coordinator will need to
    communicate with all the read replicas (note that responses already
    essentially include information on the current lease, as of recently).
    Commands also start including the node ID of the txn coordinator.
- On the read replica side:
    - A read replica needs to take special action when applying a command: it
    needs to contact the txn coordinator and deliver it a notification
    containing a reading of the timestamp cache taken at application. The
    coordinator is known for each command because the node ID was explicitly put
    in the command. The replica would thus make an RPC saying "for this command
    (corresponding to a request identified by this LAI, or maybe by these keys
    being written at these sequence numbers) this is a timestamp higher than any
    overlapping read I've served; I also promise to not serve reads at higher
    timestamps from now on until the intent is resolved".
    - Since overlapping reads and Raft command applications are no longer
    synchronized through latching, we need to synchronize them explicitly. This
    is the uglier side of this scheme; it seems we need command application to
    reach up the stack and grab latches (regular, local latches) and hold on to
    them while the command applies and while the timestamp cache is read. This
    is where the scheme seems to be in conflict with the "proposer-evaluated KV"
    mantra (for worse or better), although the conflict is fairly superficial -
    we're still not "evaluating" anything below Raft, but we are probably
    breaking some code boundary.
- On the coordinator:
    - Before staging the transaction, the coordinator needs to validate that the
    timestamps at which the various intents were written are valid. It does this
    similarly to how it does tracking of intent replication: it keeps a list of
    intents whose timestamp needs to be verified and it collects notifications
    from the read replicas of the ranges where those intents have been written.
    Once all read replicas have confirmed an intent and the txn's timestamp has
    been correspondingly bumped (if needed), the intent doesn't need to be
    tracked any more. In fact, for intents on read-replicas ranges, this
    tracking completely replaces the existing tracking for intents: the tracking
    we currently have in place cares about intents getting quorum, which a
    notification from a read replica (any single read replica) proves (since
    read replicas only notify about applied intents which means that they must
    have reached quorum).
    - If the coordinator has not received notifications from all the read
    replicas by the time it wants to commit the transaction (say the respective
    RPC's connection dropped), and the coordinator got tired of waiting, it can
    simply ask the missing read replica for a fresh read of its tscache,
    together with the key that should have been written to so that the
    coordinator's request can synchronize with the raft application. We'll
    probably have to include a LAI too, and basically wait on the replica until
    it catches up to that LAI.  
    We could also forget about the read replicas notifying the
    coordinator proactively and instead just have the coordinator initiate the
    communication.
    - The coordinator can't stage the txn until it has figured out the final
    transaction timestamp (we can't stage "speculatively" since the txn might be
    erroneously considered implicitly committed). But once the timestamp has
    been figured out, the coordinator might be able to go straight to the
    `COMMITTED` stage (skipping `STAGING`) if all the intents have been
    verified. If the transaction writes only to read-replicas ranges, then
    that will be the case.
    - Since we need to wait for the replication of intents before committing,
    we're effectively reverting from parallel commits to 2PC for the commit
    protocol (on these read-replica ranges). Different writes in a txn can be
    pipelined with one another, but writing the txn record needs to wait for the
    rounds of replication to finish. As such, from the perspective of a writing
    coordinator, committing a txn takes at least two round trips: one to all the
    read replicas for an intent, another to a majority quorum for the txn
    record. I think this is not too bad since what matters here is not so much
    write latency, but a write's "contention footprint" which was already not
    benefiting from parallel commits. TODO: count hops more rigurously !!!
    
One thing to think about is exactly how to deal with transactions that write
under one lease and then try to commit under another. The set of read replicas
that the coordinator needs to get timestamps from is the one corresponding to
the lease under which the transaction commits. The simplest thing is to tie
transactions touching read replicas to their original lease, and roll them back
if they try to commit under a different one.
 

Question: how does this interact with batching of proposals? Perhaps we need to
disable batching on these ranges.

### Lease Changes

We introduce the notion of "read replicas", which are stored on Range leases
(which might be better thought of now as Range "configurations"). Each
`roachpb.Lease` records a single leaseholder and its node liveness epoch, along
with zero or more read replicas and their node liveness epochs. The Lease
structure still maintains a monotonic sequence number, which is incremented
whenever the configuration present in the Lease changes and is included on each
Raft proposal.

We assume that any Lease with read replicas is an epoch-based lease.

The read replicas technically do not need to be part of a `Lease`; they could
just be represented by in-memory state on the leaseholder. But putting them in
the `Lease` provides for a natural mechanism by which the read replicas find out
that they are, in fact, read replicas. Putting them in a `Lease` also makes them
work nice with lease transfers; a transfer can just copy over the existing read
replicas which continue serving while the transfer is ongoing. It also gives us
a natural mechanism for informing kvclients of the read replicas by using the
existing mechanism for keeping the range caches up to date.

### Adding a Read Replica

#### Cooperative

_Cooperative configuration changes that are allowed when the replicas affected
by the change are live and can be made aware of the adjustment. They are an
optimization. Currently, `TransferLease` requests fall into this category._

For simplicity, we're considering only the case when the node coordinating the
process of removing a read replicas is the leaseholder. Indeed, it's the
replication queue on the leaseholder node that's performing this action. Adding
a new read replica only requires synchronization with the current leaseholder;
it does not require communication with the new read replica itself. The
leaseholder will propose a new lease (an extension of the existing one with one
more replica). Once the new lease is proposed, it might apply on the new replica
before it applies on the leaseholder. For this reason, the leaseholder needs to
start respecting the proposed lease immediately as its proposed.
`RemoteLatchesRequest` can thus be sent to a replica even before that replica
applies a lease anointing it as a read replica. The replica will not freak out
on this request; it will simply respond immediately (since it has no latches),
and it will indicate that it hasn't served any reads recently by returning a
zero `max_timestamp`; any reads that it might have served in the past are
already reflected in the current leaseholder because they must have been below
the start of the current lease.

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
that it stops serving reads. Only after the leaseholder finds out the highest timestamp at
which the replica served reads the replica can be removed from the lease.

For simplicity, we're considering only the case when the node coordinating the
process of removing a read replicas is the leaseholder. Indeed, it's the
replication queue on the leaseholder node that's performing this action. The leaseholder
will do a `StopConsistentReads()` RPC, telling the replica to not act as a read replica
for the current lease any more.
```proto
message StopConsistentReadsRequest {
   int lease_seq = 1;
}
```
In response, the replica will return its timestamp cache info. For simplicity, this can be
a high-water mark. Or, we could take the opportunity to introduce a precedent for returning
full-fidelity tscache info (which would also be beneficial on lease transfers to minimize their
impact on overlapping transactions). The replica will start returning `NotLeaseholderErrors` for
the duration of the current lease, and it will naturally try to acquire a lease for itself once
the current lease expires.
```proto
message StopConsistentReadsResponse {
   hlc.Timestamps max_read_ts = 1;
}
```
The leaseholder will inject this info into its tscache, and then quickly propose a new lease
without the removed replica. If the leaseholder crashes before proposing a new lease, then a new
lease will eventually be proposed by someone else. Once that is applied, the replica will either go
back to serving reads (if its part of the new lease) or not.

#### Non-Cooperative

Removing a read replica without the read-replica's approval requires
invalidating the current read replica's node liveness epoch. Only after this
point can the read replica be removed from the lease (after also waiting for the
previous lease to expire). If a `RemoteLatchesRequest` fails, the leaseholder
will retry it for the duration of the current lease, but, since it's likely that
the respective replica needs to be dropped from the lease, the leaseholder will
not extend the existing lease. As soon as the liveness record of the replica
expires, the leaseholder will increment the replica's epoch and propose a new
lease without the replica. Until that liveness record expires, the range is
unavailable for writes; until then the replica might be serving reads, so the
leaseholder can't ignore the failures to acquire latches.

We don't really have mechanisms for incrementing a node's epoch while that node
is running and able to heartbeat its liveness record. So, this means that a
replica that's partitioned from the leaseholder but otherwise able to heartbeat
its liveness record causes unbounded write unavailability. This is analogous to
fragility we already have in our leaseholders: a leaseholder that's able to
heartbeat its record but otherwise partitioned away from a quorum in its range
causes write unavailability for the range. In the case of read-replicas, this
kind of fragility is exacerbated. Fixing it seems to require the introduction of
indirect communication between replicas - either through the gossip network, or
perhaps through the liveness record/range - in order to get cooperative behavior
even when direct communication is broken. We won't do anything about it here.

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
liveness epoch before removing the read replica from the lease. !!! and it also involves waiting for 
the liveness record's expiration.

## Drawbacks

The major drawbacks of this approach are that it is somewhat complex, it
introduces a latency penalty on writes to Ranges with one or more read replicas,
and it requires an explicit configuration decision.

The two "optimistic" alternatives below are able to avoid this latency penalty
and this configuration decision, but at the expense of increased transaction
retries under contention and concerns around stale reads.

## Alternatives

A number of different designs has been proposed in the [Optimistic Global
Transactions](Optimistic Global Transactions) document. They all try to improve
the latency of transactions in geo-distributed clusters, but none achieve the
main objective here - in-region reads for some tables. All the alternatives imply
at least 1/2 RTT worth of latency per read or per transaction.

Another idea discussed both recently and historically is around marking some
tables such that all reads on them are performed at some lower timestamps (say,
3s in the past). As such, reads would automatically be eligible to be served by
followers. The attractiveness of that idea is that it seems simpler to implement
and it aims for writes to not block reads. There's a couple of problems without
great solutions:
1. The consistency between replicas becomes problematic - a replica might start
serving a value before another one. This can be helped by waiting out the
uncertainty period (max clock offset) on stale reads that see a value in their
immediate future.
2. All reads are stale by definition. This can be helped by having writes wait
out the staleness period before returning to the client so causality between
transactions can be respected.
3. It's unclear how to deal with transactions that write to both regular and
special tables. For example, if one writes a parent row in a stale-read table
and a child row in a regular table, it'd be obviously bad if they became visible
to readers at different times (it'd be referential integrity violation, besides
txn atomicity). It's unclear how to deal with this, particularly in a way that
both keeps the property that writes don't block reads and also keeps writes on
regular tables having regular properties (impossible?).

### Comparison to quorum leases

[Moraru et al.](https://www.cs.cmu.edu/~dga/papers/leases-socc2014.pdf) propose
"quorum leases" for Paxos-based systems, with a similar purpose of allowing
multiple replicas to serve strongly-serializable reads. In their scheme, lease
configurations are committed through Paxos, and then a leaseholder replicas gets
to activate its lease once all the other replicas promise it to not consider
writes committed without this leaseholder's acknowledgement. Together with that
promise, each replica communicates a log index that the leaseholder must be
caught up to. Thus, to consider a write committed, the proposer of that write
must gather votes from a quorum that needs to include all of the leaseholder
replicas. In contrast to this proposal, quorum leases don't require the proposer
to first acquire any latches; this difference stems from the fact that the
system they're considering is not MVCC and thus reads and writes don't have
timestamps. In our case, the keys that we write are constructed with a
timestamp, and deriving that timestamp at evaluation time needs timestamp cache
information from all the leaseholders.  

If we implement the pipelining of distributed latching and (speculative)
replication, then our scheme doesn't seem too dissimilar, except that committing
(from Raft's perspective) individual writes is done without needing the
leaseholders in the quorum, and synchronizing with the leaseholders is deferred
until transaction commit time. The read leases paper is not concerned with
transactions, just with "committing" individual writes.
