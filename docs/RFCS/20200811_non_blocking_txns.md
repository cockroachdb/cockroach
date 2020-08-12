- Feature Name: Non-Blocking Transactions
- Status: draft
- Start Date: 2020-08-11
- Authors: Nathan VanBenschoten
- RFC PR: #52745
- Cockroach Issue: None

# Summary

Non-blocking transactions are a variant of CockroachDB's standard transaction
protocol that are optimized for writing to read-mostly or read-only (excluding
maintenance events) data. The transaction protocol and the replication schema
that it is paired with differ from standard read-write transactions in two
important ways:
- non-blocking transactions support a replication scheme over the Ranges that
  they operate on which allow all followers to perform **non-stale** follower
  reads.
- non-blocking transactions are **minimally disruptive** to reads over the data
  that they modify, even in the presence of read/write contention.

The ability to serve reads from follower replicas is beneficial both because it
can reduce read latency in geo-distributed deployments and because it can serve
as a form of load-balancing for concentrated read traffic in order to reduce
tail latencies. The ability to serve **non-stale** follower reads makes the
functionality applicable to a far larger class of read-only and read-write
transactions.

The ability to perform writes on read-heavy data without causing conflicting
reads to block for long periods of time is beneficial for providing predictable
read latency. When customers ask for the `READ COMMITTED` isolation level, this
is what they are actually asking for.

This proposal serves as an alternative to the [Consistent Read Replicas
proposal](https://github.com/cockroachdb/cockroach/pull/39758). Whereas the
Consistent Read Replicas proposal enforces consistency through communication,
this proposal enforces consistency through semi-synchronized clocks with bounded
uncertainty.

# Working Assumptions

The bulk of this proposal is founded on the idea that at some time in the
future, we will be able to dramatically reduce the clock uncertainty that we
feel comfortable running with, either across the board or at least within
CockroachCloud.

Wide area network (WAN) round-trip time (RTT) latencies can be as large as 200ms
in the global deployment scenarios that we are interested in (ref:
[AWS](https://docs.aviatrix.com/_images/inter_region_latency.png),
[Azure](https://docs.aviatrix.com/_images/arm_inter_region_latency.png),
[GCP](https://docs.aviatrix.com/_images/gcp_inter_region_latency.png)). The
current default clock uncertainty offset is 500ms. This proposal explores the
half of the design space in which clock uncertainty bounds drop below WAN RTT
time. Within this regime, it becomes cheaper to establish causality between
events using clocks and waiting than by communicating with distant nodes.

As such, the uncertainty bounds necessary for this proposal to make sense are
much lower than what we currently use, but are also well within the realm of
possibility for software-only solutions. Atomic clocks and TrueTime are not
prerequisites!

For the rest of this proposal, we will assume that the clock uncertainty offset
is configured to 30ms. We will also assume that the global cluster topologies
that we are interested in have diameters of 120ms RTTs. 

_NOTE: portions of this RFC that are unchanged from #39758 have been quoted to
avoid unnecessary effort and to make contrasting the two RFCs easier._

> # Motivation
> 
> Closed timestamps and [follower
> reads](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180603_follower_reads.md)
> provide a mechanism to serve *consistent stale reads* from follower replicas of
> a Range without needing to interact with the leaseholder of that Range. There
> are two primary reasons why a user of CockroachDB may want to use follower
> reads:
> 1. to avoid wide-area network hops: If a follower for a Range is in the same
>    region as a gateway and the leaseholder for that Range is in a separate
>    region as the gateway, follower reads provides the ability to avoid an
>    expensive wide-area network jump on each read. This can dramatically reduce
>    the latency of these reads.
> 2. to distribute concentrated read traffic: Range splitting provides the ability to
>    distribute heavy read and write traffic over multiple machines. However, Range
>    splitting cannot be used to spread out concentrated load on individual keys 
>    Follower reads provides a solution to the problem of concentrated read
>    traffic by allowing the followers of a Range, in addition to its leaseholder,
>    to serve reads for its data.
> 
> However, this capability comes with a large asterisk. Follower reads are only
> suitable for serving _**historical**_ reads from followers. They have no ability
> to serve consistent reads at the current time from followers. Even with
> [attempts](https://github.com/cockroachdb/cockroach/pull/39643) to reduce the
> staleness of follower reads, their historical nature will always necessarily
> come with large UX hurdles that limit the situations in which they can be used.
> 
> The most damaging of these hurdles is that, for all intents and purposes, follower
> reads cannot be used in any read-write transaction - their use is limited to read-only
> transactions. This dramatically reduces their usefulness, which has caused us to
> look for other solutions to this problem, such as [duplicated indexes](https://www.cockroachlabs.com/docs/> stable/topology-duplicate-indexes.html)
> to avoid WAN hops on foreign key checks.
> 
> Another hurdle is that the staleness they permit requires buy-in, so accessing
> them from SQL [requires application-level changes](https://github.com/cockroachdb/cockroach/blob/master/docs/> RFCS/20181227_follower_reads_implementation.md).
> Users need to specify an `AS OF SYSTEM TIME` clause on either their statements
> or on their transaction declarations. This can be awkward to get right and
> imposes a large cost on the use of follower reads. Furthermore, because a
> statement is the smallest granularity that a user can buy-in to follower reads
> at, there is a strong desire to support [mixed timestamp statements](https://github.com/cockroachdb/> cockroach/issues/35712),
> which CockroachDB does not currently support.
> 
> Because of all of these reasons, follower reads in its current form remains a
> specialized feature that, while powerful, is also not usable in a lot of
> important cases. It's becoming increasingly clear that to continue improving
> upon our global offering, we need a solution that solves the same kinds of
> problems that follower reads solves, but for all forms of transaction and
> without extensive client buy-in. This RFC presents a proposal that fits within a
> spectrum of possible solutions to address these shortcomings.
> 
> ## What's wrong with the "duplicate index" pattern
> 
> CRDB's current "solution" for people wanting local consistent reads is the
> [duplicated index pattern](https://www.cockroachlabs.com/docs/stable/topology-duplicate-indexes.html).
> This RFC proposes replacing that pattern with capabilities built into the lower
> layers of the system, on the argument that it will result in a simpler, cheaper
> and more reliable system. This section discusses the current pattern (in
> particular, its problems).
> 
> The duplicate indexes idea is that you create a bunch of indexes on your table,
> one per region, with each index containing all the columns in the table (using
> the `STORING <foo>,...` syntax). Then you create a zone config per region,
> pinning the leaseholders of each respective index to a different region. Our query
> optimizer is locality-aware, and so, for each query trying to access the table
> in question, it selects an index collocated with the gateway in the same
> locality. We're thus taking advantage of our transactional update semantics to
> keep all the indexes in sync and get the desired read latencies without any help
> from the KV layer.
> 
> While neat, there's some major problems with this scheme which make it a tough
> proposition for many applications. Some of them are fixable with more work, at
> an engineering cost that seems greater than the alternative in this RFC, others
> seem more fundamental assuming no cooperation from KV.

### Problem 1: Read Latency under Contention

Without contention, reads using the duplicate index pattern are served locally
at local latencies. With an index + pinned leaseholder in each region, any read
on a "duplicate index" table can be served in a few milliseconds without a WAN
hop. Better yet, these reads are not stale, so they can be comfortably used in
read-only and read-write transactions (often during foreign key checks).

But these local read latencies can provide a false sense of performance
predictability. While the duplicate index pattern optimizes for read latency, it
badly pessimizes write latency. On the surface, this seems to be the expected
tradeoff. The problem is that when reads and writes contend, reads can get
blocked on writes and have to wait for writing transactions to complete. If the
writing transaction is implicit, this results in at least 1 WAN RTT of blocking.
If the writing transaction is explicit, this results in at least 2 WAN RTTs of
blocking. If the writing transaction is doing other work, this blocking can be
unbounded.

Reads that users expect to be fast (1ms-3ms) can quickly get two orders of
magnitude slower (120ms-240ms or more) due to any read/write contention. As
we've seen, this can be a shocking violation of expectations for users.

Over the past few months, we've seen users recognize this issue, both with the
duplicate index pattern and with standard CockroachDB transactions. The
corresponding ask has often been to support a `READ COMMITTED` isolation level.
Our response is to perform stale reads using `AS OF SYSTEM TIME` to avoid
contention, but this pushes the burden onto the readers to accept stale data in
exchange for more predictable read latency. This seems backwards, as avoiding
blocking should really be the responsibility of the infrequent writers. It also
only works if the readers are read-only transactions, as read-write transactions
cannot use `AS OF SYSTEM TIME`, which removes a large class of use cases like
foreign key checks in read-write transaction.

> ### Problem 2: Ergonomics
> 
> So you’ve got 15 regions and 20 of these global tables. You’ve created 15*20=300
> indexes on them. And now you want to take a region away, or add a new one. You
> have to remember to delete or add 20 indexes. Tall order. And it gets better:
> say you want to add a column to one of the tables. If, naively, you just do your
> `ALTER TABLE` and add that column to the primary key, then, if you had any
> `SELECT *` queries (or queries autogenerated by an ORM) which used to be served
> locally from all regions, now all of a sudden all those indexes we’ve created
> are useless. Your application falls off a cliff because, until you fix all the
> indexes, all your queries travel to one central location. The scheme proves to
> be very fragile, breaking surprisingly and spectacularly.
> 
> What would it take to improve this? Focusing just on making the column
> components of each index eventually consistent, I guess we'd need a system that
> automatically performs individual schema changes on all the indexes to bring
> them up to date, responding to table schema changes and regions coming and
> going. Tracking these schema changes, which essentially can't be allowed to
> fail, seems like a big burden; we have enough trouble with sporadic
> user-generated schema changes without a colossal amplification of their number
> by the system. We'd also presumably need to hide these schema changes from the
> user (in fact, we need to hide all these indexes too), and so a lot of parts of
> the system would need to understand about regular vs hidden indexes/schema
> changes. That alone seems like major unwanted pollution. And then there's the
> issue of wanting stronger consistency between the indexes, not just eventual
> consistency; I'm not sure how achievable that is.
> 
> ### Problem 3: Fault tolerance
> 
> You’ve got 15 regions - so 15 indexes for each reference table - and you’ve used
> replication settings to “place each index in a region”. How exactly do you do that?
> You've got two options:
> 1. Set a constraint on each index saying `+region=<foo>`. The problems with this
> is that when one region goes down (you’ve got 15 of them, they can’t all be
> good) you lose write-availability for the table (because any writes needs to
> update all the indexes). So that's no good.
> 
> 2. Instead of setting a constraint which keeps all of an index’s replicas in one
> region, one can let the system diversify the replicas but set a “leaseholder
> preference” such that reads of the index from that region are fast, but the
> replicas of the index are more diverse. You got to be careful to also constrain
> one replica to the desired leaseholder region, otherwise the leaseholder
> preference by itself doesn't do anything. Of course, as things currently stand,
> you have to do this 15 times. This configuration is resilient to region failure,
> but there's still a problem: any leaseholder going away means the lease goes out
> of region - thus making reads in the now lease-less region non-local (because we
> only set up one replica per region).  
> 
> So, all replicas in one region is no good, a single replica in one region is not
> good, and, if the replication factor is 3, having 2 replicas in one region is
> also not good because it’s equivalent to having all of them in one region. What
> you have to do, I guess, is set a replication factor of at least 5 *for each
> index* and then configure two replicas in the reader region (that’s 3*5=15
> replicas for every index for a 3-region configuration; 10*5=50 replicas in a
> 10-region configuration). In contrast, with this RFC's proposal, you'd have 2
> replicas per region (so 2 per region vs 5 per region). Our documentation doesn't
> talk about replication factors and fault tolerance. Probably for the better,
> because these issues are hard to reason about if you don't spend your days
> counting quorums. This point shows the problem with doing such replication
> schemes at the SQL level: we hide from the system the redundancy between the
> indexes (and between the replicas of different indexes), and so then we need to
> pay a lot to duplicate storage and replication work just to maintain the
> independent availability of each index.

# Guide-level explanation

## Non-Blocking Transactions

The RFC introduces the term "non-blocking transaction", which is a variant of
the standard read-write transaction that performs locking in a manner such that
contending reads by other transactions can avoid waiting on its locks.

The RFC then introduces the term "non-blocking Range", which is a Range in which
any transaction that writes to it is turned into a non-blocking transaction, if
it is not one already. In doing so, these non-blocking Ranges are able to
propagate [closed timestamps](20180603_follower_reads.md) in the future of
present time. The effect of this is that all replicas in a non-blocking Range
are expected to be able to serve transactionally-consistent reads at the present
time. In this sense, all followers in a non-blocking Range implicitly behave as
"consistent read replicas".

In the reference-level explanation, we will explore how non-blocking
transactions are implemented, how their implementation combines with
non-blocking Ranges to provide consistent read replicas, how they interact with
other standard read-only and read-write transactions, and how they interact with
each other.

For now, it is sufficient to say that "non-blocking transactions" are completely
hidden from users. Instead, users interact with "non-blocking Ranges" by
choosing which key ranges should consist of non-blocking Range. These Ranges are
configured using zone configurations, though we'll likely end up adding a nicer
abstraction on top of this (e.g. `CREATE REFERENCE TABLE ...`).

In the future, we could explore running non-blocking transactions on standard
Ranges, which would still provide the "non-blocking property" (i.e. less
disruptive to conflicting reads) without providing the "consistent reads from
any follower" property.

> ## Example Use Cases
> 
> ### Reference Tables
> 
> It is often the case that a schema contains one or more tables composed of
> immutable or rarely modified data. These tables fall into the category of "read
> always". In a geo-distributed cluster where these tables are read across
> geographical regions, it is highly desirable for reads of the tables to be
> servable locally in all regions, not just in the single region that the tables'
> leaseholders happens to land. A common example of this arises with foreign keys
> on static tables.
> 
> Until now, our best solution to this problem has been [duplicated indexes](https://www.cockroachlabs.com/docs/v19.1/topology-duplicate-indexes.html).
> This solution requires users to create a collection of secondary SQL indexes that
> each contain every single column in the table. Users then use zone configurations
> to place a leaseholder for each of these indexes in every geographical locality.
> With buy-in from the SQL optimizer, foreign key checks on these tables are able
> to use the local index to avoid wide-area network hops.
> 
> A better solution to this problem would be to use non-blocking Ranges for
> these tables. This would reduce operational burden, reduce write
> amplification, avoid blocking on contention, and increase availability.
> 
> ### Read-Heavy Tables
> 
> In addition to tables that are immutable, it is common for schemas to have some
> tables that are mutable but only rarely mutated. An example of this is a user
> profile table in an application for a global user base. This table may be read
> frequently across the world and it expected to be updated infrequently.

# Reference-level explanation

## Detailed design

High-level overview:

- Non-blocking transactions read and write at `non_blocking_duration` in the
  future of present time.
- After committing, they wait out the `non_blocking_duration` before
  acknowledging the commit to the client.
- Conflicting non-stale readers ignore future writes until they enter their
  uncertainty interval, after which they wait until the write's timestamp is
  above the reader's clock before reading the value (maximum wait of max clock
  offset = 30ms).
- Non-blocking Range leaseholders close out timestamps in the future of present
  time.
- Non-blocking Range followers receive these future-time closed timestamps and
  can serve non-stale follower reads.
- KV clients are aware that they can route read requests to any replica in a
  Non-blocking Range.

### Aside: "Present Time" and Uncertainty

Today, all transactions in CockroachDB pick a provisional commit timestamp from
their gateway's local HLC when starting. This provisional commit timestamp is
where the transaction performs its reads and where it initially performs its
writes. For various reasons, a transaction may move its timestamp forward, which
eventually dictates its final commit timestamp. However, a standard transaction
maintains the invariant that its commit timestamp is always lagging "present"
time.

The meaning of present time is somewhat ambiguous in a system with
semi-synchronized clocks. For the purpose of this document, we can define it as
the time observed on the node in the system with the fastest clock. In a well
behaved cluster that respects its configured maximum clock offset bounds,
present time must be no more than `max_offset` in the future of the time
observed on any other node in the system. This guarantee is the foundation upon
which uncertainty intervals enforce single key linearizability within
CockroachDB. If a write were to commit from the fastest node in the cluster at
"present time", a causally dependent read from the slowest node would be
guaranteed to see the write either at a lower timestamp than its provisional
timestamp or at least in its uncertainty interval. Therefore, if reading
transactions makes sure to observe all values in their uncertainty interval,
stale reads are not possible and linearizability is preserved.

### Writing in the Future

As defined above, "non-blocking transactions" perform locking in such a way that
contending reads do not need to wait on their locks. In practice, this means
that non-blocking reads **perform their writes at a timestamp in advance of
present time**. In a sense, we can view this as scheduling a change to happen at
some time in the future.

The result of this is that the values written by a non-blocking transaction are
committed and their locks removed (i.e. intents resolved) by the time that a
read of conflicting keys needs to observe the effect of a non-blocking
transaction. If the locks protecting a non-blocking transaction are removed by
the time that its effects drop below the read timestamp of a conflicting read
then the read can avoid the questions: "Did this conflicting transaction commit?
Should I trust its effects?". Instead, it can simply go ahead and read the value
(*) because it knows that the transaction committed, no coordination required.

This need for coordination to determine whether a read should observe the
effects of a conflicting write is the fundamental reason why writes can be so
disruptive to conflicting reads. We saw that in [Problem 1: Read Latency under
Contention](#Problem-1:-Read-Latency-under-Contention). A read that conflicts
with the lock and the provisional value of a write cannot determine locally
whether that lock and value are pending, committed, or aborted. Instead, it
needs to reach out to that transaction's record. Transaction priorities dictate
whether the read waits or whether it can force a pending writer out of its way.
Either way, coordination is still required, and as a result, the read may need
to perform perform a remote network hop (or several).

By scheduling the writes sufficiently far in the future, we ensure that reads
never observe locks or provisional values, only committed values that have been
scheduled to go into effect at a specific point in time. Coordination on the
part of the read is no longer required. As a result, non-blocking writes are no
longer disruptive to conflicting reads.

\* Things are never quite this easy. See [Uncertainty Intervals: To Wait or Not to Wait](#Uncertainty-Intervals:-To-Wait-or-Not-to-Wait).

#### Synthetic Timestamps

Writing in the future is new for CockroachDB. In fact, talking about time in the
future in any capacity has been traditionally frowned upon. Instead, we try to
only ever pass around HLC timestamps that were pulled from a real HLC clock.
This ensures that if the timestamp is ever used to
[Update](https://github.com/cockroachdb/cockroach/blob/bbbfdbd1919c6de411742c8442cfc3903d33ee86/pkg/util/hlc/hlc.go#L326-L332)
an HLC clock, the resulting clock is guaranteed to still be within the
`max_offset` of all other nodes.

So we need to be careful with deriving future timestamps from timestamps pulled
from an HLC. To that end, this RFC proposes the introduction of "synthetic
timestamps". Synthetic timestamps are identical to standard timestamps (64 bit
physical, 32 bit logical), except that they make no claim about the value of the
clock that they came from. As such, they are allowed to be arbitrarily
disconnected from the clocks in the system.

To distinguish between "synthetic" and "real" timestamps, this RFC proposes that
we reserve a high-order bit in either the physical or the logical component of
the existing timestamp structure to denote this fact.

Outside of identifying synthetic vs. real timestamps, we must make one more
change. The hybrid-logical clock structure exposes a method called `Update` that
forwards its value to that of a real timestamp received from another member of
the cluster. The best way to understand this method and its usage is that
`Update` guarantees that after it returns, the local HLC clock will have a value
equal to or greater than the provided timestamp and that no other node's HLC
clock can have a value less that `timestamp - max_offset`.

For "real" timestamps, it is cheap to implement this operation by ratcheting a
few integers within the HLC because we know that the timestamp must have come
from another node, which itself must have been ahead of the slowest node by less
that `mox_offset`, so the ratcheting operation will not push our HLC further
than the `max_offset` away from the slowest node.

For "synthetic" timestamps, we have to be more careful. Since the timestamp says
nothing about the "present time", we can't just ratchet our clock's state.
Instead, `HLC.Update` with a synthetic timestamp needs to wait until the HLC
advances past the synthetic timestamp either on its own or due to updates from
real timestamps. By waiting, we once again ensure that by the time the method
call returns, no other node's HLC clock can have a value less that `timestamp -
max_offset`.

#### Commit Wait: Not Just for the Cool Kid With the Fancy Hardware

The original [Spanner paper](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/65b514eda12d025585183a641b5a9e096a3c4be5.pdf)
discussed how the system added a delay to all transactions before returning to
the client to ensure that all nodes in the system had clocks in excess of the
commit timestamp of a transaction before that transaction was acknowledged. This
was discussed in section 4.1.2 and section 4.2.1 of the paper:

> Before allowing any coordinator replica to apply the commit record, the coordinator
> leader waits until TT.after(s), so as to obey the commit-wait rule described in
> Section 4.1.2. Because the coordinator leader chose s based on TT.now().latest, and
> now waits until that timestamp is guaranteed to be in the past, the expected wait
> is at least 2 ∗ . This wait is typically overlapped with Paxos communication. After
> commit wait, the coordinator sends the commit timestamp to the client and all other
> participant leaders. 

To this point, CockroachDB has avoided this concern because of its use of
uncertainty intervals. Instead of transactions ensuring that all nodes in the
system have clocks in excess of the commit timestamp of a transaction before
acknowledging it to the client, **CockroachDB ensures that all nodes in the
system have clocks in excess of the commit timestamp of a transaction -
`max_offset` (a weaker guarantee) before acknowledging it to the client**. This
is satisfied trivially because, to this point, transactions have always written
at or below "present time", which is by definition no further than `max_offset`
ahead of the slowest node in the cluster. CockroachDB then implements
uncertainty intervals on subsequent transactions to make up for this weakened
guarantee and ensure linearizability just like Spanner.

If we start committing transactions in the future, this guarantee is no longer
trivially satisfied. If we want to ensure linearizability for the writes of
these non-blocking transactions, we need to do a little extra work to ensure
that the clocks on all nodes are sufficiently close to the commit timestamp of
the transactions before acknowledging their success to clients.

Conveniently, we already have a formalism for this – `hlc.Update`. Commit Wait
would naturally fall out of calling `hlc.Update(txn.CommitTimestamp)` before
acknowledging any transaction commit to a client. This would reduce to a no-op
for standard transactions, and would result in a wait of up to
`non_blocking_duration` for non-blocking transactions. This wait artificially
increases the latency of non-blocking transactions, but critically, it ensures
that we continue to preserve linearizability.

It is worth noting that, like Spanner, this CommitTimestamp is chosen before the
final round of consensus replication. This means that the wait will overlap with
consensus communication and will therefore be less disruptive to non-blocking
transactions than it may initially seem.

#### Uncertainty Intervals: To Wait or Not to Wait

Non-blocking transactions also force us to rethink uncertainty intervals and
question both how and why they work. Uncertainty intervals are timestamp ranges
within which a transaction is unable to make claims of causality. Given a
bounded clock offset of `max_offset` between nodes, a transaction cannot
definitively claim that a write that it observes within a `max_offset` window on
either side of its original timestamp if causally ordered before it. To ensure
that all possible causal dependencies are captured and linearizability is
enforced, a transaction will bump its timestamp to ensure that it observes all
values in its uncertainty interval. This either incurs a transaction refresh or
a full transaction restart.

The writes performed by non-blocking transactions are no exception. When a read
observes a write performed by a non-blocking transaction in its uncertainty
interval, it will need to bump its timestamp so that it observes the write.

The complication here is that if a reading transaction bumped its timestamp
above a value written by a non-blocking transaction in its uncertainty interval,
it could end up with a timestamp in the future of "present time". This could
risk resulting in a stale read if this read immediately committed and then
triggered a causally dependent read that hit a slower gateway where the written
value was not considered in the second read's uncertainty interval. However,
careful readers may notice that if the first read-only transaction respected the
Commit Wait rule then it would have been delayed before committing and the stale
read would not be possible.

But relying on Commit Wait here violates a desirable property of our transaction
model – that committing a read-only transaction is a no-op. To recover this
property, we'll want to wait out the uncertainty before the refresh/retry.
Again, the easiest way to do this is to `hlc.Update(txn.ReadTimestamp)` before
allowing a refreshed/retried transaction to read at its new timestamp. This
again becomes a no-op when the new ReadTimestamp is "real" and not "synthetic".

### Future-Time Closed Timestamps

To this point, we've talked about writing in the future as a means to reduce
contention, but doing so has a second major benefit – it enables non-stale
follower reads if coupled with a closed timestamp mechanism that closes off
timestamps in the future.

This RFC proposes that "non-blocking Ranges" use a separate closed timestamp
tracker that is configured to close time out at least WAN RTT + max_offset in
the future. This ensures that all followers in these ranges will hear about and
active closed timestamps above "present time" so that they can serve all
"present time" reads locally.

The details of this change are TBD.

#### Aside: Writing in Future vs. Closing Time in Future

It may be evident to readers that there is some inherent but fuzzy connection
between the idea of writing in the future and closing time in the future. It is
interesting to point out that in systems like Spanner and YB which assign
monotonically increasing timestamps to all writes within a replication group and
therefore "instantaneously" close out time, these two operations are one in the
same.

CockroachDB's transaction and replication model are not quite set up to support
this. Transactions in CockroachDB are optimized for committing at their original
read timestamp, so they don't like when their writes get bumped. This allows for
optimistic, lockless reads. Replication in CockroachDB is below the level of
evaluation (see [proposer evaluated kv](20160420_proposer_evaluated_kv.md)), so
the timestamp of a replicated value need to be determined before
replication-level sequencing. This allows for evaluation-level parallelism and
less need for determinism during evaluation.

For these reasons, the closed timestamp of a Range lags the timestamps of writes
in a Range and the closed timestamp is more of a Range-level concern and less of
a Request-level concern in CockroachDB (i.e. a single request cannot dictate the
closed timestamp for a Range). This is why the closed timestamp dictates the
time at which requests perform writes (as we'll see in the next section) instead
of the other way around.

### Becoming a Non-Blocking Transaction

As stated earlier, this proposal does not expose "non-blocking transactions" to
users directly. Instead, it exposes "non-blocking Ranges" to users. It was also
mentioned earlier that a "non-blocking Range" will have a closed timestamp
tracker that is leading "present time" by some duration. As it turns out,
configuring this closed timestamp tracker to lead "present time" by
`non_blocking_duration` is enough to implement "non-blocking transactions"
without any other server-side changes.

Any standard transaction that writes to a "non-blocking Range" will naturally
get pushed into the future. If it had performed writes in the past, these will
eventually get moved up to the new commit timestamp. If it had performed reads
in the past, these will eventually get refreshed up to the new commit timestamp
before committing. The new Commit Wait logic in the `txnCommitter` will ensure
that these transactions naturally wait before retuning to the client.

So in a very real sense, "non-blocking transactions" will not exist in the code,
although it is still useful to classify any transaction that commits with a
commit timestamp in the future of "present time" as non-blocking (i.e. those
with synthetic commit timestamps).

### Non-blocking and Standard Transaction Interactions

There are a number of interesting interactions that can happen if we allow
non-blocking transactions to touch (read from and/or write to) standard ranges
and interact with standard transactions. These interactions can increase the
latency of conflicting writes made by standard transactions. However, they will
not meaningfully increase the latency of conflicting reads made by standard
transactions.

first \ second           | standard read | standard write | non-blocking txn's read | non-blocking txn's write
-------------------------|---------------|----------------|-------------------------|-------------------------
standard read            | both bump ts cache, no interaction | bump ts cache, may bump write ts, but still below present time | both bump ts cache, no interaction | bump ts cache, no interaction
standard write           | read ignores write if below, read waits on write if above | 2nd write waits on 1st write, bumps write ts above 1st write | read waits on write | non-blocking write waits on standard write
non-blocking txn's read  | both bump ts cache, no interaction | **bump ts cache, bump write ts, standard write becomes non-blocking write, must wait on commit** | both bump ts cache, no interaction | bump ts cache, may bump write ts
non-blocking txn's write | **no interaction if write above uncertainty, read waits up to max_offset if write within uncertainty** | **standard write waits on non-blocking write, bumps write ts, standard write becomes non-blocking write, must wait on commit** | read ignores write if below, read waits on write if above | 2nd write waits on 1st write, bumps write ts above 1st write

Interesting interactions in bold.

Most of this goes away if we don't allow non-blocking transactions to touch
standard ranges, but such a restriction would be a serious limitation to their
utility.

### Implementation Touch Points

* Implement synthetic timestamps
  * add bit on hlc.Timestamp
  * wait on hlc.Update with synthetic timestamps
* Implement CommitWait
  * wait in txnCommitter after transaction commit
    * hlc.Update(txn.CommitTimestamp)
  * reduces to no-op for standard transactions
* Introduce Non-blocking Range concept
  * Closed timestamp `non_blocking_duration` in future
    * To do this, need to split up per-Store closed timestamp tracker
    * Tune, maybe based on a Range's replication latency
        * Hook up to cluster setting
  * Add to zone configs
* Route present-time reads to followers in non-blocking Ranges
  * Similar to existing follower read logic, but Range specific
    * Add bit on RangeDescriptor?

#### Tuning non_blocking_duration

The `non_blocking_duration` is the distance in the future at which non-blocking
transactions acquire locks and write provisional values. The non-blocking
property of these transactions is provided when this is far enough in the future
such that a non-blocking transaction is able to navigate its entire operation
set, commit, and resolve intents before its commit timestamp its passed by
"present time + max clock offset". Further, the non-blocking property of these
transaction is provided on followers when all of these effects are replicated
and applied on followers by the time that the non-blocking transaction's commit
timestamp is passed by "present time + max clock offset".

As such, some tuning of this value will need to be performed, likely taking into
account transaction latency and replication latency.

## Drawbacks

Complexity. But not overly so, especially compared to Consistent Read Replicas.

Only effective if we can reduce the clock uncertainty interval dramatically.

## Alternatives

### Consistent Read Replicas

See the [Consistent Read Replicas proposal](https://github.com/cockroachdb/cockroach/pull/39758).

Consistent Read Replicas provide non-stale reads from followers, which is one of
the two major wins of this proposal. However, they do not provide the
non-blocking property of this proposal. Consistent Read Replicas are still
disruptive to reads when reads and writes contend.

### Optimistic Global Transactions

Link: https://docs.google.com/document/d/17SC35GR-3G61JCUl-lTnZ4o-UMMEQNMS_p4Ps66EkSI.

Optimistic Global Transactions was a second proposal that grew organically out
of the Consistent Read Replicas proposal. The ideas were complex but powerful.
As it turns out, there are a large number of parallels that can be drawn to this
proposal if we imaging that all Ranges are placed in this "non-blocking mode".
The most important of these parallels is that reads in transactions begin local
and only need to go global to perform verification at the end. However, the big
improvement we've made here is that we've solved the initially stale reads
problem by adding the Commit Wait at the end of other writing transactions!

In fact, with just two small extensions to this non-blocking transactions
proposal, we arrive at exactly the same place as the Optimistic Global
Transactions proposal:
- defer writes until commit time, only "upgrade" to a non-blocking transaction
  at commit time so that all reads can be local during transaction evaluation.
- adapt commit protocol to acquire read locks after read validation, perform
  GlobalParallelCommit to parallelize read validation with writes.

The "hubs and spokes" architecture proposed in the Optimistic Global
Transactions is just as applicable to this proposal as it was to that one.

## References

[1] Spanner: https://storage.googleapis.com/pub-tools-public-publication-data/pdf/65b514eda12d025585183a641b5a9e096a3c4be5.pdf
  * Specifically, see _Section 4.2.3. Schema-Change Transactions_, which briefly mentions a scheme that sounds similar to this, although for the purpose of running large schema changes.

[2] Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases: https://cse.buffalo.edu/tech-reports/2014-04.pdf

[3] Beyond TrueTime: Using AugmentedTime for Improving Spanner: https://cse.buffalo.edu/~demirbas/publications/augmentedTime.pdf
