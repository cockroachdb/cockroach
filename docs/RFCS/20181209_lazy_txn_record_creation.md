- Feature Name: Lazy Transaction Record Creation (a.k.a Deprecate BeginTransaction)
- Status: completed
- Start Date: 2018-12-09
- Authors: Nathan VanBenschoten
- RFC PR: #32971
- Cockroach Issue: #25437

# Summary

Remove transaction record creation from serving as the first step of every non-1PC transaction. Defer transaction record creation as late as possible in the lifetime of a transaction, often skipping a transaction record with the `PENDING` state entirely.

This will provide a performance improvement, simplify the transaction model, and pave the way for the implementation of the more ambitious [parallel commits RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180324_parallel_commit.md).

# Motivation

There are a few motivations for this change with varying levels of urgency.

The most important reason to make this change is that the [parallel commits RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180324_parallel_commit.md) doesn't work without it. Eager creation of transaction records causes a latching stall that forces the commit of a transaction record to wait on the replication of its creation (i.e. `EndTransaction` waits on `BeginTransaction`). We could try to address this through some kind of below-latch manager pipelining (previously referred to as "below CommandQueue pipelining"), but we don't currently understand how to make that work in practice. Instead, this RFC proposes that we allow "fast-enough" transactions (those that never need a heartbeat) to skip the `PENDING` state entirely. This avoids the latching stall and allows parallel commits to achieve single-round trip transactions.

The other reason to make this change is that it avoids a sizable key-value write during the first writing operation in a transaction. Writing the transaction record is always piggy-backed on to another Raft consensus round, but the extra write does increase the size of the corresponding Raft entry and results in an extra RocksDB write. Eliminating this should provide a noticeable improvement in performance of write-heavy workloads that perform explicit transactions.

Finally, removing eager transaction record creation simplifies the transaction model and is in line with the goal to eventually have a fully idempotent transaction model.

# Guide-level explanation

## The Transaction State Machine

The current transaction state machine works as follows:
1. Create the transaction record
2. Perform writes by laying down intents, pointing them at the transaction record
3. Periodically heartbeat the transaction record
4. Repeat steps 2 & 3 as many times as necessary
5. Commit the transaction record
6. Resolve all intents (asynchronously)

The new transaction state machine will be as follows:
1. Perform writes by laying down intents, pointing them at where the transaction record will be written
2. Periodically heartbeat the transaction record, creating it if necessary
3. Repeat steps 1 & 2 as many times as necessary
4. Commit the transaction record, creating it if necessary
5. Resolve all intents (asynchronously)

### Properties Preserved

1. Only the transaction coordinator that owns a transaction can create its transaction record in the `PENDING` state or create/move its transaction record in/to the `COMMITTED` state. This is important to avoid an entire class of bugs that could result in concurrent transactions "reviving" a finalized transaction.
2. Any transaction can move a transaction record to the `ABORTED` state.
3. Outside of extreme priority mismatches, a contending transaction will wait until a transaction has been inactive for at least `TxnLivenessThreshold` before aborting it. As we'll see below, this RFC actually strengthens this property.

## The Role of Transaction Records

Transaction records serve three different roles in the CockroachDB transaction model:

1. they serve as the single linearization point of a transaction. Transactions are considered `COMMITTED` or `ABORTED` when the Raft entry that changes the transaction record to one of these two states is committed. This role interacts with the `EndTransaction` and `PushTxn` request types.
2. they serve as the source of truth for the liveness of a transaction. Transaction coordinators heartbeat their transaction record and concurrent transactions observe the state of a transaction's record to learn about its disposition. This role interacts with the `HeartbeatTxn`, `PushTxn`, and `QueryTxn` request types.
3. they perform bookkeeping on the resources held by finalized (`COMMITTED` or `ABORTED`) transactions. A `COMMITTED` and sometimes an `ABORTED` transaction record will point at each of the transaction's key spans, allowing both the original transaction coordinator and other transactions to resolve and later garbage-collect the intents. This role interacts with the `ResolveIntent`, `ResolveIntentRange`, and `GC` request types.

Interestingly, there are alternative means of achieving each of these goals that give transaction records a smaller or larger role in the transaction model. For instance, the [parallel commits RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180324_parallel_commit.md) proposes a new linearization point for transactions. Another example is that transaction liveness checks could circumvent the transaction record entirely by talking to transaction coordinators directly.

This RFC does not make any changes to the roles that a transaction record plays in the transaction model.

## Concurrent Transactions

As mentioned in the previous section, the transaction record serves as the source of truth for the liveness of a transaction. This means that contending transactions that need to coordinate with a certain transaction look at that transaction's record to determine what to do. This typically takes the form of a variant of the `PushTxn` request. The `PushTxn` request visits the transaction record of the "pushee" and makes decisions based on its state. If it is `COMMITTED` or `ABORTED`, the request returns that to the "pusher" immediately. If it is `PENDING`, priorities are taken into account. For all but the most extreme priorities, a push blocks and the `txnwait.Queue` gets involved if a transaction record is sufficiently "active". The transactions liveness is based on its starting timestamp and its last heartbeat timestamp. If a transaction goes too long without a heartbeat, it is considered "expired" and can be aborted by any pusher.

Lazily creating transaction records adds a complication here. Now that we're no longer writing a transaction record as early as possible [1] in the lifetime of a transaction, its very easy for a concurrent transaction to observe an intent of an ongoing transaction, push its record, and find nothing there. In fact, we expect this to be the case for any push in the first `min(txn duration, DefaultHeartbeatInterval)` of a transaction's lifetime. Currently, a push that doesn't find a record writes it as `ABORTED` and moves on. That isn't workable if we want to increase the amount of time without a transaction record.

We get around this by using the existence of an intent at a given timestamp as evidence of client-instructed activity. Instead of just considering the transaction record's original and heartbeat timestamp to determine if it is expired, we also take into consideration the intent's timestamp. We know that the client had to have been alive at the timestamp that the intent was written to issue the write [2]. This means that it is now possible for a transaction to wait on another transaction without the pushee's transaction record existing yet. This works as expected - either the transaction record is eventually written by the transactions first heartbeat and the concurrent transaction continues waiting or the transaction record is never written and the "activity" attributed to the transaction due to the intent becomes stale enough that the transaction can be considered expired.

[1] note that we currently don't ensure that a transaction record is written before writing other intents. Instead, we allow a transaction's first writing batch to be issued in parallel with its `BeginTransaction` request. This means that the referenced race was always possible, it was just unlikely. If the transaction record was delayed, a contending push, triggered by running into one of the parallel writes, could have aborted the transaction record before it was ever laid down. This RFC will ensure that we actually handle this race properly in the future.

[2] currently clients write intents at their OrigTimestamp, regardless of their current Timestamp. There is ongoing work to make clients write intents at their current Timestamp. Writing intents at Timestamp will improve the ability for intents to convey liveness information, but it's not critical for this approach described here to work.

# Reference-level explanation

## Detailed design

### TxnCoordSender

The `TxnCoordSender` no longer sends `BeginTransaction` requests if it observes that the cluster version setting can handle lazy transaction record creation.

The `TxnCoordSender` will also need to be more careful about never sending a committing `EndTransaction` request once it has sent an aborting `EndTransaction` request. This is because the aborting `EndTransaction` request could clean up all trace of the transaction, allowing the committing `EndTransaction` request to succeed. This might already be the case today.

### BeginTransaction

Other than being marked as deprecated, this request will behave the same.

### HeartbeatTxn

`batcheval.HeartbeatTxn` will be adjusted to create transaction records that are missing in the `PENDING` state instead of throwing an error.

It is critical that we don't allow `batcheval.HeartbeatTxn` to recreate an aborted and GCed transaction record. To ensure this, we compare the transaction's `OrigTimestamp` against the `TxnSpanGCThreshold` when creating a transaction record. This `OrigTimestamp` will always be the lower bound of the transaction's activity. If we compared the transaction's "last active" time against the `TxnSpanGCThreshold` then a later heartbeat could indicate more recent client activity and slip past the `TxnSpanGCThreshold` check.

### EndTransaction

`batcheval.EndTransaction` will be adjusted to create transaction records directly in the `COMMITTED` or `ABORTED` state without requiring that one already exists in the `PENDING` state.

A similar check to that discussed above will be performed on the `TxnSpanGCThreshold`.

### PushTxn

`batcheval.PushTxn` must undergo the largest transformation with this change. It must be adjusted to be able to handle missing transaction records without immediately marking the transaction as `ABORTED`. It will do so by using the provided intent `TxnMeta` to synthesize a `PENDING` pushee transaction if it can't find a transaction record. Critically, it will never actually persist this `PENDING` transaction record, but it will act as if the transaction record is what it pulled off disk. The synthetic record's last active timestamp will be set to the timestamp of the intent for purposes of transaction expiration.

A synthetic record will be considered `ABORTED` immediately if its timestamp is beneath the Replica's `TxnSpanGCThreshold`.

`batcheval.PushTxn` handling of extant transaction records will not change. 

By synthesizing transaction records based on the last active timestamp of an intent, `batcheval.PushTxn` will allow transactions to continue executing without fear of being immediately aborted even without having written a transaction record. As long as the transaction commits or begins heartbeating its transaction record within `TxnLivenessThreshold`, it will be safe from rogue aborts. Since most transactions have a duration less than `TxnLivenessThreshold`, most transactions won't ever need to write a transaction record in the `PENDING` state at all.

For most cases, the push will now fail and the pusher will enter the `txnwait.Queue`. However, there are still cases where the push will succeed, specifically with extreme priorities or expired transactions.

#### Successful PUSH_ABORT

Nothing changes here. Regardless of whether the transaction record is synthetic or not, it can be persisted if being moved directly to the `ABORTED` state. This is similar to what `batcheval.PushTxn` already does for missing transaction records.

#### Successful PUSH_TIMESTAMP

This case is more tricky. An explicit design goal here is to avoid allowing other transactions from creating a `PENDING` transaction record for a transaction. This is an important property because it simplifies a number of other state transitions. 

This complicates PUSH_TIMESTAMP requests, who want to ensure that a transaction can only commit above a certain timestamp. Currently, the request does this by modifying the existing transaction record and moving its timestamp. However, with lazily creating transaction records, a PUSH_TIMESTAMP request may find no transaction record through which to convey this information.

To facilitate this behavior without allowing other transactions from writing `PENDING` transaction records, this RFC proposes that PUSH_TIMESTAMP requests use the read timestamp cache to convey this information. A successful PUSH_TIMESTAMP request will bump the read timestamp cache for the local transaction key to the desired timestamp. `EndTransaction` requests will then check the read timestamp cache (in addition to the write timestamp cache, [maybe](#replica-side-writetooold-avoidance)) and will forward their txn timestamp accordingly. This draws clear parallels to the other uses of the read timestamp cache and is much less of an abuse than the current use of the write timestamp cache to store finalized transaction IDs.

This is a rare operation, so there is little concern of it adding strain to the read timestamp cache. Frequent page rotations in the cache will have the exact same effect as they do on any other transactional writes, so that is not a concern either.

#### Transaction Record GC

There are a number of interesting cases around transaction record cleanup that arise once we allow lazily creating transaction records. Before discussing them, let's first review the current mechanics of transaction record garbage collection:
- transaction records can be GCed by two sources: `EndTransaction` requests and the GC queue. 
- transaction records can be GCed if they are `COMMITTED` or `ABORTED`. If a transaction record is `PENDING` and processed by the GC queue, it will first be aborted before being GCed.
- `EndTransaction` requests can only be issued by the transaction's own coordinator, not by concurrent transactions. This property is held [even in the parallel commits RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180324_parallel_commit.md#transaction-record-recreation). This means that any `EndTransaction` request that GCs a transaction record was known to the transaction's coordinator.
- `COMMITTED` transaction records are only GCed after resolving all intents.
- For committing `EndTransaction` requests, this takes two paths, depending on whether all intents are resolved synchronously or not.
- The GC queue atomically bumps the `TxnSpanGCThreshold` while garbage collecting transaction records equal to or beneath that timestamp.

What this means in practice is that a non-replayed request from the transaction coordinator itself should never stumble into an already GCed transaction record without also finding itself beneath the `TxnSpanGCThreshold`. Requests could be replayed, but that is [protected against](#Replay-Protection-/-Idempotency) as well.

Thing are more interesting for concurrent transactions, which have no guarantee about the state that they observe a transaction in. It is completely valid that a concurrent transaction observes a missing transaction record that was either `COMMITTED` or `ABORTED` and later GCed without having any indication of what happened to it. This is already the case, and we opt for rewriting the transaction record as `ABORTED` in that case. This isn't great, but it also won't cause any issues.

The situation becomes a little worse now that `batcheval.PushTxn` will handle (often wait for) missing transaction records. In fact, a `PushTxn` request has little way of conclusively determining whether a transaction record has not yet been written or has already been finalized and GCed. This won't cause any correctness issues, but it means that naively a `PushTxn` may end up waiting for a transaction record that has already been GCed simply because it saw activity on one of the transaction's intents within the `TxnLivenessThreshold`.

It is unclear how to avoid this issue entirely. This RFC's current thinking is a pusher should query the intent that created the `WriteIntentError` immediately upon entering the `txnwait.Queue`. All intents must be resolved by a transaction coordinator before it is allowed to GC the transaction record. As such, by verifying that the intent still exists when entering the `txnwait.Queue`, we remove a race where pushers may wait on transaction records that have already been cleaned up eagerly by their coordinator. However, this detection can be fooled by replayed writes, which can result in intents even after a transaction has been `ABORTED` or `COMMITTED`. We expect that this is a sufficiently rare case that the effect of delaying concurrent transactions up to `TxnLivenessThreshold` will be a non-issue. However, if it becomes an issue, we may need to explore alternate txn liveness check methods, like talking to transaction coordinators directly.

### QueryTxn / txnwait.Queue

The `txnwait.Queue` queries both the pusher and the pushee transaction using `QueryTxn` requests while waiting for changes to either. While doing so, the `txnwait.Queue` has access to `TxnMeta` objects for both of these transactions. Either the `QueryTxn` request or the `txnwait.Queue` itself will need to be made aware that transaction records are now created lazily. `QueryTxn` requests will also need to be adjusted to return waiting dependent transactions even for missing transaction records.

Currently the `txnwait.Queue` works properly even if the pusher does not have a transaction record but does not behave properly when the pushee does not have a transaction record. There are a few alternatives to fixing this:
- `QueryTxn` could use its provided `TxnMeta` object to synthesize a transaction record when one does not exist. This is nice from the perspective of hiding complication behind an abstraction. It is also consistent with the change to return dependent transactions for missing transaction records. However, one concern is that this might undermine the purpose of `QueryTxn` in hypothetical situations where the abstraction is undesirable.
- `txnwait.Queue` could use its provided `TxnMeta` object to synthesize a transaction record for the pushee when one is not returned from `QueryTxn`.
- `txnwait.Queue` could have an explicit code path to deal with a missing transaction record for the pushee that takes the provided `TxnMeta` object into account without explicitly synthesizing a record.

This current thinking is that the first alternative should be pursued.

### Replay Protection / Idempotency

Transaction replays are a serious concern. If a batch or a series of batches gets duplicated or lost and later shows up, we can't allow it to have adverse effects. Currently, this is guarded against by updating the write timestamp cache on `EndTransaction` requests and consulting the write timestamp cache on `BeginTransaction` requests. Because the old transaction state machine required a transaction record to commit and that a `BeginTransaction` request would fail if it ran into the write timestamp cache, this prevented replays of transactions.

Now that we no longer have `BeginTransaction` requests, things are a little different. However, the same approach can be used. We can update the write timestamp cache on `EndTransaction` requests and consult it on committing `EndTransaction` requests. This will prevent more than a single `EndTransaction` request from executing successfully.

However, if we wanted to, we could do more. We can use MVCC to enforce this replay protection for us. If the EndTransaction request is replayed alone then it is harmless to let it write a new `COMMITTED` transaction record. If any writes are replayed after a successful commit then they will necessarily hit WriteTooOld errors and will be pushed to a new timestamp. If those writes are in the same batch as the EndTransaction then they will prevent it from committing. If those writes are in different batches from the EndTransaction then they will not be resolvable.

| Initial Action               | Replay                                | Protection                                                   |
|------------------------------|---------------------------------------|--------------------------------------------------------------|
| txn aborted by coordinator   | replay commit                         | not allowed by client, see [TxnCoordSender](#TxnCoordSender) |
| txn aborted by coordinator   | replay abort                          | could happen, won't cause issues                             |
| txn committed by coordinator | replay abort                          | not allowed by client, see [TxnCoordSender](#TxnCoordSender) |
| txn committed by coordinator | replay commit with writes in batch    | protected by WriteToOld errors                               |
| txn committed by coordinator | replay commit without writes in batch | could happen, won't cause issues                             |
| txn aborted by other txn     | commit or replay commit               | hit aborted txn record or TxnSpanGCThreshold                 |
| txn aborted by other txn     | replay abort                          | could happen, won't cause issues                             |

#### Replica-Side WriteTooOld Avoidance

Unfortunately, this currently breaks down because we have an optimization that avoids blocking transaction commit on WriteTooOld errors in two places. The first is during [1PC transaction evaluation](https://github.com/cockroachdb/cockroach/blob/8f30db0f07e940667bc34314ec6a446491a29790/pkg/storage/replica.go#L6036) and the second is during [EndTransaction evaluation](https://github.com/cockroachdb/cockroach/blob/8f30db0f07e940667bc34314ec6a446491a29790/pkg/storage/batcheval/cmd_end_transaction.go#L401). These cases allow a transaction to commit through a WriteTooOld error even without client-side approval. This breaks our ability to use MVCC to provide replay protection. It's unclear how important this optimization is as it's only applicable to heavily contended transactions with no refresh spans (i.e. blind writes). Blind writes from SQL are not very common and the optimization only saves a single round-trip to the client, who will never need an actual transaction retry because it can be trivially refreshed.

#### Commit Triggers

Above we assumed that EndTransactions without corresponding writes could harmlessly be replayed because they are side-effect free. This doesn't take commit triggers into account. If we wanted to allow this in full, we would need to make commit triggers idempotent.

#### Proposed Strategy

Because of the added complication of using MVCC to handle replays, we're opting not to pursue this approach immediately. Instead, we will simply use the write timestamp cache to protect against repeat `EndTransaction` requests.

### 1PC Transactions

Without `BeginTransaction` requests, the detection of "one-phase commit transactions" gets a little more tricky. Previously, a 1PC transaction was recognizable simply by looking for batches with `BeginTransaction` and `EndTransaction` requests together. `DistSender` made sure to never send these in the same batch unless all writes in the batch ended up on the same Range.

This detection is moderately more difficult to make efficient without `BeginTransaction`. The straightforward but wrong approach would be to compare the key bounds of each of the `IntentSpans` in the batch's `EndTransaction` with the Range it is evaluating on. If all `IntentSpans` are in the Range's bounds, the batch can be evaluated as a 1PC transaction. This gets tripped up when the txn has written before but only to the range that its record is on.

A workable approach would be to only increment the transaction's sequence number on writes and to check whether the batch contains all of the sequence numbers up to the exclusive upper bound sequence on the `EndTransaction` request. This simplifies to checking the first write in the batch to see if it has sequence number 1, but that simplification will break down under parallel commits, so it will not be pursued.

### Intent Timestamps

Now that intent timestamps are being used to determine the last time that a client was definitely active, we need to make sure that it tracks that information. Luckily, `intent.Txn.Timestamp` gives us exactly this! For an intent to have a certain timestamp, the client must have been alive at that timestamp to instruct the intent to be written.

A concern here is that intents can be pushed by other transactions to higher timestamps. Conveniently, when an intent is pushed, only the `MVCCMetadata.Timestamp` and the provisional timestamped key-value are changed. The Timestamp in the TxnMeta struct (`MVCCMetadata.Txn.Timestamp`) itself is [left unchanged](https://github.com/cockroachdb/cockroach/blob/fd28ed991385b446028f870d0049122fcabc94e3/pkg/storage/engine/mvcc.go#L2142-L2147).

## Drawbacks

This change is invasive and has the potential to be destabilizing. We shouldn't do it if we aren't willing to address some fallout from it. That said, I have a working prototype with most of the design items listed here implemented and it doesn't seem to be hitting very many issues with unit tests.

The other drawback is that the migration for this could be a little tricky. I think everything will work if we leave the BeginTransaction code-paths in place and make the decision on whether to use the request type or not on the client based on the active cluster version. This works because the current proposal manages to avoid adding any new state to the transaction record or to intents. An earlier prototype did add new state to intents, which would have made the migration much more difficult.

## Rationale and Alternatives

The only viable alternative to this change that allows parallel commits to work is lifting the mutual exclusion between evaluation and replication of BeginTransaction and EndTransaction requests. We have thrown around the idea of "pipelining" beneath the replica latching level, but there are no clear ideas on how this would actually work.

## Unresolved questions

- Will removing the transaction record auto-GC mechanism cause a performance hit? Will delaying all other transaction record GC runs cause one? Or will that actually speed things up? See section [Transaction Record GC](#transaction-record-gc).
- Should we remove server-side WriteTooOld avoidance? See section [Replica-side WriteTooOld avoidance](#replica-side-writetooold-avoidance).
- Is there any reason to keep `BeginTransaction` around? It seems like any test that needs a transaction record can just use a `HeartbeatTxn` request.
