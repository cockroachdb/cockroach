# Parallel Commits

Consensus latencies are the price CockroachDB pays for its consistency guarantees; they are not usually incurred on monolithic or NoSQL databases, and can be used as an argument against the use of CockroachDB.

As a database that prides itself on geo-distributed use cases, we must strive to reduce the latency incurred by common OLTP transactions to near the theoretical minimum: the sum of all read latencies plus one consensus latency.

This is far from true at the time of writing: chatty SQL transactions eat consensus latency on every write (unless they can use the `RETURNING NOTHING` crutch) due to the naive underlying implementation. Luckily, work to change this is underway: [#16026] proposes a mechanism which lets SQL transactions incur only a single write latency by (morally) deferring all writes to commit time.

Unfortunately, writing at commit time in itself is suboptimal since the KV layer cannot send writes in parallel with a commit operation, and splits them back up into two rounds of consensus. In effect, transaction latencies for virtually any real schema after [#16026] are still double what they could be.

This tech note outlines a solution that avoids this problem, works well for reasonably sized (i.e. small) OLTP transactions and naturally falls back to today's suboptimal behavior for transactions with a large write set.

In short, it allows `DistSender` to send `EndTransaction` in parallel with the remainder of the final batch, by putting the transaction in a newly introduced `STAGED` status, and returning immediately to the client after. As a crucial performance optimization, an asynchronous second `EndTransaction` then flips the transaction record from `STAGED` to `COMMITTED` and resolves the intents.

This document contains two proposals, basic and extended.

|                     | today | basic | extended |
|---------------------|-------|-------|----------|
| commit in parallel  |   n   |   y   |     y    |
| resolve in parallel |   n   |   n   |     y    |


Compared to today, the basic proposal reduces the latency of a given transaction for the client that issues it by one round trip. The extended proposal additionally reduces the latency for all contending clients by one round trip.

The basic proposal can only commence intent resolution after marking the transaction record as `COMMITTED`. Swift intent removal is important to reduce wait times under contention, and the extended proposal improves on this by commencing intent resolution once the `STAGED` record and the intents have been laid down.

The price to pay for this extension is having to embed a transaction ID into every versioned value. This is an invasive but well-contained change that should not be taken lightly. The sibling tech note on [Node Transaction IDs](node-txn-ids.md) explores this circle of ideas, and in particular proposes a transaction ID scheme that avoids overhead for the per-version ID storage. The author is of the opinion that the basic proposal should be implemented first.

Credit for inspiring much of the write-up goes to @andyk.

## The Problem

Consider the (implicit) transaction

```sql
INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z');
```

where the table `t` has a simple KV schema and has been split at `2` and `3`. Assume also that there is no 1PC optimization. Since this is an implicit transaction, the SQL layer will send it as a single KV batch of the form

```
BeginTransaction(1)
CPut(1)
CPut(2)
CPut(3)
EndTransaction(1)
```

And, when split by range boundaries, `DistSender` will consider the subbatches

```
BeginTransaction(1)
CPut(1)
EndTransaction(1)
```

```
CPut(2)
```

```
CPut(3)
```

To minimize latency, you would want to send these three batches in parallel. However, this is not possible today.

To see why not, consider doing so and finding that the first two batches apply successfully, whereas the third fails (say, with a unique constraint violation).

The first batch both created and committed the transaction record, so any intents written become live. However, the third value is never written; we have lost a write.

To work around this, `DistSender` detects batches which try to commit a transaction, and forces the commit to be sent in a separate batch when necessary. This would result in the following requests to be sent in parallel first:

```
BeginTransaction(1)
CPut(1)
```

```
CPut(2)
```

```
CPut(3)
```

and then, contingent upon the success of the above wave,

```
EndTransaction(1)
```

This effectively doubles the latency (ignoring the client<->gateway latency).

## Outline Of The Solution And Terminology

At the heart of the proposal is a new transaction status `STAGED`. We want the coordinator to be able to

1. send `EndTransaction` with status `STAGED` in parallel with the last batch
1. on success of all writes, return to the client, and
1. asynchronously send `EndTransaction` with status `COMMITTED`.

Additionally, we change the semantics of what it means for a transaction to commit. As it stands, they are

> A transaction is committed if and only if there is a transaction record with status COMMITTED.

The proposed changed condition (referred to as the **commit condition**) is:

> A transaction is committed if and only if there is a transaction record with status COMMITTED, or with status STAGED and all of the writes of that transaction are present a) for the basic proposal: in intent form; b) for the extended proposal: in value or intent form.

We refer to a transaction in status `STAGED` for which the *commit condition* holds as **implicitly committed**; when there is a `COMMITTED` transaction record, it is **explicitly committed**. In typical operations, encountered transaction records are usually *explicitly committed*. However, when coordinators crash or become disconnected, this does not hold and we need `STAGED` transaction records to contain enough information to check the *commit condition*. To achieve this, the set of intent spans for the writes in the last batch is included in the staging `EndTransaction` request; we call these intent spans the **promised writes**. In fact, to later arrive at a consistent `COMMITTED` record (which needs to contain all writes for GC purposes), it will also separately contain the intent spans for all prior batches.

The changes suggested in this document chiefly affect `DistSender` and the storage layer (`Store`). The `STAGED` transaction status is never encountered by consumers of `DistSenders` and must not be used by them.

### Performance Implications

There is an extra write to the WAL due to the newly introduced intermediate transaction status and as a result, throughput may decrease (which is bad) while latencies decreases (which is good). In the long term, we may be able to piggy-back the committing `EndTransaction` on other Raft traffic to recuperate that loss. See #22349 which discusses similar ideas in the context of intent resolution.

### Changes In Transactional Semantics

The suggested proposals require a straightforward but subtle adjustment of our transactional semantics.

#### No-Op Writes

The set of **promised writes** are writes that *need to leave an intent*. This is in contrast to the remaining *intent spans*, which only need to be a *superset* of the actually written intents. As an example, consider a point deletion; in certain scenarios, this may not leave a tombstone behind and consequently there wouldn't be an intent or later committed value. This makes it impossible to determine whether that particular *promised write* actually happened and leads to anomalies.

Thus, we must never issue [no-op writes](https://github.com/cockroachdb/cockroach/issues/23942) in the final batch of a transaction. This is more of a conceptual problem than a practical one but we need to make this invariant rigid to avoid breaking it as the codebase evolves.

A similar problem occurs with transactions that want to resume after hitting an application error in their final batch. For example, a client transaction today could issue a `ConditionalPut` which returns with a `ConditionFailedError` in their final batch and might decide to try a different write after. To the best of the author's knowledge, we don't make use of this today, but it becomes strictly illegal with parallel commits. Once a `STAGED` transaction record is sent, the *promised writes* must not change. The client transaction should be marked as aborted and explicitly rolled back via an `ABORT`ing `EndTransaction` issued by the client to inform concurrent transactions of the abort proactively.

#### Ranged Intents

A problem we need to address is that transactions may contain *ranged intents*. They are a result of either ranged writes (`DeleteRange`) or the condensing of spans (for large transactions). For a transaction containing ranged writes in its final batch, it's expensive and complicated to accurately check the commit condition, and we consider it out of scope.

Instead, we mandate that transactions which contain ranged intents in their last batch fall back to sending only a `COMMIT`ing `EndTransaction` in a separate, final, batch. They thus skip the `STAGED` status and correspondingly, we never have to check the commit condition for a ranged intent.

`DeleteRange` is used in point deletions by the SQL layer in common OLTP workloads; we can change it to return the affected keys (if there aren't too many; or perhaps SQL can stop using `DeleteRange` as discussed in #23258). Span condensing on the other hand should already have been tuned accordingly so that it does not affect common OLTP transactions, though this should be checked and also tracked.

## Status Resolution Process

The goal of the status resolution process is to either abort a transaction or to determine its having committed, by trying to prevent one of the transaction's declared *promised writes* of the final batch, which are stored in a `STAGED` transaction record.

### When to run this?

There are some preliminary considerations on when to *initiate* the process. A `STAGED` transaction is typically encountered by a conflicting transaction and most likely will flip to `COMMITTED` momentarily. In many (the majority?) of cases, the transaction will commit before we can abort it anyway; it is more efficient to wait for that to happen. This is especially true if the priorities dictate that we want to wait for that transaction to finish; then we only want to force status resolution if we suspect that the coordinator has died (see below). This suggests unconditionally waiting for a short moment (~a few latencies or a heartbeat timeout) before taking action.

### PreventIntentRequest

At the heart of the process is trying to prevent an intent of the transaction to be laid down (which is only possible if it isn't already there) at at the provisional commit timestamp. To achieve this efficiently, we introduce a new point read request, `PreventIntentRequest`. This request populates the timestamp cache (as any read does) and, in the basic version, returns whether there is an intent at the given key for the specified transaction and timestamp. In the extended proposal, it also accepts a committed value at the right timestamp with the correct embedded transaction ID.

If the check fails, the read returns with a structured error (`WritePreventedError`) but still populates the timestamp cache (similar to a conditional put), thus guaranteeing that the intent will not be laid down at that timestamp later on. Returning an error is a performance optimization: a (possibly moderately large) batch of these requests will be passed to `DistSender`, and returning a structured error early short-circuits this execution when it is no longer necessary; the caller interprets the structured error and knows that it has succeeded in preventing the promised write at the specified timestamp.

Note that `PreventIntentRequest` is odd in that it only really needs to populate the timestamp cache on error. This could be optimized but is likely not relevant, as use of the request in practice should be rare.

### The full process

To run full status resolution, a client runs the following process.

1. retrieve the *promised writes* from the `STAGED` transaction record, and note the transaction ID and provisional commit timestamp. Note that we are guaranteed that [these are all point requests](#ranged-intents).
1. construct a batch at the provisional commit timestamp that contains a `PreventIntentRequest` for each *promised write*, and includes the provisional commit timestamp and the transaction ID.
1. Run the batch, and act depending on the outcome:
    1. on `WritePreventedError`, an intent was prevented and we can move forward and abort the transaction. But note a subtlety: the transaction may have restarted and written a new `STAGED` record with a higher provisional commit timestamp, and may now be (implicitly)or explicitly) committed. Thus, if the provisional commit timestamp changed, we start over with the status resolution process. We may want to add a flag `abort_on_restart` to the transaction record which can be set in this case to avoid many iterations of this process when necessary.
    1. on other errors, retry as appropriate but check the transaction record for updates before each retry.
    1. on `nil` error, change the status of the transaction record to `COMMITTED` while synchronously resolving the local intents. Hand the full remaining set of intent spans to the intent resolver (on the path that garbage collects the transaction record on completion). (This is just what happens on `EndTransaction` today). Note that the fact that we discovered the transaction as committed implies that the original client, if still alive, can only ever issue an `EndTransaction` that attempts to `COMMIT` the transaction as well. This request may fail due to the GC'ed transaction record, but this is of no consequence.

## Intent Handling At The Gateway

In the happy case, the gateway is the first to know that a transaction has (implicitly) committed.

### In The Basic Proposal

In the basic proposal, we cannot make use of this fact: the intent resolution proceeds as today once the transaction record is *explicitly committed*. This in turn guarantees that when trying to recover the true transaction status from a `STAGED` transaction, a client may assume that a prevented intent implies that the transaction is aborted.

### In The Extended Proposal

In the extended proposal, the coordinator (`DistSender`) starts resolving intents once it learns that the transaction has *implcitly committed*, i.e. once it has received successful responses from all requests forming the last batch. We go through the details below, but the big implication on the status resolution process is that pointed out under [PreventIntentRequest](#PreventIntentRequest): when checking for an promised write, we may now find a committed version at the right timestamp, and must be able to determine whether this was written by the transaction in question.

The consequence of this is that we must be able to reconstruct the transaction ID from every versioned write (which was written transactionally). This is not hard to achieve but is invasive and most of all a concern of space usage; current transaction IDs (16 byte UUIDs) are large. This motivates a parallel tech note about [changing the format of our transaction IDs](node-txn-ids.md) which in the common case allows storing only a varint-encoded NodeID (one byte in small clusters) to recover the transaction ID.

We will not explore the embedding of NodeIDs further in this document. It should be discussed separately when implementing the extended proposal is desired.

Recall that today, the committing `EndTransactionRequest`

1. synchronously resolves the intents colocated with the transaction record, and returns spans for the remaining intents which
1. the intent resolver takes care of. If it resolved successfully,
1. the transaction record is removed eagerly.

Now that intent resolution happens at `DistSender`, this process simplifies. Instead of 1), which was code specific to `EndTransaction`, `DistSender` sends (via itself) a batch containing the *explicit commit* and the `ResolveIntentRequest`s for all of the transaction's spans (we may want to consult the intent resolver's semaphore to preserve existing behavior). This will naturally send the explicit commit with the maximal set of intents local to the transaction record. 2) similarly moves to `DistSender` contingent on the success of 1).

## Interaction with 1PC Transactions

The 1PC path is an optimization that applies to transactions which apply wholesale to a single range. In this case, `DistSender` does *not* split off the `EndTransaction`; it sends the whole batch to the range and the range in turn avoids creating the transaction records and intents in the first place, relying instead on the atomicity of Raft commands.

With parallel commits, there is no functional change here, though `DistSender` must be careful not to break this functionality with the newly added code. The `STAGED` transaction status should not occur for 1PC transactions.

- example racing transaction restart

## Examples
The examples below illustrate common scenarios for the final batch (where `ki` lives on range `i`). Time flows from left to right and each line corresponds to one goroutine (which may be reused for compactness). A batch directed at a single range is grouped in curly braces.

### Happy Case (basic proposal)

The transaction writes its three writes and staged txn record in parallel to three ranges. When all operations come back as success, the client receives the responses, and an asynchronous request marks the transaction record as committed, saving others the expensive manual verification of this fact. The intents are resolved after commit (note that there is no write to `k2` in the final batch).

Note that a write colocated with the transaction record is resolved with the commit.

```
Write(k1)   ok                                  Resolve(k1) ok
{Write,Stage}(k2)  ok {Commit,Resolve}(k2)   ok
Write(k3)        ok                             Resolve(k3)   ok
```

### Happy Case (extended proposal)

Like the last case, but the intents are resolved before waiting for `Commit(k1)`. Note that a write colocated with the transaction record is resolved with the commit.

```
Write(k1)   ok      Resolve(k1)  ok
Write(k2)     ok
Stage(k2)  ok         {Commit,Resolve}(k2)   ok
Write(k3)        ok Resolve(k3)                 ok
```

### 1PC transaction (both proposals)

Maintain today's behavior.

```
{Begin(k1), W(k1), Commit(k1)}   ok
```

In the below example, the 1PC attempt fails since the range has split. The remainder of the history would follow the basic example for the basic or extended proposal, respectively.

```
{Begin(k1), W(k1), Commit(k1)} err(RangeKeyMismatch)
```


### Unhappy Case 1 : failed write

A write fails hard (this includes `ConditionFailedError`); the transaction coordinator aborts the transaction.

```
{Write,Stage}(k1) ok                        {Abort,Resolve}(k1)
Write(k2)       non-retryable-error                            Resolve(k2)
Write(k3)    ok                                                Resolve(k3)
```

The history with a retryable error is similar except that the write may be retried.

### Unhappy Case 2: coordinator dies without staged txn record

The coordinator disappears without *explicitly* committing the transaction. After a timeout, a concurrent transaction decides to run status resolution.

```
Stage(k1)      crash
Write(k2)    ok
Write(k3)  ok
conflicting client:
Read(k3)            status-resolution(k1) Abort(k1) Resolve(k1)
                                                    Resolve(k2)
                                                    Resolve(k3)
```

### Unhappy Case 3: coordinator dies with implicitly committed transaction

The intent resolutions following `Commit(k1)` have been omitted for brevity.

```
Stage(k1)    ok
Write(k2)       ok crash
Write(k3)  ok
conflicting client:
Read(k3)           status-resolution PreventIntent(k2) present
                                     PreventIntent(k3) present Commit(k1)
```

### Unhappy Case 4: coordinator dies with txn record, but missing promised write

The intent resolutions following `Abort(k1)` have been omitted for brevity.

```
Stage(k1) ok
Write(k2)  ok crash
Write(k3)     fail
conflicting client:
Read(k3)           status-resolution PreventIntent(k2) present
                                     PreventIntent(k3) prevented Abort(k1)
```

### Unhappy Case 5: transaction restart on staging record

No staged transaction record is created, transaction restarts (omitted).

```
Stage(k1) err(txn restart)
Write(k2)  ok
```

### Unhappy Case 6: transaction restart on write

TODO(tschottdorf): think this through better. This is an interesting case.

The transaction has its timestamp bumped in one of its final writes. A conflicting client may end up initiating the status resolution process and could find one of the intents at a higher timestamp than expected, which signals to it that the transaction is likely retrying.

This is a case in which coordinator-based conflict resolution would help because it would save the status resolution step. Now the best we can do is send another update the transaction record, but this is not efficient for contended scenarios.

```
Stage(k1) ok
Write(k2)   ok(write_too_old=true)
```

[#16026]: https://github.com/cockroachdb/cockroach/issues/16026
