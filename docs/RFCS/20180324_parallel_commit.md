- Feature Name: parallel commit
- Status: completed
- Start Date: 2018-03-24
- Authors: Tobias Schottdorf, Nathan VanBenschoten
- RFC PR: #24194
- Cockroach Issue: #30999

# Summary

Cut the commit latency for the final batch of a transaction in half, from two rounds of consensus down to one, by addressing a shortcoming of the transactional model which does not allow `EndTransaction` to be sent in parallel with other writes.

This is achieved through the introduction of a new transaction status `STAGED` which can be written in parallel with the final batch. This transaction status is usually short-lived; during failure events, transactions may be abandoned in this status and are recovered via a newly-introduced *status resolution process*.

With proper batching, this change translates directly into a reduction of SQL write latencies.

The latency until intents are resolved remains the same (two rounds of consensus). Thus, contended workloads are expected to profit less from this feature.

# Motivation

Consensus latencies are the price CockroachDB pays for its consistency guarantees; they are not usually incurred on monolithic or NoSQL databases, and can be used as an argument against the use of CockroachDB.

As a database that prides itself on geo-distributed use cases, we must strive to reduce the latency incurred by common OLTP transactions to near the theoretical minimum: the sum of all read latencies plus one consensus latency.

This is far from true at the time of writing: chatty SQL transactions incur a consensus latency on every write (unless they can use `RETURNING NOTHING`) due to the naive underlying implementation.

Consider the (implicit) transaction

```sql
INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z');
```

where the table `t` has a simple KV schema and has been split at `2` and `3`. Since this is an implicit transaction, the SQL layer will send it as a single KV batch of the form

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

This RFC should be considered in conjunction with #16026, which avoids write latencies until commit time (incurring only read latencies until then). With #16026 alone, transaction latencies would roughly equal the sum of read latencies, plus *two* rounds of consensus. This proposal reduces it to the minimum possible: the sum of read latencies, plus *one* round of consensus.

# Guide-level explanation

As seen in the last section, the fundamental problem is that transactions can't be marked as committed until there is proof that the final batch of writes succeeded.

A new transaction status `STAGED` avoids this problem by populating a transaction record with enough information for *anyone* to prove (or disprove!) that these writes are all present. If they are, the transaction must be marked committed; otherwise, it can be marked as aborted. This means that the coordinator can return to the client early when it knows that these writes have succeeded, and can send the actual commit operation lazily, as a performance optimization.

Omitting error handling for now, we want the coordinator to be able to

1. send `EndTransaction` with status `STAGED` in parallel with the last batch
1. on success of all writes, return to the client,
1. asynchronously send `EndTransaction` with status `COMMITTED`, which as a side effect
1. resolves the intents.

To achieve this, the semantics of what it means for a transaction to commit change. As it stands, they are

> A transaction is committed if and only if there is a transaction record with status COMMITTED.

The proposed changed condition (referred to as the **commit condition**) is:

> A transaction is committed if and only if a) there is a transaction record with status COMMITTED, or b) one with status STAGED and all of the intents written in the last batch of that transaction are present.

We refer to a transaction in status `STAGED` for which the *commit condition* holds as **implicitly committed**.

When there is a `COMMITTED` transaction record, it is **explicitly committed**. In typical operations, encountered transaction records are usually *explicitly committed*.

However, when coordinators crash or become disconnected, this does not hold and we need `STAGED` transaction records to contain enough information to check the *commit condition*. To achieve this, the set of intent spans for the writes in the last batch is included in the staging `EndTransaction` request; we call these intent spans the **promised writes**.

In fact, to later arrive at a consistent `COMMITTED` record (which needs to contain all writes for GC purposes), it will also separately contain the intent spans for all prior batches.

The changes suggested in this document chiefly affect `DistSender` and the conflict resolution machinery. The `STAGED` transaction status is never encountered by consumers of `DistSenders` and must not be used by them.

## Example Notation

Throughout the document, examples illustrate the algorithm. In the examples, key `ki` lives on range `i`; time flows from left to right and each line corresponds to one goroutine (which may be reused for compactness). A batch directed at a single range is grouped in curly braces. A vertical line demarcates that the events to the right wait for the events on the left to complete. `Write(ki)`, `Stage(ki)`, `Commit(ki)` are requests (from the final batch in the transaction) sent by `DistSender`. `Resolve(ki)` is invoked from the intent resolver of the replica housing the transaction record.

The examples are accompanied by explanations which may contain additional details not reflected in the notation.

We now go through some basic examples from the point of view of the `DistSender` machinery.

## Example (basic happy case)

The transaction writes two writes and staged txn record in parallel to three ranges. When all operations come back successfully, the client receives an ack for its commit. An asynchronous request marks the transaction record as committed (this is not required for correctness).

Note that the transaction did not write to `k2` in the final batch, but has written there earlier (for that's where the transaction record is anchored). We elide the intent resolution for it which happens as part of the `Commit`.

```
Write(k1)   ok     |                  |Resolve(k1) ok
Stage(k2)  ok      |Commit(k2)      ok
Write(k3)        ok|                  |Resolve(k3)   ok
                   ^- ack commit to client
```

## Example (1PC commit)

Today's behavior is maintained: whenever the final batch of the transaction addresses into a single range, the batch is sent as-is. In particular, no `STAGED` transaction record is ever written. Additionally, if the complete transaction is only one batch, the 1PC optimization (which avoids writing the transaction record in the first place) can apply on the range receiving it.

### 1PC transaction (success)

```
{Begin(k1), Write(k1), Commit(k1)}   ok
```

### 1PC transaction (range split then success)

In the below example, the 1PC attempt fails since the range has split.

```
                                       .- range key mismatch
                                       v
{Begin(k1), Write(k1, k2) Commit(k1)} err

```

Instead, the request is retried and sent in the "conventional" way (the intent at `k1` is resolved as part of the `Commit`):

```
{Begin, Write, Stage}(k1)  ok|Commit(k1)
Write(k2)                 ok |          |Resolve(k2)
                             ^- ack commit to client
```

### Failed write

####  Non-retryable

A write fails hard (a good example is `ConditionFailedError`, which corresponds to a unique constraint violation); the error bubbles up to the client:

```
{Write,Stage}(k1) ok
Write(k2)       err <- returned to client
Write(k3)    ok
```

The client then aborts (or retries) the transaction:

```
Abort(k1) ok|
            |Resolve(k2)
            |Resolve(k3)
```

The client has to stop using the transaction for the current epoch (i.e. it has to restart the txn), or new intents written would not be reflected in the staged transaction record, and thus the *commit condition*.

#### Retryable

Writes in the final batch can have outcomes that force a transaction retry: their timestamp can be pushed, they can catch a `WriteTooOldFlag`, or they incur a straight-up `TransactionRetryError`.

```
Stage(k1)   ok
Write(k2)       retry <- returned to client
Write(k3)    ok
```

On the face of it, this looks very similar to the non-retryable case, and from the perspective of `DistSender`, it is. However, viewing this from the point of view of the *commit condition*, this is different because the transaction is potentially abortable: it has not laid down a committable intent at `k2`. However, as explained later, concurrent transactions will only make use of this if the transaction record looked abandoned, which wouldn't be the case.

# Reference-level explanation

## Detailed design

### txnCommitter

The parallel commit machinery lives in a `txnInterceptor` called `txnCommitter`. The interceptor
lives below `txnPipeliner` in the `TxnCoordSender`'s interceptor stack. It gets activated when a
`BatchRequest` containing at least one request plus a committing `EndTransaction` arrives. These
other requests may be `BeginTransaction` requests, writes, `QueryIntent` requests, or any other
transactional requests.

The code first checks whether the batch is eligible for parallel commit. This means that the batch contains no

- ranged (write) request. At the time of writing, the only such request type is `DeleteRange`, and we tend to try to use it less (#23258). Handling ranged requests is difficult (near impossible). Consider a `DeleteRange` that writes into a span and lays down an intent (on the single affected key), and a later (unrelated) write into that range (on a previously empty key). The status resolution process needs to know that the span related to the promised write was written atomically so that it can conclude from finding one intent that all intents are there. This implies that the ranged requests corresponding to promised writes must not be split across ranges, which adds an unreasonable amount of complexity.
- commit trigger. Commit triggers are only used by internal transactions for which the transaction record's lease is usually colocated with the client running the transaction (so that only one extra round trip to the follower is necessary to commit on the slow path). Support for this can be added later: add a commit trigger to the `STAGED` proto and, if set, don't allow the commit trigger to change when the transaction commits explicitly.

If the batch is not eligible, the batch passes through the `txnCommitter` unchanged. It will be
sent with a `COMMITTED` status as always.

When it is eligible, a copy of the `EndTransaction` request is created, with the status changed to `STAGED`, and the `PromisedWrites` field is populated from the other requests in the batch according to the protobuf below.

```
  // promised_writes is only set when the transaction record is in
  // status `STAGED` (part of the parallel commit mechanism) and is
  // the set of key spans with associated sequence numbers at which
  // the transaction's writes from the last batch were directed. This
  // is required to be able to determine the true status of a
  // transaction whose coordinator abandoned it in `STAGED` status.
  // The so-called status resolution process needs to decide whether
  // the promised writes are present. If so, the transaction is
  // `COMMITTED`. Otherwise, it is `ABORTED`.
  //
  // The parallel commit mechanism is only active for batches which
  // contain no range requests or commit trigger.
  repeated message SequencedWrite {
    bytes key = 1;
    int32 seq = 3;
  } promised_writes = 17;
```

In the happy case, all requests come back successfully. Errors (from above the routing layer) are propagated up the stack, with the client (`TxnCoordSender`) restarting (includes refreshing), or aborting the transaction as desired (to avoid forcing concurrent transactions into the status resolution process).

Note that RPC-level errors from the final batch will be turned into ambiguous commit errors as they are today (this happens at the RPC layer and wont be affected by the changes proposed here).

If the writes come back, it is checked whether they require a transaction restart. This is the case if the returned transaction has had its timestamp pushed, or if it has the `WriteTooOld` flag set. If this is the case, a transaction retry error is synthesized and sent up the `TxnCoordSender`'s interceptor stack. Note that `TxnCoordSender` also expects to learn about the intents which were written and abuses the `client.Sender` interface to this effect; these semantics are kept, though during implementation widening the interface between `TxnCoordSender` and `DistSender` to account for refreshes will be considered.

If the transaction *can* commit, the `txnCommitter` returns the final transaction to the client and asynchronously sends an `EndTransactionRequest` that finalizes the transaction record (and as a side effect, eagerly resolves the intents). Expected responses to the `EndTransactionRequest` are RPC errors (timeouts, etc) and success. In particular, we can assert that the commit is not rejected.

When adding in this new interceptor, we will also move `txnIntentCollector` below the `txnPipeliner`,
so that the new order between them becomes: `txnPipeliner` -> `txnIntentCollector` -> `txnIntentCollector`.
Getting the refresh behavior correct may also require us to move `txnSpanRefresher` above all three
of these interceptors. There are no known issues with making those rearrangements.

### DistSender

DistSender is adjusted to allow `EndTransaction` requests to be sent in parallel with other
requests if its status is anything other than `COMMITTED`. In practice, this will never be used with
`PENDING` or `ABORTED` statuses, but it means that `EndTransaction` requests with a `STAGING`
status can be sent in parallel with other requests in its batch.

### Replica

The `txnCommitter` is too high up the stack to know whether a committing `EndTransaction` request
can skip the `STAGING` status and move straight to a `COMMITTED` status. This is the case when
all promised writes in the request are on the same range as the transaction record. This detection
could be performed in the `DistSender`, but that's not desirable because it leaks too much knowledge
about transactions into the `DistSender`.

Instead, Replica is made aware of `EndTransaction` requests with the `STAGING` status. In a similar
fashion to how Replicas detects 1-phase commit batches, it is made to check for `STAGING` `EndTransaction`
requests that can skip the `STAGING` phase and move straight to `COMMITTING`. When the request finishes,
it will return to the txn coordinator, informing it of the new transaction status.

### Status Resolution Process

The status resolution process deals with the case in which `DistSender` writes a `STAGED` transaction record but fails to finalize it, leaving behind an abandoned transaction (i.e. one that hasn't been heartbeat recently).

Its goal is to either abort the transaction or to determine that it is in fact committed, by trying to prevent one of the transaction's declared *promised writes* of the final batch, which are stored in a `STAGED` transaction record.

The status resolution process is triggered by a reader or writer which encounters an intent, or by the GC queue as it tries to remove old transaction records. In today's code, they issue a `PushTxnRequest` which may be held up by the txn wait queue.

With the introduction of the `STAGED` transaction status, push requests may fail even though the pushee is abandoned, and so the txn wait queue (on the leaseholder of the range to which the `STAGED` transaction is anchored) triggers the *status resolution process* when a transaction record is discovered as `STAGED` and abandoned, making sure to have only one such process in flight per transaction record.

#### PushTxn

`PushTxn` can't simply change the status of a `STAGED` transaction. Instead, it returns such transactions verbatim. All consumers of `PushTxnResult` are updated to deal with this outcome, and must call into the status resolution machinery instead.

An alternative to be considered is to return an error instead. This doesn't work well for batches of push requests, though. If during implementation we decide for the first option, we may also consider removing `TransactionPushError` in the process.

#### QueryIntent(Prevent=true)

At the heart of the process is trying to prevent an intent of the transaction to be laid down (which is only possible if it isn't already there) at at the provisional commit timestamp. To achieve this efficiently, we introduced a new point read request, `QueryIntent`, which optionally prevents missing intents from ever being written in the future. This request populates the timestamp cache (as any read does) and returns whether there is an intent at the given key for the specified transaction, timestamp, and at *at least* the specified sequence number. We don't check the exact sequence number because a batch could contain overlapping writes, in which case only the latest sequence number matters. If we trust that `PromisedWrites` has been faithfully populated, checking for "greater than or equal" is equivalent to (but simpler than) computing and keeping only the last write to a given key's sequence number.

The request also returns whether there was an intent of the transaction *above* the expected timestamp. If this happens, the transaction has restarted or been pushed, and should instruct the caller to check the transaction record for a new update (since status resolution isn't kicked off unless a transaction looks abandoned, this may not be worth it in practice).

As an optimization, we might return a structured error when an intent was prevented (but still populate the timestamp cache), to short-circuit execution of the remainder of the batch.

#### The Algorithm

1. wait until the transaction is abandoned (to avoid spurious aborts) -- this is done by the txn wait queue in the common path.
1. retrieve the *promised writes* from the `STAGED` transaction record, and note the transaction ID and provisional commit timestamp.
1. construct a batch at the provisional commit timestamp that contains a `QueryIntent(Prevent=true)` for each *promised write*, and includes the provisional commit timestamp and the transaction ID.
1. Run the batch, and act depending on the outcome:
    1. if an intent was prevented, abort the transaction. But note a subtlety: the transaction may have restarted and written a new `STAGED` record with a higher provisional commit timestamp, and may now be (implicitly or explicitly) committed. In this case our aborting `EndTransaction` would fail as it checks the provisional commit timestamp against the record's.
    1. on other errors, retry as appropriate but check the transaction record for updates before each retry.
    1. if all intents were found in place, propose a committing `EndTransaction`. Note that the fact that we discovered the transaction as committed implies that the original client, if still alive, can only ever issue an `EndTransaction` that attempts to `COMMIT` the transaction as well.

#### Transaction record recreation

The status resolution mechanism in conjunction with the eager transaction GC path can lead to transaction entries being recreated in the wrong status. There are two kinds of eager GC (though the difference is immaterial here): First, transaction records which on commit have only intents in the same range are immediately deleted and second, after commit, when the intent resolver has resolved the external intents, the transaction record will be deleted (via an extra consensus write).

For example, take an *implicitly committed* transaction. While a status resolution is in process and has failed to prevent any of the intents, a concurrent writer discovers one of the intents and prepares to push the transaction. Before it does that, the status resolution succeeds, the transaction is committed, the intents resolved, and the transaction record removed thanks to eager GC. When the push is invoked, it recreates the transaction record as `ABORTED`. This is not an anomaly because the intent is at this point already committed, but it's on dangerously thin ice because the aborter is now likely to assume that they have actually aborted the competing transaction. In the [improving ambiguous results][#improving-ambiguous-results] section, a transaction tries to abort its own record to avoid having to return an ambiguous result to the SQL client. With the above race, it could erroneously return a transaction aborted error even though the transaction actually committed.

To address this race, we will introduce a distinction between committing `EndTransactionRequest`s issued by the status resolution process and those issued by `TxnCoordSender`. The transaction record will only be deleted after the `TxnCoordSender` has sent its commit or through the GC queue.

#### Examples

Assume that the transaction promised writes at `k1`, `k2`, and `k3` and is anchored at `k1`. `t1` is its provisional commit timestamp when the record is discovered.

##### Missing intent (at k2)

```
QueryIntent(k1, t1)      found|Abort(k1, t1) ok GC(k1) ok
QueryIntent(k2, t1) prevented |
QueryIntent(k3, t1)  found    |
```

##### Missing intent racing with restart

An intent at t1 is prevented, but before the transaction record can be aborted, the transaction restarts and manages to (implicitly) commit. As the abort fails, the resolution process observes a transaction record with new activity and waits, observing the commit shortly thereafter. Note that the transaction would be heartbeat throughout this process, so that status resolution would not be kicked off in the first place under normal conditions.

```
QueryIntent(k1, t1)  found  | Write(k1, t2)          ok     |
QueryIntent(k2, t1)prevented|{Write,Stage}(k2, t2) ok|      |Commit(k2) ok GC(k2) ok
QueryIntent(k3, t1)  found  | Write(k3, t2)          |    ok|
                              |Abort(k1, t1)           |fail  |Wait       ok
                                                                 ↑
                                            sees either no txn record or committed
                                            one; either way, not `STAGED` any more
```

In the other scenario, the abort would have beaten the `Stage(k2)` and the transaction would be aborted.

##### Multiple status resolutions racing

Status resolution only writes a single key (the transaction record) and does so conditionally, and intents are only resolved after this conditional write. As a result, having multiple status resolutions racing does not cause anomalies.

In the history below, two status resolutions race so that the second `QueryIntent` sees the commit version (and concludes that it has prevented the write). As it tries to update the transaction record, it fails and realizes that the transaction is now committed.

```
QueryIntent(k1, t1) found Commit(k1, t1) Resolve(k1)ok|
QueryIntent'(k1,t1)                                   |prevented Abort(k1, t1) fail
```

### Performance Implications

There is an extra write to the WAL due to the newly introduced intermediate transaction status and as a result, throughput may decrease (which is bad) while latencies decrease (which is good). In the long term, we may be able to piggy-back the committing `EndTransaction` on other Raft traffic to recuperate that loss. See #22349 which discusses similar ideas in the context of intent resolution.

### No-Op Writes

The set of **promised writes** are writes that *need to leave an intent*. This is in contrast to the remaining *intent spans*, which only need to be a *superset* of the actually written intents. Today, this is true: any successful "write" command leaves an intent.

But, it is a restriction for the future: We must never issue [no-op writes](https://github.com/cockroachdb/cockroach/issues/23942) in the final batch of a transaction, and appropriate care must be taken to preserve this invariant. As of https://github.com/cockroachdb/cockroach/commit/9d7c35f, we assert against these no-op writes for successful point writes. Even with this added level of protection, we'll need to remember never to issue no-op writes as promised writes.

### Error handling

Today, a client transaction could issue a `ConditionalPut` in the final batch, receive a `ConditionFailedError`, and might decide to try a different write after. This becomes strictly illegal with parallel commits as it allows for races that cause anomalies. Once a `STAGED` transaction record is sent, the *promised writes* (for that epoch) must not change. A client must restart or roll back the transaction following any error (but note that the client could use a read instead of the conditional put in the first place, which is slightly less performant).

Care must be taken to guard against this. For example, SQL `UPSERT` handling might be susceptible to such bugs.

### Improving Ambiguous Results

At the `DistSender`-level, we may see more ambiguous commits. An ambiguous commit occurs when a commit operation (which includes an *implicit commit*) was sent but it is not known whether it was processed or not. Since we send more writes in parallel with the commit after this change, the chance of such errors increases. We can make use of the `STAGED` status to improve on this as necessary. After an ambiguous operation, we run the status resolution process (skipping the transaction record lookup; we may not have written our staged record but we know the promised writes and provisional commit timestamp anyway). If we manage to prevent a write, we may return to the client with a transaction retry error instead. Otherwise, if there is a transaction record, we can also run the process hoping for a positive outcome.

Note that we may not just try to abort our own transaction record. The record may have become *implicitly committed* and then *explicitly committed* by a status resolution process, and then garbage collected. (And even if the record weren't garbage collected, the status resolution process may have proven that the record was committed but then would find it aborted, which is a confusion we'd be wise to avoid).

### Metrics

Metrics will be added for the number of status resolution attempts as well as the number of final batches eligible/not eligible for parallel commit. If the last metric is substantial, we need to lift some of the conditions around admissibility.

### Interplay with #26599 (transactional write pipelining)

After #26599, when the transaction tries to commit, there may be outstanding in-flight RPCs from earlier batches. Instead of waiting for those to complete, the `client.Txn` will pass promised writes along with the final batch, which DistSender will include in the staged transaction record and also send out as `QueryIntent`s (which will be treated as writes, i.e. they may end up being Raft no-ops or batched up with another request to a range -- that is OK, but needs to be tested). There are a few possible outcomes:

- in the likely case, none of the intents will be prevented (and they are found at the right timestamps), as they have been dispatched earlier; the `QueryIntent`s succeed, as does the rest of the batch, and `DistSender` announces success to the client.
- an intent is prevented or found at the wrong timestamp. This is treated like a write having failed or having pushed the transaction, respectively.

Care will need to be paid to splitting batches with `EndTransaction(status=STAGING)` requests from `QueryIntent` requests for pipelined writes to the same range as the txn record. If care is not taken, the `EndTransaction` request will be blocked by the `spanlatch.Manager` with the rest of its batch and no speedup will be observed by the client. In fact, in conjunction with the [Replica-level detection](#replica), this will behave almost identically to how the case would behave today.

This will require that we allow multiple batches to the same range be sent in parallel.

## Drawbacks

- The complexity introduced here is nontrivial; it's especially worrying to share the knowledge of whether a transaction is committed between `DistSender` and `EndTransaction`. The system becomes much more prone to transactional anomalies when unexpected commits/aborts are sent, or when clients retry inappropriately. New invariants are added and need to be protected.
- The extra WAL write will likely show up in some benchmarks. Avoiding it adds extra complexity, but this may be required when this mechanism is first introduced.

## Rationale and Alternatives

There does not appear to be a viable alternative to this design. The impact of not doing this is to accept that commit latencies remain at roughly double what they could be.

### Extended proposal

There is an extended form of this proposal which allows the intents to be resolved in parallel with the commit (as opposed to after it). This alternative was presently considered as out of scope, as it requires transaction IDs to be embedded into all committed versions (this is required for the status resolution process). Doing so requires a significant engineering effort, but few of the details in this proposal change.

## Unresolved questions

No fundamental questions presently.
