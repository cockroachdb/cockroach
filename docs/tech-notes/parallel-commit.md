# Parallel Commits

Consensus latencies are the price CockroachDB pays for its consistency guarantees; they are not usually incurred on monolithic or NoSQL databases, and can be used as an argument against the use of CockroachDB.

As a database that prides itself on geo-distributed use cases, we must strive to reduce the latency incurred by common OLTP transactions to near the theoretical minimum: the sum of all read latencies plus one consensus latency.

This is far from true at the time of writing: chatty SQL transactions eat consensus latency on every write (unless they can use the `RETURNING NOTHING` crutch) due to the naive underlying implementation. Luckily, work to change this is underway: [#16026] proposes a mechanism which lets SQL transactions incur only a single write latency by (morally) deferring all writes to commit time.

Unfortunately, writing at commit time in itself is suboptimal since the KV layer cannot send writes in parallel with a commit operation, and splits them back up into two rounds of consensus. In effect, transaction latencies for virtually any real schema after [#16026] are still double what they could be.

This tech note outlines a solution that avoids this problem, works well for reasonably sized (i.e. small) OLTP transactions and naturally falls back to today's suboptimal behavior for transactions with a large write set.

In short, it allows `DistSender` to send `EndTransaction` in parallel with the remainder of the final batch, by putting the transaction in a newly introduced `STAGED` status, and returning immediately to the client after. As a crucial performance optimization, an asynchronous second `EndTransaction` then flips the transaction record from `STAGED` to `COMMITTED` and resolves the intents.

This document contains two proposals, basic and extended. Both remove a round of consensus from the client latency, but the basic proposal can only commence intent resolution after marking the transaction record as `COMMITTED`, which adds up to the same latency as today. Speedy intent removal is important to reduce wait times under contention, and the extended proposal improves on this by commencing intent resolution at the very moment the `STAGED` record and the associated intents have been written; this is the very moment at which the transaction is proven committed and is thus optimal.

The price to pay for this extension is having to embed a transaction ID into every versioned value. This is an invasive but well-contained change. While there's no reason to do this in the first pass, there are various upsides to embedding the transaction ID for other purposes as well. The sibling tech note on [Node Transaction IDs](node-txn-ids.md) explores this circle of ideas, and in particular proposes a transaction ID scheme that avoids overhead for the per-version ID storage.

Credit for inspiring much of the write-up goes to @andyk.

## The problem

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

## A solution

The solution is being able to send everything in parallel, and the problem we have to solve is that of lost writes. To address these, we have to change the semantics of what it means for a transaction to commit. As it stands, they are

> A transaction is committed if and only if there is a transaction record with status COMMITTED.

The proposed change (where `STAGED` is a new transaction status) is:

> A transaction is committed if and only if there is a transaction record with status COMMITTED, or if has has status STAGED and all of the writes of that transaction are present (either in intent or resolved form).

The idea is that the txn coordinator sends the commit in parallel with status `STAGED` and then, after returning success to the client, updates the status to `COMMITTED`. This means that in the common case, transactions are not encountered in `STAGED` state.

The basic and extended proposal differ in their handling of intents. The basic proposal allows intent resolution only when the transaction record is `COMMITTED`. This in turn guarantees that when trying to recover the true transaction status from a `STAGED` transaction, a client may assume that a missing intent (which won't be writable in the future) implies that the transaction is aborted. In the extended proposal, intent resolution begins as soon as `DistSender` knows the transaction has commited (i.e. after having received successful responses from all requests forming the last batch). In this case, when checking intended writes, they may already be committed and must be linkable to the transaction which wrote them.

The write-up from now on implicitly assumes the extended proposal unless noted otherwise.

A condition is no good unless we can check it, so if a transaction is encountered in `STAGED` state, we must solve the following problem:

> Given any unresolved write (intent or txn record) of a given txn in status `STAGED`, a KV client can a) discover the full set of *intended* txn writes (including the txn record) and b) determine whether they are all present.

With this in hand, anyone wondering whether a given transaction has committed can discover all the writes and, in particular, can *abort* the transaction by preventing one of them.

To address a), I propose the following:

1. each intent references the transaction record (i.e. ID and base key)
1. the transaction record in status `STAGED` contains the full set of point writes of the batch which wrote the transaction record.
1. each committed value contains the txn ID (*only in the extended proposal*)

Note that the first item is already true today. The second one is morally more than true; today, committed transaction records already contain *all* of the writes of the transaction for GC purposes. We augment the transaction record so that it contains the intents picked up in the last batch separately as we know that all previous writes are safe and thus don't need to be checked again.

The remaining problem with 2) is that transactions may contain "range intents". These are a result of either ranged writes (`DeleteRange`) or the condensing of spans (for large transactions). For a ranged intent, it's impossible to determine whether all of the writes are present.

`DeleteRange` is used in point deletions by the SQL layer in common OLTP workloads; we can change it to return the affected keys (if there aren't too many; or perhaps SQL can stop using `DeleteRange`). If there are too many or if span condensing occurred, this is a large transaction and we simply use range intents and fall back to today's behavior of deferring `EndTransaction`; see the corresponding fallback section below.

The third item is new and is necessary in the extended proposal, in which it is legal to resolve intents with a `STAGED` transaction record (after proving that it is in fact `COMMITTED`, but before marking it as such), as demonstrated by the following scenario.

1. find `STAGED` transaction record; want to know if it is in fact committed
1. within it, grab the list of writes carried out in parallel with the staging
1. check one of the intended write's keys; it's not an intent but has the right timestamp
1. was it written by the transaction? Not clear without embedded transaction ID.

This motivates a parallel tech note about [changing the format of our transaction IDs](node-txn-ids.md).

Note that in the basic proposal, if a write is encountered at the right timestamp but is not an intent, then either the transaction is in fact aborted, or someone has raced us and marked the txn committed and resolved the intent. Thus, we have to check the transaction record again; we may now find it is committed (and otherwise, we `ABORT` it).

The examples below illustrate common scenarios. Time flows from left to right.

### Happy Case (basic proposal)

The transaction writes its three writes and staged txn record in parallel. When all operations come back as success, the client receives the responses, and an asynchronous request marks the transaction record as committed, saving others the expensive manual verification of this fact.

```
Write(k1)   ok                    Resolve(k1,k2,k3) ok
Stage(k1)  ok      Commit(k1)   ok
Write(k2)        ok
```

### Happy Case (extended proposal)

Like the last case, but the intents are resolved before waiting for `Commit(k1)`.

```
Write(k1)   ok     Resolve(k1,k2,k3) ok
Stage(k1)  ok      Commit(k1) ok
Write(k2)        ok
```

### Unhappy Case 1 : failed write

A write fails; the transaction coordinator aborts the transaction.

```
Write(k1) ok                        Abort(k1)
Stage(k1)   ok
Write(k2)       non-retryable-error
Write(k3)    ok
```

The history with a retryable error is similar except that the write may be retried.

### Unhappy Case 2: coordinator dies without txn record

```
Write(k1)    ok
Stage(k1)      crash
Write(k2)    ok
Write(k3)  ok
conflicting client:
Read(k3)            discover-txn Abort(k1) Resolve(k1,k2,k3)
```

### Unhappy Case 3: coordinator dies with all intended writes staged

```
Write(k1)    ok
Stage(k1)    ok
Write(k2)       ok crash
Write(k3)  ok
conflicting client:
Read(k3)           discover-txn Check(k1,k2,k3) Commit(k1) Resolve(k1,k2,k3)
```

### Unhappy Case 4: coordinator dies with txn record, but missing intended write

```
Write(k1) ok
Stage(k1) ok
Write(k2)  ok crash
Write(k3)     fail
conflicting client:
Read(k1)           discover-txn Check(k1,k2,k3) Abort(k1) Resolve(k1,k2,k3)
```

## Pushing a transaction

Pushing follows much of the same logic except in the case in which we find a `STAGED` transaction entry. In that case, we must perform the discovery step as outlined above. When the true result is known, we send a consensus write that updates the status and resolves intents as appropriate. With Node Transaction IDs, we first contact the coordinator which keeps an LRU cache of committed/aborted transaction IDs, saving the costly discovery step in the common case.

## Aborting a transaction

The canonical way of aborting a transaction remains writing the transaction record, which is possible unless it has state `COMMITTED`. Otherwise, it's possible that the transaction managed to write that txn record, but missed one of its intended writes, which we then must discover and prevent; for this we introduce a read-write command `CheckIntent` which checks the intent or versioned key at the commit timestamp (if any). If the write was not found and the reader intends to abort, the command populates the timestamp cache and returns with an error (to short-circuit fanout of the remainder of the batch).

## Transaction record GC

Nothing changes except that when encountering a `STAGED` transaction, we must perform discovery and either update the state to `COMMITTED` or `ABORTED`, depending on the outcome.

## Intent Resolution

Currently, intents are resolved as a side effect of an `EndTransaction` which changes the status to either `COMMITTED` or `ABORTED`. This remains the same in the basic proposal; for the extended proposal, we can move the intent resolution to `DistSender` (which is first to learn the transaction's true state). In turn, to save redundant work, the mechanism which synchronously resolves intents colocated with the transaction record on `COMMIT` needs to be removed or the corresponding intents be omitted in `DistSender`.

As an alternative, we could make intent resolution a side effect of *proposing* a successful commit (since the existence of the proposal alone proves the commit). This can be slightly less advantageous compared to doing it from `DistSender`. Since there's one more hop.

## 1PC Transactions

No change here, but the new code in `DistSender` needs to make sure to maintain the current behavior.

## Fallback Behavior

When a transaction employs ranged intents in its final batch, we can't discover the full write set and thus can't use the new behavior proposed here. Luckily, the fallback is straightforward: we promise not to write ranged intents in the last batch, i.e. such a transaction must commit via the extra latency, and correspondingly the set to check on a `STAGED` record contains no ranged intents (with the obvious implementation, it is in fact empty).

The rule is easy to enforce at the DistSender level. We employ today's code for delaying `EndTransaction` if the intended write set contains a ranged intent.

[#16026]: https://github.com/cockroachdb/cockroach/issues/16026
