# Parallel Commits

As a database that has to deal with consensus latencies and prides itself on geo-distributed use cases, we have to become optimal in how much latency occurs. We are suboptimal in one extremely common case and must address this.

This tech note outlines a solution that should work well for reasonably sized (i.e. small) OLTP transactions and naturally falls back to today's suboptimal behavior for transactions with a large write set.

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

The basic solution, of course, is to send everything in parallel, and the problem we have to solve is that of lost writes. To address these, we have to change the semantics of what it means for a transaction to commit. As it stands, they are

> A transaction is committed if and only if there is a transaction record with status COMMITTED.

We need to change this to something like this (where `STAGED` is a new transaction status):

> A transaction is committed if and only if there is a transaction record with status COMMITTED, or if has has status STAGED and all of the writes of that transaction are present (either in intent or resolved form).

The idea is that the txn coordinator sends the commit in parallel with status `STAGED` and then, after returning success to the client, updates the status to `COMMITTED`. This means that in the common case, transactions are not encountered in `STAGED` state.

A condition is no good unless we can check it, so we must solve the following problem:

> Given any unresolved write (intent or txn record) of a given txn in status `STAGED`, a KV client can a) discover the full set of *intended* txn writes (including the txn record) and b) determine whether they are all present.

With this in hand, anyone wondering whether a given transaction has committed can discover all the writes and, in particular, can *abort* the transaction by preventing one of them.

To address a), I propose the following:

1. each intent references the transaction record (i.e. ID and base key)
1. the transaction record, when in status STAGED (or COMMITTED, for GC reasons) contains the full set of point writes of the txn
1. each committed value contains the txn ID

Note that the first item is already true today. The second one is almost true, with the exception that transactions may contain "range intents". These are a result of either ranged writes (`DeleteRange`) or the condensing of spans (for large transactions). `DeleteRange` is used in point deletions by the SQL layer in common OLTP workloads; we can change it to return the affected keys (if there aren't too many). If there are too many or if span condensing occurred, this is a large transaction and we simply use range intents and fall back to today's behavior of deferring `EndTransaction`; see the corresponding fallback section below.

The third item is new and is necessary, as the following scenario demonstrates.

1. find transaction record
1. find an intended write within
1. check the corresponding key; it's not an intent but has the right timestamp
1. was it written by the transaction? Not clear.

It motivates a later section about switching the format of our transaction IDs.

## Pushing a transaction

NB: this section changes with the later section about communicating directly with the coordinator. This section deals with the case in which the coordinator is not reachable.

Pushing follows much of the same logic except in the case in which we find a `STAGED` transaction entry. In this case, we must perform the discovery step as outlined above. When the true result is known, we send a consensus write that updates the status and resolves intents as appropriate.

## Aborting a transaction

The canonical way of aborting a transaction remains writing the transaction record, which is possible unless it has state `COMMITTED`. Otherwise, it's possible that the transaction managed to write that txn record, but died while trying to write one of its writes, which we then must discover and prevent; this in turn is easily done by populating the timestamp cache for that key to a logical tick above the commit timestamp or, even easier, just reading it via a specialized ranged read command.

The transaction record may contain lot of point intents, so this case in which we have to check all intented writes can be expensive. We can optimize it by separating out the intents that were written in parallel with the commit record; having done that, the check is always cheaper than the last write batch of the transaction (due to not requiring consensus), and it can simply be passed to `DistSender`. This can be deferred.

## Transaction record GC

Nothing changes except that when encountering a `STAGED` transaction, we must perform discovery and either update the state to `COMMITTED` or `ABORTED`, depending on the outcome.

## Intent Resolution

Currently, intents are resolved as a side effect of an `EndTransaction` which changes the status to either `COMMITTED` or `ABORTED`. While this is still possible after the change, it's less optimal as this is now one round of consensus later than previously. Instead, `DistSender`, upon having received all of its writes back (including the `STAGED` commit record), should send the intent resolution on its way in parallel with `COMMIT`. This also obviates a current optimization in which the intents local to the txn record are removed synchronously with the commit.

As an alternative, we could make intent resolution a side effect of *proposing* a successful commit (since the existence of the proposal alone proves the commit). This can be slightly less advantageous compared to doing it from `DistSender`.

## Fallback behavior

When a transaction employs ranged intents, we can't discover the full write set and thus can't use the new behavior proposed here. Luckily, the fallback is straightforward: If any of the writes is ranged, we never have to discover them unless there is a `COMMITTED` txn record. If there is one, it contains the ranged intended write. We add the following guarantee:

A txn record which is written as `COMMITTED` and contains a ranged write must only come into existence *after* all the ranged writes within have been carried out.

Thanks to this rule, ranged writes can simply be ignored when discovering the write set: the creator of the txn record guarantees having waited for them to go through already, so they can be assumed existing.

The rule is easy to enforce at the DistSender level. We employ today's code for delaying `EndTransaction` if the intended write set contains a ranged intent. This is slightly suboptimal in the case in which the ranged intents belong to earlier batches (and are thus already written); we could separate out the "past" and "current" write sets to allow `DistSender` to do the optimal thing (I would defer that optimization, though).

## Compact transaction IDs

Today, we use UUIDs as transaction IDs, but this wastes a lot of potential space savings which are more relevant once every single versioned key contains the txn ID.

We can switch to transaction IDs which are composed of

1. the NodeID
1. the transaction's original timestamp (which is unique per node even across restarts, thanks to our HLC guarantees and sleeping out the MaxOffset)

This allows for smaller versioned keys in the common case in which the transaction commits at its original timestamp, in which case the timestamp is omitted from the TxnID stored in the version and all we store is the NodeID.
When the transaction *was* pushed, we additionally store the delta to the base timestamp (i.e. if the value is at 1000 but the transaction was originally 200, we store a delta of 800).

## Communicating with the txn coordinator directly

Having the `NodeID` in the transaction ID unlocks an alternative mechanism to handling transactional conflicts by contacting the coordinator directly.

For example, when finding a conflicting intent, the txn can send a (streaming) RPC to the remote coordinator, compare priorities, and either wait for the coordinator to signal completion or prompt it to abort the transaction.

This will often be faster (depending on latencies between the involved nodes), thanks to the absence of consensus and polling in this path, and could replace the txn wait queue, though deadlock detection needs to be taken care of.

When the coordinator isn't reachable within a heartbeat timeout (or however long we're willing to wait), abort the transaction the hard way.

### Phasing out HeartbeatTxn

Now that a coordinator is directly reachable, we can consider not sending HeartbeatTxn. The only reason for keeping it is that during exotic network partitions, some nodes may find their transactions aborted by other nodes unable to contact them.

