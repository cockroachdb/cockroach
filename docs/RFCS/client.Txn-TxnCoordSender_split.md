- Feature Name: Eliminating the `client.Txn` ↔ `TxnCoordSender` split
- Status: draft/in-progress/completed/rejected/obsolete
- Start Date: 2017-05-17
- Authors: Andrei Matei (andreimatei1@gmail.com)
- RFC PR: #16000
- Cockroach Issue: #13376, #10511


# Summary

The current split between the `client.Txn` and the `TxnCoordSender` doesn’t make
sense any more. The current distinction between them is historical and has
already led to hacks around the `Sender` interface through which the layers
currently communicate. Also, the current split stands in the way of DistSQL,
which needs access to some of the `TxnCoordSender` functionality but doesn’t
actually send requests through it.

This RFC is about agreeing that the current split is useless and inconvenient,
and that the functionality of `client.Txn` and `TxnCoordSender` belongs in a
single unified layer. In doing so, we’ll be recognizing the fact that the
*client* is and will be collocated with the *transaction coordinator*.

# Background and motivation

KV requests go through a hierarchy of `Sender`s, on their way from a client
(i.e. code in the `internal/client` package, notably `client.DB` (a handle to a
cluster) and `client.Txn` (a KV-level transaction) and to the `replica` that
will evaluate the request, propose Raft commands and then apply the commands.
This hierarchy is supposed to implement a separation of concerns. Each member of
the hierarchy is a
[`client.Sender`](https://github.com/cockroachdb/cockroach/blob/659d611/pkg/internal/client/sender.go#L26).
The hierarchy currently is: `TxnCoordSender`→ `DistSender` → `Stores` → `Store`
→ `Replica`.

For transactional requests, above the `TxnCoordSender` sits the `client.Txn`
object. This `Txn` sends `BatchRequest`s into the `TxnCoordSender` (using the
`client.Sender` interface).

The `client.Txn` object maintains state about the underlying KV distributed
transaction (i.e. it contains a `roachpb.Transaction` proto which accumulates
updates as requests go through and responses come from the server).
Historically, before there was SQL, what’s now the `internal/client` package was
meant to be distributed as a library to users to embed in their apps. That is no
longer the case (except for some tools and acceptance tests that use the
`ExternalClient`; see later).

The `TxnCoordSender` object is supposed to act as a man-in-the-middle between a
`client.Txn` and the lower-level `DistSender` and coordinate transaction state.
It performs three main functions:

1. It heartbeats the *transaction record* of each transaction while it’s in
   progress, thus keeping the transaction alive.
2. It keeps track of written key ranges throughout the life of the transaction
   and, when the client commits or rolls back the transaction, it attaches these
   *intent ranges* to the `EndTransaction` request so that the server can clean
   them up.
3. It exports stats and metrics about in-progress transactions.

The `TxnCoordSender` generally has more code that deals with a transaction
throughout its lifecycle than the `client.Txn` does; the `TxnCoordSender` was
always supposed to run on the server, under Cockroach’s control, whereas updates
to client libraries need to be made across languages and are harder to roll-out.

There’s a number of problems with the current separation; I’ve broken them down
into existing warts and new DistSQL-specific issues.

**Existing problems:**

1. The `Context` used by the heartbeat-loop goroutine is bogus. Logically, the
   heartbeating operation needs to be tied to the lifetime of the KV
   transaction. However, since the `Sender` interface doesn’t permit us to pass
   any “transaction scope” to the `TxnCoordSender`, we arbitrarily use the
   context that has been passed by the higher level for the first write
   operation as the “transaction’s ctx”. At the moment, I believe that happens
   to work for the SQL layer’s use of the `TxnCoordSender`, as all queries in a
   SQL transaction share a context, but we’ll probably need separate contexts in
   the future when we’ll want to support cancelling individual queries.
   [cockroachdb/cockroach#10511](https://github.com/cockroachdb/cockroach/issues/10511)
2. There’s two copies of the `roachpb.Transaction` proto to worry about while a
   transaction is in progress - `client.Txn` embeds a proto and the
   `TxnCoordSender` has a copy in its internal map. We cross-update between the
   two copies, although probably in incomplete ways (there doesn’t seem to be an
   update on a request’s way to the `DistSender`, although I guess there should
   be). In any case, the code dealing with these cross-updates is quite
   inscrutable, since it involves communication between the layers through the
   structures that flow trough the `Sender` interface -
   `BatchRequest/BatchResponse/pErr` - as opposed to more direct proto updates.
3. The `client.Txn` and the `TxnCoordSender` can only communicate through the
   `Sender` interface, which means that errors passed up from `TxnCoordSender`
   need to be `pErr`s. For this reason, we need to define errors created by the
   `TxnCoordSender` (e.g. the `HandledRetryableTxnError`) as `ErrorDetails`.
   This is inconvenient and should be unnecessary, as the errors don’t need to
   travel over any wires.
4. Because the `TxnCoordSender` is in charge of heartbeating the transaction
   record and it has no good way of informing anybody else about heartbeating
   failures, higher layers only find out about these failures when they attempt
   to send the next request (they should find out at the end of the current
   in-flight request, if any, except that we currently have a bug where we don't
   check this status on a request's return path). This is not ideal; we should
   be able to proactively cancel an in-flight query and inform the SQL client
   that the SQL-level transaction needs to be restarted.

**DistSQL-specific problems:**
The DistSQL issues stem from the fact that KV requests performed by DistSQL
queries don’t go through the `TxnCoordSender` on the gateway (they also don’t go
through `TxnCoordSenders` on other nodes, but that’s less relevant to this
discussion).

More specifically, we have the following issues:


1. The `TxnCoordSender` currently doesn’t allow for reads to be distributed
   through DistSQL if the transaction has already performed any writes.
   Currently, attempting a read request in a writing transaction through DistSQL
   on a node other than the gateway results in the famous error: `writing
   transaction timed out or ran on multiple coordinators`. That’s why currently
   DistSQL simply refuses to execute any queries in writing transactions, and we
   fall back to non-DistSQL execution.  
  [cockroachdb/cockroach#13376](https://github.com/cockroachdb/cockroach/issues/13376)  
  That error is produced by an assertion in the `TxnCoordSender` that says that
  the `TxnCoordSender` must have state for a writable transaction. There’s a
  point to this assertion - the results of reads can only be returned to a
  client only if the data has been read while the transaction record was being
  heartbeated. Otherwise, it’d be possible for the read to have missed previous
  writes performed in the same transaction: if the txn record was not
  heartbeated for long enough for whatever reason (say, a heartbeat attempt
  fails), then the transaction might have been considered aborted and its
  intents might have been cleaned up.  
  This check is good, and should stay somewhere. However, currently it has to be
  in the `TxnCoordSender`, since the `TxnCoordSender` alone knows when a
  transaction has been abandoned, but being in the `TxnCoordSender` means that
  `DistSQL` reads don’t have an easy way to do the check.

2. Duplicate restart logic: some functionality from the `TxnCoordSender` already
   needed to be duplicated. On retryable errors, the `TxnCoordSender` prepares
   the `Transaction` proto for the next try of the higher level. For retryable
   errors received by DistSQL, because there’s no `TxnCoordSender` involved, the
   logic for preparing the proto was duplicated.
3. Stats: the `TxnCoordSender` exports stats and metrics about in-progress and
   transactions. Because it doesn’t see DistSQL transactions, these stats are
   probably incomplete.
4. Intents: the `TxnCoordSender` accumulates intent ranges for all write
   requests that go through it. It’s unclear if and how DistSQL will perform
   writes, but if that happens, the fact that intents are hidden in the
   `TxnCoordSender`'s state will surely get in the way; DistSQL will either want
   to be able to directly insert intents in the structure that tracks them, or
   it will want to completely take over the tracking of intents.
5. Hacky sanity-check interface: DistSQL reads currently sometimes pierce the
   veil of the `Sender` interface and peek into the `TxnCoordSender`'s state to
   assert it’s copacetic. Because of current package dependency ordering, this
   is done through an ugly `SenderWithDistSQLBackdoor` interface.



# Proposal

The proposal is to logically merge the `client.Txn` and the `TxnCoordSender`. I
don’t know concretely what the result will look and how the code from the
current `TxnCoordSender` will be distributed, but the effect will be that there
will no longer be a `Sender` interface bottleneck in communicating between the
current two layers, there will no longer be two copies of the `Transaction`
proto (instead there will be a single, authoritative, one) and the heartbeating
of the transaction record will be correctly tied to the transaction lifespan.

The imagined steps are something like:
1. Move the `TxnCoordSender` wholesale from the `kv` package into the `client`
   package and rename it to `TxnCoordinator`.
1. Stop using the `client.Sender` interface to communicate between the
   `client.Txn` and the `TxnCoordinator`.
1. Promote the `TxnMetrics` object from a member of the `TxnCoordSender`
   singleton to be its own higher-level singleton that `client.Txn` has access
   to directly for counting aborts/commits etc. Delete the
   [`TxnCoordSender.printStatsLoop()`](https://github.com/cockroachdb/cockroach/blob/fdf4b71/pkg/kv/txn_coord_sender.go#L242)
   method. It periodically prints V(1) log messages like `log.Infof(ctx, "txn
   coordinator: %.2f txn/sec, %.2f/%.2f/%.2f/%.2f %%cmmt/cmmt1pc/abrt/abnd, "+
   "%s/%s/%s avg/σ/max duration, %.1f/%.1f/%d avg/σ/max restarts (%d samples
   over %s)"); I don't think they're useful. Getting rid of this method saves
   us from having to find a new home for it.
1. Move heartbeating from the `TxnCoordinator` to the `client.Txn`. Each `Txn`
   will spawn a new goroutine to do its heartbeating (there's already a
   goroutine per transaction). With heartbeating out of the `TxnCoordinator`,
   we can hopefully now get rid of the `TxnCoordinator`'s internal map of
   `roachpb.Transaction` protos.
1. Absorb all the `TxnCoordinator.Send()` code into `client.Txn.Send()` and
   delete `TxnCoordinator`.


# Unresolved questions


1. What do we do about the present cases where the *client* is actually not
   collocated with the `TxnCoordSender`? Namely, there is an `ExternalClient`
   RPC interface that can be used to connect a `client.Txn` to a remote
   `TxnCoordSender` via RPCs. This used by some debug CLI commands and by
   implementers of the
   [Cluster](https://github.com/cockroachdb/cockroach/blob/f7315c0/pkg/acceptance/cluster/cluster.go#L36)
   interface (used in some acceptance tests). All these uses seem to be vestiges
   of another age and another mindset.
