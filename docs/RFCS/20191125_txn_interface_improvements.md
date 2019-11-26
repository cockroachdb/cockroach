- Feature Name: client.Txn interface v2
- Status: draft
- Start Date: 2019-11-15
- Authors: Andrei
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#25329](https://github.com/cockroachdb/cockroach/issues/25329) [#22615](https://github.com/cockroachdb/cockroach/issues/22615)

# Summary

This RFC discusses additions to the client.Txn interface that make it
thread-safe and otherwise safer. A new notion of a "transaction handle" is
introduced, allowing a sort of fork-join usage model for concurrent actors that
want to use a shared transaction. The client is put in charge of explicitly
restarting a transaction after retriable errors, instead of the current model
where the client.Txn restart automatically behind the scenes. Moving this
responsibility to the client makes it possible for the client to synchronize the
concurrent actors before restarting.

# Motivation

The transaction (`client.Txn`) is the interface through which SQL and other
modules interact with the KV module. It is a foundational interface in our
codebase and, as such, it deserves to be clean, safe, and powerful. At the
moment, there’s two big problems with it:
1) It generally doesn’t support concurrent use. Performing concurrent operations
(in particular, sending a BatchRequest while another one is in flight) results
in an error. Historically, this didn't result in an error, but instead in data
corruption. This restriction has surprised people repeatedly (we even had two
tests dedicated to asserting that concurrent requests work; despite the
appearances, they did not work).
There's also good reasons for wanting to perform concurrent (parallel)
operations - for example validating foreign keys in the background.
2) It requires the user to synchronize its state with the txn state and, the
minute it fails to do so and the state get out of sync, tragedy follows (making
it unsafe). Ignoring a retriable error (because, for example, you want to ignore
any other error) is currently illegal. When the client.Txn returns a retriable
error to the client, it also restarts the transaction transparently. Failure by
the client to communicate this restart to a retry loop results in committing
half a transaction. This also was a cause of surprise for people.

These two issues stem from the same fact: transactions react internally to
retriable errors assuming that every request coming afterwards will pertain to
the next iteration of the transaction. This implies no concurrent use because,
if there was concurrent use, there'd be no way to distinguish requests intented
for the previous epoch versus requests intended for the new one).  
In addition, our read span refreshing code does not currently handle
concurrency: if a requests causes a refresh, any other request straddling that
refresh is not properly refreshed. Consider the following scenario:
* an actor sends a request *r*
* while the request is in flight, another request *r1* is sent and returns,
  requesting a refresh
* the `TxnCoordSender` successfully refreshes all the requests prior to *r* and
  *r1* (note that r*'s read spans have not yet been recorded) and bumps the
  transaction's read timestamp
* *r* returns. Its result cannot simply be returned to the client; it needs to
  be refreshed or retried at the bumped timestamp. The code is not currently
  equipped to do so.
  
There's a particular case where we do allow concurrent use of a txn: DistSQL
flows that can't be completely "fused" use a `LeafTxn`. Leaves allow concurrent
use by inhibiting transparent restarts and refreshes. Disabling refreshes is a
pity. In particular when the flow running entirely on the gateway, we'd like to
be able to refresh in the middle of the flow. (When the flow is distributed,
refreshing is trickier; see
[#24798](https://github.com/cockroachdb/cockroach/issues/24798). The changes
proposed here still move us in the right direction for supporting refreshes in
the middle of distributed queries too).

# Guide-level explanation

We're proposing an interface that supports concurrent use and is safe. We’ll
become safe by putting the client in charge of restarting transactions. We’ll
allow for concurrent use by not allowing the client to restart transactions
while it’s also performing requests that belong to the previous transaction
attempt.

The idea is to split the sets of operations that a transaction can perform in
two: operations that can be performed concurrently and operations that assume
exclusive control over the txn. The latter operations will continue to use
the `client.Txn` struct. The former will start using a new interface:
`client.TxnHandle`. The `client.Txn` will be coded defensively
such that most illegal uses are caught. Each handle is single-threaded, but
there can be multiple handles at the same time. The `client.Txn` keeps track of
how many extant handles there are at any time, and will refuse to do dangerous
things while there’s other handles out there. For backwards compatibility and
also simplicity of non-concurrent use cases, the `client.Txn` will also contain
the `client.TxnHandle` interface - so if you have a `client.Txn`, you can do
everything on it (single-threadedly).

The interfaces will look like this:

```go
client.TxnHandle:
	Get(), Scan(), Put(), Delete(), etc.
	Fork() -> returns client.TxnClosableHandle

client.Txn:
	embeds client.TxnHandleImpl
	Commit(), Rollback(), Step(), GetSavepoint(), RollbackToSavepoint()
	Restart()

client.TxnClosableHandle:
	embeds client.TxnHandle
	Close()
```

The Txn will keep track of the number of extant handles - created via `Fork()`
but not yet `Close()d`. While there’s any, calling methods specific to the `Txn`
will error. This means that the client needs to synchronize (i.e. `Close()`) all
the handles before `Restart()`ing. Implementation-wise, all these interfaces are
wrappers on top of a shared `TxnCoordSender`. Transactional state and timestamp
management would be shared by all handles.

The point of the `TxnClosableHandle` is to make it so that there’s no `Txn.Close()`,
but there is a `Txn.Fork().Close()`.

# Reference-level explanation

## Detailed design

The `client.Txn` would keep track of the number of open handles. Otherwise, all
the handles (and the `client.Txn` which is itself a handle) share all the state
(in particular the `TxnCoordSender`). The `TxnCoordSender` already has internal
locking for allowing some concurrent requests. This support will be extended, in
particular in the refresher interceptor as we'll see.

### Refreshes

A request straddling a refresh (i.e. request is sent, then the txn is refreshed,
then the request finishes and returns a result) is problematic because the
response cannot simply be returned to the client; the response would also need
to be refreshed at some point before the results of the request that triggered
the refresh are returned.

We have the following options for mixing refreshes and concurrent requests: 

1. When a response asks for a refresh, we could block that refresh until all the
in-flight requests finish (note that the TxnCoordSender knows all the requests
that are in flight). Once they all finish (and once their read spans are also
recorded and their response is returned to the client), then we can refresh.
We’d also block new requests from starting. The problem here is that, if one of
these concurrent requests causes the refresh to fail, it’s too to retry that
request at the new read timestamp (since the result was already delivered to the
client). This seems suboptimal, but it’s also analogous the handling of all the
requests that finished before the refresh was requested.

2. We could also start refreshing immediately once a responses asks for it. We’d
block new requests from starting. As concurrent requests finish, we’d also
refresh them one by one. If refreshing one of them fails, we’d retry that
particular request at the new read timestamp. Retrying works because of
idempotency, although it’s a bit weird - idempotency generally applies for
identical requests, not requests with different timestamps. However I think it
works out because we don’t have requests that write different values depending
on the timestamp they’re evaluated at.

3. We could do 2), but instead of waiting for concurrent requests to finish and
then refreshing them, we could wait for them to finish and then simply retry
them at the new timestamp without attempting to refresh them. This is more
pessimistic than 2) because if refreshing a request is successful, that’s
cheaper than re-evaluating the request (particularly since the request has
writes that need to be replicated). Note that we’d still need to wait for the
original requests to finish rather than just canceling them because of concerns
of evaluating the two attempts out of order.

Option 1) seems the simplest.

### Error handling

For non-retriable errors, this RFC doesn’t change the error handling story (so
the latest is the Savepoints RFC which touches that story). Ignoring an error is
OK.
For retriable errors, once one of them is encountered (and not washed away by
refreshing), no further operations will be permitted on any handles. Attempting
any operation will result in a specific error telling the client that a
concurrent operation encountered a retriable error and a Restart is expected.
Thus, the client is expected to `Close()` all handles and then call `txn.Restart()`
(or `RollbackToSavepoint()`). The updated state of the transaction that a
`Restart()` will use (updated timestamps, need for new txn) is saved
internally from the retriable error(s).
We could allow further operation on different handles - in fact we could also
allow further operations on the handle that encountered the retriable error.
However, this seems likely to confuse users.


## Rationale and Alternatives

An alternative to the concurrency part is to simply dissallow concurrency. We
have write pipelining on the server-side, which is already a form of
concurrency. However, concurrent read requests seem to be a very natural ask.


## Unresolved questions

1. Should different handle be allowed to use different reference sequence
numbers for reads? As it stands, the plan is to have all handles share the read
sequence number (as set by the `txn Set()` method). In particular, if
`txn.Set()` is never called, then all the handles read "current" data, and the
behavior of concurrent overlapping reads and writes is undefined. Is there a
reason to allow a particular handle to be associated with a different read
sequence number so it can be isolated from concurrent overlapping writes?
2. For using the same txn on different nodes through distributed DistSQL flows,
we have the `RootTxn`/`LeafTxn` distinction. Is there something to unify between
handles and leaves? The point of handles is to allow the txn to keep track of
extant concurrent actors , whereas the point of leaves is to allow txn state to
be disseminated to remote nodes, updated , and collected back. So I don't think
there's much to unify, but perhaps someone can open my eyes.
3. Should we take the opportunity and make a `client.Txn` unusable after a
`TransactionAbortedError`? The current code is pretty surprising - the
`client.Txn` transparently switches the `TxnCoordSender` from under it in case
of `TransactionAbortedError`. From the client’s perspective, a
`TransactionAbortedError` is no different from any other retriable error. But in
reality an abort is quite different - the transaction doesn’t get to keep any of
the locks it acquired (the whole point of the abort is to force a txn to release
its locks). As far as KV is concerned, what follows is a completely new
transaction. The abstraction that “the transaction is the same” is a bit leaky
since transaction ids printed in different logs are different. I think there’s
no state on the current `client.Txn` object that we want to keep after an abort
(or, rather, I don’t think there’s any state that we currently propagate that we
should propagate; I think there’s state we currently don’t propagate but should - the
`ObservedTimestamps`). So I think it’d be a good idea to expose this difference
to clients by not allowing a `Restart()` call after a `TransactionAbortedError`
and, instead, making the client create a new `Txn`. Tying the lifetimes of a
`client.Txn` and `TxnCoordSender` together would allow us to pool these objects
together. db.Txn(closure{}), which implements a retry loop, could still retry on
TransactionAbortedError like it’s doing currently.
   
## Notes

I'm currently also fighting on some adjacent fronts: [allowing refreshes in
distributed DistSQL
flows](https://github.com/cockroachdb/cockroach/issues/24798) and 
[rethinking the refreshing mechanics and deferment of handling some 
retriable errors](https://groups.google.com/a/cockroachlabs.com/forum/#!topic/kv/V4Gw9556Pm0).
They're disentangled from this RFC, but they all touch how refreshes work. 

