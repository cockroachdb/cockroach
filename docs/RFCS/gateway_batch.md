- Feature Name: gateway_batch
- Status: completed
- Start Date: 2015-08-06
- RFC PR: [#1998](https://github.com/cockroachdb/cockroach/pull/1998),
          [#2141](https://github.com/cockroachdb/cockroach/pull/2141),
          [#2406](https://github.com/cockroachdb/cockroach/pull/2406)
- Cockroach Issue: [#2130](https://github.com/cockroachdb/cockroach/issues/2130)

# Summary

All requests processed at the client gateway to nodes should be Batch requests.
They should be split along range descriptor boundaries by the client gateways
prior to sending RPCs to the cluster, replacing the current multi-range logic.
This is the gateway portion of a major performance optimization: having Ranges
submit whole batches to Raft and execute them atomically within a single write
to RocksDB (which in turn allows most transactions to be optimized away).

# Motivation

This saves a lot of round-trips for the most common use cases and lays the
ground work for major server-side performance improvements.

# Detailed design

The proposition below assumes #1988.

`DistSender` is in charge of breaking a `Batch` up into pieces transparently.
That involves going through the batch, looking up the range descriptors for
each affected key range, and creating a batch for each each new recipient
`Range`. For the order in which they are sent, there's a caveat for
transactional batches:

In the (hopefully) most common case, requests contained within affect only one
`Range` and the `Batch` can be sent to that `Range` as a whole. If that is not
the case and we end up with several chunks of `Batch`, it's clear that the one
containing `EndTransaction` must be sent last. `DistSender` needs to be aware
of that. `TxnCoordSender` is changed to ensure that the list of intents from
the batch is added to `EndTransaction` in this case.

Schematically, `DistSender.Send()` would turn into something along the lines of
```go
// SendBatch is called by (*TxnCoordSender).Send().
func (ds *DistSender) SendBatch(b *roachpb.BatchRequest) (roachpb.BatchResponse, error) {
  // Retry loop here.
  // ...
  {
    // Chop up the batch. This is already in a retry loop.
    var chopped []*roachpb.BatchRequest = ds.Split(b)
    // `adapt` **must** be idempotent! It may be executed multiple times on
    // retryable errors, in particular with stale descriptors.
    // EndTransaction needs to go last; but TxnCoordSender has already
    // inspected the batch and made sure that the list of intents is set.
    b = adapt(b)

    // Send the reordered batch requests one by one, much like ds.Send()
    // ...

  // Recombine the multi-range requests back into one.
  // ...
  }
  return response
}
```

Another reasonable optimization, namely sending chunks of the batch in
parallel, could be carried out similarly: In this case, `adapt` would not only
operate on the slice, but return a `Plan` consisting of different stages, each
containing multiple chunks (extremes: each stage consisting of one one-element
batch = sequential querying as we do it now; one stage of `N` batches (to
different ranges) = completely parallel). Non-transactions would go completely
parallel, transactions are a bit more involved (going parallel might lay down a
lot of intents and do a lot of work for nothing) but will always have a last
stage with the chunk containing `EndTransaction`.

Since in many tests, `TxnCoordSender` has to talk to a `LocalSender` instead
of a `DistSender`, we may want to stick to the `client.Sender` interface though
and keep the signature `Send(roachpb.Call)`.

## Transaction restarts/updates

Updating the transaction record on certain errors is currently a duty of
`TxnCoordSender`, but will have to be carried out by `Store`. Thus,
`(*TxnCoordSender).updateResponseTxn()` needs to move and is called in
`(*Store).ExecuteCmd()` instead.

## Placeholders and Multi-Range requests

Some result types (notably `Scan`) implement a `Combinable` interface which
is used to glue together the subranges of the result contained on different
`Range`s. Combining those is easiest if the index of request in a `Batch` is
equal across all chunks. For any index with at least two non-placeholders requests,
these requests must implement `Combinable`. Similar considerations hold for
truncation, which is more complicated: once over limit, all future chunks of
the batch should turn that request into a placeholder to avoid querying
unnecessary further data.

Therefore, when we split a batch, each new batch has the same size as the
original batch, interspersed with placeholder values (`nil`?) as necessary. Each
request has the same index in the new batch as it had in the original batch.
Most indexes will have a non-nil value in exactly one batch, but a request that
spans multiple ranges will be represented in more than one.

## Batch and Transaction wrapping

When a request arrives which is not a batch, wrap it in one appropriately. This
should be close to where `maybeBeginTxn` is today. `OpRequiresTxnError` should
be handled nearby, creating the transaction and retrying in-place.

## Store/Replica

As discussed under [Unresolved Questions](#unresolved-questions), in an ideal
world we would propose batches to Raft. Since that is complex, the scope here
is less ambitious:

The main retry loop in `(*Store).ExecuteCmd()` gets an additional outer loop
which expands the Batch and deals with each request separately, aborting on
the first unhandled error.

This means that `Replica` will be blissfully unaware of this RFC.

## Tracing

Tracing will need an update, but mostly content-wise. It prints the request
type in various locations, and those will all be `Batch` after this change.

# Drawbacks

* slight overhead for non-batch calls. It's not expected that there be many of those though.

# Alternatives

* different interfaces between `TxnCoordSender` and `DistSender` are possible. One could even argue that the order of `DistSender` and `TxnCoordSender` might be the wrong way around.

# Unresolved questions

## Replica/Raft batches

A large reduction in network-related delay results if the replica proposes the
`BatchRequest`s into Raft without prior expansion for performance. This is
worthy of another RFC but here are some basic details.

It is going to require considerable refactoring around the command queue, and
timestamp and response caches which currently serialize those requests in
`add{Read,Write}Cmd`, including making sure requests in the same batch have the
same rules applied to them.

Applying such a batch would entail looping through its individual requests and
executing them in the same batch, stopping on the first error encountered, if
any. Such an error discards the `engine.Batch` as usual, but #1989 applies.

Further optimizations to this are possible: conflict resolution using this
approach could be inefficient: The first such conflict will have the whole batch
retry. It could be a good optimization to execute all commands unless "critical"
errors pop up (a `WriteIntentError` not being one of them).
