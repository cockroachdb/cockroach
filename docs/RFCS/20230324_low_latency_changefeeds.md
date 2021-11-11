- Feature Name: Low latency changefeeds (take 2)
- Status: draft
- Start Date: 2023-03-24
- Authors: Andrew Werner
- RFC PR: TODO
- Cockroach Issue: [#36289](https://github.com/cockroachdb/cockroach/issues/36289)

# Summary

Cockroach `CHANGEFEED`s have high latency between commit and emit in the
steady state due to how they ensure that they have an up-to-date schema
for a given event. In most cases, CHANGEFEED row emission is frequent and
schema changes are rare. This RFC leverages the existing descriptor leasing
protocol and its two-version invariant to shift the coordination burden from
the CHANGEFEED to the schema change, allows the CHANGEFEED to emit a row in
common case without doing any schema-related work.

# Motivation

`CHANGEFEED`s must wait to know that a schema change has not occurred before
being able to emit a row sent from a rangefeed. A schema change may prevent a
row from being emitted due to a need to run a backfill, fail, or change the
interpretation of that row<sup id="a1">[1](#f1)</sup>. This currently works by
blocking, and buffering, events emitted from a rangefeed to the a `CHANGEFEED`
processor and waiting until the schema at the timestamp of the event has been
"proven". This process of proving the schema involves polling the complete set
of descriptors at a frequency controlled by the
`changefeed.experimental_poll_interval` cluster setting (which defaults to
1 second).

In most settings, at least, settings running changefeeds, we would rather force
schema changes to do more work in order to avoid the need to wait for anything
before emitting a CHANGEFEED row.

# Technical design

This RFC leverages the [descriptor leasing protocol](
20151009_table_descriptor_lease.md) and some associated descriptor state to
mark a descriptor version as locked for virtual changes. In order to perform
a schema changer, the transaction performing the schema change must first
unlock the descriptor by publishing a new descriptor version which is not
locked. A `CHANGEFEED` which holds a lease on a locked descriptor knows that
any row which is seen in the validity interval of a held lease can be
interpreted with any descriptor which can be leased for that row's timestamp.
Schema changes leverage [child transactions](
https://github.com/cockroachdb/cockroach/pull/56588) to unlock descriptors
during transaction execution. In effect, descriptor locks turn two-version
leases into single-version leases; if a lease is held for a locked descriptor,
the subsequent version and the preceding version are known to be logically
equivalent.

This new subsystem will be interacted with by two components, the changefeed
and transactions performing schema changes. The changefeeds will hold these
leases on locked descriptors during normal operation. When a schema change
encounters a locked descriptor, it will unlock it and wait for that signal to
propagate to all nodes. 

### Some `CHANGEFEED` background: SchemaFeed

The [`SchemaFeed`][SchemaFeed definition] abstraction is the mechanism by which
the CHANGEFEED "proves" that it knows the up-to-date schema for an event at a
given timestamp. This is implemented today with the above described polling
<sup id="a2">[2](#f2)</sup>. The RFC here will propose a richer implementation
of this interface by wrapping the existing implementation and determining when
it can avoid calling into the poller.

```go
// SchemaFeed is a stream of events corresponding the relevant set of
// descriptors.
type SchemaFeed interface {
	// Peek returns events occurring up to atOrBefore.
	Peek(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
	// Pop returns events occurring up to atOrBefore and removes them from the feed.
	Pop(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
}
```

### Reminders on descriptor leases

Online schema changes work by decomposing operations into a sequence of state
changes to descriptors. Each adjacent pair of versions of the descriptor are
mutually compatible, at least while leases exist on the earlier version. A
transaction may only transition a descriptor by one version at a time. Between
version-incrementing transactions, all leases on the previous version must be
drained. This protocol ensures the "two-version invariant" of descriptors:

  * At most two versions of a descriptor may be held by an active lease.
  * The versions which may be leased are the current version and its
    predecessor.

These properties imply that a new version can only be published if there are
active leases only on the current version of the descriptor (not its 
predecessor).

#### The core challenge: orchestrating locking and unlocking.

The idea of having the changefeed force the schema change to add an extra 
version to cause changes is not particularly clever. It also leads to a
straightforward-enough approach for the schemafeed to leverage the existing
leasing infrastructure. What is complex, however, is how to:

1) Make sure that the changefeed does acquire a lease on a locked descriptor
   in a timely manner if there are no schema changes.
2) Make sure that the schema change is able to make progress despite the
   existence of CHANGEFEEDs.

### The schema change transaction 

Schema changes always transactions always perform read-modify-write operations
on descriptors. Access to these descriptors is mediated by the
`descs.Collection`. In order to mutate a descriptor, the transaction must
acquire a `Mutable` copy in its `descs.Collection`. The proposal here is that
a `Mutable` descriptor cannot be exposed to a sql transaction for modification
if it is locked. The tricky bit is that we also don't want to restart schema
change transactions which encounter locked descriptors -- we want to unlock the
descriptor and then allow those schema changes to proceed.

In order to orchestrate this dance, when the process of resolving a mutable
descriptor encounters a locked descriptor, it will launch a child transaction
to remove the lock and observe the newly unlocked descriptor. 

In order for this to work, the schema change transaction must not be forced
to restart if it has already read state that the child has now modified. To
cope with this situation, we'll assume that the child transaction has logically
communicated its side-effects to the parent, and all intents will be removed
from the refresh spans of the parent.

The schema change must also ensure that the changefeeds don't re-lock the
descriptor immediately, before it is able to do its business. In order to
coordinate that interaction, the schema change will place a lock at a special
key to indicate that a schema change is under-way. The changefeed will write to 
the key in the same transaction that is used to place the lock. In that way, 
the changefeed logic to re-lock the descriptor will wait for the concurrent
schema change.

### Single-version SchemaFeed

We'll implement a `SchemaFeed` which wraps the existing, polling `SchemaFeed`
by acquiring and holding leases. When a call to `Peek` or `Pop` comes in, it
will ensure that it holds the appropriate leases and then will return,
otherwise it will delegate to the underlying implementation.

The lease acquisition protocol will go something like:

1) Check whether a lease is held by the schemafeed for the descriptor that
   is valid for the row and that descriptor is locked. If so, allow the event
   to propagate.
2) If the lease is valid, but is not locked and there is not on-going schema
   change, launch the task to lock the descriptor.
   3) A complication is long-running schema changes. By default, if we don't
      lock the descriptor at all during schema change jobs, we'll potentially
      see higher latency during backfills and what not. We could envision a
      scheme whereby we classify the state of the schema change and lock
      accordingly. This is left for future work.
4) Delegate to the underlying schema feed.

#### Locking a descriptor

There should be a node-level mechanism which can schedule a descriptor version
to be locked. The call should be non-blocking and low-overhead, only leading to
background work if there is evidence that there is work to be done.

#### Why this works

This all works because you know that if you held a descriptor lease that was
valid for a row at a given timestamp, the only possible leases you could ever
hold for that timestamp are the version you had leased, the prior version, or
the subsequent version; a given timestamp can have at most two descriptor
versions which can ever apply to them, the canonical version and the prior
version. If you know that the predecessor and the successor of the leased
descriptor which covers the row are logically identical to that descriptor,
it doesn't matter which version you use to decode the row (they'll all be
the same).

#### Holding on to the leases

It might be somewhat expensive to reacquire and release the lease for the
descriptor on every row. Acquiring a lease on a descriptor is an in-memory
lookup in a data structure a mutex lock to update the ref-count. This isn't
super expensive, but for every row might be substantial overhead. If this
proves to be the case, the changefeed could potentially cache leases and
drop them after some timeout in which no rows were seen. This should be
considered future work.

## Drawbacks

The design forces the existence of the concept of a child transaction. This
concept in a very closely related form and usage first featured in the
[transactional schema change RFC](./20200909_transactional_schema_changes.md).
The core idea is that there is a transaction which runs synchronously during
the execution of a parent transaction and it:
* observes the ancestor transactions' writes
* passes through the ancestor transactions' locks
* communicates an edge from the parent to itself for deadlock detection
* can commit before the parent transaction
* cannot commit after the parent transaction

These are useful to irrecoverably cause some visible state change to internal
system structures during the execution of the parent transaction, and can
wait for these operations to be observed by the rest of the system. In effect,
these child transactions allow transactions to coordinate through the key-value
store with concurrent transactions without violating the kv-layer's enforced
serializable isolation.

This concept exists elsewhere in the database literature. Consider the section
in [`ARIES`](https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf) called
"NESTED TOP ACTIONS":

> There are times when we would like some updates of a transaction to be
committed, irrespective of whether the transaction ultimately commits or
not. We do need the atomicity property for these updates themselves. This is
illustrated in the context of file extension. After a transaction extends a file
which causes updates to some system data in the database, other transactions
may be allowed to use the extended area prior to the commit of the extending
transaction. If the extending transaction were to roll back, then it would not
be acceptable to undo the effects of the extension.

The operations we're describing here and in the other RFC featuring child
transactions, we have similar situations.

## Rationale and Alternatives

### Introducing a secondary leasing protocol

This is discussed at length in [this draft PR](
https://github.com/cockroachdb/cockroach/pull/72645).

### Band-aids to shorten the interval

Another approach might be to find a way to change how we watch and interact
with the descriptor table. One approach might be to replace the polling with
a rangefeed. This is actually easy to do and has been implemented once in
[#45267](https://github.com/cockroachdb/cockroach/pull/45267). The problem with
that approach is that rangefeeds require resolving a keyspan before providing a
proof that all values have been seen. By default, this takes over 3s, and, thus
is worse than the polling we already had, at least, from a latency perspective.

Part of the problem with this is that it is not obvious that we cannot control
the rate of closing or resolving of a keyspan arbitrarily. One might ask, if we
could lower the closed timestamp interval on the descriptor table to something
very short, would that be a good idea? This starts to feel not unlike the global
tables built on top of
[non-blocking transactions](20200811_non_blocking_txns.md). Unfortunately, this
policy would have the side-effect of forcing all schema changing transactions to
commit close to the present. If anything, we'd love to increase the trailing
interval on this table for the sake of these transactions and their horrifically
large numbers of round-trips.

Maybe there's a middle ground to all of this. In practice, changefeeds only
care about a handful of descriptors. Also, while the changefeed protocol
currently doesn't know anything about it, a scan of a key range effectively
that key range via the timestamp cache. If we could find a way to have a
rangefeed connected to a leaseholder rapidly close out timestamps by bumping
the timestamp cache, that could be an approach to reducing the latency.

While I do think there might be something to this, it will still require hearing
from the leaseholder of the relevant portions of the descriptor table, and, thus
will be higher latency than a system which moves any coordination off of the
critical path.

# Explain it to folk outside of your team

TODO(ajwerner)
Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions

* Talk about the risks of extra round-trips and fairness in the
  case where transactions are forced to restart AND don't use cockroach_savepoint
  or some other lock-preserving protocol.

# Footnotes

<b id="f1">1</b> Due to the online nature of schema changes, a transaction
writing at time t3 may use the interpretation of the schema as it was at t1,
assuming a schema change occurred at t2. A `CHANGEFEED` must present the row
under the interpretation of the row using the schema committed at t2. This
interpretation will eventually always apply to the row at that timestamp,
even if it was not the interpretation of the coordinator for that write.
[↩](#a1)

<b id="f2">2</b>
Another note about the existing implementation is that the events are also used
to trigger lease refreshing in the regular lease manager. This is a bit of an
implementation detail, but the long-and-short of it is that a higher level of
the `ChangeAggregator` processor will except to be able to ask for leases for
the table at the event's timestamp and will assume that it's going to get the
right version. [↩](#a2)

[SchemaFeed definition]: https://github.com/cockroachdb/cockroach/blob/0da97734d4ea2eb7c79bd79805a9c16626ee361f/pkg/ccl/changefeedccl/schemafeed/schema_feed.go#L57-L68
