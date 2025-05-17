- Feature Name: Low-latency `CHANGEFEED`s (take 3)
- Status: draft
- Start Date: 2023-03-28
- Authors: Andrew Werner
- RFC PR: TODO
- Cockroach Issue: [#36289](https://github.com/cockroachdb/cockroach/issues/36289)

# Summary

In the steady state, Cockroach `CHANGEFEED`s have high commit-to-emit
latency. For correctness, the `CHANGEFEED` must have an up-to-date view
of the catalog metadata required to interpret an event. Generally, 
`CHANGEFEED` events are frequent and schema changes are rare; it's worth it
to make schema changes more expensive for better `CHANGEFEED`s. This RFC
leverages the existing descriptor leasing protocol and its two-version
invariant to shift the coordination burden from the `CHANGEFEED` to the
schema changes.

A limitation of this design is that users will need to explicitly "lock"
tables for which data should be low-latency, which will prevent or
degrade the ability to perform schema changes on those tables. In the
simplest form, tables which are locked cannot be changes. In the more
advanced form, only the declarative schema changer will know how to
interact with those tables.

The end result will be that in the steady state, a `CHANGEFEED` on a
locked table will not need to wait to emit rows, reducing the latency
from `~1s` to `~5ms`.

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

In most settings, at least, settings running `CHANGEFEED`s, we would rather force
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

This proposal has two distinct phases:
* **Phase 1**: Implement explicit schema change locking for low-latency. 
* **Phase 2**: Augment (declarative) schema changes to operate on locked descriptors.

In Phase 1, there will be a new table storage parameter `changefeed_optimized`
which will prevent most schema changes from operating on that table. In order
to perform a schema change, one will need to remove the parameter from the
table.

In Phase 2, schema changes supported by the declarative schema changer
will be supported even on tables with `changefeed_optimized`. While that
schema change is running, the latency from commit-to-emit may be elevated.

Upon completion of the declarative schema changer, all schema changes should
be supported for `changefeed_optimized` tables, and, potentially, that could
become the default.

## Background

### Some `CHANGEFEED` background: SchemaFeed

The [`SchemaFeed`][SchemaFeed definition] abstraction is the mechanism by which
the `CHANGEFEED` "proves" that it knows the up-to-date schema for an event at a
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

The descriptor leasing protocol provides access to a descriptor which
is valid for the timestamp at which it was acquired, from its modification
time up to its current expiration time. The protocol promises that for any
given hlc timestamp, there are at most versions of a descriptor that can
apply. However, it's also the case that for any given hlc timestamp, there
is exactly one "canonical" version of the descriptor. As discussed above,
it is sufficient for SQL to use a valid version when interpreting a row
(this allows transactions to execute concurrent with schema changes:
"online"). It is not okay for `CHANGEFEED`s; they need to be sure that they
use the "canonical" version to encode the row.

## Phase 1: `changefeed_optimized` / Frozen Descriptors

### Locked descriptors

The key concept this change introduces is the notion of a locked descriptor.
A locked descriptor has a bit stored in its serialization which has a key
property: a locked descriptor version is equivalent to its predecessor
and successor versions. 

This is upheld by the transactions which lock and unlock a descriptor. The
act of locking a descriptor is to publish a new version of the descriptor
which is identical to the current (unlocked) version, but with the locked
bit set. The act of unlocking a descriptor is to publish a new version of the
descriptor which is identical to the current (locked) version, but with
the locked bit cleared.

This protocol, combined with the guarantees of the two-version invariant
provided by the descriptor leasing protocol is the key tool to allow
`CHANGEFEED`s to emit rows without further coordination. If a `CHANGEFEED`
ever determines that it holds a lease for a locked descriptor which has
a validity interval containing the timestamp of a row, it knows that it
can safely decode the row with any descriptor the lease manager will
acquire for that row.

The observation we can make is that if we know that a locked descriptor
is a valid descriptor for a timestamp, then we know that the canonical
descriptor will be equivalent to that locked descriptor. Therefore,
so long as we have leased a locked descriptor, we don't need to wait.

In this proposal, there's a bit about how the `CHANGEFEED` should implement
this behavior, and then discussion of managing the lifecycle of a locked
descriptor: first manually (Phase 1), and then automatically by schema
changes (Phase 2).

### The `CHANGEFEED`-changes needed: a fresh SchemaFeed

We'll implement a `SchemaFeed` which wraps the existing, polling `SchemaFeed`
by acquiring and holding leases. When a call to `Peek` or `Pop` comes in, it
will ensure that it holds the appropriate leases and then will return,
otherwise it will delegate to the underlying implementation.

The lease acquisition protocol will go something like:

* Acquire a lease for the row timestamp for the descriptor:
  * If the leased descriptor is locked, allow the event to propagate.
    * Note that the lease may not be released until the local clock
      is at least past the hlc timestamp of the row which was emitted.
      this matters because the row write may have occurred in the future.
      It much be the case that the lease remained valid until the timestamp
      of its use.
  * Else, delegate to the underlying schema feed.

#### Optimization to reduce overhead:

It might be somewhat expensive to reacquire and release the lease for the
descriptor on every row. Acquiring a lease on a descriptor is an in-memory
lookup in a data structure a mutex lock to update the ref-count. This isn't
super expensive, but for every row might be substantial overhead. If this
proves to be the case, the `CHANGEFEED` could potentially cache leases and
drop them after some timeout in which no rows were seen. This should be
considered future work.

Furthermore, the need to make sure to not drop leases too early when
dropping leases on writes to global tables hints that there's going to need
to be some structure around retaining leases in the implementation of the
schema feed.

### Preventing schema changes on locked descriptors

We'll add the `IsLocked` property to the descriptor interface. If the
descriptor is locked, then no schema changes other than unlocking the
descriptor may be performed. We'll need to add logic to all schema changes
to make sure that they respect this property.

Note that there probably are whole classes of changes we can allow, but
that's left for open question -- starting out by saying no schema changes
are allowed is more obviously correct for stage 1.

### Manually locking the descriptor

#### Syntax

Consider leveraging the below syntax to perform the locking:

```sql
ALTER TABLE t SET (changefeed_optimized=t);
```

Unlocking would be:

```sql
ALTER TABLE t RESET (changefeed_optimized);
```
or
```sql
ALTER TABLE t SET (changefeed_optimized=f);
```

#### Implementation

Given the properties we need to uphold, we can only allow this operation
if it is the only change made to the table in the current transaction.
Enforcing this property shouldn't be too hard. We just need to make sure
that when executing the statement, there have been no previous modifications
to the descriptor in the transaction. Logic will also need to be put in
place to make sure that descriptors which were just unlocked do not get
later modified in a transaction.

A shortcut for the early part of `Phase 1` would be to only allow the
setting or resetting of the relevant storage parameter in a single-statement
implicit transaction that does not touch any other storage parameter.

## Phase 2: Unlocking as part of a schema change

The requirement that a user take explicit action to allow schema changes to
occur is onerous. A better user experience would be for schema changes to
do a little bit of extra work such that there might occur momentary blips
in `CHANGEFEED` latency, but during steady-state execution, the latency
remains low. Phase 2 is all about removing the need for users to unlock
and re-lock descriptors when they want to perform changes.

In a sense, the locking is very similar to other sequencing operations
performed to make online schema changes safe: we need the implementation
of the schema change to deal with managing the lock state. The challenge
is that we have to change the behavior of all schema changes to behave
in a uniform manner regardless of the operation being performed.

While this is challenging to imagine in the legacy schema changer, it is
not hard to see how to model the locking of descriptors in the context of
the declarative schema changer. By default, we'll block all schema changes
on locked descriptor which occur using the legacy implementation, and
focus our efforts on teaching the declarative schema changer to plan out
operations to lock and unlock the descriptor.

### Declarative Schema Changer

_note to reader: this is the weakest section of this document_

The lock here is weird relative to all other elements because in an
ideal world, it'll toggle back and forth to enabled and disabled whenever
the job is performing a long-running operation. We could model the lock
as an element which targets a `TRANSIENT_PUBLIC` state, which means that
it starts out public, moves to absent, and then goes back to public.

We can also add some rules that say that the transition from public
has previous transaction precedence with all other two-version operations,
or potentially all other operations. These rules can enforce that the
first transaction which runs will only unset the lock bit and the last
transaction which runs will re-lock it.

#### Backfills and Validations

Ideally we'd re-lock the descriptor for every backfill and validation
stage. This optimization could, potentially, be injected into plan by
the planner after the rest of the plan is built. This is a bit awkward
because it might make the lock element state get out of sync. 

Doing something pragmatic to lock the descriptors during backfills, at
least sometimes, may be worthwhile. 

## Drawbacks

This design introduces, particularly in Phase 1 but also in Phase 2 before
the declarative schema changer is fully implemented, real usability pain
points in order to achieve its goals. Users have to explicitly opt in to
the behavior.

## Rationale and Alternatives

This design choice improves on the previous major attempts by its focus on
simplicity, incremental nature, and organizational decoupling. The very-simple
Phase 1 will provide immediate benefit to some customers at the cost of
some usage inconvenience. Phase 2 fits naturally into the vocabulary of
ongoing schema change work, and provides a compelling story for an end-state.

### This, but with child-transactions

In [this draft PR](https://github.com/cockroachdb/cockroach/pull/99623),
we talk about effectively the same leasing ideas but with
more complete support for existing schema changes by moving the logic to
unlock the descriptor explicitly into, but preceding, the transaction
performing the schema change. It also pushes the work of locking the descriptor
into the `CHANGEFEED`. On the whole, it's a complex dance to avoid the
usability drawbacks.

An early motivation for the language in this proposal was that it leveraged
child transactions, which are a center-point in the [transactional schema
changes RFC](./20200909_transactional_schema_changes.md). The thinking was
that that alignment might help de-risk and accelerate that later project.

That dependency is that proposal's fatal flow. It requires a new, heavy,
under-specified KV-level concept in order for this project to succeed.
That was a mistake. Indeed, that proposal can be seen as future work
of this proposal -- even the required extension of this proposal to
the transactional schema changes RFC.

### Introducing a secondary leasing protocol

An earlier idea, which was prototyped to a reasonable degree, was to introduce
a wholly new leasing protocol that provides single-version gaurantees, and to
have schema changes pre-empt those single-version leases. It was a lot of new
moving parts.

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

Maybe there's a middle ground to all of this. In practice, `CHANGEFEED`s only
care about a handful of descriptors. Also, while the `CHANGEFEED` protocol
currently doesn't know anything about it, a scan of a key range effectively
that key range via the timestamp cache. If we could find a way to have a
rangefeed connected to a leaseholder rapidly close out timestamps by bumping
the timestamp cache, that could be an approach to reducing the latency.

While I do think there might be something to this, it will still require hearing
from the leaseholder of the relevant portions of the descriptor table, and, thus
will be higher latency than a system which moves any coordination off of the
critical path.

#### Global `system.descriptor` table and a RangeFeed backed `SchemaFeed`

In MR serverless, we've made the `system.descriptor` table global. That means
that a rangefeed-backed `SchemaFeed` (like in [#45267](
https://github.com/cockroachdb/cockroach/pull/45267)) would actually know
the canonical schema version for all writes to all tables which were not
in the future. Writes to global tables would have to wait.

This might at first seem a rather legitimate option, if only we could
sort out the complexities and tradeoffs of making it operationally
viable to pull this off. However, in single-region topologies or in
topologies where the latency between nodes is not large, using the
future writes protocol has lots of costs and effectively no benefits. To
force those latencies onto schema changes in that setting feels not worth
it.

The biggest thing holding this choice back is that making the descriptor
table global in all settings where the user wants low-latency changefeeds
is just way too much coupling.

# Unresolved questions

* Should the `CHANGEFEED` lock the descriptor directly rather than the user
  locking it independently of the `CHANGEFEED`?
  * In a world where there are no schema changes allowed while a descriptor
    is locked, it seems hard to both to implement and explain automatic
    locking as a side-effect of running the `CHANGEFEED`.
    * If the job is paused or not actively running, then what? Can we remove
      it? Does this force tight coupling between the descriptor and the
      `CHANGEFEED` jobs? How bad would that be?
  * In phase 2, where the schema change can proceed using the declarative
    schema changer, then it might be nice to just lock it whenever any
    `CHANGEFEED` uses the table, or maybe just always?
* Should `changefeed_optimized` also enable rangefeeds for that table
  as a way to decouple from the cluster setting? We do already support
  the low-level KV primitives needed to implement this?
  * No reason to couple those projects. Using a storage param makes sense
    and is described in: 
  [#67964](https://github.com/cockroachdb/cockroach/issues/67964)
* Is `changefeed_optimized` a good name for the storage parameter?
* Does the storage parameter need to be a kv-pair? If so, what's the
  value type?

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

[SchemaFeed definition]: https://github.com/cockroachdb/cockroach/blob/0da97734d4ea2eb7c79bd79805a9c16626ee361f/pkg/ccl/`CHANGEFEED`ccl/schemafeed/schema_feed.go#L57-L68
