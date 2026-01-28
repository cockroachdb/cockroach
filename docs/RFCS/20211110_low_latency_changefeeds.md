- Feature Name: Low latency changefeeds
- Status: draft
- Start Date: 2021-11-10
- Authors: Andrew Werner
- RFC PR: TODO
- Cockroach Issue: [#36289](https://github.com/cockroachdb/cockroach/issues/36289)

# Summary

Cockroach `CHANGEFEED`s have high latency between commit and emit in the
steady state due to how they ensure that they have an up-to-date schema
for a given event. In most cases, CHANGEFEED row emission is frequent and
schema changes are rare. This RFC proposes a leases protocol which, by
shifting the coordination cost from the CHANGEFEED to the schema change,
allows the CHANGEFEED to emit a row without doing any schema-related work.

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

In most settings, at least, settings running changefeeds, we would force
schema changes to do more work in order to avoid the need to wait for anything
before emitting a CHANGEFEED row.

# Technical design

This RFC introduces a new subsystem into cockroach called single-version
descriptor leases. These leases differ from the traditional
[schema leases](20151009_table_descriptor_lease.md) in that they prevent any
new version from being created as opposed to allowing one new version to be
created. A single-version lease is like a read lock which prevents any
writes to the descriptor.

This new subsystem will be interacted with by two components, the changefeed
and transactions performing schema changes. The changefeeds will hold these
leases during normal operation. When a transaction goes to commit, it will need
to ensure that there are not single-version leases which will be held at the
timestamp at which it commits. These leases will be tied to
[sqlliveness](20200615_sql_liveness.md) sessions to avoid the need to implement
any heart-beating.

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
	// Run synchronously runs the SchemaFeed. It should be invoked before any
	// calls to Peek or Pop.
	Run(ctx context.Context) error

	// Peek returns events occurring up to atOrBefore.
	Peek(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
	// Pop returns events occurring up to atOrBefore and removes them from the feed.
	Pop(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
}
```

### The writing transaction: Preemptor

Before a SQL transaction which performed any schema changes commits, we have
certain invariants we want to uphold. The main one is that we won't violate the
two-version lease invariant by committing. This RFC introduces a new invariant
to ensure: the single-version lease invariant. In order to ensure that no
single-version lease will be invalidated, the RFC introduces a mechanism:
the `Preemptor`.

The `Preemptor` has the following protocol. For all descriptors being written: 

1. Write an intent to a key which prevents new leases from being established
2. (using a separate transaction, at the provisional commit timestamp) check to
   see if there are any live. If none, protocol concludes.  
3. (using a separate transaction) write a notification to pre-empt any leases.
4. (in parallel with 3) launch a rangefeed to watch for leases to drop.
5. Wait for all existing leases to drop.
    6. Note that this does not need to wait for a resolved timestamp as the
       acquisition protocol (yet to be described).
    7. However, if any new leases which were not originally tracked get
       written and appear via the rangefeed, we'll know the preempting
       transaction has been aborted and we'll need to return an error.
       If we don't, then we may block forever, as that new lease won't
       see a notification.
6. Force the writing transaction above which invoked the preemptor above the
   timestamp of the last lease drop (see open question regarding causality).

The programming interface could be something like:

```go
type Preemptor interface {
	EnsureNoSingleVersionLeases(
	    ctx context.Context,
	    txn *kv.Txn,
	    descriptors catalog.DescriptorIDSet, 
	) error
}
```

This call will go before the two-version invariant checking around
[here](https://github.com/cockroachdb/cockroach/blob/9196ba040fa8e48c9a71a74ce65f938427e361fe/pkg/sql/conn_executor_exec.go#L877-L879)

### Single version lease state

A straw-man of the state
```sql
-- Note that this could probably be shoved into one table without
-- much effort to avoid using too many system tables.

CREATE TABLE single_version_leases (
    descriptor_id INT,
    session_id BYTES, -- the empty bytes value will be a sentinel
    instance_id INT, -- for debugging
    PRIMARY KEY (descriptor_id, session_id)
)

CREATE TABLE single_version_lease_notifications (
    descriptor_id INT,
)
```

### Single-version SchemaFeed

We'll implement a `SchemaFeed` which wraps the existing, polling `SchemaFeed`
by acquiring and holding leases. When a call to `Peek` or `Pop` comes in, it
will ensure that it holds the appropriate leases and then will return,
otherwise it will delegate to the underlying implementation.

The lease acquisition protocol will go something like:

1) Read the lease key corresponding to the empty session, pre-emptors use this
   to hold locks to prevent new leases from being created during execution.
2) Write the new lease value.
3) Commit.
4) Use the timestamp as the start time of the validity interval.

Then, watch for notifications which occurred since the lease acquisition time.
When a notification occurs, drop the lease, then try to re-acquire. The 
attempt to re-acquire will block on the intent of the transaction which sent
the notification. In the meantime, calls to the methods of the `SchemaFeed`
will be delegated because we won't have a lease for all descriptors covering
the time interval.

One important thing we'll need to track is the highest timestamp of an event
emitted. The transaction which removes the lease needs to use a timestamp above
that timestamp.

As a sanity check we can expose a non-blocking call to determine whether there
are any events and the timestamp at which the earliest occurs in order to make
sure we didn't royally mess something up. 

## Drawbacks

It is a relatively complex dance that involves a good bit of state and a
pretty involved protocol. It also retains a fallback in the face of a
schema change which is not great.

## Rationale and Alternatives

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

* It might be nice to be able to toggle this leasing on and off. Unfortunately,
  just flipping a cluster setting is not good enough; there might be outstanding
  leases in use by a changefeed.
  * Maybe it's not that big of a deal if we parallelize the checking for the
    leases with checking for one version in the commit protocol. That's already
    a round-trip. If the setting turns it off, then that round-trip will not
    result in the writing transaction needing to do anything.
* There's some causality items that will need dealing with. I think it's all
  good but leaving this as a place to stash some rambling notes.
  * Nathan recently has been working to narrow the surface area which causes
    causality to propagate through clocks. This is good, in a sense, because
    a less informed, younger me might have just assumed what causality
    propagation we had in place was enough to prevent any of the problems
    which could arise.
  * One thing is that we need to make sure that the writing timestamp of
    the transaction to drop the lease is above all event timestamps the lease
    was used to prove.

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
