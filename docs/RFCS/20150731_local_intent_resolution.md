- Feature Name: local_intent_resolution
- Status: completed
- Start Date: 2015-07-30
- RFC PR: [#1873](https://github.com/cockroachdb/cockroach/pull/1873)
- Cockroach Issue: [#1821](https://github.com/cockroachdb/cockroach/issues/1821) (now obsolete)

# Summary

Asynchronous intent resolution upon Transaction completion should be moved from client gateway to `Range`. An `EndTransaction` call should contain a definitive list of intents belonging to the `Transaction`, and the `Range` executing the `EndTransaction` call (i.e. the base `Range` for the `Transaction`) can in most cases resolve most (if not all) intents synchronously and often garbage-collect the record right away. In all other cases, the Range will perform the asynchronous resolution of the remaining intents, and a `TransactionGCQueue` can periodically scrub those `Transaction`'s records. 

# Motivation

Currently, the client gateways (`TxnCoordSender`) involved in executing a Transaction (`Txn`) keep individual records of the intents written throughout the `Txn`. Upon completion (that is, when `EndTransaction` either commits or aborts the `Txn`), the client gateway executing that command asynchronously resolves the intents tracked by it. There are some issues with that:

* intent resolution happens fairly late and is a few round-trip latencies away from the most likely intent location: The range which holds the `Txn`'s record.
* a `Txn` carried out over multiple coordinators will not have all of its intents resolved, only those on the coordinator which executes `EndTransaction`.
* partially due to the above, it's hard to garbage-collect `Txn` records: it's not clear whether there are any open intents somewhere else in the system.

Not resolving intents is not a correctness issue, but can significantly reduce performance in the presence of contention as conflict resolution requires a lot of round-trip delays and potential backoff. Not garbage-collecting `Txn` records is also not a correctness issue, but necessary at some point.

At the Range level, we can deal with most of these issues, and adding a bit more data to the `Transaction` records which can not be gc'ed immediately allows speedy cleanup of the rarer cases as well.

# Detailed design

## Disallow cross-coordinator Transactions
Dealing with multiple coordinators handling a single `Txn` is tricky: It's hard to keep clients from using more than one gateway reliably without additional complexity, but the natural path to take with the suggested changes is to declare all intents which are not contained in the transaction record illegal. This could mean that parts of a client's `Transaction` might get lost upon commit when written through another gateway, unless we take precautions.
An easy and reliable method (for correctly operating clients) is to require that the request which starts the `Txn` on the coordinator comes with a bare `Txn` header.

This provides a simple method for clients to stick to one coordinator, and gives us an authoritative list of intents. Any intent not in the list is considered aborted; that way ill-behaved clients may lose writes, but otherwise we would not be able to gc `Txn` records ever.

## Gateway changes

The logic for resolving intents on `EndTransaction` is changed: Instead of trying to resolve the intents, their keys are bundled up and sent along with the `EndTransaction` call. Once that call has returned, the coordinator's work for that `Txn` is done.

Heartbeating a `Transaction` which just got committed could recreate the freshly-gc'ed record; some care needs to be taken that only the first heartbeat can create a record (all others should `CPut`), and that the heartbeat goroutine ends before the gateway passes on the `EndTransaction` call. This isn't a correctness issue, though.

## Range changes

The logic for `EndTransaction` is updated to reflect the following enhancements:

* the intent list is split into range-local and non-local intents.
* all local intents are atomically added to the `EndTransaction` batch
* the transaction record is updated so that its authoritative list of intents contains only the non-local intents; if there are no non-local intents, the record can be gc'ed immediately.
* after the batch commits and only on the leading replica, the unresolved intents are resolved in a `Batch` asynchronously. Once resolved, the `Txn` record should be updated (as with any intent resolution; the functionality can be factored out into a single location). If the `Txn` is without intents after the update, it can be removed.
* The GC queue (or a new queue) should groom the `Transaction` records of each `Range` and re-trigger the resolution process described above for all records which have open intents (which will remove them when done). This should only ever have to do actual work in the event of node crashes, etc.

All in all, this should be fairly straightforward and reduces some complexity and shared responsibility regarding intent resolution.

# Drawbacks

Clients will have their `Txn` aborted if switching gateways: This could be a long-term concern for long-running `Txn`s or clients which talk to a non-sticky load-balancer. Via @bdarnell:
> At Google, mapreduces would originally have to restart if their (single) master task failed.
> This was enough of a problem that they needed to implement master failover (eventually - it was tolerated for a long time).

This isn't enough of an issue now to consider, but good to keep in mind. We can always soften the single-coordinator constraint later, but will need to put more complexity into the resolution process in that case (for instance, the client could track the intents), and may have to reconsider heartbeating (with many coordinators, a `Txn` would otherwise consume many goroutines).

# Alternatives

# Unresolved questions
