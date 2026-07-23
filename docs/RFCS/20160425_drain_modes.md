- Feature Name: drain_modes
- Status: completed
- Start Date: 2016-04-25
- Authors: Tobias Schottdorf (tobias.schottdorf@gmail.com), Alfonso Subiotto Marqués
- RFC PRs: [#6283](https://github.com/cockroachdb/cockroach/pull/6283), [#10765](https://github.com/cockroachdb/cockroach/pull/10765)
- Cockroach Issue: [#9541](https://github.com/cockroachdb/cockroach/issues/9541), [#9493](https://github.com/cockroachdb/cockroach/issues/9493), [#6295](https://github.com/cockroachdb/cockroach/issues/6295)

# Summary
Propose a draining process for a CockroachDB node to perform a graceful
shutdown.

This draining process will be composed of two reduced modes of operation that
will be run in parallel:

* `drain-clients` mode: the server lets on-going SQL clients finish up to some
  deadline when context cancellation and cleanup is performed, and then politely
  refuses new work at the gateway in the manner which popular load-balancing SQL
  clients can handle best.
* `drain-leases` mode: all range leases are transferred away from the node, new
  ones are not granted (in turn disabling most queues and active gossipping) and
  the draining node will not be a target for lease or range transfers. The
  draining node will decline any preemptive snapshots.

These modes are not related to the existing `Stopper`-based functionality and
can be run independently (i.e. a server can temporarily run in `drain-clients`
mode).

# Motivation
In our Stopper usage, we've taken to fairly ruthlessly shutting down service of
many components once `(*Stopper).Quiesce()` is called. In code, this manifests
itself through copious use of the `(*Stopper).ShouldQuiesce()` channel even when
not running inside of a task (or running long-running operations inside tasks).

This was motivated mainly by endless amounts of test failures around leaked
goroutines or deadlocking operations and has served us reasonably well, keeping
our moving parts in check.

However, now that we're looking at clusters and their maintenance, this simple
"drop-all" approach isn't enough. See, for example #6198, #6197 or #5279. This
RFC outlines proposed changes to accommodate use cases such as

* clean shutdown: instead of dropping everything on the floor, politely block
  new clients and finish open work up to some deadline, transfer/release all
  leases to avoid a period of unavailability, and only then initiate shutdown
  via `stopper.Stop()`. This clean shutdown avoids:
    * Blocking schema changes on the order of minutes because table descriptor
    leases are not properly released [#9493](https://github.com/cockroachdb/cockroach/issues/9493)
    * Increased per-range activity caused by nodes trying to pick up expired
    epoch-based range leases.
    * Leaving around intents from ongoing transactions.
* draining data off a node (i.e. have all or some subset of ranges migrate
  away, and wait until that has happened).
  Used for decommissioning, but could also be used to change hard-drives
  cleanly (i.e. drain data off a store, clean shutdown, switch hdd, start).
* decommission (permanently remove), which is "most of the above" with the
  addition of announcing the downtime as forever to the cluster.
* drain for update/migration (see the `{F,Unf}reeze` proposal in #6166):
  Essentially here we'll want to have the cluster in drain-clients mode and then
  propose a `Freeze` command on all Ranges.

This work will also result in
* Auditing and improving our cancellation mechanism for individual queries.
* Improving compatibility with server health-checking mechanisms used by third
  party proxies.

# Detailed design
Implement the following:

```go
// Put the SQL server into drain-clients mode within deadline, cancelling
// connections as necessary. A zero deadline undoes the effects of any
// prior drain operation.
(*pgwire.Server).DrainClients(deadline time.Time) error

// Put the Node into drain-lease mode if requested, or undo any previous
// mode change.
(*server.Node).DrainLeases(bool) error
```

## SQL/drain-clients mode:
The `v3conn` struct will be extended to point to the server. Before and after
reading messages from the client, a `v3conn` will check the draining status on
the server. If both `draining` is set to `true` and `v3conn.session.TxnState.State`
is `NoTxn`, the `v3conn` will exit the read loop, send an
[appropriate error code](#note-on-closing-sql-connections), and close the
connection, thereby denying the reading and execution of statements that aren't
part of an ongoing transaction. The `v3conn` will also be made aware of
cancellation of its session's context which will be handled in the same way.

All active sessions will be found through the session registry
([#10317](https://github.com/cockroachdb/cockroach/pull/10317/)). Sessions will
be extended with a `done` channel which will be used by `pgwire.Server` to
listen for the completion of sessions. Sessions will be created with a context
derived from a cancellable parent context, thus offering a single point from
which to cancel all sessions.

Additionally, `pgwire.Server` will not create new sessions once in `draining`
mode.

After `draining` behavior is enabled, we have three classes of clients
characterized by the behavior of the server:
* Blocked in the read loop with no active transaction. No execution of any
  statements is ongoing.
* Blocked in the read loop with an active transaction. No execution of any
  statements is ongoing but more are expected.
* Not blocked in the read loop. The execution of (possibly a batch) of
statements is ongoing.

A first pass through the registry will collect all channels that belong to
sessions of the second and third class of clients. These clients will be given a
deadline of `drainMaxWait` (default of 10s) to finish any work. Note that once
work is complete, the `v3conn` will not block in the read loop and will instead
exit. Once `drainMaxWait` has elapsed or there are no more sessions with active
transactions, the parent context of all sessions will be canceled. Since a
derived context is used to send RPCs, these RPCs will be canceled, resulting in
the propagation of errors back to the Executor and the client. Additionally,
plumbing will have to be added to important local work done on nodes so that
this context cancellation leads to the interruption of this work.

`pgwire.Server` will listen for the completion of the remaining sessions up to
a timeout of 1s. Some sessions might keep going indefinitely despite a canceled
context.

The final stage is to delete table descriptor leases from the `system.lease`
table to avoid future schema changes blocking. `LeaseManager` will be extended
to delete all leases from `system.lease` with a `refcount` of 0. Since there
might still be ongoing sessions, `LeaseManager` will enter a `draining` mode in
which if any lease's `refcount` is decremented to 0, the lease will be deleted
from `system.lease`. If sessions are still ongoing after this point, a log
message would warn of this fact.

### Note on closing SQL connections
The load balancing solutions for Postgres that seem to be the most popular are [PGPool](http://www.pgpool.net/docs/latest/en/html/) and
[HAProxy](https://www.haproxy.com/). Both systems have health check mechanisms
that try to establish a SQL
connection[[1]](http://www.pgpool.net/docs/latest/en/html/#HEALTH_CHECK_USER)
[[2]](https://www.haproxy.com/doc/aloha/7.0/haproxy/healthchecks.html#checking-a-pgsql-service).
HAProxy also supports doing only generic TCP checks. Because of how these health
checks are performed, sending back a SQL error during the establishment of a
connection will result in the draining node being correctly marked as down.

For failures during an existing or new session, PGPool has a
`fail_over_on_backend_error` option which is triggered when

> 57P01 ADMIN SHUTDOWN
> admin_shutdown

is received from the backend. PGPool will retain session information under some conditions[[3]](http://pgsqlpgpool.blogspot.com/2016/07/avoiding-session-disconnection-while.html)
and can reconnect transparently to a different backend.

HAProxy does not have similar functionality and both SQL/TCP errors are
forwarded to the client.

We should therefore return

> 57P01 ADMIN SHUTDOWN
> admin_shutdown

to reject new connections and close existing ones for compatibility with
PGPool's automatic failover and both PGPool's and HAProxy's health checks.
However, clients should retry in the case that HAProxy is being used and want
transparent failover when the establishment of a connection errors out.

## Node/drain-leases mode:
`(*server.Node).DrainLeases(bool)` iterates over its store list and delegates
to all contained stores. `(*Replica).redirectOnOrAcquireLease` checks
with its store before requesting a new or extending an existing lease.

The `Liveness` proto will be extended with a `draining` field which will be
taken into account in `Allocator.TransferLeaseTarget` and
`Allocator.AllocateTarget`, resulting in no leases or ranges being transferred
to a node that is known to be draining. Updating the node liveness record will
trigger a gossip of the node's draining mode.

Transfers to a draining node could still happen before the gossiped `Liveness`
has reached the source of the transfer. This will only be handled in the case
of range transfers.

A draining node will decline a snapshot in `HandleSnapshot` if its store is
draining. In the case of lease transfers, since they are proposed and applied as
raft commands, there is no way for the recipient to reject a lease.

[NB: previously this RFC proposed modifying the lease transfer mechanism so that
a draining node could immediately send back any leases transferred to it while
draining. Since the draining process is a best effort to avoid unavailability
and it's not sure this addition would be necessary or produce significant
benefits, the modification of the lease transfer mechanism has been left out
but could be implemented in the future if necessary]

To decrease the probability of unavailability, an optional 10s timeout will be
introduced to wait for the gossip of the draining node's `Liveness` to
propagate. This option will be off by default and operators will have the
option of turning it on to prioritize availability over shutdown speed.

After this step, leases that a node's replicas currently hold will be
transferred away using `AdminTransferLease(target)` where `target` will be found
using `Allocator.TransferLeaseTarget`.

To allow commands that were sent to the node's replicas to complete, the
draining node will wait for its replicas' command queues to drain up to a
timeout of 1s. This timeout is necessary in the case that new leases are
transferred to the node and it receives new commands.

## Server/adminServer:

```go

type DrainMode int
const (
  DrainClient DrainMode = 1 << iota
  DrainLeases
)

// For example, `s.Drain(DrainClient | DrainLeases)`
(*server.Server).Drain(mode DrainMode) error
```

and hook it up from `(*adminServer).handleQuit` (which currently has no way of
accessing `*Server`, only `*Node`, so a shortcut may be taken for the time
being if that seems opportune.

# Drawbacks

# Alternatives
* Don't offer a `drainMaxWait` timeout to ongoing transactions. The idea of the
  timeout is to allow clients a grace period in which to complete work. The
  issue is that this timeout is arbitrary and it might make more sense to
  forcibly close these connections. It might also make sense to offer operators
  to set this timeout via an environment variable.
* Reacquire table leases when the node goes back up. This was suggested in
  [#9493](https://github.com/cockroachdb/cockroach/issues/9493) but does not fix
  the issue of having to wait for the lease to expire if the node does not come
  back up.

# Future work
* Move ranges to another node if we're draining the node for decommissioning or
  reject a shut down if not doing so would cause Raft groups to drop below
  quorum.
* Change the lease transfer mechanism so a transferrer can transfer its
  timestamp cache's high water mark which would act as the low water mark of the
  recipient's timestamp cache. This is conditioned on not [inserting reads in
  the command queue](https://forum.cockroachlabs.com/t/why-do-we-keep-read-commands-in-the-command-queue/360).

# Unresolved questions
