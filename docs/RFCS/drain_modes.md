- Feature Name: drain_modes
- Status: draft
- Start Date: 2016-04-25
- Authors: Tobias Schottdorf (tobias.schottdorf@gmail.com), Alfonso Subiotto Marqués
- RFC PRs: [#6283](https://github.com/cockroachdb/cockroach/pull/6283), [#10765](https://github.com/cockroachdb/cockroach/pull/10765)
- Cockroach Issue: [#9541](https://github.com/cockroachdb/cockroach/issues/9541), [#9493](https://github.com/cockroachdb/cockroach/issues/9493), [#6295](https://github.com/cockroachdb/cockroach/issues/6295)

# Summary
Propose two reduced modes of operation of a running CockroachDB node.

* `drain-clients` mode: the server lets on-going SQL clients finish up to some
  deadline when context cancellation and cleanup is performed, and then politely
  refuses new work at the gateway in the manner which popular load-balancing SQL
  clients can handle best.
* `drain-leases` mode: all range leases are transferred away from the node, new
  ones are not granted (in turn disabling most queues and active gossipping) or
  accepted. The draining node also declines the replication of a range to it.

These modes are not related to the existing `Stopper`-based functionality and
are not one-way (i.e. a server can temporarily run in `drain-clients` mode).
Typically, to avoid unavailability, a server under maintenance will move from
running to `drain-clients` to both.

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
  new clients and finish open work up to some deadline, then transfer/release
  all leases to avoid a period of unavailability, and only then initiate
  shutdown via `stopper.Stop()`. This clean shutdown avoids:
    * Blocking schema changes on the order of minutes because table leases are
    not properly released [#9493](https://github.com/cockroachdb/cockroach/issues/9493)
    * Free-for-all per-range activity to pick up expired epoch-based range
    leases.
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
* Improving compatibility with server health-checking mechanisms done by third
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
part of a transaction. The `v3conn` will also be made aware of any cancellation
of its session's context which will be handled in the same way.

A `WaitGroup` field will be added both to `pgwire.Server` and `sql.Session`.
This will be used for the `pgwire.Server` to wait until there are no `Session`s
left. All active sessions will be found through the session registry
([#10317](https://github.com/cockroachdb/cockroach/pull/10317/)).

Additionally, the admin server (our only other source of new sessions) will stop
creating new sessions once in `draining` mode.

After `draining` behavior is enabled we have three classes of clients
characterized by the behavior of the server (note that we do not accept more
clients):
* Blocked in the read loop with no active transaction.
* Blocked in the read loop with an active transaction.
* Not blocked in the read loop.

The first class will be taken care of by a first pass that checks for no active
transaction (`session.TxnState.State == NoTxn`) and canceling the session
context if so.

The second and third class will be given a deadline of `drainMaxWait` (default
of 10s) to finish any work. Note that once work is complete, the `v3conn` will
not block in the read loop and will instead exit. Once `drainMaxWait` has
elapsed, the context of any active session will be canceled. Since a derived
context is used to send RPCs, these RPCs will be canceled. Additionally,
plumbing will have to be added to important local work done on nodes so that
this context cancellation leads to the interruption of this work.

The `WaitGroup` will be used to wait for all sessions to finish.

The final stage is to execute `DELETE FROM system.lease WHERE nodeID = nodeID`.
When closing sessions, the refcount of held leases was decremented. However, the
leases themselves are not deleted from the lease table unless the lease is for
an outdated version of a table.

### Note on closing SQL connections
The load balancing solutions for Postgres that seem to be the most popular are [PGPool](http://www.pgpool.net/docs/latest/pgpool-en.html) and
[HAProxy](https://www.haproxy.com/). Both systems have health check mechanisms
that try to establish a SQL
connection[[1]](http://www.pgpool.net/docs/latest/pgpool-en.html#HEALTH_CHECK_USER)
[[2]](https://www.haproxy.com/doc/aloha/7.0/haproxy/healthchecks.html#checking-a-pgsql-service).
HAProxy also supports doing only generic TCP checks. Because of how these health
checks are performed, sending back a SQL error during the establishment of a
connection will result in the draining node being correctly marked as down.

For failures during an existing or new session, PGPool has a
`fail_over_on_backend_error` option which is triggered when

> 57P01 ADMIN SHUTDOWN
> admin_shutdown

Is received from the backend. PGPool will retain session information under some conditions[[3]](http://pgsqlpgpool.blogspot.com/2016/07/avoiding-session-disconnection-while.html)
and can reconnect transparently to a different backend.

HAProxy does not have similar functionality and both SQL/TCP errors are
forwarded to the client.

We should therefore return

> 57P01 ADMIN SHUTDOWN
> admin_shutdown

To reject new connections and close existing ones for compatibility with
PGPool's automatic failover and both PGPool's and HAProxy's health checks.
However, clients should retry in the case that HAProxy is being used and want
transparent failover when the establishment of a connection errors out.

## Node/drain-leases mode:
`(*server.Node).DrainLeases(bool)` iterates over its store list and delegates
to all contained stores. `(*Replica).redirectOnOrAcquireLease` checks
with its store before requesting a new or extending an existing lease.

Any leases that the node currently holds will be transferred away using
`AdminTransferLease(target)` where target will be found using
`Allocator.TransferLeaseTarget`.
After the lease is transferred, the replica will wait for its command queue to
drain.

`StoreDescriptor` will be extended with a `draining` field which will be taken
into account in `Allocator.TransferLeaseTarget`, resulting in no leases being
transferred to a node that is known to be draining. A draining replica will
return a `LeaseRejected` error in `applyNewLeaseLocked` in the case that a lease
is transferred to it before the gossiped `StoreDescriptor` has reached the
source of the transfer.

`Allocator.AllocateTarget` will be modified in the same way so that ranges are
not replicated to a draining node. A draining node will decline a snapshot in
`HandleSnapshot` if its store is in `drain-leases` mode.

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
  forcibly close these connections. It might also make sense to offer clients to
  set this timeout via an environment variable.
* Reacquire table leases when the node goes back up. This was suggested in
  [#9493](https://github.com/cockroachdb/cockroach/issues/9493) but does not fix
  the issue of having to wait for the lease to expire if the node does not come
  back up.


# Future work
* Moving ranges to another node if we're draining the node for decommissioning.
Not doing this could affect QPS as the draining node's removal from the raft
group could block writes. However, this healing happens down the road anyway and
trying to do this could end up adding too much complexity. Without a certainty
that this is necessary, it would be best to defer this until it becomes an issue.

# Unresolved questions
