- Feature Name: drain_modes
- Status: completed
- Start Date: 2016-04-25
- Authors: Tobias Schottdorf (tobias.schottdorf@gmail.com)
- RFC PR: #6283
- Cockroach Issue:


# Summary

Propose two reduced modes of operation of a running CockroachDB node.

* `drain-clients` mode: the server lets on-going SQL clients finish (up to some
  deadline) and then politely refuses new work at the gateway in the manner
  which popular load-balancing SQL clients can handle best.
* `drain-leases` mode: roughly speaking, all range leases expire and new
  ones are not granted (in turn disabling most queues and active gossipping).

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

However, not that we're looking at clusters and their maintenance, this simple
"drop-all" approach isn't enough. See, for example #6198, #6197 or #5279. This
small RFC outlines proposed changes to accommodate use cases such as

* clean shutdown: instead of dropping everything on the floor, politely block
  new clients and finish open work, then expire all leases to avoid a period
  of unavailability, and only then initiate shutdown via `stopper.Stop()`.
* draining data off a node (i.e. have all or some subset of ranges migrate
  away, and wait until that has happened).
  Used for decommissioning, but could also be used to change hard-drives
  cleanly (i.e. drain data off a store, clean shutdown, switch hdd, start).
* decommission (permanently remove), which is "most of the above" with the
  addition of announcing the downtime as forever to the cluster.
* drain for update/migration (see the `{F,Unf}reeze` proposal in #6166):
  Essentially here we'll want to have the cluster in drain-clients mode and then
  propose a `Freeze` command on all Ranges.


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

The implementations should be straightforward and are kept brief.

## SQL/drain-clients mode:

Essentially `ServeConn` in the presence of a deadline should politely refuse
new connections with an appropriate error code such as

> 08004  SQLSERVER REJECTED ESTABLISHMENT OF SQLCONNECTION
> sqlserver_rejected_establishment_of_sqlconnection")

(http://www.postgresql.org/docs/9.0/static/errcodes-appendix.html)

and a similar appropriate error for existing connections which try to issue
SQL commands after the deadline has expired.

We should try to close existing connections while they're in-between
transactions (i.e. after a command finishes but before we send serverMsgReady).
If a command comes in while the server is draining, we should execute it and
then close afterwards. If no command comes in then we can just close the
connection (which will race against the possibility that a client is sending a
command at that time, but there's not much we can do about that in the
postgresql protocol).

We should also consider this in the context of file descriptor limits (#6230):
if we're close to the limit we would like to be able to shed load by draining
idle connections.

If there is no good support for these error codes, possibly the right solution
is to unbind the listener. @bdarnell commented:

> We should do some research about how widely-supported these error codes are.
For example, pgbouncer doesn't seem to do anything with them. I'm concerned
that in practice the best way to load balance clients may turn out to be a
tcp-level tool like haproxy, in which case we may have to use cruder techniques
(e.g. closing the listening socket, which would also imply re-separating pgport
from the unified port).

This is considered outside of the scope of the RFC, but issue #6295 was filed.

## Node/drain-leases mode:

`(*server.Node).DrainLeases(bool)` iterates over its store list and delegates
to all contained stores. `(*Replica).redirectOnOrAcquireLease` checks
with its store before requesting a new or extending an existing lease.

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

We could augment `*Stopper`, but that seems more trouble than it's worth since
the required functionality does not generalize well.

# Unresolved questions
