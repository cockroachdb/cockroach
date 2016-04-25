- Feature Name: drain_modes
- Status: draft
- Start Date: 2016-04-25
- Authors: Tobias Schottdorf (tobias.schottdorf@gmail.com)
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)


# Summary

Propose three (or rather, two new) modes of operation of a running CockroachDB node and the
transitions between them:

* active mode: the server operates normally. Can change from/to:
* no-client mode: the server lets on-going SQL clients finish (up to some
  deadline) and then politely refuses new work at the gateway in the manner
  which popular load-balancing SQL clients can handle best. Can change from/to:
* passive mode: roughly speaking, all leader leases expire and new ones are
  not granted (in turn disabling most queues and active gossipping).

These modes are not related to the existing `Stopper`-based functionality.

# Motivation

In our Stopper usage, we've taken to fairly ruthlessly shutting down service of
many components once `(*Stopper).Quiesce()` is called. In code, this manifests
itself through copious use of the `(*Stopper).ShouldDrain()` channel even when
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
  Essentially here we'll want to have the cluster in passive mode and then
  propose a `Freeze` command on all Ranges.


# Detailed design

Implement the following:

```go
// Put the SQL server into no-client mode within deadline, cancelling
// connections as necessary. A zero deadline undoes the effects of any
// prior drain operation.
(*pgwire.Server).Drain(deadline time.Time) error

// Put the Node into passive mode if requested, or undo any previous such
// mode change.
(*server.Node).SetPassive(bool) error
```

The implementations should be straightforward and are kept brief.

## SQL/no-client mode:

Essentially `ServeConn` in the presence of a deadline should politely refuse
new connections with an appropriate error code such as

> 08004  SQLSERVER REJECTED ESTABLISHMENT OF SQLCONNECTION
> sqlserver_rejected_establishment_of_sqlconnection") 

(http://www.postgresql.org/docs/9.0/static/errcodes-appendix.html)

and a similar appropriate error for existing connections which try to issue
SQL commands after the deadline has expired.

## Node/passive mode:

`(*server.Node).SetPassive(bool)` iterates over its store list and delegates
to all contained stores. `(*Replica).redirectOnOrAcquireLeaderLease` checks
with its store before requesting a new or extending an existing lease.

## Server/adminServer:

```go

type DrainMode int
const (
  Active DrainMode = iota
  NoClient
  Passive
)

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
