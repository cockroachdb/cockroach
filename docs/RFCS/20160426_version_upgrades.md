- Feature Name: Version upgrades
- Status: rejected
- Start Date: 2016-04-10
- Authors: Tobias Schottdorf
- RFC PR: [#5985](https://github.com/cockroachdb/cockroach/pull/5985)
- Cockroach Issue:

# Rejection notes

This RFC lead us to reconsider Raft proposals and lead us to consider (and
decide for) leaseholder-evaluated Raft (#6166) instead, which makes Raft migrations
much rarer. We will likely eventually need some of the ideas brought forth
in this RFC, but in a less all-encompassing setting best considered then.

# Summary

**This is a draft but certainly not the solution. It serves mostly to inspire
discussion and to hopefully iterate on. See the "Drawbacks" section and model
cases below.**

Come up with a basic framework for dealing with migrations. We require a
migration path from the earliest beta version (or, at least from an early beta
version) into 1.0 and beyond. There are two components: the big picture and a
potentially less powerful version which we can quickly implement to keep
development (relatively) seamless.

# Motivation

Almost every small change in Raft requires a proper migration story. Even
seemingly harmless changes (making a previously nullable field non-nullable) do
since they can let Replicas which operate at different versions diverge.
It's a no-brainer that we need a proper migration progress, with the holy grail
being rolling updates (though stop-the-world is acceptable at least during
beta).

Of course, versioning doesn't stop at Raft and, as we'll see, changes in Raft
quickly spider out of control. This is a first stab at and collection of
possible issues.

# What (some) others do

## VoltDB

https://docs.voltdb.com/AdminGuide/MaintainUpgradeVoltdb.php

The primary mode is shutdown everything, replace, restart. Otherwise, setup
two clusters and replicate between them; upgrade the actual cluster only after
having promoted the other cluster. This seems really expensive and involves
copying all of the production data. My guess is that folks just use the first
option in practice.

## RethinkDB

As usual, they seem to be on the right track (but they might also be in a less
complicated spot than we are):

http://www.rethinkdb.com/docs/migration/

> 1.16 or higher: Migration is handled automatically. (This is also true for
> upgrading from 1.14 onward to versions earlier than 2.2.) After migration,
> follow the “Rebuild indexes” directions.
1.13–1.15: Upgrade to RethinkDB 2.0.5 first, rebuild the secondary indexes by
following the “Rebuild indexes” directions, then upgrade to 2.1 or higher.
(Migration from 2.0.5 to 2.1+ will be handled automatically.)
1.7–1.12: Follow the “Migrating old data” directions.
1.6 or earlier: Read the “Deprecated versions” section.

This looks like for recent versions, you just stop the process, replace the
binary and run the new thing. I did not see anything indicating online
migrations, so this is similar to VoltDB's first option.

## Percona XtraDB Cluster

Seems relatively complicated, but it is an online process.

https://www.percona.com/doc/percona-xtradb-cluster/5.6/upgrading_guide_55_56.html

## Cassandra

Rolling restarts, relatively straightforward it seems. Essentially, draining
the node, stop it, update the version, start the new version. It appears that
downgrading is more involved (there's no clear path except through a data dump
and starting from scratch with the old version). They also have it easier than
us.

http://docs.datastax.com/en/archived/cassandra/1.2/cassandra/upgrade/upgradeChangesC_c.html

# High level design

Bundle all execution methods (everything multiplexed to by `executeCmd`) in a
collection `ReplicaMethods` which is to be specified (but sementically
speaking, it is a map of command to implementation of the command).
It is this entity which is being versioned. Add `var MaxReplicaVersion int64`
to the `storage` package. The version stored there is the version of the
current code, and marks that and all previous versions as supported (at some
point, we'll likely want a lower bound as well).

In a nutshell, each `*Replica` keeps track of its active version (persisted to
a key and cached in-memory) and uses an appropriately synthesized
`ReplicaMethods` to execute (Raft and read) commands. A migration occurs as a
new `ChangeVersion` Raft command is applied (it is tbd how `ChangeVersion`
itself is versioned) and replaces the synthesized version of `ReplicaMethods`
with one corresponding to the new version.

## User-facing

### Upgrades

The most obvious transition is a version upgrade. In the happy case, all
replicas have a version that is higher than that requested by `ChangeVersion`;
they can seamlessly process all following commands with the new semantics.
It is this case which is easily achieved: stop all the nodes, update all the
binaries, restart the cluster (it operates at the old version), and then
trigger an online update. Or, for a rolling upgrade, stop and update the nodes
in turn, and then trigger the upgrade - two methods, same outcome.

The unhappy upgrade case corresponds to not having upgraded all binaries before
sending `ChangeVersion`. The only correct option is to die - nothing can be
processed any more since the semantics are unknown. This case should be avoided
by user-friendly tooling - an "upgrade trigger" could periodically check
until all of the Replicas can be upgraded, with appropriate warnings logged.
It should be possible we can automatically update after a rolling cluster
restart in that way, but we may prefer to keep it explicit (`./cockroach update
latest`?).

### Downgrades

Users will want to be able to undo an upgrade in case they encountered a newly
introduced bug or degradation. There are two types of downgrades: Either, it's
only the `ReplicaVersion` which must decrease (i.e. keep the new binary, but
run old `ReplicaMethods`), or it is the binary itself which needs to be
reverted to an older build (i.e. likely a bug unrelated to this versioning).

The first case is trivial: Send an appropriate `ChangeVersion`. In the second
case, we do as in the first case, and then we can do a (full or rolling)
cluster restart with the desired (older) binary.

## Developer-facing

The developer-facing side of this system should be easy to understand and
maintain. We need to keep old versions of functionality, but they should be
clearly recognizable as such and easy to "ignore". Likewise, we need lints
to make sure that a relevant code change results in the proper migration, and
need to be able to test migrations. For this, we

* augment test helpers and configurations so that servers/replicas can be spun
  up at any version (defaulting to `ReplicaMaxVersion`) to make it easy to test
  specific versions (or even to run tests against ranges of versions).
* keep the "latest" version of the commands in a distinguished position (close
  to where they are now, with mild refactoring to establish some independence
  from `(*Replica)`.
* when changing the semantics of a command, copy the "old" code to its
  designated new location keyed by the (now old) version ID (for example,
  `./storage/replica_command.125.RequestLease.go`):
  ```go
  versionedCommands[125][RequestLease] = func(...) (...) { ... }
  ```

Let's look at some model cases for this naive approach.

### Model case: Simple behavioral change

Assume we change `BeginTransaction` so that it fails unless the supplied
transaction proto has `Heartbeat.Equal(OrigTimestamp)`. It's tempting to
achieve that by wrapping around the previous version, but doing that a couple
of times would render the code unreadable. Instead, copy the existing code
out as described above for the previous version and update the new code
in-place. This is the case in which the system actually works.

### Model case: Unintended behavioral change

Assume someone decides that our heartbeat interval needs to be smaller
(motivated by something in the coordinator) and changes
`base.DefaultHeartbeatInterval` without thinking it through. That variable
is used in `PushTxn` to decide whether the transaction record is abandoned, so
during an update we might have transactions which are both aborted and not
aborted. Many such examples exist and I'm afraid even Sysiphus could get tired
of finding and building lints around them all.

### Model case: Simple proto change

Assume we change `(*Transaction).LastHeartbeat` from a nullable to a
non-nullable field (as in #5753). This is still very tricky because the logic
hides in the protobuf generated code - encoding a zero timestamp is different
from omitting the field completely (as would happen with a nullable one), but
using the new generated code instantly switches on this changed behavior. Thus,
we must make a copy of all commands which serialize the Transaction proto and
replace the marshalling with one that (somehow) makes sure the timestamp is
skipped when zero, in all previous versions. That's correct if we're sure that
we never encountered (and never will) a non-nil zero LastHeartbeat.

Proto changes are quite horrible. See this next example (#5845)

### Model case: adding a proto field

Same as the nullability change. All old versions of the code must not use the
new marshalling code. The old version of the struct and its generated code
must be kept in a separate location. For example, when adding a field `MaxOffset`
to `RequestLease`, the previous version of `RequestLease` and its code are kept
at `./storage/replica_command.<vers>.go` and the old version converts
`roachpb.RequestLease` to `OldRequestLease` (dropping the new field) and then
marshals that to disk.
Of course the lease is also accessed from other locations (loading the
lease), so that requires versioning as well, leading to, essentially, a mess.

See #5817 for a more involved change which requires additions to a proto and
associated changes in `ConditionalPut`.

### Model case: adding a new Raft command

We've talked repeatedly about changing the KV API to support more powerful
primitives. This would be a sweeping change, but a basic building block is
introducing a new Raft command. Theoretically, both up- and downgrading these
should be possible in the same ways as discussed before, but there's likely
additional complexity.

# Drawbacks

See above. A lot of code duplication and it's difficult to enforce that
anything which needs a migration has that pointed out to it. Some of that
might be remedied through refactoring (moving all replica commands into
their own package and lint against access across that package boundary).

The versioning scheme here will work well only if the changes are confined to
within the Raft commands and don't affect serialization. Proto changes are
going to be somewhat more involved, even in simple cases.

# Alternatives

One issue with the above is the code complexity involved in migrations. This
could potentially be attenuated by

## Embracing adjacent versions more

Supporting upgrades only between adjacent versions could enable us to automate
a lot of the versioning effort (by automatically keeping two versions of the
protobufs, Raft commands, etc) and requiring less hand-crafting. The obvious
down-side is that users need to upgrade versions one at a time, which can be
very annoying. This could be attenuated partially by supplying an external
binary which runs the individual steps (`cockroach-update latest`) one after
another.

## Embrace stop-the-world more
The complexity in the current migration is due to a new binary having to act
exactly like an old binary until the version trigger is pulled. That almost
translates to having all previous versions of the code base embedded in it, and
being able to switch atomically (actually even worse, atomically on each
Replica). Allowing each binary to just be itself early on sounds much less
involved.

However, not having the version switch orchestrated through Raft presents other
difficulties: We won't be able to do rolling updates, and even for offline
updates we must make sure that all nodes are stopped with the same part of the
logs applied, and no unapplied log entries present (as it's not clear what
version they've been proposed with). That naturally leads again to a Raft
command:

The update sends a `ChangeVersion` (which is more of a `StopForUpgrade` command
in this section) to all Replicas, with the following semantics:
* It's the highest committed command when it applies (i.e. the Replica won't
  acknowledge receipt of additional log entries after the one containing
  `ChangeVersion`) and,
* upon application, the Replica stalls (but not the node,
  to give all replicas a change to stall). Only then
* the process exits (potentially after preparing the data for the desired
  version if it's a downgrade).
* When the process restarts, it checks that it's running the desired version of the binary, performs any data
  upgrades it has to do, and then resumes operation.

As long as the data changes are reversible, this should do the trick.
There's a chance of leaving the cluster in an undefined state should the
migration crash (we likely won't be able to do it atomically, only on each
replica, but that could be remedied with tooling (to manually drive the process
forward). In effect, this would version everything by binary version.

For example,
* when adding a (non-nullable) proto field to `Lease`, the
forward and background migrations would simply delete all `Lease` entries
(since that is the simplest option and works fine: the only enemy is setting
range leases with a zero value where there shouldn't be one)
* the `LastHeartbeat` nullability change would not have to migrate anything.
* more complicated proto changes could still require taking some of the actual
  old marshalling code to transcode parts of the keyspace in preparation for
  running under a different version.

## Embrace inconsistency more
Maybe some inconsistencies can be acceptable (encoding a zero timestamp vs. nil
timestamp, etc) and trying to maintain consistency seamlessly over migrations
isn't useful while running a mixed cluster. Instead, disable (panicking on
failed) consistency checks while the cluster is mixed-version, and re-enable it
after a migration queue has processed the replica set.

# Unresolved questions
Many, see above.
