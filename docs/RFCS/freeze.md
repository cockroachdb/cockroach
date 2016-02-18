- Feature Name: Data/network freeze
- Status: draft
- Start Date: 2016-02-18
- Authors: Ben Darnell
- RFC PR:
- Cockroach Issue:


# Summary

This RFC outlines the plan for freezing our data formats and network
protocols.

# Motivation

We currently make backwards-incompatible changes to data formats
without providing any means to upgrade without data loss. This will
need to stop before beta for obvious reasons.

# Detailed design

## Freeze plan

The freeze will proceed in several steps.

### Stage 0 (now)

Anything goes; changes to on-disk formats do not require any kind of
migration path.

### Stage 1: Guaranteed upgrade path

In stage 1, we require backwards-compatibility with data written by
any previous stage 1 build. It should always be possible to upgrade by
stopping all of the old nodes and then bringing up the new version.
It's OK at this stage if old and new versions cannot be run
concurrently, or if the migration process takes some time (e.g.
rewriting all data before the node can start up).

### Stage 2: Online upgrades

Beginning in stage 2, we require that any upgrade be able to be
performed without taking the cluster offline: old and new nodes must
be able to coexist.

## Affected code

Any code could potentially be affected by the freeze, but areas that
will deserve special scrutiny include:

* All `.proto` definitions
* The packages `keys` and `util/encoding`
* All system tables (defined in `sql/system.go`)

## Migration strategies

It is difficult to come up with a universal migration strategy, since
different changes will require different approaches (for example,
`.proto` changes could perhaps be made by rewriting data on disk at
startup, while changes to key construction may require the change to
be coordinated in a distributed fashion). Therefore we leave the
specifics of a migration process until the need arises.

To facilitate future changes, we will introduce version numbers at
several levels. Initially the behavior around these version numbers
will be conservative and cross-version communication will be limited.
That makes these version numbers a blunt instrument to be reserved for
major changes.

* The on-disk format (via a file that lives outside RocksDB). Servers
  will refuse to load a database with a higher version number than
  they understand.
* The network protocol (via GRPC header). Servers and clients will
  treat a higher version number than they understand as an error.
* Gossip (perhaps via a new node attribute). The rebalance/allocation
  system will not choose to place a replica on a node with a different
  version number.

# Drawbacks

After the freeze, some changes will be much harder to make.

# Alternatives

None.

# Unresolved questions

* When exactly do we begin the freeze?
* Is there anything else worth doing at this point to facilitate future migrations?
