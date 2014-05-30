# Cockroach [![Build Status](https://secure.travis-ci.org/cockroachdb/cockroach.png)](http://travis-ci.org/cockroachdb/cockroach) [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach)

A Scalable, Geo-Replicated, Transactional Datastore

## Current Status

### May 30th, 2014

It seems that a tweet about Cockroach has brought this project into
the spotlight, making a status update a requirement.

A little history: I started writing the design document
(https://docs.google.com/document/d/11k2EmhLGSbViBvi6_zFEiKzuXxYF49ZuuDJLe6O8gBU/edit?usp=sharing)
in January, 2014. In February, I started some of the implementation,
primarily as a means to really get a handle on Go as a systems
programming language. To that end, I started on the gossip network
because it's a self-contained piece with just enough interesting
complexity to make it non-trivial. At that time, Andy Bonventre
and Shawn Morel started making contributions. All of us have put in
bits and pieces of work on the system on spare nights and weekends.

As things currently stand, the system IS NOT OPERATIONAL. Not even
as a lame demo. That will change soon.

I've been busy trying to align this project with my current employer's
business interests so I can start working on it full time and
hopefully garner some additional manpower along the way. If that
doesn't work out, I will continue the project in my spare time.
Given the amount of interest the system seems to have
garnered (based on recent visitor statistics), I'm going to dedicate
a more consistent effort. It seems the world is ready for an open
source database with these capabilities. It would be a shame not to
make it a reality.

Please, if anyone is interested in contributing, feel free to contact
the developer group at cockroach-db@googlegroups.com.

Spencer Kimball

## Getting and building

A working [Go environment](http://golang.org/doc/code.html) and [prerequisites for building
RocksDB](https://github.com/cockroachdb/rocksdb/blob/master/INSTALL.md) are both presumed.
```bash
go get github.com/cockroachdb/cockroach
cd $GOPATH/src/github.com/cockroachdb/cockroach
./bootstrap.sh
make
```

## Design

Cockroach is a distributed key/value datastore which supports ACID
transactional semantics and versioned values as first-class
features. The primary design goal is global consistency and
survivability, hence the name. Cockroach aims to tolerate disk,
machine, rack, and even datacenter failures with minimal latency
disruption and no manual intervention. Cockroach nodes are symmetric;
a design goal is one binary with minimal configuration and no required
auxiliary services.

Cockroach implements a single, monolithic sorted map from key to value
where both keys and values are byte strings (not unicode). Cockroach
scales linearly (theoretically up to 4 exabytes (4E) of logical
data). The map is composed of one or more ranges and each range is
backed by data stored in [RocksDB][0] (a variant of [LevelDB][1]), and is
replicated to a total of three or more cockroach servers. Ranges are
defined by start and end keys. Ranges are merged and split to maintain
total byte size within a globally configurable min/max size
interval. Range sizes default to target 64M in order to facilitate
quick splits and merges and to distribute load at hotspots within a
key range. Range replicas are intended to be located in disparate
datacenters for survivability (e.g. { US-East, US-West, Japan }, {
Ireland, US-East, US-West}, { Ireland, US-East, US-West, Japan,
Australia }).

Single mutations to ranges are mediated via an instance of a
distributed consensus algorithm to ensure consistency. We’ve chosen to
use the [Raft consensus algorithm][2]. All consensus state is stored in
[RocksDB][0].

A single logical mutation may affect multiple key/value pairs. Logical
mutations have ACID transactional semantics. If all keys affected by a
logical mutation fall within the same range, atomicity and consistency
are guaranteed by [Raft][2]; this is the fast commit path. Otherwise, a
non-locking distributed commit protocol is employed between affected
ranges.

Cockroach provides snapshot isolation (SI) and serializable snapshot
isolation (SSI) semantics, allowing externally consistent, lock-free
reads and writes--both from an historical snapshot timestamp and from
the current wall clock time. SI provides lock-free reads and writes
but still allows write skew. SSI eliminates write skew, but introduces
a performance hit in the case of a contentious system. SSI is the
default isolation; clients must consciously decide to trade
correctness for performance. Cockroach implements a limited form of
linearalizability, providing ordering for any observer or chain of
observers.

Similar to [Spanner][3] directories, Cockroach allows configuration of
arbitrary zones of data. This allows replication factor, storage
device type, and/or datacenter location to be chosen to optimize
performance and/or availability. Unlike Spanner, zones are monolithic
and don’t allow movement of fine grained data on the level of entity
groups.

A [Megastore][4]-like message queue mechanism is also provided to 1)
efficiently sideline updates which can tolerate asynchronous execution
and 2) provide an integrated message queuing system for asynchronous
communication between distributed system components.

[0]: http://rocksdb.org/
[1]: https://code.google.com/p/leveldb/
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
[4]: http://research.google.com/pubs/pub36971.html

-----------------

For design details visit: https://docs.google.com/document/d/11k2EmhLGSbViBvi6_zFEiKzuXxYF49ZuuDJLe6O8gBU/edit?usp=sharing

#### SQL - NoSQL - NewSQL Capabilities

![SQL - NoSQL - NewSQL Capabilities](/resources/doc/sql-nosql-newsql.png?raw=true)
