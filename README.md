<img style="float: right" src="/resources/doc/color_cockroach.png?raw=true"/>

# Cockroach [![Build Status](https://secure.travis-ci.org/cockroachdb/cockroach.png)](http://travis-ci.org/cockroachdb/cockroach)  [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach) ![Project Status](http://img.shields.io/badge/status-alpha-red.svg)

A Scalable, Geo-Replicated, Transactional Datastore

## Status

**ALPHA**

* Gossip network
* Cluster initialization and joining
* Basic Key-Value REST API
* NO: Raft consensus, range splitting, transactions (!)

## Next Steps

See [TODO](https://github.com/cockroachdb/cockroach/blob/master/TODO)

## Instructions for building Cockroach Docker Container

* Follow the [instructions for installing Docker on your host
system](http://docs.docker.com/installation/).
*        (cd deploy ; ./build-docker.sh)

## Local Cluster Setup

*        (cd deploy; ./local-cluster.sh [start|stop])

## Get in touch

+ cockroach-db@googlegroups.com
+ \#cockroachdb on freenode

## Contributing

See [CONTRIBUTING.md](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md)

## Design

For full design details, see the [original design doc](https://docs.google.com/document/d/11k2EmhLGSbViBvi6_zFEiKzuXxYF49ZuuDJLe6O8gBU/edit?usp=sharing).

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

#### SQL - NoSQL - NewSQL Capabilities

![SQL - NoSQL - NewSQL Capabilities](/resources/doc/sql-nosql-newsql.png?raw=true)

## Architecture

Cockroach implements a layered architecture, with various
subdirectories implementing layers as appropriate. The highest level of
abstraction is the SQL layer (currently not implemented). It depends
directly on the [structured data API][5] ([structured/][6]). The structured
data API provides familiar relational concepts such as schemas,
tables, columns, and indexes. The structured data API in turn depends
on the [distributed key value store][7] ([kv/][8]). The distributed key
value store handles the details of range addressing to provide the
abstraction of a single, monolithic key value store. It communicates
with any number of [cockroach nodes][9] ([server/][10]), storing the actual
data. Each node contains one or more [stores][11] ([storage/][12]), one per
physical device.

![Cockroach Architecture](/resources/doc/architecture.png?raw=true)

Each store contains potentially many ranges, the lowest-level unit of
key-value data. Ranges are replicated using the [Raft][2] consensus
protocol. The diagram below is a blown up version of stores from four
of the five nodes in the previous diagram. Each range is replicated
three ways using raft. The color coding shows associated range
replicas.

![Range Architecture Blowup](/resources/doc/architecture-blowup.png?raw=true)

[0]: http://rocksdb.org/
[1]: https://code.google.com/p/leveldb/
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
[4]: http://research.google.com/pubs/pub36971.html
[5]: http://godoc.org/github.com/cockroachdb/cockroach/structured
[6]: https://github.com/cockroachdb/cockroach/tree/master/structured
[7]: http://godoc.org/github.com/cockroachdb/cockroach/kv
[8]: https://github.com/cockroachdb/cockroach/tree/master/kv
[9]: http://godoc.org/github.com/cockroachdb/cockroach/server
[10]: https://github.com/cockroachdb/cockroach/tree/master/server
[11]: http://godoc.org/github.com/cockroachdb/cockroach/storage
[12]: https://github.com/cockroachdb/cockroach/tree/master/storage
