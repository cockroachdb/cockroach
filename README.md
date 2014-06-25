# Cockroach [![Build Status](https://secure.travis-ci.org/cockroachdb/cockroach.png)](http://travis-ci.org/cockroachdb/cockroach)  [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach) ![Project Status](http://img.shields.io/badge/status-alpha-red.svg) 

A Scalable, Geo-Replicated, Transactional Datastore

## Status

**ALPHA**

* Gossip network
* Cluster initialization and joining
* Basic Key-Value REST API
* NO: Raft consensus, range splitting, transactions (!)

## Simple Setup

* node1> ./cockroach init hdd=path
* node1> ./cockroach start -gossip=:8081 -rpc_addr=:8081 -http_adr=:8080 -data_dirs=hdd=path

* node2> ./cockroach start -gossip=node1:8081 -http_adr=:8080 -data_dirs=hdd=path

## Next Steps

* More versatile store descriptions
* Flesh out node allocation via gossip
* Range splitting

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

[0]: http://rocksdb.org/
[1]: https://code.google.com/p/leveldb/
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
[4]: http://research.google.com/pubs/pub36971.html

-----------------

#### SQL - NoSQL - NewSQL Capabilities

![SQL - NoSQL - NewSQL Capabilities](/resources/doc/sql-nosql-newsql.png?raw=true)
