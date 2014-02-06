Cockroach
=========

A Scalable, Geo-Replicated, Transactional Datastore

Cockroach is a distributed key:value datastore which supports ACID
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
backed by data stored in leveldb and replicated to a total of three or
more cockroach servers. Ranges are defined by start and end
keys. Ranges are merged and split to maintain total byte size within a
globally configurable min/max size interval. Range sizes default to
target 64M in order to facilitate quick splits and merges and to
distribute load at hotspots within a key range. Range replicas are
intended to be located in disparate datacenters for survivability
(e.g. { US-East, US-West, Japan }, { Ireland, US-East, US-West}, {
Ireland, US-East, US-West, Japan, Australia }).

Single mutations to ranges are mediated via an instance of a
distributed consensus algorithm to ensure consistency. We’ve chosen to
use the Raft consensus algorithm. All consensus state is stored in
leveldb.

A single logical mutation may affect multiple key/value pairs. Logical
mutations have ACID transactional semantics. If all keys affected by a
logical mutation fall within the same range, atomicity and consistency
are guaranteed by Raft; this is the fast commit path. Otherwise, a
non-locking distributed commit protocol is employed between affected
ranges.

Cockroach provides snapshot isolation (SI) and serializable snapshot
isolation (SSI) semantics, allowing externally consistent, lock-free
reads--both from an historical snapshot timestamp and from the current
wall clock time. SI provides wait-free reads and writes but still
allows write skew. SSI eliminates write skew, but introduces a
performance hit in the case of a contentious system. SSI is the
default isolation; clients must consciously decide to trade
correctness for performance. Cockroach implements a limited form of
linearalizability, providing ordering for any observer or chain of
observers.

Similar to Spanner directories, Cockroach allows configuration of
arbitrary zones of data. This allows replication factor, storage
device type, and/or datacenter location to be chosen to optimize
performance and/or availability. Unlike Spanner, zones are monolithic
and don’t allow movement of fine grained data on the level of entity
groups.

A Megastore-like message queue mechanism is also provided to 1)
efficiently sideline updates which can tolerate asynchronous execution
and 2) provide an integrated message queuing system for asynchronous
communication between distributed system components.

-----------------

For design details visit: https://docs.google.com/document/d/11k2EmhLGSbViBvi6_zFEiKzuXxYF49ZuuDJLe6O8gBU/edit?usp=sharing

