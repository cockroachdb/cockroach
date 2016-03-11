![logo](/resource/doc/cockroach_db.png?raw=true "Cockroach Labs logo")


[![Circle CI](https://circleci.com/gh/cockroachdb/cockroach.svg?style=svg)](https://circleci.com/gh/cockroachdb/cockroach) [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach) ![Project Status](https://img.shields.io/badge/status-alpha-red.svg) [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cockroachdb/cockroach?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## A Scalable, Survivable, Strongly-Consistent SQL Database

**Table of Contents**

- [What is CockroachDB](#what-is-cockroachdb)
- [Status](#status)
- [Running CockroachDB Locally](#running-cockroachdb-locally)
- [Deploying CockroachDB in the cloud](#deploying-cockroachdb-in-the-cloud)
- [Running a multi-node cluster](#running-a-multi-node-cluster)
- [Getting in touch and contributing](#get-in-touch)
- [Talks](#talks)
- [Design](#design) and [Datastore Goal Articulation](#datastore-goal-articulation)
- [Architecture](#architecture) and [Client Architecture](#client-architecture)

## What is CockroachDB

CockroachDB is a distributed SQL database built on top of a transactional and consistent key:value store. The primary design goals are support for ACID transactions, horizontal scalability, and survivability, hence the name. CockroachDB implements a Raft consensus algorithm for consistency. It aims to tolerate disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention. CockroachDB nodes (RoachNodes) are symmetric; a design goal is homogeneous deployment (one binary) with minimal configuration.

## Status

CockroachDB is currently in alpha. See our
[Roadmap](https://github.com/cockroachdb/cockroach/issues/2132) and
[Issues](https://github.com/cockroachdb/cockroach/issues) for a list of features planned or in development.

## Running CockroachDB Locally

### Environment Setup

#### Native (read: without Docker)

* set up the dev environment (see [CONTRIBUTING.md](CONTRIBUTING.md))
* `make build`

#### Using Docker

Install Docker! On OSX ([official docs](https://docs.docker.com/engine/installation/mac/#from-your-shell)):
```bash
# install docker and docker-machine:
$ brew install docker docker-machine
# install VirtualBox:
$ brew cask install virtualbox
# create the VM (this will also start it):
$ docker-machine create --driver virtualbox default
# if the VM exists but isn't running, start it:
$ docker-machine start default
# set up the environment for the docker client:
$ eval $(docker-machine env default)
```
Other operating systems will have a similar set of commands. Please check Docker's documentation for more info.

Pull the CockroachDB Docker image and drop into a shell within it:
```bash
docker pull cockroachdb/cockroach
docker run -p 26257:26257 -t -i cockroachdb/cockroach shell
# root@82cb657cdc42:/cockroach#
```

### Bootstrap and talk to a single node

Note: If you’re using Docker as described above, run all the commands described below in the container’s shell.

Setting up Cockroach is easy, but starting a test node is even easier. All it takes is running:

```bash
./cockroach start --insecure &
```

Verify that you're up and running by visiting the cluster UI. If you're running
without Docker (or on Linux), you'll find it at
[localhost:26257](http://localhost:26257); for OSX under Docker, things are a
little more complicated and you need to run `docker-machine ip default` to get
the correct address (but the port is the same).

##### Built-in client

Now let's talk to this node. The easiest way to do that is to use the `cockroach` binary - it comes with a built-in sql client:

```bash
./cockroach sql --insecure
# Welcome to the cockroach SQL interface.
# All statements must be terminated by a semicolon.
# To exit: CTRL + D.
192.168.99.100:26257> show databases;
+----------+
| Database |
+----------+
| system   |
+----------+
192.168.99.100:26257> SET database = system;
OK
192.168.99.100:26257> show tables;
+------------+
|   Table    |
+------------+
| descriptor |
| eventlog   |
| lease      |
| namespace  |
| rangelog   |
| reporting  |
| users      |
| zones      |
+------------+
```

Check out `./cockroach help` to see all available commands.


## Deploying CockroachDB in the cloud

For a sample configuration to run an insecure CockroachDB cluster on AWS using [Terraform](https://terraform.io/),
see [cloud deployment](https://github.com/cockroachdb/cockroach/tree/master/cloud/aws).

## Running a multi-node cluster

We'll set up a three-node cluster below.

The code assumes that `$NODE{1,2,3}` are the host names of the three nodes in the cluster.

```bash
# Create certificates
./cockroach cert create-ca
./cockroach cert create-node 127.0.0.1 ::1 localhost $NODE1 $NODE2 $NODE3
./cockroach cert create-client root
# Distribute certificates
for n in $NODE1 $NODE2 $NODE3; do
  scp -r certs ${n}:certs
done
```

Now, on node 1, initialize the cluster (this example uses `/data`; yours may vary):

```bash
./cockroach start --store=/data1
```

Then, add nodes 2, 3, etc. to the cluster by specifying the `--join` flag to connect to any already-joined node.

```bash
./cockroach start --store=/data2 --join=${NODE1}:26257
```

Verify that the cluster is connected on the web UI by directing your browser at
```
https://<any_node>:8080
```

## Get in touch

We spend almost all of our time here on GitHub, and use the [issue
tracker](https://github.com/cockroachdb/cockroach/issues) for
bug reports.

For development related questions and anything else, message our mailing list at [cockroach-db@googlegroups.com](https://groups.google.com/forum/#!forum/cockroach-db). We recommend joining before posting, or your messages may be held back for moderation.

### Contributing

We're an Open Source project and welcome contributions.
See [CONTRIBUTING.md](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md) to get your local environment set up.
Once that's done, take a look at our [open issues](https://github.com/cockroachdb/cockroach/issues/), in particular those with the [helpwanted label](https://github.com/cockroachdb/cockroach/labels/helpwanted), and follow our [code reviews](https://github.com/cockroachdb/cockroach/pulls/) to learn about our style and conventions.

## Talks

* [Venue: Annual RocksDB meetup at Facebook HQ](https://www.youtube.com/watch?v=-ij2OiDTxz0), by [Spencer Kimball] (https://github.com/spencerkimball) on (12/02/2015), 21min.<br />
  CockroachDB's MVCC model.
* [Venue: Code Driven NYC](https://www.youtube.com/watch?v=tV-WXM2IJ3U), by [Spencer Kimball] (https://github.com/spencerkimball) on (10/28/2015), 30min.<br />
  Architecture & Overview.
* [Venue: Golang UK Conference 2015](https://www.youtube.com/watch?v=33oqpLmQ3LE), by [Ben Darnell](https://github.com/bdarnell) on (08/21/2015), 52min.<br />
* [Venue: Data Driven NYC](https://youtu.be/TA-Jw78Ms_4), by [Spencer Kimball] (https://github.com/spencerkimball) on (06/16/2015), 23min.<br />
  A short, less technical presentation of CockroachDB.
* [Venue: NY Enterprise Technology Meetup](https://www.youtube.com/watch?v=SXAEZlpsHNE), by [Tobias Schottdorf](https://github.com/tschottdorf) on (06/10/2015), 15min.<br />
  A short, non-technical talk with a small cluster survivability demo.
* [Venue: CoreOS Fest](https://www.youtube.com/watch?v=LI7uaaYeYmQ), by [Spencer Kimball](https://github.com/spencerkimball) on (05/27/2015), 25min.<br />
  An introduction to the goals and design of CockroachDB. The recommended talk to watch if all you have time for is one.
* [Venue: The Go Devroom FOSDEM 2015](https://www.youtube.com/watch?v=ndKj77VW2eM&index=2&list=PLtLJO5JKE5YDK74RZm67xfwaDgeCj7oqb), by [Tobias Schottdorf](https://github.com/tschottdorf) on (03/04/2015), 45min.<br />
  The most technical talk given thus far, going through the implementation of transactions in some detail.

### Older talks

* [Venue: The NoSQL User Group Cologne](https://www.youtube.com/watch?v=jI3LiKhqN0E), by [Tobias Schottdorf](https://github.com/tschottdorf) on (11/5/2014), 1h25min.
* [Venue: Yelp!](https://www.youtube.com/watch?feature=youtu.be&v=MEAuFgsmND0), by [Spencer Kimball](https://github.com/spencerkimball) on (9/5/2014), 1h.


## Design

This is an overview. For an in depth discussion of the design, see the [design doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).

For a quick design overview, see the [CockroachDB tech talk slides](https://docs.google.com/presentation/d/1tPPhnpJ3UwyYMe4MT8jhqCrE9ZNrUMqsvXAbd97DZ2E/edit#slide=id.p)
or watch a [presentation](#talks).


CockroachDB is a distributed SQL database built on top of a transactional and consistent key:value store. The primary design goals are support for ACID transactions, horizontal scalability and survivability, hence the name. CockroachDB implements a Raft consensus algorithm for consistency. It aims to tolerate disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention. CockroachDB nodes (RoachNodes) are symmetric; a design goal is homogeneous deployment (one binary) with minimal configuration.

CockroachDB implements a single, monolithic sorted map from key to value
where both keys and values are byte strings (not unicode). CockroachDB
scales linearly (theoretically up to 4 exabytes (4E) of logical
data). The map is composed of one or more ranges and each range is
backed by data stored in [RocksDB][0] (a variant of [LevelDB][1]), and is
replicated to a total of three or more CockroachDB servers. Ranges are
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

CockroachDB provides snapshot isolation (SI) and serializable snapshot
isolation (SSI) semantics, allowing externally consistent, lock-free
reads and writes--both from an historical snapshot timestamp and from
the current wall clock time. SI provides lock-free reads and writes
but still allows write skew. SSI eliminates write skew, but introduces
a performance hit in the case of a contentious system. SSI is the
default isolation; clients must consciously decide to trade
correctness for performance. CockroachDB implements a limited form of
linearalizability, providing ordering for any observer or chain of
observers.

Similar to [Spanner][3] directories, CockroachDB allows configuration of
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

![SQL - NoSQL - NewSQL Capabilities](/resource/doc/sql-nosql-newsql.png?raw=true)

## Datastore Goal Articulation

There are other important axes involved in data-stores which are less
well understood and/or explained. There is lots of cross-dependency,
but it's safe to segregate two more of them as (a) scan efficiency,
and (b) read vs write optimization.

#### Datastore Scan Efficiency Spectrum

Scan efficiency refers to the number of IO ops required to scan a set
of sorted adjacent rows matching a criteria. However, it's a
complicated topic, because of the options (or lack of options) for
controlling physical order in different systems.

* Some designs either default to or only support "heap organized"
  physical records (Oracle, MySQL, Postgres, SQLite, MongoDB). In this
  design, a naive sorted-scan of an index involves one IO op per
  record.
* In these systems it's possible to "fully cover" a sorted-query in an
  index with some write-amplification.
* In some systems it's possible to put the primary record data in a
  sorted btree instead of a heap-table (default in MySQL/Innodb,
  option in Oracle).
* Sorted-order LSM NoSQL could be considered index-organized-tables,
  with efficient scans by the row-key. (HBase).
* Some NoSQL is not optimized for sorted-order retrieval, because of
  hash-bucketing, primarily based on the Dynamo design. (Cassandra,
  Riak)

![Datastore Scan Efficiency Spectrum](/resource/doc/scan-efficiency.png?raw=true)

#### Read vs. Write Optimization Spectrum

Read vs write optimization is a product of the underlying sorted-order
data-structure used. Btrees are read-optimized. Hybrid write-deferred
trees are a balance of read-and-write optimizations (shuttle-trees,
fractal-trees, stratified-trees). LSM separates write-incorporation
into a separate step, offering a tunable amount of read-to-write
optimization. An "ideal" LSM at 0%-write-incorporation is a log, and
at 100%-write-incorporation is a btree.

The topic of LSM is confused by the fact that LSM is not an algorithm,
but a design pattern, and usage of LSM is hindered by the lack of a
de-facto optimal LSM design. LevelDB/RocksDB is one of the more
practical LSM implementations, but it is far from optimal. Popular
text-indicies like Lucene are non-general purpose instances of
write-optimized LSM.

Further, there is a dependency between access pattern
(read-modify-write vs blind-write and write-fraction), cache-hitrate,
and ideal sorted-order algorithm selection. At a certain
write-fraction and read-cache-hitrate, systems achieve higher total
throughput with write-optimized designs, at the cost of increased
worst-case read latency. As either write-fraction or
read-cache-hitrate approaches 1.0, write-optimized designs provide
dramatically better sustained system throughput when record-sizes are
small relative to IO sizes.

Given this information, data-stores can be sliced by their
sorted-order storage algorithm selection. Btree stores are
read-optimized (Oracle, SQLServer, Postgres, SQLite2, MySQL, MongoDB,
CouchDB), hybrid stores are read-optimized with better
write-throughput (Tokutek MySQL/MongoDB), while LSM-variants are
write-optimized (HBase, Cassandra, SQLite3/LSM, CockroachDB).

![Read vs. Write Optimization Spectrum](/resource/doc/read-vs-write.png?raw=true)

## Architecture

CockroachDB implements a layered architecture, with various
subdirectories implementing layers as appropriate. The highest level of
abstraction is the [SQL layer][5], which depends
directly on the structured data API. The structured
data API provides familiar relational concepts such as schemas,
tables, columns, and indexes. The structured data API in turn depends
on the [distributed key value store][7] ([kv/][8]). The distributed key
value store handles the details of range addressing to provide the
abstraction of a single, monolithic key value store. It communicates
with any number of [RoachNodes][9] ([server/][10]), storing the actual
data. Each node contains one or more [stores][11] ([storage/][12]), one per
physical device.

![CockroachDB Architecture](/resource/doc/architecture.png?raw=true)

Each store contains potentially many ranges, the lowest-level unit of
key-value data. Ranges are replicated using the [Raft][2] consensus
protocol. The diagram below is a blown up version of stores from four
of the five nodes in the previous diagram. Each range is replicated
three ways using raft. The color coding shows associated range
replicas.

![Range Architecture Blowup](/resource/doc/architecture-blowup.png?raw=true)

## Client Architecture

RoachNodes serve client traffic using a fully-featured SQL API which accepts requests as either application/x-protobuf or
application/json. Client implementations consist of an HTTP sender
(transport) and a transactional sender which implements a simple
exponential backoff / retry protocol, depending on CockroachDB error
codes.

The DB client gateway accepts incoming requests and sends them
through a transaction coordinator, which handles transaction
heartbeats on behalf of clients, provides optimization pathways, and
resolves write intents on transaction commit or abort. The transaction
coordinator passes requests onto a distributed sender, which looks up
index metadata, caches the results, and routes internode RPC traffic
based on where the index metadata indicates keys are located in the
distributed cluster.

In addition to the gateway for external DB client traffic, each RoachNode provides the full key/value API (including all internal methods) via
a Go RPC server endpoint. The RPC server endpoint forwards requests to one
or more local stores depending on the specified key range.

Internally, each RoachNode uses the Go implementation of the
CockroachDB client in order to transactionally update system key/value
data; for example during split and merge operations to update index
metadata records. Unlike an external application, the internal client
eschews the HTTP sender and instead directly shares the transaction
coordinator and distributed sender used by the DB client gateway.

![Client Architecture](/resource/doc/client-architecture.png?raw=true)

[0]: http://rocksdb.org/
[1]: https://github.com/google/leveldb
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
[4]: http://research.google.com/pubs/pub36971.html
[5]: https://github.com/cockroachdb/cockroach/tree/master/sql
[7]: https://godoc.org/github.com/cockroachdb/cockroach/kv
[8]: https://github.com/cockroachdb/cockroach/tree/master/kv
[9]: https://godoc.org/github.com/cockroachdb/cockroach/server
[10]: https://github.com/cockroachdb/cockroach/tree/master/server
[11]: https://godoc.org/github.com/cockroachdb/cockroach/storage
[12]: https://github.com/cockroachdb/cockroach/tree/master/storage
