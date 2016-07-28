![logo](/resource/doc/cockroach_db.png?raw=true "Cockroach Labs logo")


[![Circle CI](https://circleci.com/gh/cockroachdb/cockroach.svg?style=svg)](https://circleci.com/gh/cockroachdb/cockroach) [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.svg)](https://godoc.org/github.com/cockroachdb/cockroach) ![Project Status](https://img.shields.io/badge/status-beta-yellow.svg) [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cockroachdb/cockroach?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## A Scalable, Survivable, Strongly-Consistent SQL Database

- [What is CockroachDB?](#what-is-cockroachdb)
- [Docs](#docs)
- [Quickstart](#quickstart)
- [Client Drivers](#client-drivers)
- [Deployment](#deployment)
- [Get In Touch](#get-in-touch)
- [Contributing](#contributing)
- [Tech Talks](#tech-talks)
- [Design](#design)

## What is CockroachDB?

CockroachDB is a distributed SQL database built on a transactional and strongly-consistent key-value store. It **scales** horizontally; **survives** disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention; supports **strongly-consistent** ACID transactions; and provides a familiar **SQL** API for structuring, manipulating, and querying data.

For more details, see our [FAQ](https://cockroachlabs.com/docs/frequently-asked-questions.html) and original [design document](
https://github.com/cockroachdb/cockroach#design).

## Status

CockroachDB is currently in beta. See our
[Roadmap](https://github.com/cockroachdb/cockroach/wiki) and
[Issues](https://github.com/cockroachdb/cockroach/issues) for a list of features planned or in development.

## Docs

For guidance on installation, development, deployment, and administration, see our [User Documentation](https://cockroachlabs.com/docs).

## Quickstart 

1.  [Install Cockroach DB](https://www.cockroachlabs.com/docs/install-cockroachdb.html).

2.  [Start a local cluster](https://www.cockroachlabs.com/docs/start-a-local-cluster.html) with three nodes listening on different ports:

    ```shell
    $ ./cockroach start --background
    $ ./cockroach start --store=cockroach-data2 --port=26258 --http-port=8081 --join=localhost:26257 --background
    $ ./cockroach start --store=cockroach-data3 --port=26259 --http-port=8082 --join=localhost:26257 --background
    ```

3.  [Start the built-in SQL client](https://www.cockroachlabs.com/docs/use-the-built-in-sql-client.html) as an interactive shell:

    ```shell
    $ ./cockroach sql
    # Welcome to the cockroach SQL interface.
    # All statements must be terminated by a semicolon.
    # To exit: CTRL + D.
    ```

4. Run some [CockroachDB SQL statements](https://www.cockroachlabs.com/docs/learn-cockroachdb-sql.html):

    ```shell
    root@:26257> CREATE DATABASE bank;
    CREATE DATABASE

    root@:26257> SET DATABASE = bank;
    SET

    root@:26257> CREATE TABLE accounts (id INT PRIMARY KEY, balance DECIMAL);
    CREATE TABLE

    root@26257> INSERT INTO accounts VALUES (1234, 10000.50);
    INSERT 1

    root@26257> SELECT * FROM accounts;
    +------+----------+
    |  id  | balance  |
    +------+----------+
    | 1234 | 10000.50 |
    +------+----------+
    ```

4. Checkout the admin UI by pointing your browser to `http://<localhost>:8080`.

5. CockroachDB makes it easy to [secure a cluster](https://www.cockroachlabs.com/docs/secure-a-cluster.html).

## Client Drivers

CockroachDB supports the PostgreSQL wire protocol, so you can use any available PostgreSQL client drivers to connect from various languages. For recommended drivers that we've tested, see [Install Client Drivers](https://www.cockroachlabs.com/docs/install-client-drivers.html).

## Deployment

-   [Manual](https://www.cockroachlabs.com/docs/manual-deployment.html) - Steps to deploy a CockroachDB cluster manually on multiple machines.

-   [Cloud](https://github.com/cockroachdb/cockroach/tree/master/cloud/aws) - Configuration files and instructions for deploying an insecure development or test cluster on GCE or AWS using [Terraform](https://terraform.io/).

## Get In Touch

### Report a Bug

For filing bugs, suggesting improvements, or requesting new features, help us out by [opening an issue](https://github.com/cockroachdb/cockroach/issues/new).

### Need Help?

-   [CockroachDB Forum](https://forum.cockroachlabs.com/) - Ask questions, find answers, and help other users.

-   [Join us on Gitter](https://gitter.im/cockroachdb/cockroach) - This is the most immediate way to connect with CockroachDB engineers.

## Contributing

We're an open source project and welcome contributions.

1.  See [CONTRIBUTING.md](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md) to get your local environment set up.

2.  Take a look at our [open issues](https://github.com/cockroachdb/cockroach/issues/), in particular those with the [helpwanted label](https://github.com/cockroachdb/cockroach/labels/helpwanted).

3.  Review our [style guide](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#style-guide) and follow our [code reviews](https://github.com/cockroachdb/cockroach/pulls) to learn about our style and conventions.

4.  Make your changes according to our [code review workflow](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#code-review-workflow).

## Tech Talks

For recordings and slides from talks given by CockroachDB founders and engineers, see [Tech Talks](https://www.cockroachlabs.com/docs/tech-talks.html).

## Design

This is an overview. For an in-depth discussion of the design and architecture, see the full [design doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md). For another quick design overview, see the [CockroachDB tech talk slides](https://docs.google.com/presentation/d/1tPPhnpJ3UwyYMe4MT8jhqCrE9ZNrUMqsvXAbd97DZ2E/edit#slide=id.p).

### Overview
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
datacenters for survivability (e.g. `{ US-East, US-West, Japan }`, `{
Ireland, US-East, US-West}` , `{ Ireland, US-East, US-West, Japan,
Australia }`).

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

#### SQL - NoSQL - NewSQL Capabilities

![SQL - NoSQL - NewSQL Capabilities](/resource/doc/sql-nosql-newsql.png?raw=true)


[0]: http://rocksdb.org/
[1]: https://github.com/google/leveldb
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
