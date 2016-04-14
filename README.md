![logo](/resource/doc/cockroach_db.png?raw=true "Cockroach Labs logo")


[![Circle CI](https://circleci.com/gh/cockroachdb/cockroach.svg?style=svg)](https://circleci.com/gh/cockroachdb/cockroach) [![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.png)](https://godoc.org/github.com/cockroachdb/cockroach) ![Project Status](https://img.shields.io/badge/status-beta-yellow.svg) [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cockroachdb/cockroach?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

## A Scalable, Survivable, Strongly-Consistent SQL Database

- [What is CockroachDB?](#what-is-cockroachdb)
- [Quickstart](#quickstart)
- [Client Drivers](#client-drivers)
- [Deployment](#deployment)
- [Get In Touch](#get-in-touch)
- [Contributing](#contributing)
- [Talks](#talks)
- [Design](#design)

## What is CockroachDB?

CockroachDB is a distributed SQL database built on a transactional and strongly-consistent key-value store. It **scales** horizontally; **survives** disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention; supports **strongly-consistent** ACID transactions; and provides a familiar **SQL** API for structuring, manipulating, and querying data.

For more details, see our [FAQ](https://www.cockroachlabs.com/docs/frequently-asked-questions.html), [documentation](https://www.cockroachlabs.com/docs), and [design overview](#overview).

## Status

CockroachDB is currently in beta. See our
[Roadmap](https://github.com/cockroachdb/cockroach/issues/2132) and
[Issues](https://github.com/cockroachdb/cockroach/issues) for a list of features planned or in development.

## Quickstart 

1.  [Install Cockroach DB](https://www.cockroachlabs.com/docs/install-cockroachdb.html).

2.  [Start a local cluster](https://www.cockroachlabs.com/docs/start-a-local-cluster.html) with three nodes listening on different ports:

    ```shell
    $ ./cockroach start --insecure &
    $ ./cockroach start --insecure --store=cockroach-data2 --port=26258 --http-port=8081 --join=localhost:26257 &
    $ ./cockroach start --insecure --store=cockroach-data3 --port=26259 --http-port=8082 --join=localhost:26257 &
    ```

3.  [Start the built-in SQL client](https://www.cockroachlabs.com/docs/use-the-built-in-sql-client.html) as an interactive shell:

    ```shell
    $ ./cockroach sql --insecure
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

    root@26257> INSERT INTO accounts VALUES (1234, DECIMAL '10000.50');
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

-   [Cloud](https://github.com/cockroachdb/cockroach/tree/master/cloud/aws) - A sample configuration to run an insecure CockroachDB cluster on AWS using [Terraform](https://terraform.io/).

## Get In Touch

When you see a bug or have improvements to suggest, please open an [issue](https://github.com/cockroachdb/cockroach/issues).

For development-related questions and anything else, there are two easy ways to get in touch:

-   [Join us on Gitter](https://gitter.im/cockroachdb/cockroach) - This is the best, most immediate way to connect with CockroachDB engineers.

-   [Post to our Developer mailing list](https://groups.google.com/forum/#!forum/cockroach-db) - Please join first or you messages may be held back for moderation.

## Contributing

We're an open source project and welcome contributions.

1.  See [CONTRIBUTING.md](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md) to get your local environment set up.

2.  Take a look at our [open issues](https://github.com/cockroachdb/cockroach/issues/), in particular those with the [helpwanted label](https://github.com/cockroachdb/cockroach/labels/helpwanted).

3.  Review our [style guide](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#style-guide) and follow our [code reviews](https://github.com/cockroachdb/cockroach/pulls) to learn about our style and conventions.

4.  Make your changes according to our [code review workflow](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#code-review-workflow).

## Talks

The best ones to start with:

-   10/28/2015: [Code Driven NYC](https://www.youtube.com/watch?v=tV-WXM2IJ3U), by [Spencer Kimball] (https://github.com/spencerkimball), 30min  
    Architecture & overview.

-   6/16/2015: [Data Driven NYC](https://youtu.be/TA-Jw78Ms_4), by [Spencer Kimball] (https://github.com/spencerkimball), 23min  
    A short, less technical presentation of CockroachDB.

Other talks of interest:

-   12/2/2015: [Annual RocksDB meetup at Facebook HQ](https://www.youtube.com/watch?v=-ij2OiDTxz0), by [Spencer Kimball] (https://github.com/spencerkimball), 21min  
    CockroachDB's MVCC model.

-   8/21/2015: [Golang UK Conference 2015](https://www.youtube.com/watch?v=33oqpLmQ3LE), by [Ben Darnell](https://github.com/bdarnell), 52min

-   6/10/2015: [NY Enterprise Technology Meetup](https://www.youtube.com/watch?v=SXAEZlpsHNE), by [Tobias Schottdorf](https://github.com/tschottdorf), 15min  
    A short, non-technical talk with a small cluster survivability demo.

-   5/27/2015: [CoreOS Fest](https://www.youtube.com/watch?v=LI7uaaYeYmQ), by [Spencer Kimball](https://github.com/spencerkimball), 25min  
    An introduction to the goals and design of CockroachDB. 

-   3/4/2015: [The Go Devroom FOSDEM 2015](https://www.youtube.com/watch?v=ndKj77VW2eM&index=2&list=PLtLJO5JKE5YDK74RZm67xfwaDgeCj7oqb), by [Tobias Schottdorf](https://github.com/tschottdorf), 45min  
    The most technical talk given thus far, going through the implementation of transactions in some detail.

-   11/5/2014: [The NoSQL User Group Cologne](https://www.youtube.com/watch?v=jI3LiKhqN0E), by [Tobias Schottdorf](https://github.com/tschottdorf), 1h 25min

-   9/5/2014: [Yelp!](https://www.youtube.com/watch?feature=youtu.be&v=MEAuFgsmND0), by [Spencer Kimball](https://github.com/spencerkimball), 1h

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
