![CockroachDB](docs/media/cockroach_db.png?raw=true "CockroachDB logo")
=======================================================================

CockroachDB is a scalable, survivable, strongly-consistent SQL database.

[![TeamCity CI](https://teamcity.cockroachdb.com/guestAuth/app/rest/builds/buildType:(id:Cockroach_UnitTests)/statusIcon.svg)](https://teamcity.cockroachdb.com/viewLog.html?buildTypeId=Cockroach_UnitTests&buildId=lastFinished&guest=1)
[![GoDoc](https://godoc.org/github.com/cockroachdb/cockroach?status.svg)](https://godoc.org/github.com/cockroachdb/cockroach)
![Project Status](https://img.shields.io/badge/status-beta-yellow.svg)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cockroachdb/cockroach?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

- [What is CockroachDB?](#what-is-cockroachdb)
- [Docs](#docs)
- [Quickstart](#quickstart)
- [Client Drivers](#client-drivers)
- [Deployment](#deployment)
- [Need Help?](#need-help)
- [Contributing](#contributing)
- [Design](#design)
- [Comparison with Other Databases](#comparison-with-other-databases)
- [See Also](#see-also)

## What is CockroachDB?

CockroachDB is a distributed SQL database built on a transactional and
strongly-consistent key-value store. It **scales** horizontally;
**survives** disk, machine, rack, and even datacenter failures with
minimal latency disruption and no manual intervention; supports
**strongly-consistent** ACID transactions; and provides a familiar
**SQL** API for structuring, manipulating, and querying data.

For more details, see our [FAQ](https://cockroachlabs.com/docs/frequently-asked-questions.html) and original [design document](
https://github.com/cockroachdb/cockroach#design).

## Status

CockroachDB is currently in beta. See our
[1.0 Roadmap](https://github.com/cockroachdb/cockroach/issues/12854) and
[Issues](https://github.com/cockroachdb/cockroach/issues) for a list of features planned or in development.

## Docs

For guidance on installation, development, deployment, and administration, see our [User Documentation](https://cockroachlabs.com/docs/).

## Quickstart

1. [Install CockroachDB](https://www.cockroachlabs.com/docs/install-cockroachdb.html).

1. [Start a local cluster](https://www.cockroachlabs.com/docs/start-a-local-cluster.html) and talk to it via the [built-in SQL client](https://www.cockroachlabs.com/docs/use-the-built-in-sql-client.html).

1. [Secure the cluster](https://www.cockroachlabs.com/docs/secure-a-cluster.html) with TLS encryption.

1. [Learn more about CockroachDB SQL](https://www.cockroachlabs.com/docs/learn-cockroachdb-sql.html).

1. [Explore core features](https://www.cockroachlabs.com/docs/demo-data-replication.html), such as data replication and fault tolerance and recovery.

## Client Drivers

CockroachDB supports the PostgreSQL wire protocol, so you can use any available PostgreSQL client drivers to connect from various languages.

- For recommended drivers that we've tested, see [Install Client Drivers](https://www.cockroachlabs.com/docs/install-client-drivers.html).

- For tutorials using these drivers, as well as supported ORMs, see [Build an App with CockroachDB](https://www.cockroachlabs.com/docs/build-an-app-with-cockroachdb.html).

## Deployment

- [Manual Deployment](https://www.cockroachlabs.com/docs/manual-deployment.html) - Steps to deploy a CockroachDB cluster manually on multiple machines.

- [Cloud Deployment](https://www.cockroachlabs.com/docs/cloud-deployment.html) - Guides for deploying CockroachDB on various cloud platforms.

- [Orchestration](https://www.cockroachlabs.com/docs/orchestration.html) - Guides for running CockroachDB with popular open-source orchestration systems.

## Need Help?

- [CockroachDB Forum](https://forum.cockroachlabs.com/) - Ask
  questions, find answers, and help other users.

- [Join us on Gitter](https://gitter.im/cockroachdb/cockroach) - This
  is the most immediate way to connect with CockroachDB engineers.

- For filing bugs, suggesting improvements, or requesting new
  features, help us out by
  [opening an issue](https://github.com/cockroachdb/cockroach/issues/new).

## Contributing

We're an open source project and welcome contributions.

1.  See [CONTRIBUTING.md](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md) to get your local environment set up.

2.  Take a look at our [open issues](https://github.com/cockroachdb/cockroach/issues/), in particular those with the [helpwanted label](https://github.com/cockroachdb/cockroach/labels/helpwanted).

3.  Review our [style guide](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#style-guide) and follow our [code reviews](https://github.com/cockroachdb/cockroach/pulls) to learn about our style and conventions.

4.  Make your changes according to our [code review workflow](https://github.com/cockroachdb/cockroach/blob/master/CONTRIBUTING.md#code-review-workflow).

## Design

This is an overview. For an in-depth discussion of the design and architecture, see the full [design doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).

For another quick design overview, see the [CockroachDB tech talk slides](https://docs.google.com/presentation/d/1tPPhnpJ3UwyYMe4MT8jhqCrE9ZNrUMqsvXAbd97DZ2E/edit#slide=id.p).

### Design Goals

CockroachDB is a distributed SQL database built on top of a
transactional and consistent key:value store.

The primary design goals are support for ACID transactions, horizontal scalability and survivability, hence the name.

It aims to tolerate disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention.

CockroachDB nodes (RoachNodes) are symmetric; a design goal is homogeneous deployment (one binary) with minimal configuration.

### How it Works in a Nutshell

CockroachDB implements a single, monolithic sorted map from key to value
where both keys and values are byte strings (not unicode).

The map is composed of one or more ranges and each range is backed by
data stored in [RocksDB][0] (a variant of [LevelDB][1]), and is
replicated to a total of three or more CockroachDB servers. This
enables CockroachDB to scale linearly — theoretically up to 4 exabytes
(4E) of logical data.

Ranges are defined by start and end keys. Ranges are merged and split
to maintain total byte size within a globally configurable min/max
size interval. Range sizes default to target 64M in order to
facilitate quick splits and merges and to distribute load at hotspots
within a key range. Range replicas are intended to be located in
disparate datacenters for survivability (e.g. `{ US-East, US-West,
Japan }`, `{ Ireland, US-East, US-West}` , `{ Ireland, US-East,
US-West, Japan, Australia }`).

Single mutations to ranges are mediated via an instance of a
distributed consensus algorithm to ensure consistency. We’ve chosen to
use the [Raft consensus algorithm][2]. All consensus state is also
stored in [RocksDB][0].

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

## Comparison with Other Databases

To see how key features of CockroachDB stack up against other databases,
visit the [CockroachDB in Comparison](https://www.cockroachlabs.com/docs/cockroachdb-in-comparison.html) page on our website.

## See Also

- [Tech Talks](https://www.cockroachlabs.com/community/tech-talks/) by CockroachDB founders and engineers
- [The CockroachDB User documentation](https://cockroachlabs.com/docs/)
- [The CockroachDB Blog](https://www.cockroachlabs.com/blog/)
- Key Design documents:
  - [Serializable, Lockless, Distributed: Isolation in CockroachDB](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/)
  - [Consensus, Made Thrive](https://www.cockroachlabs.com/blog/consensus-made-thrive/)
  - [Trust, But Verify: How CockroachDB Checks Replication](https://www.cockroachlabs.com/blog/trust-but-verify-cockroachdb-checks-replication/)
  - [Living Without Atomic Clocks](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/)
  - [The CockroachDB Architecture Document](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)

[0]: http://rocksdb.org/
[1]: https://github.com/google/leveldb
[2]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[3]: http://research.google.com/archive/spanner.html
