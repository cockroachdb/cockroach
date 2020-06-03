<p align="center">
  <img src='docs/media/cockroach_db.png?raw=true' width='70%'>
</p>

---

CockroachDB is a cloud-native SQL database for building global, scalable cloud services that survive disasters.

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

For more details, see our [FAQ](https://cockroachlabs.com/docs/stable/frequently-asked-questions.html) or [architecture document](
https://www.cockroachlabs.com/docs/stable/architecture/overview.html).

<p align="center">
  <a href='https://www.youtube.com/watch?v=VgXiMcbGwzQ'> <img src='docs/media/explainer-video-preview.png' width='70%'> </a>
</p>

## Docs

For guidance on installation, development, deployment, and administration, see our [User Documentation](https://cockroachlabs.com/docs/stable/).

## Quickstart

1. [Install CockroachDB](https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html).
2. [Start a local cluster](https://www.cockroachlabs.com/docs/stable/start-a-local-cluster.html)
   and talk to it via the [built-in SQL client](https://www.cockroachlabs.com/docs/stable/use-the-built-in-sql-client.html).
3. [Learn more about CockroachDB SQL](https://www.cockroachlabs.com/docs/stable/learn-cockroachdb-sql.html).
4. Use a PostgreSQL-compatible driver or ORM to
   [build an app with CockroachDB](https://www.cockroachlabs.com/docs/stable/build-an-app-with-cockroachdb.html).
5. [Explore core features](https://www.cockroachlabs.com/docs/stable/demo-data-replication.html),
   such as data replication, automatic rebalancing, and fault tolerance and recovery.

## Client Drivers

CockroachDB supports the PostgreSQL wire protocol, so you can use any available PostgreSQL client drivers to connect from various languages.

- For recommended drivers that we've tested, see [Install Client Drivers](https://www.cockroachlabs.com/docs/stable/install-client-drivers.html).
- For tutorials using these drivers, as well as supported ORMs, see [Build an App with CockroachDB](https://www.cockroachlabs.com/docs/stable/build-an-app-with-cockroachdb.html).

## Deployment

- [Test Deployments](https://www.cockroachlabs.com/docs/stable/deploy-a-test-cluster.html) - Easiest way to test an insecure, multi-node CockroachDB cluster.
- Production Deployments
    - [Manual](https://www.cockroachlabs.com/docs/stable/manual-deployment.html) - Steps to deploy a CockroachDB cluster manually on multiple machines.
    - [Cloud](https://www.cockroachlabs.com/docs/stable/cloud-deployment.html) - Guides for deploying CockroachDB on various cloud platforms.
    - [Orchestration](https://www.cockroachlabs.com/docs/stable/orchestration.html) - Guides for running CockroachDB with popular open-source orchestration systems.

## Need Help?

- [CockroachDB Community Slack](https://cockroa.ch/slack) - Join our slack to
  connect with our engineers and other users running CockroachDB.
- [CockroachDB Forum](https://forum.cockroachlabs.com/) and
  [Stack Overflow](https://stackoverflow.com/questions/tagged/cockroachdb) - Ask questions,
  find answers, and help other users.
- [Troubleshooting documentation](https://www.cockroachlabs.com/docs/stable/troubleshooting-overview.html) -
  Learn how to troubleshoot common errors, cluster setup, and SQL query behavior.
- For filing bugs, suggesting improvements, or requesting new features, help us out by
  [opening an issue](https://github.com/cockroachdb/cockroach/issues/new).

## Building from source

See [our wiki](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181338446/Getting+and+building+from+source) for more details.

## Contributing

We welcome your contributions! If you're looking for issues to work on, try
looking at the [good first issue](https://github.com/cockroachdb/cockroach/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
list. We do our best to tag issues suitable for new external contributors with
that label, so it's a great way to find something you can help with!

See [our
wiki](https://wiki.crdb.io/wiki/spaces/CRDB/pages/73204033/Contributing+to+CockroachDB)
for more details.

Engineering discussion takes place on our public mailing list,
[cockroach-db@googlegroups.com](https://groups.google.com/forum/#!forum/cockroach-db),
and feel free to join our [Community Slack](https://cockroa.ch/slack) (there's
a dedicated #contributors channel!) to ask questions, discuss your ideas, or
connect with other contributors.

## Design

For an in-depth discussion of the CockroachDB architecture, see our
[Architecture
Guide](https://www.cockroachlabs.com/docs/stable/architecture/overview.html).
For the original design motivation, see our [design
doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).

## Comparison with Other Databases

To see how key features of CockroachDB stack up against other databases,
check out [CockroachDB in Comparison](https://www.cockroachlabs.com/docs/stable/cockroachdb-in-comparison.html).

## See Also

- [Tech Talks](https://www.cockroachlabs.com/community/tech-talks/) (by CockroachDB founders and engineers)
- [CockroachDB User Documentation](https://cockroachlabs.com/docs/stable/)
- [The CockroachDB Blog](https://www.cockroachlabs.com/blog/)
- Key design documents
  - [Serializable, Lockless, Distributed: Isolation in CockroachDB](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/)
  - [Consensus, Made Thrive](https://www.cockroachlabs.com/blog/consensus-made-thrive/)
  - [Trust, But Verify: How CockroachDB Checks Replication](https://www.cockroachlabs.com/blog/trust-but-verify-cockroachdb-checks-replication/)
  - [Living Without Atomic Clocks](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/)
  - [The CockroachDB Architecture Document](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)
