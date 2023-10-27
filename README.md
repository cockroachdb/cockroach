<p align="center">
  <img src='docs/media/cockroach_db.png?raw=true' width='70%'>
</p>

---

cockroachdb is a cloud-native distributed sql database designed to build,
scale, and manage modern, data-intensive applications. 

- [what is cockroachdb?](#what-is-cockroachdb)
- [docs](#docs)
- [starting with cockroach cloud](#starting-with-cockroachcloud)
- [starting with cockroachdb](#starting-with-cockroachdb)
- [client drivers](#client-drivers)
- [deployment](#deployment)
- [need help?](#need-help)
- [contributing](#contributing)
- [design](#design)
- [comparison with other databases](#comparison-with-other-databases)
- [see also](#see-also)

## what is cockroachdb?

cockroachdb is a distributed sql database built on a transactional and
strongly-consistent key-value store. it **scales** horizontally;
**survives** disk, machine, rack, and even datacenter failures with
minimal latency disruption and no manual intervention; supports
**strongly-consistent** acid transactions; and provides a familiar
**sql** api for structuring, manipulating, and querying data.

for more details, see our [faq](https://cockroachlabs.com/docs/stable/frequently-asked-questions.html) or [architecture document](
https://www.cockroachlabs.com/docs/stable/architecture/overview.html).

<p align="center">
  <a href='https://www.youtube.com/watch?v=vgximcbgwzq'> <img src='docs/media/explainer-video-preview.png' width='70%'> </a>
</p>

## docs

for guidance on installation, development, deployment, and administration, see our [user documentation](https://cockroachlabs.com/docs/stable/).

## starting with cockroachcloud

we can run cockroachdb for you, so you don't have to run your own cluster.

see our online documentation: [quickstart with cockroachcloud](https://www.cockroachlabs.com/docs/cockroachcloud/quickstart.html)

## starting with cockroachdb

1. install cockroachdb:  [using a pre-built executable](https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html) or [build it from source](https://www.cockroachlabs.com/docs/v21.1/install-cockroachdb-linux#build-from-source).
2. [start a local cluster](https://www.cockroachlabs.com/docs/stable/start-a-local-cluster.html) and connect to it via the [built-in sql client](https://www.cockroachlabs.com/docs/stable/use-the-built-in-sql-client.html).
3. [learn more about cockroachdb sql](https://www.cockroachlabs.com/docs/stable/learn-cockroachdb-sql.html).
4. use a postgresql-compatible driver or orm to [build an app with cockroachdb](https://www.cockroachlabs.com/docs/stable/hello-world-example-apps.html).
5. [explore core features](https://www.cockroachlabs.com/docs/stable/demo-data-replication.html), such as data replication, automatic rebalancing, and fault tolerance and recovery.

## client drivers

cockroachdb supports the postgresql wire protocol, so you can use any available postgresql client drivers to connect from various languages.

- for recommended drivers that we've tested, see [install client drivers](https://www.cockroachlabs.com/docs/stable/install-client-drivers.html).
- for tutorials using these drivers, as well as supported orms, see [example apps](https://www.cockroachlabs.com/docs/stable/example-apps.html).

## deployment

- [cockroachcloud](https://www.cockroachlabs.com/docs/cockroachcloud/quickstart) - steps to create a [free cockroachcloud cluster](https://cockroachlabs.cloud/signup?referralid=githubquickstart) on your preferred cloud platform.
- [manual](https://www.cockroachlabs.com/docs/stable/manual-deployment.html) - steps to deploy a cockroachdb cluster manually on multiple machines.
- [cloud](https://www.cockroachlabs.com/docs/stable/cloud-deployment.html) - guides for deploying cockroachdb on various cloud platforms.
- [orchestration](https://www.cockroachlabs.com/docs/stable/orchestration.html) - guides for running cockroachdb with popular open-source orchestration systems.

## need help?

- [cockroachdb community slack](https://go.crdb.dev/p/slack) - join our slack to connect with our engineers and other users running cockroachdb.
- [cockroachdb forum](https://forum.cockroachlabs.com/) and [stack overflow](https://stackoverflow.com/questions/tagged/cockroachdb) - ask questions, find answers, and help other users.
- [troubleshooting documentation](https://www.cockroachlabs.com/docs/stable/troubleshooting-overview.html) - learn how to troubleshoot common errors, cluster setup, and sql query behavior.
- for filing bugs, suggesting improvements, or requesting new features, help us out by [opening an issue](https://github.com/cockroachdb/cockroach/issues/new).

## building from source

see [our wiki](https://wiki.crdb.io/wiki/spaces/crdb/pages/181338446/getting+and+building+from+source) for more details.

## contributing

we welcome your contributions! if you're looking for issues to work on, try looking at the [good first issue list](https://github.com/cockroachdb/cockroach/issues?q=is%3aopen+is%3aissue+label%3a%22good+first+issue%22). we do our best to tag issues suitable for new external contributors with that label, so it's a great way to find something you can help with!

see [our wiki](https://wiki.crdb.io/wiki/spaces/crdb/pages/73204033/contributing+to+cockroachdb) for more details.

engineering discussions take place on our public mailing list, [cockroach-db@googlegroups.com](https://groups.google.com/forum/#!forum/cockroach-db). also please join our [community slack](https://go.crdb.dev/p/slack) (there's a dedicated #contributors channel!) to ask questions, discuss your ideas, and connect with other contributors.

## design

for an in-depth discussion of the cockroachdb architecture, see our
[architecture
guide](https://www.cockroachlabs.com/docs/stable/architecture/overview.html).
for the original design motivation, see our [design
doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).

## licensing

current cockroachdb code is released under a combination of two licenses, the [business source license (bsl)](https://www.cockroachlabs.com/docs/stable/licensing-faqs.html#bsl) and the [cockroach community license (ccl)](https://www.cockroachlabs.com/docs/stable/licensing-faqs.html#ccl).

when contributing to a cockroachdb feature, you can find the relevant license in the comments at the top of each file.

for more information, see the [licensing faqs](https://www.cockroachlabs.com/docs/stable/licensing-faqs.html).

## comparison with other databases

to see how key features of cockroachdb stack up against other databases,
check out [cockroachdb in comparison](https://www.cockroachlabs.com/docs/stable/cockroachdb-in-comparison.html).

## see also

- [tech talks](https://www.cockroachlabs.com/community/tech-talks/) (by cockroachdb founders, engineers, and customers!)
- [cockroachdb user documentation](https://cockroachlabs.com/docs/stable/)
- [the cockroachdb blog](https://www.cockroachlabs.com/blog/)
- key design documents
  - [serializable, lockless, distributed: isolation in cockroachdb](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/)
  - [consensus, made thrive](https://www.cockroachlabs.com/blog/consensus-made-thrive/)
  - [trust, but verify: how cockroachdb checks replication](https://www.cockroachlabs.com/blog/trust-but-verify-cockroachdb-checks-replication/)
  - [living without atomic clocks](https://www.cockroachlabs.com/blog/living-without-atomic-clocks/)
  - [the cockroachdb architecture document](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)
