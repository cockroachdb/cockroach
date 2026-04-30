<!-- Title and subtitle for this page come from the .svx frontmatter
(see site/src/routes/(docs)/introduction/+page.svx); this file holds
the body. -->

## Motivation

Witnesses are a proposed addition to CockroachDB that require much fewer
resources. Up to half of full voting CockroachDB replicas can be replaced by
witnesses, which offers compelling cost benefits.

There are multiple designs for witnesses, each with different trade-offs, based
on how they participate in log replication. Common across all of them is that
they do not maintain a state machine, i.e. fully materialized view of the raft
log.

| type of witness   | state machine | stores (log) |
|-------------------|---------------|--------------|
| full (log)        | no            | yes, full    |
| partial (log)     | no            | yes, partial |
| logless           | no            | no           |

The primary motivation is cost: disk-related, network-related, and CPU-related
costs roughly tripartition share the hardware cost of a cluster; reducing cost
of TCO is an Engineering priority.

Replacing **one full voter** with a witness reduces the write-related cost
across each dimension as follows.
A caveat is that these are the cost reductions that apply directly to *only the
cost directly dedicated to serving writes*. See e.g. [Cloud
Estimations](https://docs.google.com/document/d/1of3KsIch-aJ28dIodqUYTHNLOZDLYw3YqsAJQA46NjM/edit?tab=t.0)
for more holistic estimates. The Cost column assumes that CPU, Network, and Disk
equipartition the hardware TCO of a cluster, which is directionally correct according to
Peter Mattis [here](https://cockroachlabs.slack.com/archives/C0KB9Q03D/p1755173490866979?thread_ts=1755129518.896309&cid=C0KB9Q03D).

### Logless witness

| dimension       | n=3      | n=5      |
|-----------------|----------|----------|
| CPU             | ~28%     | ~18%     |
| Network         | 50%      | 25%      |
| Disk throughput | 33%      | 20%      |
| Disk usage      | 33%      | 20%      |
| **Cost**        | **~37%** | **~21%** |

### Full-log witness

| dimension       | n=3      | n=5      |
|-----------------|----------|----------|
| CPU             | ~10%     | ~6%      |
| Network         | 0%       | 0%       |
| Disk throughput | ~30%     | ~18%     |
| Disk usage      | ~30%     | ~18%     |
| **Cost**        | **~13%** | **~8%**  |

State-machine work (apply, compaction, stored bytes) dominates log work for
typical CRDB workloads, so the full-log Disk rows land close to the logless
figures.

See [cost-details.md](./cost-details.md) for the experiments and
per-dimension derivations.


