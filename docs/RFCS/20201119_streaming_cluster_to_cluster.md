- Feature Name: Streaming Replication Between Clusters
- Status: in-progress
- Start Date: 2020-11-19
- Authors: BulkIO Team
- RFC PR: 56932
- Cockroach Issue: #57433

# Streaming Replication Between Clusters

## Summary

This document describes a mechanism for **replicating changes made to a key-span** of one cluster to another cluster, either directly from one cluster to another or indirectly, writing to and playing back a changelog in some external storage like S3.

This replication is done by capturing and copying every key-value change made to that key-span. During replication, the span in the destination cluster must be **offline** i.e. unavailable to SQL and internal traffic and processes. To bring it online, the replication process must take steps to terminate in **consistent** state, that is, such that it has replicated all writes as of _and only as of_ a single logical timestamp from the source cluster.


## Motivation

Cluster-to-cluster replication or "streaming backup/restore" is an often requested feature, particularly for primary/secondary deployments. In such deployments the secondary would provide an option of **rapid failover** (RTO) on the order of minutes compared to waiting for a full RESTORE, as well as **minimal data loss** (RPO) on the order of seconds, compared to taking periodic backups.

In our typically recommended deployments, replicas placed in different failure domains provide high availability and reduce the need to depend on recovery tools -- and their RPO and RTO characteristics -- for events like node failures or even region failures. However some operators have constraints that preclude such deployments, but still have availability needs that thus make RTO critical. And even some operators of HA clusters require the additional security of a **separate and isolated failover cluster** to mitigate risk of cluster-level or control-plane failures.

Finally online migrations between clusters, for example **moving a tenant** between two clusters, is another use-case that could leverage streaming replication, allowing a minimal downtime migration compared to doing a blocking BACKUP and then a RESTORE while offline during the cut-over.

## Design Considerations

### Replaying Logical SQL Writes Versus Replicating Raw Data

If we just replicated the logical SQL writes -- the INSERTs, UPDATEs and DELETEs -- that were sent to a tenant, table or database from one cluster to another cluster, the second cluster would then re-evaluate those and then write the results to its own storage. Given the complexity of the systems and subsystems involved in executing such statements, this approach gives very weak guarantees that the second cluster actually contains an _identical_ replica of the data in the first: that re-evaluation could produce a different result due to non-determinism, such as due to clock skew, randomized decisions, hardware or network conditions, etc.  The configuration -- from the schema to the settings, users, grants, etc -- would all also need to match for this second cluster to be an identical, drop-in replacement, however this would be very difficult to achieve: simply sending the same schema changes to both clusters could see a change succeed on one but fail on the other, or even just take longer to complete. For example, what happens if a column addition is still ongoing on the secondary when the primary starts to send writes that manipulate that column?

These challenges are not unlike those we previously encountered with maintaining exact replicas in our KV layer when we replicated higher-level operations rather the results of those operations, before we [migrated to the latter](https://github.com/cockroachdb/cockroach/pull/6166).

A more robust approach to maintaining a true, identical replica is to copy all of the raw stored data -- the actual key-value pairs. This can yield a high degree of confidence that the second cluster actually contains an exact replica of the first, as it contains exactly the same bytes.


### What can be replicated?

Given a mechanism, described in more detail below, for exactly replicating the key-value pairs in a key-span between clusters, what spans make sense to replicate?

One could imagine replicating any given table, by simply determining its span and then replicating that. However "a table" consists of more than just its row data. Perhaps most prominently, it has its schema which is stored in a descriptor and is needed to make sense of that row data. That schema may in turn reference other objects in the cluster such as the users who have been granted privileges on that table, user-defined types for its columns, parent databases and schemas, foreign keys to other tables, etc. Furthermore there may be background jobs in the system's job queue acting on that table or optimizer statistics that pertain to that table in the system's stats store but are needed to plan successful queries on that table.

Additionally, most applications interact with multiple tables at once, and may rely on transactions across them being consistent. To be a drop-in replacement, to which that application could cut over, a secondary standby cluster needs to have all the tables, consistent with each other, as well as the cluster's configuration: the same users, granted the same privileges, configured with the same settings, etc.

Thus, in most deployments, the span that makes the most sense to replicate is that which encapsulates all the table data and all the metadata. Given the metadata is stored in system tables, this is the entire SQL table key-span.


### Replicating State Into An Online Cluster

Replicating the key-span of "the whole cluster" -- all user tables and all system tables -- poses its own set of challenges.

Chief among these is that the cluster being replicated _into_ has its own system tables. The recipient cluster is an online, functional cluster: operators need to have user accounts to be able to login to start and control the job that is receiving and writing this replicated data, the cluster's settings may need to be correctly configured to connect to the stream and correctly handle the cluster's hardware, network, clocks. etc. The restore itself is a job, with persisted state in the jobs table, and relies on the jobs subsystem being active to run it.

However the source cluster's configuration and state -- its users table, jobs table, settings, etc, as mentioned above, need to be replicated for it to be a true replica. Somehow the recipient cluster needs to maintain its own state and configuration while it is a recipient, but simultaneously also receive and write the replicated state, and it cannot simply write the incoming state over its own.

For example, if a job is created and starts executing on the source cluster, it must be replicated over to the destination cluster. However if it is written to the destination cluster's jobs table, given that it is established above that the destination cluster has a running jobs system to run the replication job itself, it would potentially start executing on the destination cluster as well. This is a problem though, as it is still executing on the source cluster and the results of the execution on the source cluster are being replicated. Additionally, as the execution updates job's persisted state, a conflicting execution would overwrite that.

Similar situations arise with other background processes like stats computations or expiry/cleanup processes. In short, correctly **replicating into a key-span requires that it be offline**, i.e. nothing else be reading or more importantly writing to it.

Herein lies the challenge that has historically made replicating between clusters difficult: the destination cluster simultaneously needs to be **online** to run the replication process, but replicated data needs to be streamed into an **offline** cluster. This would appear to present a paradox.


### Tenants Are Easier To Replicate

The introduction of multi-tenancy functionality built the pieces required to run "virtual" clusters, which exist within a span of the keyspace of a host cluster. This change has two important facets that affect replicating between clusters.

The first important aspect of tenants is that they are fully encapsulated within a span of the host cluster's keyspace, within which they have their own system tables, users, grants, persisted background job state, etc along with the tables and row data. As discussed above, this makes that span a unit which can be useful if replicated in its entirety, without any complexity in determining what keys within it to or not to copy. But even more importantly they have _their own_ system tables, meaning a tenant -- including its system tables -- can be copied from one cluster to another without affecting the destination cluster's system tables.

Additionally virtual tenant clusters separate their execution processes from those of the host cluster's and allow for starting and stopping these execution of the tenant processes independently. Thus, between having separate system tables from the host cluster, and having the ability to control when processes which read and write to those tables are running, tenants provide a clean mechanism for side-stepping the offline/online contradiction described above. By not starting the tenant's execution processes, the tenant span is _offline_ and can be _replicated into_, by a replication process run on the _online_ host cluster. When the replication process is concluded, the tenant processes can be started and can read and write to it.

From a distance, this is similar to OS virtualization: the host or hypervisor can snapshot or copy the execution state of a guest VM, or can load the persisted state of a prior snapshot or a snapshot from another host and then resume that VM. While the guest is still suspended, i.e. not executing its own processes and changing its own state, the host can change the snapshot from the outside, but once it resumes the guest's execution, the guest then "owns" its state.


### Streaming Clusters vs Tenants

As discussed above, the tenant primitive encapsulates all potentially related data/state/etc in one well-defined prefix, and the ability to start and stop tenant processes provides the required "offline" destination keys-span in an otherwise "online" cluster.

However enterprise customers with **non-tenant** deployments want to use cluster-to-cluster replication. While it is possible that they may someday migrate to run their workloads as tenants of multi-tenant clusters (or indeed we may opt to run _all clusters _as_ _"multi-tenant" clusters even if they just host one tenant), the multi-tenancy features are not yet ready for on-premise customer deployments, and are not likely to be in the immediate term. Meanwhile there is active demand for cluster-to-cluster replication from these customers _now<sup>.</sup>_

Given that the host/tenant separation is what allowed side-stepping the online/offline contradiction, one potential solution for replicating non-tenant clusters is to invert the above online host cluster, offline guest tenant design. More specifically, by booting the processes for the second cluster in a special "recovery mode", where they read and write only within the span of a designated "recovery tenant" key prefix, then only that tenant is actually "online" with respect to SQL processes, including those running the replication job, while the rest of the keyspace of that cluster is effectively left offline, and can thus ingest replicated data.

Whereas the previous paradigm, of online host clusters replicating offline guest tenants, might be compared to or thought of in terms of OS virtualization and moving and resuming guest image snapshots, this pattern is conceptually more similar to booting a computer to a separate recovery or maintenance partition, from which one can then act on, upgrade or alter the OS installed on the main or root partition while it is safely offline.

While there will certainly be subtleties to resolve in this configuration that will be addressed in its own design document, this approach should be able to utilize the same stream format, job, helpers, producer/ingestion components, etc as steaming a tenant. Other than being run in a cluster that is booted in this special mode, wherein it stores its SQL state within a special prefix, the rest of the process is the same -- it just is streaming the whole SQL key span of the source cluster, rather than the key-span of a tenant within it.


## Detailed Streaming Replication Design

Replicating a tenant requires two main pieces:

1. A stream of all KV changes made to the tenant span.
2. A job ingesting that stream into a tenant.

These two pieces may run concurrently but physically separated e.g. to maintain a "hot standby" in a second datacenter, where the copy is ingesting changes as soon as they are emitted by source and ready to be brought online at a moment's notice. They could also instead be temporarily separated, i.e. using the persisted stream to replay later.


### Change Stream

To replicate a tenant (or a whole cluster) we need a stream of all changes to the content of that tenant span (or of the whole cluster's data spans). This has, for the most part, already been built for CDC in the form of rangefeeds, change aggregators, etc. This stream will need to be consumable by the ingesting cluster, or, alternatively, written to files that can be "replayed" later. This stream should be partitioned for distribution of the work both of producing and consuming it. The consumer however will need to know how that stream is partitioned at any given time to ensure it expects and consumes the right partitions, and the partitioning may change over time as producing cluster changes.


### Stream Logical Format

**Topology**

The number of stream partitions and their location is the **topology** of the stream. Locations can be cloud storage URIs or network addresses of nodes within a cluster.

**Generation**

Since the stream is long-lived and the producing cluster's topology may change over time such that the number of partitions of the stream changes as well, we divide the stream by time into epochs or **generations**. Within a given generation the topology of the steam is constant, i.e. it will have the same number of partitions and the partitions will have the same locations. The distSQL "flow" used to produce the stream; if the flow is re-planned to adapt to data placement changes, node failures, etc, that starts a new generation. Generations are identified by the logical start time at which they begin emitting changes. When writing to cloud storage, separate generations are stored in separate prefixes.

**Partition**

A partition is a stream of events as emitted by one CDC change aggregator within the producing cluster -- it is the output of the "kvFeed" aggregator. These events can be of one of two types **key-values** or **resolved timestamps**.

**Resolved Timestamp**

The resolved timestamp is an event which indicates that a stream has emitted all changes up to the specified timestamp. That is to say, no new events with an older timestamp will be emitted.


#### Streaming Directly Between Clusters

Operators may wish to stream directly between clusters, either to reduce operational complexity and costs by eliminating the need to have an external storage intermediary or to minimize latency associated with buffering and flushing to storage.

To stream directly between clusters, nodes in the producing cluster can allocate "outboxes" or fixed size buffers of emitted stream entries. It can then provide the addresses of these outboxes and expose them via an API so that the consuming cluster can dial them directly to fetch their contents. A consumer connected to a given outbox could hold its connection open and receive emitted events with minimal latency.


#### Streaming to/from Files

In addition to streaming directly between clusters, operators might wish to stream to an intermediary buffer which can be read by a consumer. Spinning up an intermediary buffer saves the need for maintaining outboxes on the source cluster, as well as enables decoupling of the 2 clusters.

The intermediary buffer can be considered like a file-system, such as S3. In this case, the stream client needs to provide a stream of changes on a per-generation basis, as well as the ability to start emitting changes from a particular timestamp. The streaming client should be able to determine the topology of a generation efficiently based on the files in the buffer.

The proposed format for the files produced by a stream is:

`<cluster_id>/<generation_id>/TOPOLOGY`: Describes the number and locations of the partitions for this generation.

`<cluster_id>/<generation_id>/<partition_id>/<timestamp-prefix>/<timestamp>-{data,checkpoint}`: The events emitted by the stream.

The **generation_id** is uniquely identified by the start time of the streaming job, to enable quick lookup for the specific generation that contains a given timestamp. This should be unique for every cluster, and each generation would correspond to a particular DistSQL flow that is set up on the source cluster.

The **partition_id** would uniquely identify each partition for a given generation. This is akin to the processor ID in the DistSQL flow that produces the stream. Events will be persisted as files, prefixed with the maximum logical timestamp of the batch contained in a file. **timestamp-prefix** is some prefix of the timestamp, used to chunking files into "directories" with more bounded numbers of files (i.e. to allow easier prefix-filtered enumeration). **Key-values** (`roachpb.KeyValue`s) will be batched in files and **resolved timestamps** will be emitted as checkpoint files.

Starting an ingestion stream from a given timestamp involves finding the latest generation before the given timestamp and then reading files in order starting from the latest resolved timestamp before the target timestamp. Note that files need not be added in lexicographical order, but files added before the last resolved timestamp file should be safe to ignore.

A cluster continuously ingesting a stream from files would need to poll to determine when new files are present. It may thus be beneficial to include some form of "last write" file or files in a well-known location, to indicate if/when a more expensive re-enumeration is required. Alternatively, it could potentially establish a gRPC connection directly to the source cluster to receive instant notifications of which new files are available. This however is an optimization that can be explored later if needed.

### Stream Client API

The streaming client should be able to answer requests to:

*   Get the corresponding generation and its topology for a given timestamp
*   Start reading a generation’s partition at a given timestamp (i.e. consume the events of the stream)
*   Send a notification of a new generation, as well as the start timestamp of that generation
*   Drain all events from the partitions of a generation until a given timestamp has been resolved (used when a notification of a new generation has been received.)

The API should be transparent to whether the streaming is directly cluster to cluster, or facilitated by an intermediary buffer.


### Stream Ingestion

A job in the recipient cluster will need to consume the stream of changes and ingest them into the target tenant key span, while that span is offline (i.e. no tenant VMs are running for it). The tenant record should be in an "adding" or "offline" state to ensure a VM cannot start for it.

When the operator opts to stop ingesting the stream and bring the standby cluster online, all of its ranges must be consistent, holding all of the data data up to, and only up to, a single logical timestamp from the source cluster, before it can be brought online.

Given the stream is partitioned, we expect to see changes from the same logical system timestamp in the origin cluster appear at different wall times in the different partitions of the stream: a partition may fall arbitrarily behind the others, or a burst of data in one table may mean that 100mb of changes from its partition of the stream may cover one a few seconds of timestamps while that same size buffer could cover hours in another partition. Two approaches to handle ingesting these unsynchronized partitions to produce a consistent result are either buffering the incoming and coordinating what is flushed to be consistent, or directly ingesting then then rolling back to a consistent time afterwards.


#### Terminology

**Low-water mark Resolved Timestamp**

Each partition in a generation periodically emits a resolved timestamp. The minimum resolved timestamp across all partitions is referred to as the **low-water mark resolved timestamp**. This is the most recent timestamp at which we can claim we can provide a consistent view of the data, and thus the timestamp that must be presented after roll over.

**Cut Over**

The event which designates the restoring cluster to stop listening to the incoming stream and become the primary cluster is referred to as the “cut over”.


#### Buffered Ingestion

Buffered ingestion would write the incoming streams to a buffer, and wait until all partitions have received data at least up to a given source-cluster resolved logical timestamp, and only then** flush that prefix of their buffer for that resolved timestamp** to their actual storage.

Given that a partition of the stream could fall _arbitrarily_ behind or another could burst much more data for a given time period, this implies this buffering must be prepared to hold an _unbounded _amount of incoming data before it is allowed to flush it, and thus likely will need to be disk-backed or at least able to spill to disk, potentially increasing write-amplification in the steady-state of tailing the stream.

It is worth considering what the buffered ingestion implementation could look like in a bit more detail. One proposed approach would have each node maintain a Pebble instance with the WAL disabled (and generally otherwise optimized for a write workload). (timestamp, KV) pairs would be added to the store keyed on their timestamp. Processing the event stream would behave as follows:


1. Key-value pairs would be ingested into Pebble store as _(ts, KV)_ pairs, sorted by _ts_.
2. Upon notification of the increase of the low-water mark timestamp to _ts1_, keys up to _ts1_ are read from the store (which is keyed on timestamp) and are added to a BufferingAdder to be ingested into the main data store.
3. That partition for the ingestion job can then report that it has ingested up to _ts1_.
4. ClearRange up to _ts1_ in the buffered Pebble store.

If all partitions remain relatively up to date, most interaction with this store should be in memory.  In the unhappy case where the low-water mark falls behind, the store would spill to disk.

An advantage of buffered ingestion is that once the operator decides to stop ingesting the stream, once the in-progress flushes complete, all the ranges are already consistent as of the source-cluster resolved timestamp corresponding to that flush and ready to be brought online immediately. However, if a node responsible for flushing a partition were to become unavailable during one of these flushes, the ingested data would be left in an inconsistent state. We would have a low-watermark applied timestamp, which is the latest timestamp at which all nodes have successfully flushed which lags the low-watermark applied timestamp. Since the data is only consistent up to the lwat, we are forced to RevertRange back to it if the stream were stopped in this state.


#### Direct Ingestion and Rollback

Direct ingestion -- batching up and flushing data from the incoming stream directly to ranges as it is received, with **no coordination of what is flushed** by the partitions -- would simplify the ingestion and minimize write-amplification of actually tailing the stream.

However, when the stream is stopped ingesting directly, the ranges would be inconsistent, as some partitions of the stream would have ingested up to different timestamps or above the last resolved timestamp.

Thus once the stream is stopped, directly ingested data would need to be **rolled back** to the last resolved timestamp that all partitions had ingested up to before the cluster would be ready to use. This would be done, using `RevertRange` on the entire keyspace into which ingestion occurred, reverting it to that chosen timestamp. Doing this means `RevertRange` needs to iterate _all_ the ingested data, scanning the to find and rollback those keys that are above target timestamp.

Using Time-Bound Iteration (TBI) table filtering at the storage layer could improve the runtime of RevertRange when used on very recent timestamps, changing `n` in its O(n) runtime to be just the size of those flushed SSTables that actually contain keys in the relevant period. Given the likely recent timestamp to which we'd be reverting, unless a partition of the stream had fallen very far behind, this would likely reduce the scan size to just a few minutes worth of data.

Rolling back would also have a lower-bound cost of the O(m) where m is how much has been written above the target timestamp. In practice it would be likely the O(n) cost of finding what needs to be rolled back would dominate, unless a single partition had fallen too far behind.


#### Ingestion Tradeoffs


##### Tradeoff: Storage Utilization Spikes

Both approaches may experience spikes in utilization of storage resources if one or more partitions falls far behind.

In the case of buffered ingestion, the partitions that are caught up will need to buffer the data they cannot yet flush to disk. Although these partitions will be writing to the on-disk buffer rather than the main store, the write load on the standby cluster will mirror that of the source cluster. When the lagging partition catches up to the resolved timestamp, a large storage spike is expected in flushing all of the buffered data to the main store. In summary, these storage utilization spikes are expected whenever we catch up on a stream that has fallen behind. Until we catch up, our RP and potentially RT is behind/elevated as well.

In the case of direct ingestion and rollback, during the rollback phase, a spike in storage resources is expected. In order to rollback, RevertRange needs to locate the SSTs which contain relevant data and perform point deletes over all values which have changed since the rollback time. Before the cut-over, all of the data received by the standby cluster will be ingested identically to the source cluster. Although this spike is only expected to occur once, _it will occur at a critical time for the user_: when they are cutting over to the standby cluster.

With either approach, in order to properly support a partition falling arbitrarily behind, it is important to ensure that appropriate back pressure systems are in place to avoid overloading the storage layer, regardless of when the spike is expected to occur.


##### Tradeoff: RTO

With the buffering solution, performing a cut-over should be a constant time operation since it has only ingested data that is known to be consistent, as long as all buffers have had a chance to flush.

Rolling back to the low-water mark time with the direct ingestion approach is potentially a function of how far behind one stream is. Notably, this means that the **RT will be a function of the RP**. However, this makes observability into how far behind each stream all the more critical so that the operator can ensure that the streams don’t fall too far behind.


##### Tradeoff: Node Failure Resiliency

Since these ingestion jobs are expected to be very long running, they should be resilient to node failures. This failure mode is fairly well handled by the direct ingestion approach since all ingested data is already replicated and low-watermark calculations can be based off of ingested data. Recovery looks like restarting the ingestion job from the latest low-watermark resolved timestamp.

However, the node failure case is more interesting when considering the buffered ingestion implementation. As previously mentioned, since flushing is not atomic, during a flush there will be some time where the ingested data is not in a consistent state. If a cut-over were to happen at this time, we would need to wait for all nodes to finish flushing. However, if a node were to become unavailable, the nodes that have flushed need to rollback to the last timestamp at which all partitions successfully flushed. **This leads to the realization that to fully support the buffering ingestion implementation we'll need to be able to support running a RevertRange in any case.**


##### Tradeoff: Simplicity

One strong argument against the buffered approach is its relative complexity. Not only is there added complexity in adding another component to the system, but the buffered ingestion solution requires us to handle the RevertRange case anyway.


##### Tradeoff: Summary

While Buffered Ingestion imposes a potential double write-amplification cost on the steady-state of tailing the stream, it has better RTO in that the ranges are keep nearly ready to be brought online at any time, with only in-progress flushes needing to conclude to start before they're ready in the happy case. However, to support the case of cutting over during a partial flush, it also needs to support rolling back. Directly ingesting on the other hand is simpler as we can just batch and write as we receive data, reducing the cost of the steady-state of tailing the stream, but at the expense of shifting some cost to the end, when it concludes.

In both ingestion approaches, we'll need to coordinate between ingestors of the various partitions to at least track the received or ingested change frontier, much the way changefeeds do for emitting resolved timestamps.

Given the motivation for cluster-to-cluster replication is to have a warm standby cluster, it seems likely that minimizing ongoing cost at the expense of cutover time is not the preferred trade, and instead it would be preferable to pay the extra write-amplification as we go to minimize RTO. It offers benefits such as no resource utilization spike during cut over and constant RTO in the happy case of no node-failures.  However, given the non-atomic nature of the flush across nodes, and the fact it still needs to support rolling back partial flushes, the buffered approach is actually a superset of the direct-ingest approach, and the buffering is just an optimization.

Thus the *proposed approach is start with the direct-ingestion approach with rollback* and pursue buffering as an optimization later.

**Observability Considerations**

It is important to provide metrics for monitoring the health of the replicating stream. Important metrics include:
- RP: how far back is the latest ingested consistent timestamp? This is the timestamp we would rollback to on cut-over.
  - What range or ranges are the lagging ones that are holding it back? Is it ingestion delay or is the stream lagging?
- What is the size of the pending ingestion buffers?
- How far behind are the ingested timestamps vs the recieved timestamps?
- What is the size of the unconsumed portion of the outboxes in the producing cluster i.e. how far behind is the stream consumer?
- For file-based streams, what's the latest flushed consistent timestamp/how far behind is it?
- If/when we integrate with BACKUPs (e.g. restore+replay), what's the size of the stream since last backup?


**Version-Upgrade Considerations**

In order to support version upgrades while maintaining the replication stream, the clusters should be upgraded in a particular order. The source cluster cannot finalize its upgrade before the nodes on the standby cluster is upgraded and finalized, since otherwise the source cluster could send backwards-incompatible data to the standby cluster.


## Drawbacks and Limitations


### Single Slow Range Blocks Everything

If a single node/range/etc falls behind and prevents the closed timestamp frontier from advancing, the RP for the entire cluster falls behind (since we only revert to a single consistent cluster-wide time.)

### Offline Destinations While Ingesting

In other systems, operators often opt to use their secondaries to serve some traffic, such as OLAP reporting/analytics queries or even some of their applications read traffic. This design assumes the destination replica is wholly offline. This is _mostly_ with respect to writes -- we establish that we cannot have a job or background process writing while we are replicating and still maintain correctness. However in general reads over the replicated data while we are still replicating cannot assume it is consistent -- some spans may have been replicated to different times than others until the process concludes ensuring a single, consistent replicated timestamp. It is possible however to get correct, consistent reads if they are backdated to at or before the minimum ingested resolved timestamp. However care would need to be taken when starting SQL execution processes on the replicated data that they a) enforce this and b) are read-only, and do not attempt to run the background systems, like jobs, that are discussed at length above.


### One-way Replication

This proposed design is focused solely on creating passive replicas of a primary copy -- it one-way, from that primary and does not allow for two-way replication, where the replica could also be written to. Such use cases, i.e. "active-active" pairings, are left to a single cluster with nodes in both regions.

In some 2DC cases, if two workloads are fully disjoint, i.e. do not require transactions that consistently read and commit across both, they could be to run them in two separate tenants, where each DC hosts its tenant and a streaming replica of the other. However as mentioned above, the tenancy features will not be ready for on-premise users to use in such a deployment any time soon.


### Replicating Bulk Job Output vs Replication Throughput and RPO

Replicating the entire tenant span (or entire cluster span) while keeping the destination entirely offline simplifies many things. One of those explicitly is that a job and that job's output is replicated wholesale and nothing in the destination needs to understand or coordinate with the source cluster's execution of that job. However this comes at a cost: the link used for replication -- more likely a WAN link as this is generally used for replicating between regions -- needs to have throughput available to accommodate not just the application traffic's write-rate but also that job's write-rate, which could be _much_ higher e.g. a primary key swap can bulk read and bulk-write data, likely writing much more that normal SQL clients would in the same period. If the throughput of the link is too low to handle this burst, the stream may fall behind increasing RPO potentially beyond targets. Deployments with hard RPO requirements must therefore be prepared to provision adequate links for such replication and/or rate-limit their bulk operations in their active cluster to only what can be streamed over that link.


### Replicating Whole Tenants/Clusters vs Tables/DBs

Replicating just one table or one database from one cluster to another is another common use-case for moving changes between clusters, such as to have an analytics cluster, keep a given table updated in a staging cluster, etc. Replicating whole-tenants nearly handles how to handle schema changes, grants, etc by replicating it all blindly, but any sort of individual table replication would need to figure out how to handle such cases -- what do you do if you're replicating one database but not another, but a table is created in the replicated database referencing a type in the other?

Such use cases are often unique to a specific deployment's requirements -- why they want one table but not another, and thus what they want to do in edge cases, etc can vary. Thus for such cases it likely makes more sense to have those operators use our existing CDC offerings and choose how to use those changes to keep their replicas up-to-date.


## Unresolved Questions

### Metrics
How will the user monitor that the data ingested in the standby cluster is exactly the data they expect?
How will per-partition metrics be exposed so that users can monitor if any partitions fall behind?

