// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package lease manages localized, consistently cached versions of SQL descriptors
(such as tables, views, sequences, databases, and schemas) on each CockroachDB node.
It serves as the critical bridge between Schema Changes (DDL) and query
execution (DML) by tracking exactly which descriptor versions are currently
in use across the cluster.

By providing highly available, temporally consistent schema definitions, the
lease manager ensures that queries execute efficiently. Without this layer,
every transaction would have to fetch metadata directly from the
system.descriptor table, creating a massive bottleneck and injecting
significant latency into every query.

# The Performance vs. Consistency Problem

In a distributed SQL database, descriptors dictate the shape and layout of data.
If every transaction had to query the cluster's central system.descriptor table
for schema details before executing, the system table would become a massive
bottleneck, crippling cluster throughput and injecting significant latency.

Conversely, relying on a naive local cache is dangerous. In a distributed
system, schema changes can occur concurrently with running queries. If Node A
caches a table schema and Node B drops a column, Node A might write a tuple that
violates the new schema, causing irrecoverable data corruption.

# The Lease Solution

A lease acts as a localized leasehold on a specific, immutable version of a
descriptor. It grants a node the explicit right to use that version for the
duration of a transaction. More importantly, publishing this lease to the
system.lease table makes the node's schema dependency visible to the rest of
the cluster. This allows schema change orchestrators (DDL operations) to safely
pause and wait for nodes to finish using old schemas before finalizing a
schema change.

# Core Components

  - Manager: The primary entry point and orchestration struct. It acts as the
    bridge between the node's local caches, the system tables, and range feeds.
  - descriptorState: The top-level cache entity for a single descriptor ID. It
    holds the active descriptorSet along with metadata like the maximum version
    seen and an offline flag.
  - descriptorSet: Maintains an ordered, chronologically sound slice of
    descriptorVersionState objects.
  - descriptorVersionState: Represents an instantiated, leased copy of a
    descriptor version. It embeds the catalog.Descriptor and includes a
    thread-safe reference count (refcount).
  - nameCache: A specialized cache mapping human-readable identifiers—
    (ParentID, SchemaID, ObjectName)—to canonical descriptor IDs.

# Key Architectural Invariants

# The Validity Interval ([ModificationTime, ExpirationTime))

Every leased descriptor defines a Validity Interval spanning from its creation
to its eventual expiration.
  - ModificationTime: The exact timestamp when this specific version of the
    descriptor was committed to the KV layer.
  - ExpirationTime: The timestamp when this lease becomes strictly invalid for
    new transactions.

For a transaction to successfully use a specific lease version, its
ReadTimestamp must fall strictly within this interval:
ModificationTime <= txn.ReadTimestamp < ExpirationTime.

This model guarantees perfectly consistent MVCC schema caching, including for
historical queries (AS OF SYSTEM TIME).

When a node acquires a new version (N+1) of a descriptor, it immediately
"closes" the validity interval of the previous version (N) by setting its
ExpirationTime to a fixed point in the near future. This allows existing
transactions using version N to complete their work while ensuring that all
new transactions are directed to version N+1.

# The Two-Version Invariant and DDL Synchronization

Schema changes happen in gradual state transitions. To prevent data corruption,
at most two adjacent versions of a descriptor can be actively leased cluster-wide
at any given time (e.g., version N and N+1). This guarantees that nodes across
the cluster are never more than one schema transition state apart. The lease
manager enforces this locally and will panic if it detects a violation, as it
implies a breakdown in cluster consistency.

The lease package exposes several critical blocking APIs used by schema changes
to ensure appropriate schema versions are leased across the cluster. These functions
rely on polling the system.lease table to verify cluster-wide state:
  - WaitForOneVersion: Blocks until all unexpired leases on the previous version
    of a descriptor have drained. This is used to maintain the two-version
    invariant, ensuring that no nodes in the cluster are more than one version
    apart.
  - WaitForNewVersion: Blocks until every alive node holding a lease on an
    older version of a descriptor has also acquired a lease on the latest
    version. This ensures that the newest version is visible to all active
    leaseholders.
  - WaitForInitialVersion: Blocks until a lease for the initial version (version 1)
    of a newly created descriptor has been acquired by all nodes that currently
    hold a lease on the parent schema. This ensures that any node capable of
    resolving the name via its schema lease will also have the descriptor
    cached, preventing inconsistent query planning or "table not found" errors.
    This wait applies specifically to tables and types and is controlled by the
    sql.catalog.descriptor_wait_for_initial_version.enabled cluster setting.
  - WaitForNoVersion: Blocks until no unexpired leases exist for any version
    of a descriptor. This is used during descriptor deletion (e.g., DROP TABLE)
    to ensure all nodes have stopped using the object.
  - Liveness Integration: To avoid infinite hangs if a node crashes while
    holding a lease, these wait loops rely on the sqlliveness layer. If
    system.lease shows a lease for a node, but that node's liveness record
    has expired, the wait loop safely ignores that lease.

# Session-Based Leases

The lease subsystem implements session based leasing, where leases are tethered
to the node's sqlliveness session. As long as the SQL node is healthy
(heartbeating its liveness record), the latest version of a lease has an
effectively infinite expiration.

However, this expiration is only "infinite" while it remains the newest version
held by the node. When a more recent version is acquired, the lease manager
explicitly "closes" the older version's validity interval by setting a concrete
ExpirationTime in the near future, allowing existing transactions to drain. If a
node dies, its session expires, automatically and safely invalidating all its
held leases across the cluster.

# Cache Coherence and Range Feeds

To maintain localized caches without stale reads, the lease manager actively
watches the system.descriptor table using a RangeFeed.

  - Detection of New Versions: The RangeFeed provides a real-time stream of
    updates to the system.descriptor table. When a schema change is published,
    the RangeFeed pushes the new descriptor version to all nodes. The lease
    manager observes this update and triggers a background task to refresh the
    local cache. This involves marking any existing leases for the old version
    for expiration and incrementing the local leaseGeneration count to
    invalidate metadata caches (like optimizer memos).

  - Proactive Initial Acquisition: When a new relation or type is created
    within a schema that is already leased locally, the lease manager can
    proactively acquire a lease on the new object immediately upon seeing the
    RangeFeed event. This behavior, controlled by cluster settings, ensures the
    cluster converges quickly on new schema objects and maintains consistent
    optimizer state across nodes.

  - Range Feed Recovery: A background watchdog monitors the feed. If the feed
    stops receiving updates, the node marks the cache as potentially stale.
    Upon recovery, a Full Refresh is triggered to proactively re-validate all
    currently leased descriptors.

# Locked Leasing

To maintain strong transactional consistency, the manager maintains a
closeTimestamp reflecting the high-water mark of ingested updates from the
RangeFeed. When a transaction begins query planning, it can obtain a "locked"
timestamp. This ensures that all subsequent descriptor acquisitions for that
transaction are pinned to the same point-in-time snapshot, preventing
inconsistencies that could occur if some tables were updated in the cache while
others were still being processed.

# Lease Generation

The lease manager maintains a monotonically increasing counter known as the
lease generation. It serves as a lightweight mechanism for other subsystems—such
as the SQL optimizer's memo cache or query plan caches—to quickly determine if
their cached metadata might be stale. By checking if the lease generation has
changed since a cache entry was created, these subsystems can safely invalidate
their caches without having to inspect individual descriptor versions or perform
slow lookups.

The generation counter is explicitly bumped (incremented) in the following
scenarios to broadcast that new catalog data is available:

  - RangeFeed Updates: When a new descriptor version is published to the
    system.descriptor table and the lease manager processes the update via its
    RangeFeed (e.g., to purge older versions or proactively acquire an initial
    version).
  - Descriptor Deletion: When a descriptor is marked as dropped or offline, and
    the manager purges its old versions.
  - Manual Refresh: At the completion of a full manual refresh of the local
    descriptor cache (typically triggered upon RangeFeed recovery).
  - Synchronous System Updates: During the local acquisition of specific
    system-level descriptors (like users or privileges), the generation is
    bumped synchronously. This forces dependent queries to replan immediately,
    rather than waiting for the asynchronous RangeFeed update to arrive.

# Lease Observers

Subsystems can register for granular notifications of descriptor updates using
the Observer interface via RegisterLeaseObserver. While the global lease
generation counter signals a broad catalog change, observers receive targeted
asynchronous notifications (OnNewVersion) for specific descriptor IDs as soon
as they are processed by the local RangeFeed.

This proactive notification mechanism is critical for components like
Changefeeds (schemaFeed) to track schema transitions in real-time. By reacting
immediately to new versions, these observers can accurately advance their
internal frontiers or trigger background tasks to refresh specialized caches,
ensuring high-fidelity tracking of schema state across the cluster.

# Lease Acquisition and Cache Misses

When a transaction needs a descriptor that is not currently valid in the local
cache, the manager performs a reactive acquisition:

  - KV Acquisition: The manager reads the descriptor's current state from the
    KV store and writes a corresponding lease entry to the system.lease table.
  - Singleflight Protection: To prevent a "thundering herd" effect when a
    popular table's lease expires, the manager uses a singleflight mechanism to
    ensure that only one goroutine performs the KV acquisition for a given
    descriptor at a time.
  - Memory Accounting: Every acquired lease is registered against a memory
    monitor to prevent the cache from growing unbounded and causing
    out-of-memory errors.

# Bulk Acquisition

To improve throughput for complex queries requiring many descriptors (e.g.,
information_schema queries), the manager supports Bulk Acquisition (EnsureBatch).
This allows multiple leases to be acquired in a single batched KV transaction,
amortizing the network overhead.

# Descriptor Name Resolution

Every SQL statement parsing step needs to resolve human-readable strings like
public.users into a canonical descriptor ID before formulating a query plan.
Performing a KV lookup for every identifier in a complex query would introduce
enormous latency.

To mitigate this, the lease manager maintains a specialized nameCache mapping
(ParentID, SchemaID, ObjectName) to descpb.ID. Because names can be reassigned
(e.g., via RENAME TABLE), the cache carefully tracks modification timestamps
alongside lease acquisitions and releases to ensure name resolutions accurately
reflect the point-in-time state of the database, enabling sub-millisecond query
planning.

# Handling Dropped and Offline Descriptors

When a table or other descriptor is dropped, a final "tombstone" version is
published to the system.descriptor table.

  - The takenOffline Flag: The RangeFeed observes this tombstone version and
    flags the internal descriptorState as takenOffline.
  - Immediate Eviction: Once a descriptor is offline, as soon as its active
    refcount hits zero, the manager immediately evicts it from memory and
    storage. Any subsequent Release() of an offline descriptor will prevent new
    leases from being acquired, safely blocking new queries from accessing the
    deleted object.

# Lease Cleanup

Stale leases are not immediately deleted from the system.lease table. Instead,
when a lease's validity interval is closed (due to a new version arrival), it is
added to an internal expiration queue. A background task periodically sweeps
this queue and removes physical rows from the KV store once their expiration
timestamp has passed and their in-memory reference count has dropped to zero.
This ensures the system.lease table remains lean while allowing in-flight
transactions enough time to finish.

# Multi-Region Considerations

In multi-region clusters, the lease manager uses the regionliveness
subsystem to perform region-aware counts of active leases. This allows the
system to efficiently track cluster-wide schema dependencies even when some
regions are experiencing network partitions or latency spikes, ensuring that
schema changes can proceed safely without waiting indefinitely for unreachable
nodes.

# Node Lifecycle

  - Startup: The node initializes the manager, cleans up orphaned leases from
    previous incarnations, and establishes the RangeFeed to warm caches.
  - Draining: During graceful shutdown, the node enters a draining state where
    it refuses new leases and waits for active reference counts to hit zero
    before deleting its leases from the KV store.
*/
package lease
