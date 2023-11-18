- Feature Name: Region Liveness
- Status: draft 
- Authors: Jeff Swenson
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/103727

# Summary

This RFC proposes the region liveness subsystem for the database. The region
liveness subsystem is backed by the `system.region_liveness` table and is used
to determine when a region is unavailable. Tracking region liveness allows for
SQL servers to store session state like descriptor leases in tables configured
to only survive zone failure, without allowing the failure of a region to
impact cluster level operations like schema changes.

## Requirements

* Serverless clusters can survive region failures without paying a startup
  latency cost when compared to clusters with survive zone database.
* If a region is unavailable, schema operations that do not depend on the
  unavailable region can eventually make forward progress.
* If a region is unavailable, jobs that were claimed by SQL servers in the
  unavailable region can be claimed by servers in available regions.

# Motivation

[As of 23.1](https://github.com/cockroachdb/cockroach/issues/85612), it is
possible for the `root` user to configure the system database as multi region.
In general tables read during the cold start process are configured as global
tables. Tables written during the cold start process are configured as regional
by row tables. Partitioning read and write tables in this way allows SQL
servers with a survive zone failure system database to start up without
executing any cross region read or write operations.

If a user configures any of their databases as survive region, then their
[system database is automatically promoted to survive
region](https://github.com/cockroachdb/cockroach/issues/102134). This means
writes to the regional by row tables are forced to cross region boundaries, so
configuring any database as survive region slows down serverless cold starts.

Below  is an exhaustive list of tables written to by the SQL server in the cold
start process. 

* `system.sqlliveness` is used to lease a `session_id`. The `sesion_id` is used
  to lease `system.sql_instances` rows and `system.job` rows.
  ```sql
  CREATE TABLE public.sqlliveness (
      session_id BYTES NOT NULL,
      expiration DECIMAL NOT NULL,
      crdb_region system.public.crdb_internal_region NOT NULL,
      CONSTRAINT "primary" PRIMARY KEY (crdb_region ASC, session_id ASC)
  ) LOCALITY REGIONAL BY ROW AS crdb_region
  ```
* `system.sql_instances` is used to lease a `id` for the SQL server, which is
  used to lease descriptors in the `system.lease` table. `system.sql_instances`
  is also used for sql server <-> sql server service discovery. The `id` column
  is guaranteed to be globally unique and relatively small. Ids are
  pre-allocated so that they can be claimed during start up with a region local
  query.
  ```sql
  CREATE TABLE public.sql_instances (
      id INT8 NOT NULL,
      addr STRING NULL,
      session_id BYTES NULL,
      locality JSONB NULL,
      sql_addr STRING NULL,
      crdb_region system.public.crdb_internal_region NOT NULL,
      binary_version STRING NULL,
      CONSTRAINT "primary" PRIMARY KEY (crdb_region ASC, id ASC)
  ) LOCALITY REGIONAL BY ROW AS crdb_region
  ```
* `system.lease` is used to track which version of the descriptor lease held by
  each node.
  ```sql
  CREATE TABLE public.lease (
      "descID" INT8 NOT NULL,
      version INT8 NOT NULL,
      "nodeID" INT8 NOT NULL,
      expiration TIMESTAMP NOT NULL,
      crdb_region system.public.crdb_internal_region NOT NULL,
      CONSTRAINT "primary" PRIMARY KEY (crdb_region ASC, "descID" ASC, 
          version ASC, expiration ASC, "nodeID" ASC)
  ) LOCALITY REGIONAL BY ROW AS crdb_region
  ```

For optimal cold start performance, the regional by row tables must be
configured as survive zone. A key property of these tables is that each row
belongs to an individual SQL server. Therefore if a region is known to be
unavailable, it contains no healthy SQL servers, and the tables can be treated
as if they are empty.

For all other system tables, there is no cold start impact of making them
survive region, so they will be configured as SURVIVE REGION even if the
cluster contains only SURVIVE ZONE tables.

# Proposal

The `system.region_liveness` table will be created that tracks which regions
are available. The table has the following schema:

```sql
CREATE TABLE system.public.region_liveness (
    crdb_region system.public.crdb_internal_region NOT NULL,
    unavailable_at TIMESTAMP NULL,
    CONSTRAINT region_liveness_pkey PRIMARY KEY (crdb_region ASC)
) LOCALITY GLOBAL
```

A region is unavailable if `unavailable_at` happens before the logical
timestamp of the transaction. If a region is unavailable, servers may treat the
`system.sqlliveness`, `system.sql_instances`, and `system.lease` table as if
they are empty in that region.

If a SQL server thinks a region is unavailable, the server sets the
`unavailable_at` column for that region. The server picks a timestamp for
`unavailable_at` that is far enough in the future to guarantee all existing
`system.sqlliveness` sessions will have expired by the time the region is
considered unavailable.

When a server wishes to create or extend a `system.sqlliveness` session, it
must consult the `system.region_liveness` table. If `unavailable_at` IS NOT
NULL, then the server must pick a `sqlliveness` expiration timestamp that comes
before the `unavailable_at` timestamp.

If `unvailable_at` is not null, but set to a timestamp after the transaction
timestamp, a SQL server can attempt to set it to null in a transaction
modifying only the region_liveness table. If `unavailable_at` is before the
transaction timestamp, then the server must delete all rows in the
`system.sqlliveness`, `system.sql_instances`, and `system.lease` tables in the
transaction that sets `unavailable_at` to `NULL`. The deletion is necessary
because servers in other regions may have acted as if the rows in the region
did not exist.

# Details

## Region Liveness

### How is the `unavailable_at` timestamp picked?

When writing to the `unavailable_at` timestamp, an expiration time must be
picked that is far enough in the future that all `system.sqlliveness` will have
expired by the time the region is treated as unavailable. This decision must be
made without reading from the `system.sql_instances` table, because if the
region is unavailable, then reading the unavailable region's rows in the
`system.sql_instances` table will timeout. 

The simplest option is to pick an `unavailable_at` timestamp that is the
transaction timestamp +
[server.sqlliveness.ttl](https://github.com/cockroachdb/cockroach/blob/37e61e9e245ac623b461e7917761c27d0223f1bb/pkg/sql/sqlliveness/slinstance/slinstance.go#L42).
It’s not necessary to account for clock skew when picking `unavailable_at`
because the logic is based on transaction timestamp and the SQL servers writing
to the `system.sqlliveness` table are reading from the `system.region_liveness`
table in a transaction. 

There is one danger in picking the `unavailable_at` timestamp based on the
`system.sqlliveness.ttl`, which is the `system.sqlliveness.ttl`  is a setting
that users may change.

Consider:
T0: a session is created in region-a that expires at T0+30=T30
T1: the session expiration setting is changed to +5 seconds.
T2: region-a is marked as expired at T0+5=T5. 

In this example, the region-a will be treated as expired at T5, while there is
still an active session in the region, which violates the
`system.region_liveness` table’s invariants. 

Here are a few ways the risk of changing the `system.sqlliveness.ttl` setting
could be mitigated.
* Option 1: Look at historical values of the setting and pick the max value
  when fencing a region. This is the most work, but the infrastructure for
  reading multiple versions of a setting would also be useful for improving the
  behavior of cluster setting version gates.
* Option 2: Pick something like 2x the setting value as the region expiration
  timestamp. Validate the setting to prevent users from increasing it by more
  than 2x at a time. Validating the setting prevents individual setting changes
  from introducing unsafe configuration, but multiple setting changes can
  introduce unsafe configuration unless the validation looks at all historical
  values. At best this and `Option 3` are a half measure, they avoid most
  likely causes of unsafe configuration, but do not avoid all cases.
* Option 3: Introduce another setting for region expiration. Have validation to
  enforce region expiration is greater than the session expiration value.
* Option 4: Remove the setting and use a hard coded value instead.

For the initial implementation, this problem will be ignored. The multi-region
system database is only used by serverless deployments and the serverless team
will use `ALTER TENANT ALL SET CLUSTER SETTING` to override
`system.sqlliveness.ttl` and prevent users from changing it. 

### Avoiding Transaction Refreshes when Setting `unavailable_at`

The CRDB commit protocol contains a hazard that makes it easy to violate the
constraint that `txn_timestamp + system.sqlliveness.ttl < unavailable_at`. When
committing a transaction, if the timestamp cache for a row comes after the
transaction timestamp, the commit process will refresh the reads and push the
commit timestamp into the future. This can be avoided by setting a commit
deadline equal to the transaction timestamp in the transaction that writes the
`unavailable_at` timestamp. That way if the transaction would push the commit
timestamp, committing the transaction will fail. The code attempting to set
`unavailable_at` can retry the transaction, but will need to pick a new
`unavailabe_at` timestamp.

### Using the KV API

Because `system.sqlliveness`, `system.sqlinstances`, and `systemleases` are
involved in bootstrapping the SQL layer, the subsystems interacting with the
tables use the raw KV API instead of the SQL execution layer. Likewise code
writing to the new `system.region_liveness` table will use the KV API directly.
It is okay to use SQL to interact with the `system.region_liveness` table as
long as the SQL is not used during the SQL bootstrap process.

### Why `unavailable_at` Instead of `expiration`?

An alternative design to tracking an `unavailable_at` for each region is the
table could contain an `expiration` timestamp. SQL servers would periodically
advance the `expiration` timestamp and it would be used as an upper bound for
`system.sqlliveness` `expiration` column. The `expiration` design mirrors the
behavior of the `systemsqlliveness table` and is a little easier to reason about. 

The problem with heart beating an `expiration` column in
`system.region_liveness` is SQL servers are not always running. So the
`expiration` column would always be in the past during the cold start process.
Advancing `expiration` in the cold start process would require a cross-region
write, which defeats the whole purpose of making the tables regional by row and
survive zone.

### Why Use a Global Table Instead of Regional by Table?

Most systems interact with the `system.region_liveness` table via reads. Making
`system.region_liveness` a global table allows for transactions to read from a
local replica. It would be possible to make `system.region_liveness` regional
by table if the `system.sqlliveness` heart beating process used stale reads.
But using stale reads makes the correctness more difficult to reason about.

### When is a Region Marked as Unavailable?

The interface for the `system.region_liveness` table will contain a
`ProbeLiveness(region string)` function that attempts to read from the
`system.sqlinstance` table in the probed region. If a region can not be reached
within 15 seconds, `unavailable_at` is set to start the process of fencing out
the region.

15 seconds is chosen because it gives enough time for leases to transfer in the
case the lease holder of the `system.sqlinstance` table died. The lease
transfer latency is controlled by the `COCKROACH_RANGE_LEASE_DURATION` and
defaults to 6 seconds.

Code interacting with the `system.sqlliveness` and `system.lease` tables will
trigger a liveness probe if query to a region's rows times out.

### `system.region_liveness` Needs a Fixed Descriptor ID

There is a limited number of remaining static descriptor IDs in the system
database. Since `system.region_liveness` will be involved in bootstrapping the
schema subsystem, it would be difficult to use a dynamically allocated
descriptor ID. Since CRDB is effectively out of static descriptor IDs, an ID
that was used, but then removed from the system database's initial schema, will
be used for `system.region_liveness`.

## SQL Liveness Details

### SQL Liveness Heart Beating

Whenever the `system.sqlliveness.expiration` column is extended, the
`system.region_liveness.unavailable_at` column for the region should be checked
to ensure the region is available at the timestamp selected for the session. If
the region is unavailable at the desired timestamp, the SQL liveness subsystem
will clamp the expiration to the instant the region is unavailable. 

### `sqlliveness.Reader.IsAlive`

The main API to the `system.sqlliveness` table is the `IsAlive` method that
returns `true` if a session may be alive and `false` if a session is dead. Once
a session is considered dead, it must stay dead forever.

`IsAlive` will have two key changes made to it:
1. If the region is unavailable, all sessions from the region will be treated
   as if they are dead. Every time a new `session_id` from a region is
   encountered, `sqlliveness` will read the `system.region_liveness` table to
   ensure the region is still down. 
2. If `IsAlive` fails to read from a region, is will trigger a liveness probe
   for the region.

## Descriptor Leasing Details

### Lease Expiration vs `session_id` Expiration

There are two timestamps that are relevant for leases and lease expiration.

1. The `system.lease.expiration` column.
2. The `system.sqlliveness.expiration` column. The relationship between a lease
   and the sqlliveness session is indirect. The `system.lease.nodeID` column
   stores a `system.sql_instances.id`, which is leased by a
   `system.sqlliveness.session_id`.

The `system.lease.expiration` timestamp will usually exceed the
`system.sqlliveness.expiration` time. But the lease subsystem is programmed
such that a server only assumes the lease lives until the SQL liveness
session's expiration timestamp. This means it is safe to ignore leases in an
unavailable region, because creating a `system.sqlliveness.session_id` to claim
the `sql_instance.id` row that owns the unexpired lease will require marking
the region as available, which will delete all stale leases in the region
before servers are allowed to start up.

### Pseudo Code for Advancing Descriptor Version

A descriptor version is allowed to advance to version n+1 as long as there are
no outstanding leases for version n-1. When searching for descriptor leases,
the lease subsystem will skip unavailable regions.

```
txn := BeginTxn()
regionLiveness := ReadRegionLiveness(txn)
for region in AllRegions():
    unavailableAt, exists := regionLiveness[region]
    if exists and unavailableAt.Before(txn.Time()):
       continue // skip unavailable regions
    if hasLease(region, descriptor, version):
       return true
return false
```

## SQL Instances

### ID Allocation

The `system.sql_instances.id` allocation loop will be rewritten to ignore
unavailable regions. This is safe because instances in the region will be
deleted by the transaction that restores region availability.

### Instance Range Feed

There is a rangefeed over the `system.sql_instances` table. The rangfeed
maintains an instance cache that is used to find servers for distsql. There are
two changes required to the cache that maintains the rangefeed.
1. The cache might see duplicate instance ids if a region is unavailable and an
   instance id used by the unavailable region is claimed by an available
   region. An easy way to work around this is to store the
   `system.sql_instances` row in the cache with the more recent transaction
   timestamp.
2. The rangfeed should be split by region so that the cache can be initialized
   when a region is unavailable.

## Jobs

The job subsystem runs a periodic scan to reclaim jobs acquired by a dead
`system.sqlliveness.session_id`. The Jobs claim loop uses the
`sqlliveness.Reader.IsAlive`, which will be enhanced to consult
`system.region_liveness`, so job system specific changes are not needed to
benefit from the `system.region_liveness` table. 

## System Database

Once region liveness is available, we need to configure most tables in the
system database so that they survive region failures. The regional by row
tables will be configured so that they only survive zone failures.

## Backporting

The current plan is to backport this functionality to 23.2. This is reasonably
safe because the functionality will be controlled by a setting and will only be
enabled in multi-region serverless clusters.

The overall release process is:
1. 23.2 will include the `system.region_liveness` table.
2. A minor release will include the logic to consult `system.region_liveness`
   table and the `ProbeLiveness` function will be guarded by a system setting.
3. Once the minor release is fully deployed, the serverless team will enable
   the `ProbeLiveness` feature. If the serverless team needs to roll back the
   release, they will first have to disable the `ProbeLiveness` feature.
