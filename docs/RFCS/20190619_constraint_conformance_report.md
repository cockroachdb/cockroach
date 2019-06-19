- Feature Name: Insights into Constraint Conformance
- Status: draft
- Start Date: 2019-06-19
- Authors: Andrei Matei
- RFC PR: [#38309](https://github.com/cockroachdb/cockroach/issues/14113)
- Cockroach Issue: [#14113](https://github.com/cockroachdb/cockroach/issues/14113)
                   [#19644](https://github.com/cockroachdb/cockroach/issues/19644)
                   [#31597](https://github.com/cockroachdb/cockroach/issues/31597)
                   [#26757](https://github.com/cockroachdb/cockroach/issues/26757)
                   [#31551](https://github.com/cockroachdb/cockroach/issues/31551)

# Summary

The set of features described here aim to provide admins with visibility into
aspects of replication and replica placement. In particular admins will be able
to access a report that details which replication constraints are violated.


# Motivation

As of this writing, CRDB administrators have poor insight into data placement in
relation to the constraints they have set and in relation with other
considerations - specifically replication factor and replica diversity. This can
lead to real problems: it’s unclear when a whole locality can be turned down
without losing quorum for any ranges.  
As a result of this work, admins will also be able to script actions to be taken
when partitioning, or another zone config change, has been “fully applied”.


# Guide-level explanation

## Background

Reminder for a few CRDB concepts:

**Replication zone configs and constraints:** key spans can be grouped into
*“replication zones” and properties can be set for each zone through a “zone
*config”. Among others, a zone config specifies the replication factor for the
*data in the zone (i.e. the replication factor of all the ranges in the zone), a
*list of constraints, and a list of lease preference policies.  
**Replication Constraints:** used to restrict the set of nodes/stores that can
*hold a replica for the respective ranges. Constraints can say that only nodes
*marked with certain localities or attributes are to be used or that certain
*nodes/stores have to be avoided. Constraints can also be specified at the level
*of a replica, not only a range; for example, constraints can be set to require
*that some ranges have one replica in locality A, one in B and one in C.  
**Lease preferences:** Lease preferences are expressed just like constraints, except
they only affect the choice of a leaseholder. For example, one can say that the
leaseholder needs to always be in a specific locality. Unlike constraints, not
being able to satisfy a lease preference doesn’t stop a lease acquisition.  
**Replica diversity:** Given a set of constraints (or no constraints), CRDB
tries to split replicas evenly between localities/sublocalities that match the
constraint. The implementation computes a "diversity score" between a pair of
replicas as the inverse of the length of the common prefix of localities of the
two respective stores. It then computes a range's diversity score as the
average diversity score across all replica pairs. The allocator considers a
range under-diversified if there is another store that could replace an
existing replica and result in a higher range diversity score.

## Constraint conformance

Currently the only insight an administrator has into the health of replication
is via the Admin UI which displays a counter for under-replicated ranges and one
for unavailable ranges, and timeseries for over-replicated/under-replicated/unavailable.
There’s nothing about constraints, lease preferences or diversity. Also there’s
no information on over-replicated ranges - although hopefully those will go away
once we get atomic group membership changes. Also, the way in which these
counters are computed is defective: the counters are incoherent and, for
example, if all the nodes holding a partition go away, the ranges in that
partition are not counted (see
https://github.com/cockroachdb/cockroach/issues/19644).

Besides the general need for an administrator to know that everything is
copacetic, there are some specific cases where the lack of information is
particularly unfortunate:


1. Testing fault tolerance: A scenario people ran into was testing CRDB’s
resiliency to networking failures. A cluster is configured with 3 Availability
Zones (AZs) and the data is constrained to require a replica in every AZ (or
even without constraints, the implicit diversity requirement would drive towards
the same effect). One would think that an AZ can go away at any time and the
cluster should survive. But that’s only colloquially true. After an AZ goes away
and subsequently recovers, another AZ cannot disappear right away without
availability loss; the 2nd AZ has to stay up until constraint conformance is
re-established. Otherwise, the 2nd AZ likely has 2 replicas for the ranges that
were migrated away during the first outage.
As things stand, it’s entirely unclear how one is supposed to know when it’s
safe to bring down the 2nd AZ. (Of course, here we’re talking about disaster
testing, not the use of node decomissioning.)
1. Partitioning a table and running performance tests that assume the partioning
is in place. For example, say I want to partition my data between Romania and
Bulgaria and test that my Bulgarian clients indeed start getting local
latencies. Well, partitioning is obviously not instantaneous and so it’s
entirely unclear how long I’m supposed to wait until I can conduct my test.
Same with lease preferences.

## Proposal

We’ll be introducing three ways for admins to observe different aspects of
replication state: a cluster-wide constraint conformance report visible in the
AdminUI, new jobs for tracking the progress of constraint changes, and two
crdb_internal virtual tables that can be queried for similar information to the
report - one table would list zone-level constraint violations, another would
list the violations on a more granular per-object basis.
For providing some insights into the diversity of ranges in different zone
configs, there's also going to be a crdb_internal virtual table that lists the
localities whose unavailability would result in range unavailability (e.g. "if
locality US goes away, half of the ranges in the default zone would lose
quorum").


### crdb_internal.constraint_violations virtual table

A virtual table describing all constraint and leaseholder preference violations is created:
```sql
CREATE TABLE crdb_internal.constraint_violations (
-- the timestamp when this report was generated. Constant throughout the table.
generated_at timestamp,
-- enum: under_replication, over_replication, constraint, diversity, lease_preference
violation_type string,
-- zone is the name of the zone that this report refers to.
zone string,
-- constraint is populated if violation_type is constraint. It identifies which
-- constraint this report refers to.
constraint string,
-- message is a free-form message describing the violation.
message string,
-- the average rate (in ranges/s and MiB/s) by which this quantity changed since
-- the previous time the report was generated. A negative rate means things are
-- improving (i.e. amount of violating data is going down).
change_rate_ranges_per_sec int,
change_rate_mb_per_sec int,
PRIMARY KEY (zone, constraint),
);
```

These tables will be populated based on perdically collected data, described
below. As such, entries might be stale. Even the constraints that we're
reporting on might be stale; a newly created/deleted constraint will not be
reflected in this table. Newly created/deleted zones will similarly not be
reflected.

Note that these tables only have entries for zones+constrints that have some
violations, in order to facilitate reading it. Or should we include all
objects, with `NULLs` in the `violation_type`/`message` fields? Or should we
offer the unfiltered table and a filtered view?


### crdb_internal.critical_localities virtual table

The `critical_localities` table exposes information about what localities would
cause data loss for different ranges - a measure related to replica diversity.
For every zone, we'll count how many ranges would lose quorum by the dissapearance
of any single locality. For the purposes of this report, we consider both more
narrow and more wide localities. E.g. if a node has
`--locality=country=us,region=east`, then we report for both localities `c=us`
and `c=us,r=east`. For the purposes of this report, we also consider a node id
as the last component of the node's locality. So, if, say, node 5 has
`--locality=country=us,region=east`, then for its ranges we actually consider
all of `c=us`, `c=us/r=east` and `c=us,r=east,n=5` as localities to report for.
If a locality is not critical for any ranges, it does not appear in this report.
The rate of change in data for which a locality is critical is also included.
If a locality is critical for some data, that data also counts for the
criticality of any wider locality (e.g. if `us/east` is critical for 1MB of
data, `us` will be critical for (at least) that 1MB).

This report strictly covers critical localities; it does not answer all
questions one might have about the data's risk. For one, there's no way to
answer questions about the risk stemming from combinations of localities (e.g.
if I'd like my data to survive the loss of one region plus a node in another
region, or of (any) two regions, this report does not help.

This report is not directly related to the notion of diversity considered by
the allocator - which tries to maximize diversity. We don't report on whether
the allocator is expected to take any action.

The table is defined as:
```sql
CREATE TABLE crdb_internal.critical_localities(
-- the timestamp when this report was generated. Constant throughout the table.
generated_at timestamp,
-- zone is the name of the zone that this report refers to.
zone string,
-- locality is the critical locality
locality string,
-- the number of ranges for which this locality is critical.
ranges int,
-- the sum of the size of all critical ranges (in MB/s).
data_size_mb int,
-- the average rate (in ranges/s and MiB/s) by which this quantity changed since
-- the previous time the report was generated. A negative rate means things are
-- improving (i.e. amount of violating data is going down). NULL if no previous
data is available to compute the velocity.
change_rate_ranges_per_sec int,
change_rate_mb_per_sec int,
PRIMARY KEY (zone, locality),
);

## Data collection

A background process will refresh the data that powers these tables once per
minute or so. The tables will contain *for every zone*:

1. How many ranges/how much data are over/under-replicated (i.e. in violation of
their configured “replication factor”).
  E.g.

    ```
    zone foo, replication factor, 1/1000 ranges have only 2 replicas, 2/1000 ranges
    have 4 replicas (out of these 2, 1/2 ranges are also under-diversified having 2
    replicas in dc=west)
    ```
1. How many ranges/how much data violates each constraint.
  E.g. 

    ```
    table foo, constraint "2 replicas in dc=east, 1 replica in dc=west", 30/1000
    ranges (1GB/30GB of data) are in violation (not enough replicas in dc=east)
    
    table baz partition "north_america", constraint "region=us", 20/1000 ranges are
    in violation (replicas outside region=us)
    ````
1. How much data violates the leaseholder preference.
1. What localities are critical for data in the respective zone and, for each
   one, how much data is at risk.

This report does not include velocity information (the `change_rate` col).
Velocity is computed only at the SQL level by looking at two consecutive
reports.

## AdminUI conformance report

We’ll add a page in the AdminUI showing the contents of the virtual table. In
the AdminUI we can do fancier things like allowing one to drill down into a zone
- expand a zone into its constituent tables.

## New jobs

We’re also going to have some data-moving operations roughly represented as
jobs: repartitioning and altering zone config properties. The idea here is to be
able to track the progress of an operation that was explicitly triggered through
a SQL command as opposed to observing the state of the cluster as a whole.
For example, when a table is repartitioned, we’ll look at how much data of that
table is now placed incorrectly (i.e. it violates a constraint) and we’ll
consider the job complete once all the table’s data is conformant with all the
constraints. Note that the initial partitioning of a table does not create any
data movement (as all the partitions inherit the same zone config) and so we
won’t create a job for it.

Altering a zone config (for a table, partition or index) will similarly cause a
job to be created. The job will be considered completed once all the ranges
corresponding to the table/index/partition are in conformance with all the
constraints. That is, the first time when all ranges in the affected zone are
found to be in compliance, we’ll declare success.

Note that the association of range movement with a job, and thus the computation
of the job’s progress, is fairly loose. The exact replication effects being
enacted by a particular partition or zone change will be immaterial to the
progress status of the respective job; the only thing will be considered for
computing the progress is the conformance of the ranges affected by the change
with all the constraints (not just the constraints being modified) - including
pre-existing constraints for the respective zone and constraints inherited from
parent zones. Doing something more exact seems hard. However this can lead to
funny effects like a progress going backwards if the conformance worsens (i.e.
number of con-conformant ranges increases for whatever reason) while a job is
“running”.

The updating of the progress report of these jobs will be done through the
periodic background process that also updates the cluster conformance report.
The completion %age is based on how many ranges were found to be non-conformant
with the change the job is tracking when the job is created.

The jobs are not cancelable by users. However a job is considered completed if
the particular partitioning or zone config alter it's tracking is superseded by
another operation (i.e. if the partitioning is changed again in case of
partitioning jobs or if the same zone config is changed again). Removing a
table partitioning or a zone config similarly marks all ongoing jobs tracking
the respective object as complete and creates a new job (tracking the table or
the parent zone config, respectively).


## Detailed design

The data powering the virtual tables and the jobs progress will be produced by
a process running on the leaseholder for range 1. Every minute, this process
will scan all of the meta2 range descriptors together with all the zone configs
(using a consistent scan slightly in the past), as well as collect lease
information (see below). For each descriptor, it’ll use logic factored out of
the allocator for deciding what constraints the range is violating (if any).
For each constraint, it’ll aggregate the (sum of sizes of) ranges violating
that constraint. Same for lease preference and replication factor. Ranges with
no active lease are considered to be conformant with any lease preference.

Critical localities are determined as follows: for each range we consider every
locality that contains a replica (so, every prefix of the --locality for all
the stores involved plus the node id). If that locality contains half or more
of the replicas, it is considered critical. A count of critical ranges /
locality is maintained.

Since we're scanning meta2, the respective node has an opportunity to update
its range descriptor cache. Which suggests that perhaps all nodes should do
this.

The resulting report will be saved in storage under a non-versioned key. The
two versions of the report will be stored at all times.
The report looks like this:

```proto
message ConstraintConformanceReport {
  repeated message zone {
    string zone;
    repeated message constraint_violation {
      string constraint;
      enum violation_type;
      string message;
      int num_ranges;
      int size_mb;
    }
    repeated message critical_localities {
      string locality;
      int num_ranges;
      int size_mb;
    }
  }
}
```

### Collecting lease and size information

The current leaseholder is not reflected in the range descriptor (i.e. in
meta2). So, in order to report on leaseholder preference conformance, the node
generating the report needs to collect this information in another way. Similarly for range size info.
We'll add a new RPC asking a node to return all the leases for its replicas
(including info on ranges with no lease). The node generating the report will
ask all other nodes for information on the leases and sizes of all its replicas.
The report generator will join that information with meta2.

Since lease information is gathered distinctly from range information, the two
might not be consistent: ranges could have appeared and dissapeared in between.
To keep it simple, we'll consider the meta2 the source of truth. Among the
information we get for a range, the most recent lease is considered. For ranges
for which the info we get from all the replicas disagrees with meta2, we'll
consider the range to be without a lease and have size 0.

The implementation of the view `crdb_internal.ranges` will also change to take
advantage of this new RPC. Currently, it's a view on top of the virtual table
`crdb_internal.ranges_no_leases` executing an
`crdb_internal.lease_holder(start_key)` (i.e. a `LeaseInfo` request) for every
range. That's too expensive. Once we moved to the new mechanism, we can also
deprecate `crdb_internal.ranges_no_leases`.

As an incidental benefit of collecting lease information this way, the node
collecting all this information can update its leaseholder cache. Which
suggests that perhaps all nodes should exchange this info with each other.

When generating the report, RPCs to all the nodes will be issued in parallel.
Timeouts will be used. The fact that info on each range is reported by all its
replicas makes us able to tolerate some RPC failures.

Service definition:

```
service Internal {
  rpc RangesInfo(empty) returns (RangesInfoResponse) {}
}

message RangesInfoResponse {
  repeated message ranges {
    range_descriptor;
    lease;
    float size_mb;
  }
}
```

### Notes

The notion of a node being alive or dead will be more nuanced in this reporting
code than it is in the allocator. The allocator only concerns itself with nodes
that have been dead for 5 minutes (`cluster setting server.time_until_store_dead`)
- that’s when it starts moving replicas away. Nodes that died more recently are
as good as live ones. But not so for this report; we can’t claim that everything
is fine for 5 minutes after a node’s death. For the purposes of reporting
under-replication, constraint conformance (and implicitly lease preference
conformance), a node will be considered dead a few seconds after it failed to
ping its liveness record. Replicas on dead nodes are discarded; expired leases
by themselves don't trigger any violation (as specified elsewhere).

When sub-zones are involved and constraints are inherited from parent to child,
violations of inherited constraints will be counted towards the parent zone, not
the child zone.

For populating the `change_rate` column, we'll read the previous version of the
report (from KV) and if it's recent enough (say, within 10m), we'll find the
corresponding rows in the old report and compute the rates.

The already existing counters and metrics -
under-replicated/over-replicated/unavailable ranges - will change to be backed
by this implementation. This will be beneficial; the current implementation,
based on different nodes reporting their counts at different time, is
inconsistent and blind to failures of individual nodes to report.


## Rationale and alternatives

Another way to get this report would be akin to what we do now for counting
unavailable ranges: have each node report the counts for the ranges that it's
resposible for (i.e. it is the leaseholder or, if there's no lease, it's the
smallest node among the replicas). That would save the need to read meta2; each
node can use the in-memory replicas structure. The downside is that dealing with
failures of a node to report is tricky and also, since each node would report at
a different time, the view across sub-reports would be inconsistent possibly
leading to non-sensical counts.

An alternative for collecting the lease information through an RPC is to have
nodes gossip their info. This would have the advantage that all nodes can keep
their cache up to date. But the quantities of data involved might be too
large. Or yet another option is to have the all the nodes periodically write
all their lease info into the database, and have the report generator node read
it from there instead of requesting it through RPCs. This sounds better for
scalability (to higher number of nodes) but also sounds more difficult to
implement. Maybe we start with the RPCs and keep this for later.

For reporting on diversity, instead of (or perhaps in addition to) the critical
localities proposed above, another idea is to report, on a per-"locality-level"
basis, the amount of data that would lose quorum if different numbers of
instances of that level were to become unavailable. For example, in a two-level
hierarchy of localities (say, country+region) plus an implicit third level
which is the node, we'd report something like: if one country is lost, this
much data is lost. If 2 countries are lost - this much. Similar for one region,
two regions, ... n regions. Similar for up to n nodes.
Comparing with the critical regions proposal as alternatives (although they
don't necessarily need to be either or), the advantages would be:
- you get some information on the criticality of combinations of localities. Namely, you get info an combinations of values on the same level in the hierarchy. You don't, however get info on combinations across levels: e.g. you still can't tell if you can survive the failure of any one dc + any other one node.

And the disadvantages:
- no locality names are presented. You can see that there is at least one
  country that's critical, or perhaps (at least one) a pair of countries, but
  you can't tell which one.
- with locality hierarchies, it's unclear if lower level should be counted
  across higher levels. For example, if there's region and zone, and in the us
  region there are no critical zones, but in the eu region there are some,
  should we just say "there are critical zones", or should we say "there are
  crtical zones in eu"?
- it's also unclear how exactly to deal with jagged localities - where the keys
  of the levels don't match between nodes. And the even more pathological case
  where even the same locality key appears on different levels (one node has
  `country=us, dc=dc1`, another one has just `dc=dc2`). One proposal is to not
  report anything in such cases, but that has downsides - adding a node with no
  `--locality` would make the whole report disappear.

## Out of scope

1. More user actions could create the types of rebalancing-related jobs that
   we've discussed here: for example adding nodes in a new locality which would
   trigger rebalancing for the purposes of diversity. That's left for the future.
1. More granular reporting at the level of a database/table/index/partition
   instead of simply at the zone level.

## Unresolved questions

1. Should the SQL statements that now create jobs also become synchronous (i.e.
   only return to the client once the corresponding job is done)? A la schema
   changes. If not, should they return the id of the job they've created?
1. Should the `crdb_internal` tables inclued entries for zones/constraints with
   no violations?
