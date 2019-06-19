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
**Replica diversity:** Given a set of constraints (or no constraints), CRDB tries to
split replicas evenly between localities/sublocalities that match the
constraint.  

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
AdminUI, new jobs for tracking the progress of constraint changes, and a
crdb_internal virtual table that can be queried for similar information to the
report.


### crdb_internal.constraint_violations virtual table

Two new system tables will be introduced:
```sql
CREATE TABLE crdb_internal.constraint_violations_zones (
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
-- the average rate (in ranges/s or MiB/s) by which this quantity changed since
-- the previous time the report was generated. A negative rate means things are
-- improving (i.e. amount of violating data is going down).
change_rate int,
PRIMARY KEY (object, constraint),
);
```
The other table will be `crdb_internal.constraint_violations_objects`. It's
identical to the first one, except it describes the data more granularly: at
the level of tables/indexes/partitions (named "objects" here) rather than zones.

A background process will refresh the data that powers these tables once per
minute or so. The tables will contain (per zone / per object):

1. How many ranges/how much data are over/under-replicated (i.e. in violation of
their configured “replication factor”).
  E.g.

    ```
    table foo, replication factor, 1/1000 ranges have only 2 replicas, 2/1000 ranges
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
1. How much data is under-diversified.
  Data is considered to be under-diversified if the allocator would like to
  move a replica away to improve it. If there's no locality to move a replica
  to, then the range is not under-diversified. So, for example, if there is a
  single region (and so all the replicas are in that region), there's no
  under-diversification (but there will be the moment a second region is
  added). Similarly, say there are 3 regions each with 3 AZs. Absent any
  constraints, there'll be a replica in each region, in a random AZ. This again
  is fine. On the other hand, lack of storage capacity in a locality is no
  excuse for under-diversification - i.e. we'll report a range as
  under-diversified even if no replica can be moved at the moment because of
  space constraints. Note that when the under-diversification is due to
  over-replication (i.e. the only violation is that two replicas are in the same
  locality), that’d be accounted for in the over-replication stats, without also
  being counted here.
  TODO: define exactly what counts as under-diversified without referring to the
  allocator rules, particularly in the face of hierarchical localities


Besides raw numbers, this report will also include velocity information (the
`change_rate` col): how did each number change in the last *x* minutes. This can
be `NULL` if there's no previous data on a particular violation.

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

The data powering the virtual table and the jobs progress will be a process
running on the leaseholder for range 1. Every minute, this process will
scan all of the meta2 range descriptors together with all the zone configs
(using a consistent scan slightly in the past), as well as collect lease
information (see below). For each descriptor, it’ll use logic factored out of
the allocator for deciding what constraints the range is violating (if any).
For each constraint, it’ll aggregate the (sum of sizes of) ranges violating
that constraint. Same for lease preference and replication factor. Ranges with
no active lease are considered to be conformant with any lease preference.

Since we're scanning meta2, the respective node has an opportunity to update
its range descriptor cache. Which suggests that perhaps all nodes should do
this.

The resulting report will be saved in storage under a non-versioned key. The
two versions of the report will be stored at all times.

### Collecting lease and size information

The current leaseholder is not reflected in the range descriptor (i.e. in
meta2). So, in order to report on leaseholder preference conformance, the node
generating the report needs to collect this information in another way. We'll
add a new RPC asking a node to return all the leases for its replicas
(including info on ranges with no lease). The node generating the report will
ask all other nodes for their leases and join that information with meta2.
This RPC will also return size information on all the ranges, which sizes will
be summed up in the conformance report for the ranges with the same problem.

Since lease information is gathered distinctly from range information, the two
might not be consistent: ranges could have appeared and dissapeared in between.
To keep it simple, we'll consider the meta2 the source of truth, and we'll
ignore lease information when it doesn't match with that.

Multiple nodes are expected to return information on the same replica; we'll
consider the latest lease among them.
If multiple nodes fail to respond and so we don't have info on some ranges, for
the purposes of the report we'll consider all the respective ranges to not have
an active lease (and presumably different alerting will fire since the
unresponsive nodes are unhealthy).


The implementation of the view `crdb_internal.ranges` will also change to take
advantage of this new RPC. Currently, it's a view on top of the virtual table
`crdb_internal.ranges_no_leases` executing an
`crdb_internal.lease_holder(start_key)` (i.e. a `LeaseInfo` request) for every
range. That's too expensive. Once we moved to the new mechanism, we can also
deprecate `crdb_internal.ranges_no_leases`.

As an incidental benefit of collecting lease information this way, the node
collecting all this information can update its leaseholder cache. Which
suggests that perhaps all nodes should exchange this info with each other.

### Notes

The notion of a node being alive or dead will be more nuanced in this reporting
code than it is in the allocator. The allocator only concerns itself with nodes
that have been dead for 5 minutes (cluster setting server.time_until_store_dead)
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
nodes gossip their info. This would have the advantage that everybody can keep their !!!
But the quantities of data involved might be too large.

## Out of scope

1. More user actions could create the types of rebalancing-related jobs that
   we've discussed here: for example adding nodes in a new locality which would
   trigger rebalancing for the purposes of diversity. That's left for the future.

## Unresolved questions

1. Darin has suggested an [additional way to present this
information](https://github.com/cockroachdb/cockroach/issues/26757#issuecomment-488380833):
a "risk table" containing a count of ranges that would become unavailable if a
different number of nodes/AZs/etc go away.
Should we include this?
1. If one has exactly two localities, and a range only has replicas in one,
should it be counted as under-diversified?
1. Should the SQL statements that now create job also become synchronous (i.e.
only return to the client once the corresponding job is done)? A la schema
changes.
