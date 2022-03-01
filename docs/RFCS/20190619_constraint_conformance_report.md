- Feature Name: Insights into Constraint Conformance
- Status: completed
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


### crdb_internal.replication_report(cached bool (default true))

We'll introduce the `crdb_internal.replication_report()` function, which
returns a report about constraint violations and critical localities as a
(single) JSON record.
```
{
  // The time when this report was generated.
  generated_at timestamp,
  // All the cluster's zones are listed.
  zones [
    {
      // zone_name and config_sql correspond to what `show zone configuration`
      // returns.
      zone_name string,
      config_sql string,

      // Each constraint that is violated is present here.
      violations [
        {
          // enum: under replication, over replication, replica placement,
          // lease preference
          violation_type string,
          // spec is the part of the zone config that is violated.
          constraint string,
          // A human readable message describing the violation.
          // For example, for a violation_type="under replication", the message
          // might be "64MB/1TB of data only (1/20000 ranges) only have 2 replica(s).
          // 128MB/1TB of data only (2/20000 ranges) only have 1 replica(s)."
          message string,
          // the number of ranges for which this locality is critical.
          ranges int,
          // the sum of the size of all critical ranges (in MB/s).
          data_size_mb int,
          // the average rate (in ranges/s and MiB/s) by which this quantity
          // changed since some previous time the report was generated. A negative rate
          // means things are improving (i.e. amount of data in violation is going down).
          // NULL if no previous data is available to compute the velocity.
          change_rate_ranges_per_sec int,
          change_rate_mb_per_sec int,
        }
      ]

      // The localities that are critical for the current zone.
      critical_localities [
        {
          locality string,
          // the number of ranges for which this locality is critical.
          ranges int,
          // the sum of the size of all critical ranges (in MB/s).
          data_size_mb int,
          change_rate_ranges_per_sec int,
          change_rate_mb_per_sec int,
        }
      ]
    }
  ]
}
```

The report is generated periodically (every minute) by a process described
below and stored in the database. Through the optional function argument, one
can ask for the cached report to be returned, or for a fresh report to be
generated.


Notes about violations:
1. All zones are present in the `zones` array, but only violated constrains are
   present in the `violations` array.
2. Per-replica replication constraints (i.e. the constraints of the form `2
   replicas: region:us, 1 replica: region:eu`) are split into their
   constituents for the purposes of this report. In this example, the report
   will list violations separately for the 2 us replicas vs the 1 eu replica.
   Also, the per-replica constraints are considered "minimums", not exact
   numbers. I.e. if a range has 2 replicas in eu, it is not in violation of the
   eu constraint. Of course, it might be in violation of other constraints.
3. Per-replica constraint conformance doesn't care about dead nodes. For
   example, if a constraint specifies that 2 replicas must be placed in the US
   and, according to a range descriptor, the range has two replicas in the US
   but one of the nodes is unresponsive/dead/decomissioned, the constraint will
   be considered satisfied. Dead nodes will promptly trigger under-replication
   violations, though (see next bullet). 
   The idea here is that, if a node dies, we'd rather not instantly generate a
   ton of violations for the purpose of this report.
4. The notion of a node being alive or dead will be more nuanced in this
   reporting code than it is in the allocator. The allocator only concerns
   itself with nodes that have been dead for 5 minutes (`cluster setting
   server.time_until_store_dead`) - that’s when it starts moving replicas away.
   Nodes that died more recently are as good as live ones. But not so for this
   report; we can’t claim that everything is fine for 5 minutes after a node’s
   death. For the purposes of reporting under-replication, constraint
   conformance (and implicitly lease preference conformance), a node will be
   considered dead a few seconds after it failed to ping its liveness record.
   Replicas on dead nodes are discarded; expired leases by themselves don't
   trigger any violation (as specified elsewhere).
   The message describing the violation due to dead nodes will have information
   about what ranges are being rebalanced by the allocator and what ranges are
   not yet receiving that treatment.
5. Violations of inherited constraints are reported only at the level of the
   zone hierarchy where the constraint is introduced; they are not reported at
   the lower levels.
6. The `message` field will generally contain the info from the `ranges` and
   `data_size_mb` fields, but in an unstructured way. In the example message in
   the JSON ("64MB/1TB of data only (1/20000 ranges) only have 2 replica(s).
   128MB/1TB of data only (2/20000 ranges) only have 1 replica(s)."), the
   message has more detailed information than the numerical fields; the
   numberical fields count all ranges in violation of the respective constraint
   without any further bucketing.
7. Since this report is possibly stale, the zone information presented can
   differ from what `show all zone configurations` returns - it might not
   include newly-created zones or newly applied configurations. That's a
   feature as it allows the consumer to reason about whether a recent change to
   zone configs had been picked up or not.


#### Critical localities

The `critical_localities` field exposes information about what localities would
cause data unavailability if the respective localities were to become
unavailable - a measure related to replica diversity.
For every zone, we'll count how many ranges would lose quorum by the dissapearance
of any single locality. For the purposes of this report, we consider both more
narrow and more wide localities. E.g. if a node has
`--locality=country=us,region=east`, then we report for both localities `c=us`
and `c=us,r=east`. For the purposes of this report, we also consider a node id
as the last component of the node's locality. So, if, say, node 5 has
`--locality=country=us,region=east`, then for its ranges we actually consider
all of `c=us`, `c=us/r=east` and `c=us,r=east,n=5` as localities to report for.
If a locality is not critical for a range, it does not appear under that zone.
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

### Data collection

A background process will compute the report once per minute or so (see the
[Detailed design section](#detailed-design)). The "velocity information" in the
report is generated by looking at the raw numbers in the previous version of
the report and computing the delta.

### AdminUI conformance report

We’ll add a page in the AdminUI showing the contents of the report. I'm
imagining that in the future we can allow one to drill down into the different
zones by expanding a zone into the databases/tables/indexes/partitions
contained in the zone. This is future work though.

The already existing range counters on the Cluster Overview page -
under-replicated/over-replicated/unavailable ranges - will change to be backed
by this implementation. This will be beneficial; the current implementation,
based on different nodes reporting their counts at different time, is
inconsistent and blind to failures of individual nodes to report. We'll also
show an asterisk linking to the conformance report if there are any constraint
violations.
The timeseries for the under-replicated/over-replicated/unavailable ranges will
also be backed by the process generating this report. The range 1 leaseholder
node is the one computing these values and so it will be one writing them to
the timeseries, but we need to do it in such a way as to not double count when
the lease changes hands. I'm not sure how to assure that exactly. Maybe the
node writing the counts will itself write 0 for all the other nodes.

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
There will also be a way to generate the data on demand.

Critical localities are determined as follows: for each range we consider every
locality that contains a replica (so, every prefix of the --locality for all
the stores involved plus the node id). If that locality contains half or more
of the replicas, it is considered critical. A count of critical ranges /
locality is maintained.

Since we're scanning meta2, the respective node has an opportunity to update
its range descriptor cache. Which suggests that perhaps all nodes should do
this.

The resulting report will be saved in storage under a regular (versioned) key.
We'll store it in proto form (as opposed to the JSON in which it is returned by
the SQL function). The velocity information is not stored; it is computed on
demand by reading past versions of the report, finding the one more than a
minute away (so, not necessarily the most recent if that one is very recent)
and computing the delta.

### Collecting lease and size information

The current leaseholder is not reflected in the range descriptor (i.e. in
meta2). So, in order to report on leaseholder preference conformance, the node
generating the report needs to collect this information in another way.
Similarly for range size info.
We'll add a new RPC - `Internal.RangesInfo` - asking a node to return all the leases for its replicas
(including info on ranges with no lease). The node generating the report will
ask all other nodes for information on the leases and sizes of all its replicas.
The report generator will join that information with meta2.
This will be a streaming RPC returning information in range key order, so that
the aggregator node doesn't have to hold too much range information in memory
at a time - instead the aggregator can do paginated (i.e. limit) scans over the
meta2 index and stream info from a merger of all the `RangeInfo` responses.

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

## Rationale and alternatives

For the presentation of the report, a natural alternative is to present it
through system table(s) or virtual table(s). That would me a more usual way of
presenting it to SQL clients than a function returning a JSON record, and it
would probably allow easier filtering. There's also not much precedent in CRDB
for returning JSON from system SQL interfaces. But unfortunately a tabular
interface does not seem to be very convenient for this case: how do you expose
the timestamp when the report is taken? How do you expose the versions of the
zone configs used? Even if you find a way to expose them, how does one query
across them consistently? There are obviously different options available, but
none of them look very clean. So I think the time is ripe to create this JSON
precedent. The computation of the "velocity" fields is also simpler with the
report being one big struct. Otherwise it wasn't entirely clear how one would
store and access the historical information required.
Note that CRDB already has some JSON processing capabilities built-in, so it is
feasible to retrieve only parts of the report (between the
`jsonb_array_elements` and the JSON path operators).

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
implement. We'll start with the RPCs and keep this for later.

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
- you get some information on the criticality of combinations of localities. 
Namely, you get info an combinations of values on the same level in the
hierarchy. You don't, however get info on combinations across levels: e.g. you
still can't tell if you can survive the failure of any one dc + any other one
node.

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
2. More granular reporting at the level of a database/table/index/partition
   instead of simply at the zone level.
3. Timeseries of rebalancing progress per constraint violation. This RFC
   proposes reporting one number per constraint violation - the average rate of
   improvement over the past minute, but no way to get historical information
   about these rates. Part of the difficulty in actually recording timeseries
   is that it's unclear how our timeseries platform would work for a dynamic
   set of timeseries (i.e. constraint violations come and go).

## Unresolved questions

1. Should the SQL statements that now create jobs also become synchronous (i.e.
   only return to the client once the corresponding job is done)? A la schema
   changes. If not, should they return the id of the job they've created?
