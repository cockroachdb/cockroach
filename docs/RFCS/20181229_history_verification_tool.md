- Feature Name: Cluster history verification tool.
- Status: draft
- Start Date: 2018-12-29
- Authors: Matt Tracy
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

The cluster verification tool is intended to help developers or DBAs check if
the general operation of an already-running cluster has had an acceptable level
of performance, based on comparing historical information about that cluster
against the expected good performance of hypothetical cluster in a similar
situation.

This tool will be useful for both verification of long-running tests which
generate complicated situations, *and* to help analyze the performance of
existing real-world clusters.

# Motivation

This is motivated most directly by a gap in our sign-off process for official
builds of CockroachDB. Currently, sign off consists primarily of a growing suite
of Go-level unit tests, as well as a set of integration-level tests which run a
small CockroachDB cluster and interact with it externally.

However, these tests are short-lived and very targeted; they largely attempt to
reproduce very specific circumstances, and operate in a pass/fail mode based on
a narrow criteria for each test; they often check only for binary events. As
such, there is a perceived gap around "real-world" testing, where a cluster is
deployed and run for a long period of time in a typical setting to see how it
performs over a general workload. This perception is evidenced by our sign-off
process for a release build which includes a release engineer manually running a
cluster for a week in a cloud environment, along with an applied workload, and
then "verifying" that the cluster behaved correctly over that time.

The "verification" of the manual test consists of little more than verifying
that no processes crashed, and that the cluster events and graphs "look right".
While this may be effective at detecting crashes, the analysis of non-fatal
cluster performace is based on little more than developer intutition, and the
criteria may thus vary widely depending on the engineer on rotation.

Additionally, this method does not consider that many "failure" conditions, such
as a cockroach process being down or a temporary loss of availability, may *not*
mean that a build of CockroachDB is invalid; in fact, it may indicate quite the
opposite, as CockroachDB's core value proposition is reliability under dire
circumstances. We thus have a true gap in grading the performance of CockroachDB
under real-world expected boundary conditions, such as process crashes; we do
not verify that performance under these conditions remains acceptable as new
builds are released.

This RFC proposes the creation of a more reliable verification tool, that can
replace the function of the build engineer checking the output of the cluster
and verifying its acceptability. The tool will provide consistent criteria
across builds; the criteria will also be more expansive and informed by
historical results from previous runs.

# Guide-level explanation

The verification process is broken into two actions:

1. Collect historical data from a target cluster. All data collected should
   be associated with a specific timespan.
2. Divide the timespan since the cluster started into set-length intervals. For
   each interval, verify if the historical data for the cluster at that time
   period indicates an acceptable cluster.

The second step, verifying collected data for a time interval, can be broken
down further into two parts:

1. Separate all collected data points into "Inputs" and "Outputs". Inputs are
  datapoints which describe the configuratino of the cluster or the specific
  load being applied to the cluster. Outputs are datapoints which are fully
  dependent on the input. Note that in some cases, a data point might be both
  an input and an output (e.g. the number of range splits occuring on the
  cluster).
2. Given the input values of the time interval, determine if each output value
  is acceptable.

The input/output verification model is a very straightforward case for supervised
machine learning; however, this requires an additional step for a human supervisor
to mark individual periods as pass/fail, which would be cumbersome. Therefore,
data verification will be provided by one of two systems:

+ A "hard coded" criteria which is based on developer intuition.
+ An ML-based criteria which is generated from supervised training sets.

Although creating these systems is somewhat straightforward, it will be very
difficult to have sufficient confidence in either of these verification until
they has been sufficiently tuned, which may take a considerable amount of
training data.

Therefore, the output of verification is not a simple "pass/fail" metric, but
rather a *report* data structure, which includes a suggested "pass/fail" grade
along with an interval-by-interval breakdown of the analyzed history; for each
interval, the report contains the Input/Output variables, along with an indication
for each output variable of whether it passed or not.

This report is convertable into a human-readable format; the most appropriate
format is likely a static, explorable HTML page providing attention-getting visual
indications, although other formats would be possible.

Finally, the interval-by-interval breakdown report is also appropriate as a
training set for the ML-based criteria; a human may choose to edit grades before
inputing them, thus providing the supervision aspect. Assuming conservative
expectations are chosen for the "hard-coded" criteria, this should speed the process
of humans supervising the learning, since a healthy cluster run will already
be pre-marked with passing grades (and a very poor run with failing grades).

# Reference-level explanation

## Detailed design

### Performance Time Interval

One key aspect of verification is that it does *not* verify cluster histories as
a whole; this does not reflect the continuous operating nature of a system like
CockroachDB. Rather, the system will experience several different load
patterns, each of which will be temporary. Some of these loads may involve
complicated conditions such as outages or online schema changes; and each cluster
run will experience a unique pattern of loads.

Therefore, all verification grades are given to a fixed-timed interval of *one
minute*. For example, if a cluster has been running for a day, then there will
be `24 * 60 == 1440` intervals which receive unique grades. A full verification
score for a cluster history may be synthesized from the aggregate grades of all
periods, but fundamentally it is these periods which are considered
independently from each other.

### Gathering Cluster Data

Data from a target cluster is gathered from the following sources.

+ Cluster Events: All Metadata events (excluding range events) should be read
  from the cluster, back to the cluster start.
+ Time Series data at the highest resolution available. (Only time series
  correlated to a pre-picked Input or Output variable should be collected).
+ Full Job history for the cluster.
+ Current cluster liveness information, needed to determine nodes down at the
  present time.
+ Current cluster time when the verification is requested.
+ Cluster identifying information (build versions, OS versions, hardware info,
  etc.)

Each of these sources can be accessed through JSON API endpoints exposed by
cockroach DB; there is no need to fan out to all nodes, information about
any node can be retrieved from any machine.

These sources are considered sufficient at this time to properly assign a grade
to each period. Additional sources NOT considered at this time, but available
for collection in the future:

+ Logfiles for cluster nodes
+ Range event log (separate from cluster event log, records range and
  replication changes)

### Generating Reports

Reports are generated in iterative fashion, starting with the earliest time on
the cluster as determined by the event log. Starting with the first 1-minute
interval the cluster is active, reports are generated for each 1-minute period
until the present time.

> There may be opportunity to parallelize this process; the cluster event log is
> designed to be processed iterively from the beginning, but other data sources
> such as time series can be queried randomly.

For each cluster time period, a grade is generated for each "output" variable. A
grade is determined by passing *all* "input" variables for the time period into
a verifier, along with the detected value of the output variable.

#### Input Variables

Input Variables are used to identify the external conditions that are being
applied to the cluster.

+ Bandwidth in/out
+ Open Connections
+ Transactions/Rollbacks per second
+ Write/Read operations per second
+ Active/Decommissioning Node Count
+ Schema Change in progress
+ Jobs in progress (Backup/Restore, Changefeed)
+ Specific Query types being run (e.g. complicated joins)
+ Network Latencies
+ Clock Skew

Some special Input variables can be classified as flags:

+ Hardware flags (e.g. SSD)
+ Load pattern flags (e.g. queue like, random access, etc.)

#### Input/Output Variables

Input/Output variables are conditions that may arise from certain combinations
of pure Input variables, but may themselves indicate that the expected value of
another output variable should be different.  The clearest example is the rate
of replication traffic; replication traffic is a function of external load, but
may be radically different depending on the specific load (e.g. queue-like load
vs. random distribution, read-heavy vs. write-heavy). However, if replication
traffic is very high, we might expect other output values (such as disk writes
or latencies) to be higher as well. Thus, variables in this category receive a
grade on each report, but may also be an input for another variable being
graded.

I/O Variables:

+ Splits/Merges
+ Transaction Restarts
+ SpanLatch queues
+ Replication Traffic (snapshots, behind, etc)
+ Queue Sizes
+ Cache Misses
+ Nodes Down
+ "Repairing" Dead Node Count

#### Output Variables

Output Variables currently selected are:

+ Service Latency
+ Errors returned to client
+ GC Pause Time
+ CPU Time
+ Memory Usage
+ Disk I/O Stats

#### Output Variable Grading

Given a set of variables, the job of a grader is to determine if each output
value "passes" given a set of input variables. In pseudo-code, this might look
like:

`report.grade[output_var_name] = grader.grade(output_var_name, output_var_value, input_vars)`

The criteria for each named output variable will differ, but in general the
grader will be ensuring that the value falls in an allowed range. To facilitate
debugging, the allowed range should also be returnable from a function such as:

`grader.allowed_range(output_var_name, input_vars)`


#### Hard-coded Grader

The initial grader will be hard-coded; the formula used to determine an allowed
range for an output variable will be a static function of the input variables.
This grader should be tuned to *conservatively pass*; that is, it should be
passing only if it is very certain that an output variable is good.

This will immediately augment the ability of engineers to analyze the history
of a cluster by dismissing obviously-good time periods, bringing focus to any
unexpected anomalies.

It will also serve to help bootstrap the supervised learning process for an ML
grader; it will generate reports that are appropriate as training input for an
ML learner, with the supervising engineer responsible for modifying pass/fail
grades that the hard-coded grader did not correctly set.

## Unresolved Design

This section is a work in progress.

### ML Grader

This grader will use ML (almost certainly a batched gradient descent) to
generate allowed ranges for each output variable based on input. To do this,
we will need to maintain a store of ML-generated weights for input variables
which can be retrieved.

#### Training ML Grader

Training ML weights is accomplished by uploading manually verified results as a
training set. Training sets will be applied in a "batch" format: when a new
cluster report is added to the training set, all periods from that report will be 
applied as a batch to existing weights. 

The ML weight store must maintain a full history of all reports applied to the
training set; this will allow us to recompute the weights if we decide later to
remove reports from the set, adjust training speed, or move to more complicated
models of learning.

#### Versioning of training data

The most complicated part of managing graders will be versioning - that is,
reports for a newer version of CockroachDB should not impact the expectations
of older versions, but *should* at least bootstrap the expectations of newer
versions which do not yet have enough data.

### API Reference
[ In Progress - specific APIs not determined yet]

#### Library
#### Command-line

## Drawbacks

## Rationale and Alternatives

This section is extremely important. See the
[README](README.md#rfc-process) file for details.

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not choosing them?
- What is the impact of not doing this?

## Unresolved questions

- What parts of the design do you expect to resolve through the RFC
  process before this gets merged?
- What parts of the design do you expect to resolve through the
  implementation of this feature before stabilization?
- What related issues do you consider out of scope for this RFC that
  could be addressed in the future independently of the solution that
  comes out of this RFC?
