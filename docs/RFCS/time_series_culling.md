- Feature Name: Time Series Culling
- Status: in progress
- Start Date: 2016-08-29
- Authors: Matt Tracy
- RFC PR: #9343
- Cockroach Issue: #5910

# Summary
Currently, Time Series data recorded by CockroachDB for its own internal
metrics is retained indefinitely. High-resolution metrics data quickly loses 
utility as it ages, consuming disk space and creating range-related overhead
without conferring an appropriate benefit.

The simplest solution to deal with this would be to build a system that deletes
time series data older than a certain threshold; however, this RFC suggests a
mechanism for "rolling up" old time series from the system into a lower
resolution that is still retained. This will allow us to keep some metrics
information indefinitely, which can be used for historical performance
evaluation, without needing to keep an unacceptably expensive amount of
information.

Fully realizing this solution has three components:

1. A distributed "culling" algorithm that occasionally searches for
high-resolution time series data older than a certain threshold and runs a
"roll-up" process on the discovered keys.
2. A "roll-up" process that computes low-resolution time series data from the
existing data in a high-resolution time series key, deleting the high-resolution
key in the process.
3. Modifications to the query system to utilize underlying data which is stored
at multiple resolutions (currently only supports a single resolution). This
includes the use of data at different resolutions to serve a single query.

# Motivation

In our test clusters, time series create a very large amount of data (on the
order of several gigabytes per week) which quickly loses utility as it ages.

To estimate how much data this is, we first observe the data usage of a single
time series. A single time series stores data as contiguous samples representing
ten-second intervals; all samples for a wall-clock hour are stored in a single
key. In the engine, the keys look like this:

| Key                                                      | Key Size | Value Size |
|----------------------------------------------------------|----------|------------|
| /System/tsd/cr.store.replicas/1/10s/2016-09-26T17:00:00Z | 30       | 5670       |
| /System/tsd/cr.store.replicas/1/10s/2016-09-26T18:00:00Z | 30       | 5535       |
| /System/tsd/cr.store.replicas/1/10s/2016-09-26T19:00:00Z | 30       | 5046       |

The above is the data stored for one time series over three complete hours.
Notice the variation in the size of the values; this is due to the fact that
samples may be absent for some ten-second periods, due to the asynchronous
nature of this system. For our purposes, we will estimate the size of a single 
hour of data for a single time series to be *5500* bytes, or 5.5K.

The total disk usage of high-resolution data on the cluster can thus be
estimated with the following function:

` Total bytes = [bytes per time series hour] * [# of time series per node] * [# of nodes] * [# of hours] `

Thus, data accumulates over time, and as more nodes are added (or if later
versions of cockroach add additional time series), the rate of new time series
data being accumulated increases linearly. As of this writing, each single-node store records
**242** time series. Thus, the bytes needed per hour on a ten-node cluster is:

`Total Bytes (hour) = 5500 * 242 * 10 = 13310000 (12.69 MiB)`

After just one week:

`Total Bytes (week) = 12.69MiB * 168 hours = 2.08 GiB`

As time passes, this data can represent a large share (or in the case of idle
clusters, the majority) of in-use data on the cluster. This data will also
continue to build indefinitely; a static CockroachDB Cluster will eventually
consume all available disk space, even if no external data is written! With just
the current time series, a ten-node cluster will generate almost a terabyte of
metrics data over a single year.

The prompt culling of old data is thus a clear area of improvement for
CockroachDB. However, rather than simply deleting data older than a threshold,
this RFC proposes a solution which efficiently keeps metrics data for a longer
time span by downsampling it to a much lower resolution on disk.

To give some context of numbers: currently, all metrics on disk are stored in a
format which is downsampled to _ten second sample periods_; this is the
"high-resolution" data. We are looking to delete this data when it is older
than a certain threshold, which will likely be set in the range of _2-4 weeks_.
We also propose that, when this data is deleted, it is first downsampled further
into _one hour sample periods_; this is the "low-resolution" data. This data
will be kept for a much longer time, likely _6-12 months_, but perhaps longer.

In the lower resolution, each datapoint represents the same data as an _entire
slab_ of high-resolution data (at the ten second resolution, data is stored in
slabs corresponding to a wall-clock hour; each slab contains up to 360 samples).
Thus, the expected data storage of the low-resolution is approximately _180x
smaller_ than the high-resolution (not 360 because the individual low-resolution
samples will include a "min" and "max" value not present at the high-resolution.
The high-resolution keys only contain a "sum" and "count" field.)

By keeping data at the low resolution, users will still be able to inspect
cluster performance over larger times scales, without requiring the storage of
an excessive amount of metrics data.

# Detailed design

## Culling algorithm

The culling algorithm is responsible for identifying high-resolution time series
keys that are older than a system-set threshold. Once identified, the keys are
passed into the rollup/delete process.

There are two primary design requirements of the culling algorithm:

1. From a single node, efficiently locating time series keys which need to be 
culled.
2. Across the cluster, efficiently distributing the task of culling with minimal
coordination between nodes.

#### Locating Time Series Keys

Locating time series keys to be culled is not completely trivial due to the
construction of time series keys, which is thus: 
`[ts prefix][series name][timestamp][source]`

> Example: "ts/cr.node.sql.inserts/1473739200/1" would contain time series data
> for "cr.node.sql.inserts" on September 13th 2016 between 4am-5am UTC,
> specifically for node 1.

Because of this construction, which prioritizes name over timestamp, the most 
recent time series data for series "A" would sort *before* the oldest time
series data for series "B". This means that we cannot simply cull the beginning
of the time series range.

The simplest alternative would be to scan the time series range looking for
invalid keys; however, this is considered to be a burdensome scan due to the
number of keys that are not culled. For a per-node time series being recorded on
a 10 node cluster with a 2 week retention period, we would expect to retain (10
x 24 x 14) = *3360* keys that should not be culled. In a system that maintains
dozens, possibly hundreds of time series, this is a lot of data for each node to
scan on a regular basis.

However, this scan can be effectively distributed across the cluster by creating
a new *replica queue* which searches for time series keys. The new queue can
quickly determine if each range contains time series keys (by inspecting 
start/end keys); for ranges that do contain time series keys, specific keys
can then be inspected at the engine level. This means that key inspections do
not require network calls, and the number of keys that can be inspected at once
is limited to the size of a range.

Once the queue discovers a range that contains time series keys, the scanning
process does not need to inspect every key on the range. The algorithm is as
follows:

1. Find the first time series key in the range (scan for [ts prefix]). 
2. Deconstruct the key to retrieve its name.
3. Run the rollup/delete operation on all keys in the range: 
    `[ts prefix][series name][0] - [tsprefix][series name][now - threshold]`
4. Find the next key on the range which contains data for a different time
series by searching for key `PrefixEnd([ts prefix][series name])`.
5. If a key was found in step 5, return to step 2 with that series name.

This algorithm will avoid scanning keys that do not need to be rolled up; this
is desirable, as once the culling algorithm is in place and has run once, the
majority of time series keys will *not* need to be culled.

The queue will be configured to run only on the range leader for a given range
in order to avoid duplicate work; however, this is *not* necessary for
correctness, as demonstrated in the [Rollup Algorithm](#rollup-algorithm)
section below.

The queue will initially be set to process replicas at the same rate as the
replica GC queue (as of this RFC, one range per 50 milliseconds).

##### Package Dependency

There is one particular complication to this method: *go package dependency*.
Knowledge on how to identify and cull time series keys is contained in the `ts`
package, but all logic for replica queues (and all current queues) lives in
`storage`, meaning that one of three things must happen:

+ `storage` can depend on `ts`. This seems to be trivially possible now, but may
be unintuitive to those trying to understand our code-base. For reference, the
`storage` package used to depend on the `sql` package in order to record event
logs, but this eventually became an impediment to new development and had to be
modified.
+ The queue logic could be implemented in `ts`, and `storage` could implement
an interface that allows it to use the `ts` code without a dependency.
+ Parts of the `ts` package could be split off into another package that can
intuitively live below `storage`. However, this is likely to be a considerable
portion of `ts` in order to properly implement rollups.

Tenatively, we will be attempting to use the first method and have `storage`
depend on `ts`; if it is indeed trivially possible, this will be the fastest
method of completing this project.

#### Culling low resolution data

Although the volume is much lower, low-resolution data will still build
up indefinitely unless it is culled. This data will also be culled by the same
algorithm outlined here; however, it will not be rolled up further, but will
simply be deleted.

## Rollup algorithm

The rollup algorithm is intended to be run on a single high-resolution key
identified by the culling algorithm. The algorithm is as follows:

1. Read the data in the key. Each key represents a "slab" of high resolution
samples captured over a wall-clock hour (up to 360 samples per hour).
2. "Downsample" all of the data in the key into a single sample; the new sample
will have a sum, count, min and max, computed from the samples in the original
key.
3. Write the computed sample as a low-resolution data point into the time series
system; this is exactly the same process as currently recorded time series,
except it will be writing to a different key space (with a different key
prefix).
4. Delete the original high-resolution key.

This algorithm is safe to use, even in the case where the same key is being
culled by multiple nodes at the same time; this is because step 3 and 4 are
currently *idempotent*. The low-resolution sample generated by each node will be
identical, and the engine-level time series merging system currently discards
duplicate samples. The deletion of the high-resolution key may cause an error on
some of the nodes, but only because the key will have already been deleted.

The end result is that the culled high-resolution key is gone, but a single
sample (representing the entire hour) has been written into a low-resolution
time series with the same name and source.

## Querying Across Culling Boundary

The final component of this is to allow querying across the culling boundary;
that is, if an incoming time series query wants data from both sides of the
culling boundary, it will have to process data from two different resolutions.

There are no broad design decisions to make here; this is simply a matter
of modifying low-level iterators and querying slightly different data. This
component will likely be the most complicated to actually *write*, but it should
be somewhat easier to *test* than the above algorithms, as there is already
an existing test infrastructure for time series queries.

## Implementation

This system can (and should) be implemented in three distinct phases:

1. The "culling" algorithm will be implemented, but will not roll-up the data in
discovered keys; instead, it will simply *delete* the discovered time series by
issuing a DeleteRange command. This will provide the immediate benefit of
limiting the growth of time series data on the cluster.

2. The "rollup" algorithm will be implemented, generating low-resolution data
before deleting the high-resolution data. However, the low-resolution data will
not immediately be accessible for queries.

3. The query system will be modified to consider the high-resolution data.

# Drawbacks

+ Culling represents another periodic process which runs on each node, which can
occasionally cause unexpected issues.

+ Depending on the exact layout of time series data across ranges, it is
possible that deleting time series could result in empty ranges. Specifically,
this can occur if a range contains data only for a single time series *and* the
subsequent range also contains data for that same time series. If this is a
common occurrence, it could result in a "trail" of ranges with no data, which
might add overhead into storage algorithms that scale with the number of ranges.

# Alternatives

### Alternative Location algorithm

As an alternative to the queue-based location algorithm, we could use a system
where each node maintains a list of time series it has written; given the name
of a series, it is easy to construct a scan range which will return all keys
that need to be culled:

`[ts prefix][series name][0] - [ts prefix][series name][(now - threshold)]`

This will return all keys in the series which are older than the threshold. Note
that this includes time series keys generated by any node, not just the current
node; this is acceptable, as the rollup algorithm can be run on any key from
any node.

This process can also be effectively distributed across nodes with the following
algorithm:

+ Each node's time series module maintains a list of time series it is
responsible for culling. This is initialized to a list of "retired" time series,
and is augmented each time the node writes a time series it has not written
before (in the currently running instance).
+ The time series module maintains a random permutation of the list; this
permutation is randomized again each time a new time series is added. This
should normalize very quickly, as new time series are not currently added while
a node is running.
+ Each node will periodically attempt to cull data for a single time series;
this starts with the first name in the current permutation, and proceeds through
it in a loop.

In this way, each node eventually attempts to cull all time series (guaranteeing
that each is culled), but the individual nodes proceed through the series in a
random order - this helps to distribute the work across nodes, and helps to
avoid the chance of duplicate work. The total speed of work can be tuned by
adjusting the frequency of the per-node culling process.

This alternative was rejected due to a complication that occurs when a time
series is "retired"; we only know about a time series name if the currently
running process has recorded it. If a time series is removed from the system,
its data will never be culled. Thus, we must also maintain a list of *retired*
time series names in the event that any are removed. This requires some manual
effort on the part of developers; the consequences for failing to do so are not
especially severe (a limited amount of old data will persist on the cluster),
but this is still considered inferior to the queue-based solution.

### Immediate Rollups

This was the original intention of the time series system: when a
high-resolution data sample is recorded, it is actually directly merged into
both the high-resolution AND the low-resolution time series. The engine-level
time series merging system would then be responsible for properly aggregating
multiple high-resolution samples into a single composite sample in the
low-resolution series.

The advantage of this method is that it does not require queries to use multiple
resolutions, and it allows for the delete-only culling process to be used. This
was also the original design of the time series system.

Unfortunately, it is not currently possible due to recent changes which were
required by the replica consistency checker. The engine-level merge component no
longer aggregates samples, it decimates (discarding only the most recent sample
for a period). This was necessary to deal with the unforunate reality of raft
command replays.

### Opportunistic Rollups

Instead of rolling up when low-resolution data is deleted, it is instead rolled
up as soon as an entire hour of high-resolution samples has been collected in a
key. That is, at 5:01 it should be appropriate to roll-up the data stored in the
4:00 key. With this alternative, cross-resolution queries can also be avoided
and the delete-only culling method can be used.

However, this introduces additional complications and drawbacks:

+ When querying at low resolution, data from the most recent hour will not be 
even partially available.
+ This requires maintaining additional metadata on the cluster about which
keys have already been rolled up.

# Unresolved questions
