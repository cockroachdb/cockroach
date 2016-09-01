- Feature Name: Time Series Rollups
- Status: draft
- Start Date: 2016-08-29
- Authors: Matt Tracy
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary
Currently, Time Series data recorded for by CockroachDB for its own internal
metrics is retained indefinitely. High-resolution metrics data quickly loses 
utility as it ages, consuming disk space and creating range-related overhead
without confering an appropriate benefit.

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

In our test clusters, this creates a very large amount of data (on the order of
several gigabytes per week) which quickly loses utility as it ages. As weeks go
on, this data can represent a large share (or in the case of smaller clusters,
the majority) of in-use data on the cluster. This data will also continue to
build indefinitely; a static CockroachDB Cluster will eventually consume all 
available disk space, even if no external data is added!

The prompt culling of old data is clearly necessary for the CockroachDB system.
However, rather than simply deleting data older than a threshold, this RFC
proposes a solution which efficiently keeps metrics data for a longer time span
by downsampling it to a much lower resolution on disk.

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
samples will include a "min" and "max" value not present at the high-resolution)

By keeping data at the low resolution, users will still be able to inspect
cluster performance over larger times scales, without requiring the storage of
an excessive amount of metrics data.

# Detailed design

## Culling algorithm

The culling algorithm is responsible for identifying high-resolution time series
keys that are older than a system-set threshold. Once identified, the keys are
passed into the rollup process.

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

As an alternative, we suggest a system where each node maintains a list of time
series it has written; given the name of a series, it is easy to construct a scan
range which will return all keys that need to be culled:

`[ts prefix][series name][0] - [ts prefix][series name][(now - threshold)]`

This will return all keys in the series which are older than the threshold. Note
that this includes time series keys generated by any node, not just the current
node; this is acceptable, as the rollup algorithm can be run on any key from
any node.

A complication of this method occurs when a time series is "retired"; we only
know about a time series name if the currently running process has recorded it.
If a time series is removed from the system, its data will never be culled.
Thus, we must also maintain a list of *retired* time series names in the event
that any are removed. This requires some manual effort on the part of
developers; however, the consequences for failing to do so are not especially
severe (a limited amount of old data will persist on the cluster).

#### Distributing the culling process

Distribution of the culling process across the cluster is another important
concern. We want to *guarantee* that all time series are eventually culled. We
also want time series to be culled quickly, which can be helped by properly
distributing the time series key space across nodes. We also want to minimize
the chance of duplicate work, which can occur when multiple nodes operate on the
same keys at the same time.

The suggested algorithm for this is as follows:

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

#### Culling low resolution data

Although the volume is exceptionally lower, low-resolution data will still build
up indefinitely unless it is culled. This data could also be culled by the same
algorithm outlined here; it could then be rolled-up further, or simply deleted
(see the ["Delete only"](#delete-only) option in the alternatives sections).

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
be somewhat easier to *test* that the above algorithms, as there is already
an existing test infrastructure for time series queries.

# Drawbacks

+ Culling represents another periodic process which runs on each node, which can
occasionally cause unexpected issues.

# Alternatives

### Delete Only

In this alternative, culled data is simply deleted and *not* rolled up into a
low-resolution time series. For this alternative, the culling algorithm does
not even need to read keys it is deleting; it can likely just issue DeleteRange
commands directly to the KV layer, using the range of keys it generates for
each time series.

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
up as soon as entire hour of high-resolution samples has been collected in a
key. That is, at 5:01 it should be appropriate to roll-up the data stored
in the 4:00 key. With this alternative, cross-resolution queries can also be avoided
and the delete-only culling method can be used.

However, this introduces additional complications and drawbacks:

+ When querying at low resolution, data from the most recent hour will not be 
even partially available.
+ This requires maintaining additional metadata on the cluster about which
keys have already been rolled up.

# Unresolved questions
