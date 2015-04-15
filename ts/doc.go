// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

/*
Package ts provides a high-level time series API on top of an underlying
Cockroach datastore. Cockroach itself provides a single monolithic, sorted
key-value map. The ts package collects time series data, consisting of
timestamped data points, and efficiently stores it in the key-value store.

Storing time series data is a unique challenge for databases. Time series data
is typically generated at an extremely high volume, and is queried by providing
a range of time of arbitrary size, which can lead to an enormous amount of data
being scanned for a query. Many specialized time series databases exist to meet
these challenges.

Cockroach organizes time series data by series name and timestamp; when querying
a given series over a time range, all data in that range is thus stored
contiguously. However, Cockroach transforms the data in a few important ways in
order to effectively query a large amount of data.

Downsampling

The amount of data produced by time series sampling can be staggering; the naive
solution, storing every incoming data point with perfect fidelity in a unique
key, would commmand a tremendous amount of computing resources.

However, in most cases a perfect fidelity is not necessary or desired; the exact
time a sample was taken is unimportant, with the overall trend of the data over
time being far more important to analysis than the individual samples.

With this in mind, Cockroach downsamples data before storing it; the original
timestamp for each data point in a series is not recorded. Cockroach instead
divides time into contiguous slots of uniform length (e.g. 10 seconds); if
multiple data points for a series fall in the same slot, they are aggregated
together.  Aggregated data for a slot records the count, sum, min and max of
data points that were found in that slot. A single slot is referred to as a
"sample", and the length of the slot is known as the "sample duration".

Cockroach also uses its own key space efficiently by storing data for
multiple samples in a single key. This is done by again dividing time into
contiguous slots, but with a longer duration; this is known as the "key
duration". For example, Cockroach samples much of its internal data at a
resolution of 10 seconds, but stores it with a "key duration" of 1 hour, meaning
that all samples that fall in the same hour are stored at the same key. This
strategy helps reduce the number of keys scanned during a query.

Finally, Cockroach can record the same series at multiple sample durations, in a
process commonly known as "rollup".  For example, a single series may be
recorded with a sample size of 10 seconds, but also record the same data with a
sample size of 1 hour. The 1 hour data will have much less information, but can
be queried much faster; this is very useful when querying a series over a very
long period of time (e.g. an entire month or year).

A specific sample duration in Cockroach is known as a Resolution. Cockroach
supports a fixed set of Resolutions; each Resolution has a fixed sample duration
and a key duration. For example, the resolution "Resolution10s" has a sample
duration of 10 seconds and a key duration of 1 hour.

Source Keys

Another dimension of time series queries is the aggregation of multiple series;
for example, you may want to query the same data point across multiple machines
on a cluster.

While Cockroach will support this, in some cases queries are almost *always* an
aggregate of multiple source series; for example, we often want to query storage
data for a Cockroach accounting prefix, but data is always collected from
multiple stores; the information on a specific range is rarely relevant, as they
can arbitrarily split and rebalance over time.

Unforunately, data from multiple sources cannot be safely aggregated in the same
way as multiple data points from the same series can be downsampled; each series
must be stored separately. However, Cockroach *can* optimize for this situation
by altering the keyspace; Cockroach can store data from multiple sources
contiguously in the key space, ensuring that the multiple series can be queried
together more efficiently.

This is done by creating a "source key", an optional identifier that is separate
from the series name itself. Source keys are appended to the key as a suffix,
after the series name and timestamp; this means that data that is from the same
series and time period, but is from different sources, will be stored adjacently
in the key space.  Data from all sources in a series can thus be queried in a
single scan.

Example

A hypothetical example from Cockroach: we want to record the size of all data
stored in the cluster with a key prefix of "ExampleApplication". This will let
us track data usage for a single application in a shared cluster.

The series name is: Cockroach.disksize.ExampleApplication

Data points for this series are automatically collected from any store that
contains keys with a prefix of "ExampleApplication". When data points are
written, they are recorded with a store key of: [store id]

There are 3 stores which contain data: 3, 9 and 10.  These are arbitrary and may
change over time, and are not often interested in the behavior of a specific
store in this context.

Data is recorded for January 1st, 2016 between 10:05 pm and 11:05 pm. The data
is recorded at a 10 second resolution.

The data is recorded into keys structurally similar to the following:

	tsd.cockroach.disksize.ExampleApplication.10s.403234.3
	tsd.cockroach.disksize.ExampleApplication.10s.403234.9
	tsd.cockroach.disksize.ExampleApplication.10s.403234.10
	tsd.cockroach.disksize.ExampleApplication.10s.403235.3
	tsd.cockroach.disksize.ExampleApplication.10s.403235.9
	tsd.cockroach.disksize.ExampleApplication.10s.403235.10

Data for each source is stored in two keys: one for the 10 pm hour, and one
for the 11pm hour. Each key contains the tsd prefix, the series name, the
resolution (10s), a timestamp representing the hour, and finally the series key. The
keys will appear in the data store in the order shown above.

(Note that the keys will NOT be exactly as pictured above; they will be encoded
in a way that is more efficient, but is not readily human readable.)

TODO(mrtracy):
The ts package is a work in progress, and will initially only service queries
for Cockroach's own internally generated time series data.
*/
package ts
