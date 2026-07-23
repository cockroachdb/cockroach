// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
The Key Visualizer enables the visualization of historical KV traffic. It's main
objective is to enable CRDB engineers and operators to quickly identify
historical and current KV hotspots. When a hotspot is identified, the user can
immediately tell:
	a) Where in the keyspace the hotspot exist(s|ed).
	b) The time when the hotspot appeared and disappeared.

The Key Visualizer can be enabled via
`SET CLUSTER_SETTING keyvisualizer.enabled = true;`

The visualization represents roachpb.RequestUnion.Requests destined for a
specific key-span on a log scale from black to red, representing "cold" and
"hot", respectively. The keyspace [Min,Max) is represented on the Y-axis, and
time is represented on the X-axis. By default, up to 2 weeks of historical data
is persisted and visualized, with a sample period of 1 Minute.

The Key Visualizer relies on the packages in this directory to collect,
down-sample, and persist the samples that are ultimately served to the user's
browser. The implementation of the visualization itself lives in
pkg/ui/workspaces/db-console/src/views/keyVisualizer.

Package spanstatsconsumer defines the SpanStatsConsumer. The SpanStatsConsumer
is responsible for configuring the collectors, fetching samples from the
collectors, downsampling the retrieved samples, and persisting the samples to
system tables. Only one instance of the SpanStatsConsumer lives in a cluster,
and the SpanStatsConsumer is the system tenant's point of access to KV and the
SpanStatsCollectors.

	Configuring Collectors:
		The consumer has the ability to control the spans that the collectors
		should count requests for. For now, the spans that the consumer
		communicates to the collector correspond 1:1 with all ranges.
		But in the future, the consumer could instruct the collector to
		provide a higher sampling resolution to the parts of the keyspace that
		are experiencing a hotspot.

	Fetching Samples and Downsampling:
		The consumer fetches samples from the Collectors and then downsamples
		each sample. Downsampling is necessary to limit the total space
		requirements of storing, transmitting, and visualizing historical data.
		Each sample is limited to 512 buckets by default, where a bucket is a
		start key, an end key, and the number of requests counted during the
		sample period. The system tenant could have many thousands of ranges
		(thus the collector produces a sample with many thousands of buckets),
		so the downsampler works to preserve resolution in the parts of the
		keyspace that are statistically interesting while quickly aggregating
		together the parts of the keyspace that aren't interesting.


	Persisting Samples:
		The consumer persists samples to the system.span_stats_samples table.
		The samples are denormalized: A sample's buckets and keys are written to
		system.span_stats_buckets and system.span_stats_unique_keys,
		respectively. This storage decision is motivated by the costs of storing
		many roachpb.Keys. Because a roachpb.Key can be arbitrarily large, it is
		only stored once, and dependent buckets refer to it by a UUID.


Package spanstatscollector defines the SpanStatsCollector. A SpanStatsCollector
instance lives on each KV node, so KV traffic across the entire cluster can be
collected. A collector sets its collection boundaries according to the consumer,
and will count every request whose span is contained inclusively within
one of the boundaries specified by the consumer. After a sample period has
passed, the collector rolls the sample over, and resets to count requests that
match the spans in the next queued boundary update.
The collector serves the rolled-over samples to the consumer when requested.

Package keyvisstorage defines abstractions for reading and writing to the system
tables that persist historical data.

Package spanstatskvaccessor is used by the SpanStatsConsumer to interact with KV
and the SpanStatsCollectors.

Package keyvissubscriber defines a rangefeed that is used to publish the
boundary updates to all the SpanStatsCollector instances. It is the
transmission mechanism that enables the SpanStatsConsumer's desired spans to
reliably reach the SpanStatsCollectors.

Package keyvisjob defines the job that orchestrates the SpanStatsConsumer.
Every sample period, the job executes:
- SpanStatsConsumer.UpdateBoundaries
- SpanStatsConsumer.GetSamples
- SpanStatsConsumer.DeleteExpiredSamples

*/

package keyvisualizer
