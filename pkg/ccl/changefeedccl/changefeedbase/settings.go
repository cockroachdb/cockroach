// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// TableDescriptorPollInterval controls how fast table descriptors are polled. A
// table descriptor must be read above the timestamp of any row that we'll emit.
//
// NB: The more generic name of this setting precedes its current
// interpretation. It used to control additional polling rates.
var TableDescriptorPollInterval = settings.RegisterDurationSetting(
	"changefeed.experimental_poll_interval",
	"polling interval for the table descriptors",
	1*time.Second,
	settings.NonNegativeDuration,
)

// DefaultFlushFrequency is the default frequency to flush sink.
// See comment in newChangeAggregatorProcessor for explanation on the value.
var DefaultFlushFrequency = 5 * time.Second

// TestingSetDefaultFlushFrequency changes defaultFlushFrequency for tests.
// Returns function to restore flush frequency to its original value.
func TestingSetDefaultFlushFrequency(f time.Duration) func() {
	old := DefaultFlushFrequency
	DefaultFlushFrequency = f
	return func() { DefaultFlushFrequency = old }
}

// PerChangefeedMemLimit controls how much data can be buffered by
// a single changefeed.
var PerChangefeedMemLimit = settings.RegisterByteSizeSetting(
	"changefeed.memory.per_changefeed_limit",
	"controls amount of data that can be buffered per changefeed",
	1<<30,
)

// SlowSpanLogThreshold controls when we will log slow spans.
var SlowSpanLogThreshold = settings.RegisterDurationSetting(
	"changefeed.slow_span_log_threshold",
	"a changefeed will log spans with resolved timestamps this far behind the current wall-clock time; if 0, a default value is calculated based on other cluster settings",
	0,
	settings.NonNegativeDuration,
)

// FrontierCheckpointFrequency controls the frequency of frontier checkpoints.
var FrontierCheckpointFrequency = settings.RegisterDurationSetting(
	"changefeed.frontier_checkpoint_frequency",
	"controls the frequency with which span level checkpoints will be written; if 0, disabled.",
	10*time.Minute,
	settings.NonNegativeDuration,
)

// FrontierCheckpointMaxBytes controls the maximum number of key bytes that will be added
// to the checkpoint record.
// Checkpoint record could be fairly large.
// Assume we have a 10T table, and a 1/2G max range size: 20K spans.
// Span frontier merges adjacent spans, so worst case we have 10K spans.
// Each span is a pair of keys.  Those could be large.  Assume 1/2K per key.
// So, 1KB per span.  We could be looking at 10MB checkpoint record.
//
// The default for this setting was chosen as follows:
//   * Assume a very long backfill, running for 25 hours (GC TTL default duration).
//   * Assume we want to have at most 150MB worth of checkpoints in the job record.
// Therefore, we should write at most 6 MB of checkpoint/hour; OR, based on the default
// FrontierCheckpointFrequency setting, 1 MB per checkpoint.
var FrontierCheckpointMaxBytes = settings.RegisterByteSizeSetting(
	"changefeed.frontier_checkpoint_max_bytes",
	"controls the maximum size of the checkpoint as a total size of key bytes",
	1<<20,
)

// ScanRequestLimit is the number of Scan requests that can run at once.
// Scan requests are issued when changefeed performs the backfill.
// If set to 0, a reasonable default will be chosen.
var ScanRequestLimit = settings.RegisterIntSetting(
	"changefeed.backfill.concurrent_scan_requests",
	"number of concurrent scan requests per node issued during a backfill",
	0,
)
