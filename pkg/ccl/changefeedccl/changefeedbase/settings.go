// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// TableDescriptorPollInterval controls how fast table descriptors are polled. A
// table descriptor must be read above the timestamp of any row that we'll emit.
//
// NB: The more generic name of this setting precedes its current
// interpretation. It used to control additional polling rates.
var TableDescriptorPollInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.experimental_poll_interval",
	"polling interval for the table descriptors",
	1*time.Second,
	settings.NonNegativeDuration,
)

// DefaultMinCheckpointFrequency is the default frequency to flush sink.
// See comment in newChangeAggregatorProcessor for explanation on the value.
var DefaultMinCheckpointFrequency = 30 * time.Second

// TestingSetDefaultMinCheckpointFrequency changes DefaultMinCheckpointFrequency for tests.
// Returns function to restore flush frequency to its original value.
func TestingSetDefaultMinCheckpointFrequency(f time.Duration) func() {
	old := DefaultMinCheckpointFrequency
	DefaultMinCheckpointFrequency = f
	return func() { DefaultMinCheckpointFrequency = old }
}

// PerChangefeedMemLimit controls how much data can be buffered by
// a single changefeed.
var PerChangefeedMemLimit = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"changefeed.memory.per_changefeed_limit",
	"controls amount of data that can be buffered per changefeed",
	1<<30,
)

// SlowSpanLogThreshold controls when we will log slow spans.
var SlowSpanLogThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.slow_span_log_threshold",
	"a changefeed will log spans with resolved timestamps this far behind the current wall-clock time; if 0, a default value is calculated based on other cluster settings",
	0,
	settings.NonNegativeDuration,
)

// IdleTimeout controls how long the changefeed will wait for a new KV being
// emitted before marking itself as idle.
var IdleTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.idle_timeout",
	"a changefeed will mark itself idle if no changes have been emitted for greater than this duration; if 0, the changefeed will never be marked idle",
	10*time.Minute,
	settings.NonNegativeDuration,
)

// FrontierCheckpointFrequency controls the frequency of frontier checkpoints.
var FrontierCheckpointFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
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
	settings.TenantWritable,
	"changefeed.frontier_checkpoint_max_bytes",
	"controls the maximum size of the checkpoint as a total size of key bytes",
	1<<20,
)

// ScanRequestLimit is the number of Scan requests that can run at once.
// Scan requests are issued when changefeed performs the backfill.
// If set to 0, a reasonable default will be chosen.
var ScanRequestLimit = settings.RegisterIntSetting(
	settings.TenantWritable,
	"changefeed.backfill.concurrent_scan_requests",
	"number of concurrent scan requests per node issued during a backfill",
	0,
)

// SinkThrottleConfig describes throttling configuration for the sink.
// 0 values for any of the settings disable that setting.
type SinkThrottleConfig struct {
	// MessageRate sets approximate messages/s limit.
	MessageRate float64 `json:",omitempty"`
	// MessageBurst sets burst budget for messages/s.
	MessageBurst float64 `json:",omitempty"`
	// ByteRate sets approximate bytes/second limit.
	ByteRate float64 `json:",omitempty"`
	// RateBurst sets burst budget in bytes/s.
	ByteBurst float64 `json:",omitempty"`
	// FlushRate sets approximate flushes/s limit.
	FlushRate float64 `json:",omitempty"`
	// FlushBurst sets burst budget for flushes/s.
	FlushBurst float64 `json:",omitempty"`
}

// NodeSinkThrottleConfig is the node wide throttling configuration for changefeeds.
var NodeSinkThrottleConfig = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		"changefeed.node_throttle_config",
		"specifies node level throttling configuration for all changefeeeds",
		"",
		validateSinkThrottleConfig,
	)
	s.SetVisibility(settings.Public)
	s.SetReportable(true)
	return s
}()

func validateSinkThrottleConfig(values *settings.Values, configStr string) error {
	if configStr == "" {
		return nil
	}
	var config = &SinkThrottleConfig{}
	return json.Unmarshal([]byte(configStr), config)
}

// MinHighWaterMarkCheckpointAdvance specifies the minimum amount of time the
// changefeed high water mark must advance for it to be eligible for checkpointing.
var MinHighWaterMarkCheckpointAdvance = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.min_highwater_advance",
	"minimum amount of time the changefeed high water mark must advance "+
		"for it to be eligible for checkpointing; Default of 0 will checkpoint every time frontier "+
		"advances, as long as the rate of checkpointing keeps up with the rate of frontier changes",
	0,
	settings.NonNegativeDuration,
)

// EventMemoryMultiplier is the multiplier for the amount of memory needed to process an event.
//
// Memory accounting is hard.  Furthermore, during the lifetime of the event, the
// amount of resources used to process such event varies. So, instead of coming up
// with complex schemes to accurately measure and adjust current memory usage,
// we'll request the amount of memory multiplied by this fudge factor.
var EventMemoryMultiplier = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"changefeed.event_memory_multiplier",
	"the amount of memory required to process an event is multiplied by this factor",
	3,
	func(v float64) error {
		if v < 1 {
			return errors.New("changefeed.event_memory_multiplier must be at least 1")
		}
		return nil
	},
)

// ProtectTimestampInterval controls the frequency of protected timestamp record updates
var ProtectTimestampInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.protect_timestamp_interval",
	"controls how often the changefeed forwards its protected timestamp to the resolved timestamp",
	10*time.Minute,
	settings.PositiveDuration,
)
