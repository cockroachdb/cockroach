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
	"github.com/cockroachdb/cockroach/pkg/util"
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
	1<<29, // 512MiB
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

// FrontierHighwaterLagCheckpointThreshold controls the amount the high-water
// mark is allowed to lag behind the leading edge of the frontier before we
// begin to attempt checkpointing spans above the high-water mark
var FrontierHighwaterLagCheckpointThreshold = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.frontier_highwater_lag_checkpoint_threshold",
	"controls the maximum the high-water mark is allowed to lag behind the leading spans of the frontier before per-span checkpointing is enabled; if 0, checkpointing due to high-water lag is disabled",
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
//   - Assume a very long backfill, running for 25 hours (GC TTL default duration).
//   - Assume we want to have at most 150MB worth of checkpoints in the job record.
//
// Therefore, we should write at most 6 MB of checkpoint/hour; OR, based on the default
// FrontierCheckpointFrequency setting, 1 MB per checkpoint.
var FrontierCheckpointMaxBytes = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"changefeed.frontier_checkpoint_max_bytes",
	"controls the maximum size of the checkpoint as a total size of key bytes",
	1<<20, // 1 MiB
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

// ScanRequestSize is the target size of the scan request response.
//
// TODO(cdc,yevgeniy,irfansharif): 16 MiB is too large for "elastic" work such
// as this; reduce the default. Evaluate this as part of #90089.
var ScanRequestSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"changefeed.backfill.scan_request_size",
	"the maximum number of bytes returned by each scan request",
	1<<19, // 1/2 MiB
).WithPublic()

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

// BatchReductionRetryEnabled enables the temporary reduction of batch sizes upon kafka message too large errors
var BatchReductionRetryEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.batch_reduction_retry_enabled",
	"if true, kafka changefeeds upon erroring on an oversized batch will attempt to resend the messages with progressively lower batch sizes",
	false,
)

// UseMuxRangeFeed enables the use of MuxRangeFeed RPC.
var UseMuxRangeFeed = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.mux_rangefeed.enabled",
	"if true, changefeed uses multiplexing rangefeed RPC",
	util.ConstantWithMetamorphicTestBool("changefeed.mux_rangefeed.enabled", false),
)

// EventConsumerWorkers specifies the maximum number of workers to use when
// processing  events.
var EventConsumerWorkers = settings.RegisterIntSetting(
	settings.TenantWritable,
	"changefeed.event_consumer_workers",
	"the number of workers to use when processing events: <0 disables, "+
		"0 assigns a reasonable default, >0 assigns the setting value. for experimental/core "+
		"changefeeds and changefeeds using parquet format, this is disabled",
	0,
).WithPublic()

// EventConsumerWorkerQueueSize specifies the maximum number of events a worker buffer.
var EventConsumerWorkerQueueSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"changefeed.event_consumer_worker_queue_size",
	"if changefeed.event_consumer_workers is enabled, this setting sets the maxmimum number of events "+
		"which a worker can buffer",
	int64(util.ConstantWithMetamorphicTestRange("changefeed.event_consumer_worker_queue_size", 16, 0, 16)),
	settings.NonNegativeInt,
).WithPublic()

// EventConsumerPacerRequestSize specifies how often (measured in CPU time)
// that event consumer workers request CPU time from admission control.
// For example, every N milliseconds of CPU work, request N more
// milliseconds of CPU time.
var EventConsumerPacerRequestSize = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.cpu.per_event_consumer_worker_allocation",
	"an event consumer worker will perform a blocking request for CPU time "+
		"before consuming events. after fully utilizing this CPU time, it will "+
		"request more",
	50*time.Millisecond,
	settings.PositiveDuration,
)

// EventConsumerElasticCPUControlEnabled determines whether changefeed event
// processing integrates with elastic CPU control.
var EventConsumerElasticCPUControlEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.cpu.per_event_elastic_control.enabled",
	"determines whether changefeed event processing integrates with elastic CPU control",
	true,
)

// RequireExternalConnectionSink is used to restrict non-admins with the CHANGEFEED privilege
// to create changefeeds to external connections only.
var RequireExternalConnectionSink = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.permissions.require_external_connection_sink",
	"if enabled, this settings restricts users with the CHANGEFEED privilege"+
		" to create changefeeds with external connection sinks only."+
		" see https://www.cockroachlabs.com/docs/stable/create-external-connection.html",
	false,
)
