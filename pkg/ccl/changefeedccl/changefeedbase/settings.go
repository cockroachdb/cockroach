// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

// TableDescriptorPollInterval controls how fast table descriptors are polled. A
// table descriptor must be read above the timestamp of any row that we'll emit.
//
// NB: The more generic name of this setting precedes its current
// interpretation. It used to control additional polling rates.
var TableDescriptorPollInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
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
	settings.ApplicationLevel,
	"changefeed.memory.per_changefeed_limit",
	"controls amount of data that can be buffered per changefeed",
	1<<29, // 512MiB
	settings.WithPublic)

// SlowSpanLogThreshold controls when we will log slow spans.
var SlowSpanLogThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.slow_span_log_threshold",
	"a changefeed will log spans with resolved timestamps this far behind the current wall-clock time; if 0, a default value is calculated based on other cluster settings",
	0,
	settings.NonNegativeDuration,
)

// IdleTimeout controls how long the changefeed will wait for a new KV being
// emitted before marking itself as idle.
var IdleTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.idle_timeout",
	"a changefeed will mark itself idle if no changes have been emitted for greater than this duration; if 0, the changefeed will never be marked idle",
	10*time.Minute,
	settings.NonNegativeDuration,
	settings.WithName("changefeed.auto_idle.timeout"),
)

// SpanCheckpointInterval controls how often span-level checkpoints
// can be written.
var SpanCheckpointInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.frontier_checkpoint_frequency",
	"interval at which span-level checkpoints will be written; "+
		"if 0, span-level checkpoints are disabled",
	10*time.Minute,
	settings.NonNegativeDuration,
	settings.WithName("changefeed.span_checkpoint.interval"),
)

// SpanCheckpointLagThreshold controls the amount of time a changefeed's
// lagging spans must lag behind its leading spans before a span-level
// checkpoint is written.
var SpanCheckpointLagThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.frontier_highwater_lag_checkpoint_threshold",
	"the amount of time a changefeed's lagging (slowest) spans must lag "+
		"behind its leading (fastest) spans before a span-level checkpoint "+
		"to save leading span progress is written; if 0, span-level checkpoints "+
		"due to lagging spans is disabled",
	10*time.Minute,
	settings.NonNegativeDuration,
	settings.WithPublic,
	settings.WithName("changefeed.span_checkpoint.lag_threshold"),
)

// SpanCheckpointMaxBytes controls the maximum number of key bytes that will be added
// to a span-level checkpoint record.
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
// SpanCheckpointInterval setting, 1 MB per checkpoint.
var SpanCheckpointMaxBytes = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"changefeed.frontier_checkpoint_max_bytes",
	"the maximum size of a changefeed span-level checkpoint as measured by the total size of key bytes",
	1<<20, // 1 MiB
	settings.WithName("changefeed.span_checkpoint.max_bytes"),
)

// ScanRequestLimit is the number of Scan requests that can run at once.
// Scan requests are issued when changefeed performs the backfill.
// If set to 0, a reasonable default will be chosen.
var ScanRequestLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.backfill.concurrent_scan_requests",
	"number of concurrent scan requests per node issued during a backfill",
	0,
	settings.WithPublic)

// ScanRequestSize is the target size of the scan request response.
//
// TODO(cdc,yevgeniy,irfansharif): 16 MiB is too large for "elastic" work such
// as this; reduce the default. Evaluate this as part of #90089.
var ScanRequestSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.backfill.scan_request_size",
	"the maximum number of bytes returned by each scan request",
	1<<19, // 1/2 MiB
	settings.WithPublic)

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
var NodeSinkThrottleConfig = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"changefeed.node_throttle_config",
	"specifies node level throttling configuration for all changefeeeds",
	"",
	settings.WithValidateString(validateSinkThrottleConfig),
	settings.WithPublic,
	settings.WithReportable(true),
)

func validateSinkThrottleConfig(values *settings.Values, configStr string) error {
	if configStr == "" {
		return nil
	}
	var config = &SinkThrottleConfig{}
	return json.Unmarshal([]byte(configStr), config)
}

// ResolvedTimestampMinUpdateInterval specifies the minimum amount of time that
// must have elapsed since the last time a changefeed's resolved timestamp was
// updated before it is eligible to updated again.
var ResolvedTimestampMinUpdateInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.min_highwater_advance",
	"minimum amount of time that must have elapsed since the last time "+
		"a changefeed's resolved timestamp was updated before it is eligible to be "+
		"updated again; default of 0 means no minimum interval is enforced but "+
		"updating will still be limited by the average time it takes to checkpoint progress",
	0,
	settings.NonNegativeDuration,
	settings.WithPublic,
	settings.WithName("changefeed.resolved_timestamp.min_update_interval"),
)

// EventMemoryMultiplier is the multiplier for the amount of memory needed to process an event.
//
// Memory accounting is hard.  Furthermore, during the lifetime of the event, the
// amount of resources used to process such event varies. So, instead of coming up
// with complex schemes to accurately measure and adjust current memory usage,
// we'll request the amount of memory multiplied by this fudge factor.
var EventMemoryMultiplier = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"changefeed.event_memory_multiplier",
	"the amount of memory required to process an event is multiplied by this factor",
	3,
	settings.FloatWithMinimum(1),
)

// ProtectTimestampInterval controls the frequency of protected timestamp record updates
var ProtectTimestampInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.protect_timestamp_interval",
	"controls how often the changefeed forwards its protected timestamp to the resolved timestamp",
	10*time.Minute,
	settings.PositiveDuration,
	settings.WithPublic)

// ProtectTimestampLag controls how much the protected timestamp record should lag behind the high watermark
var ProtectTimestampLag = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.protect_timestamp.lag",
	"controls how far behind the checkpoint the changefeed's protected timestamp is",
	10*time.Minute,
	settings.PositiveDuration)

// MaxProtectedTimestampAge controls the frequency of protected timestamp record updates
var MaxProtectedTimestampAge = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.protect_timestamp.max_age",
	"fail the changefeed if the protected timestamp age exceeds this threshold; 0 disables expiration",
	4*24*time.Hour,
	settings.NonNegativeDuration,
	settings.WithPublic)

// BatchReductionRetryEnabled enables the temporary reduction of batch sizes upon kafka message too large errors
var BatchReductionRetryEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.batch_reduction_retry_enabled",
	"if true, kafka changefeeds upon erroring on an oversized batch will attempt to resend the messages with progressively lower batch sizes",
	false,
	settings.WithName("changefeed.batch_reduction_retry.enabled"),
	settings.WithPublic)

// EventConsumerWorkers specifies the maximum number of workers to use when
// processing  events.
var EventConsumerWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.event_consumer_workers",
	"the number of workers to use when processing events: <0 disables, "+
		"0 assigns a reasonable default, >0 assigns the setting value. for experimental/core "+
		"changefeeds and changefeeds using parquet format, this is disabled",
	0,
	settings.WithPublic)

// EventConsumerWorkerQueueSize specifies the maximum number of events a worker buffer.
var EventConsumerWorkerQueueSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.event_consumer_worker_queue_size",
	"if changefeed.event_consumer_workers is enabled, this setting sets the maxmimum number of events "+
		"which a worker can buffer",
	int64(metamorphic.ConstantWithTestRange("changefeed.event_consumer_worker_queue_size", 16, 0, 16)),
	settings.NonNegativeInt,
	settings.WithPublic)

// EventConsumerPacerRequestSize specifies how often (measured in CPU time)
// that event consumer workers request CPU time from admission control.
// For example, every N milliseconds of CPU work, request N more
// milliseconds of CPU time.
var EventConsumerPacerRequestSize = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.cpu.per_event_consumer_worker_allocation",
	"an event consumer worker will perform a blocking request for CPU time "+
		"before consuming events. after fully utilizing this CPU time, it will "+
		"request more",
	50*time.Millisecond,
	settings.PositiveDuration,
)

// PerEventElasticCPUControlEnabled determines whether changefeed event
// processing integrates with elastic CPU control.
var PerEventElasticCPUControlEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.cpu.per_event_elastic_control.enabled",
	"determines whether changefeed event processing integrates with elastic CPU control",
	true,
)

// RequireExternalConnectionSink is used to restrict non-admins with the CHANGEFEED privilege
// to create changefeeds to external connections only.
var RequireExternalConnectionSink = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"changefeed.permissions.require_external_connection_sink",
	"if enabled, this settings restricts users with the CHANGEFEED privilege"+
		" to create changefeeds with external connection sinks only."+
		" see https://www.cockroachlabs.com/docs/stable/create-external-connection.html",
	false,
	settings.WithName("changefeed.permissions.require_external_connection_sink.enabled"),
)

// SinkIOWorkers controls the number of IO workers used by sinks that use
// parallelIO to be able to send multiple requests in parallel.
var SinkIOWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.sink_io_workers",
	"the number of workers used by changefeeds when sending requests to the sink "+
		"(currently the batching versions of webhook, pubsub, and kafka sinks that are "+
		"enabled by changefeed.new_<sink type>_sink_enabled only): <0 disables, 0 assigns "+
		"a reasonable default, >0 assigns the setting value",
	0,
	settings.WithPublic)

// SinkPacerRequestSize specifies how often (measured in CPU time)
// that the Sink batching worker request CPU time from admission control. For
// example, every N milliseconds of CPU work, request N more milliseconds of CPU
// time.
var SinkPacerRequestSize = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.cpu.sink_encoding_allocation",
	"an event consumer worker will perform a blocking request for CPU time "+
		"before consuming events. after fully utilizing this CPU time, it will "+
		"request more",
	50*time.Millisecond,
	settings.PositiveDuration,
)

// UsageMetricsReportingInterval is the interval at which the changefeed
// calculates and updates its usage metric.
var UsageMetricsReportingInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.usage.reporting_interval",
	"the interval at which the changefeed calculates and updates its usage metric",
	5*time.Minute,
	settings.PositiveDuration, settings.DurationInRange(2*time.Minute, 50*time.Minute),
)

// UsageMetricsReportingTimeoutPercent is the percent of
// UsageMetricsReportingInterval that may be spent gathering the usage metrics.
var UsageMetricsReportingTimeoutPercent = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.usage.reporting_timeout_percent",
	"the percent of changefeed.usage.reporting_interval that may be spent gathering the usage metrics",
	50,
	settings.IntInRange(10, 100),
)

// DefaultLaggingRangesThreshold is the default duration by which a range must be
// lagging behind the present to be considered as 'lagging' behind in metrics.
var DefaultLaggingRangesThreshold = 3 * time.Minute

// DefaultLaggingRangesPollingInterval is the default polling rate at which
// lagging ranges are checked and metrics are updated.
var DefaultLaggingRangesPollingInterval = 1 * time.Minute

var Quantize = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"changefeed.resolved_timestamp.granularity",
	"the granularity at which changefeed progress are quantized to make tracking more efficient",
	0,
	settings.NonNegativeDuration,
)
