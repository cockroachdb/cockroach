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

// ScanRequestLimit is the number of Scan requests that can run at once.
// Scan requests are issued when changefeed performs the backfill.
// If set to 0, a reasonable default will be chosen.
var ScanRequestLimit = settings.RegisterIntSetting(
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
