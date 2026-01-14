// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perftrace

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Enabled controls whether work span capture is enabled.
var Enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.work_span_capture.enabled",
	"enables work span capture for query observability (experimental)",
	true, // enabled by default for POC
)

// ReservoirSize controls the number of spans to sample per node.
var ReservoirSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.work_span_capture.reservoir_size",
	"number of work spans to sample per node before flushing",
	100,
	settings.IntInRange(10, 10000),
)

// FlushInterval controls the interval between flushes to the system table.
var FlushInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.work_span_capture.flush_interval",
	"interval between work span flushes to the system table",
	time.Second*20,
	settings.DurationInRange(10*time.Second, 10*time.Minute),
)

// DeleteRatio controls the fraction of existing spans to delete during flush.
var DeleteRatio = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.work_span_capture.delete_ratio",
	"fraction of existing spans to randomly delete during each flush (0.0 to 1.0)",
	0.01, // 1% by default
	settings.FloatInRange(0.0, 1.0),
)
