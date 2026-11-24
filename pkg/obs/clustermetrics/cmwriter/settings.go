// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// FlushInterval controls how often cluster metrics are flushed to storage.
var FlushInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.clustermetrics.flush.interval",
	"the interval at which cluster metrics are flushed to storage",
	10*time.Second,
	settings.DurationWithMinimum(1*time.Second),
)

// FlushEnabled controls whether the cluster metrics writer is active.
var FlushEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"obs.clustermetrics.flush.enabled",
	"enables periodic flushing of cluster metrics to storage",
	true,
)
