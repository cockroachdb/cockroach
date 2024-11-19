// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvissettings

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Enabled enables the key visualizer job.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"keyvisualizer.enabled",
	"enable the use of the key visualizer", false)

// SampleInterval defines the sample period for key visualizer samples.
var SampleInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"keyvisualizer.sample_interval",
	"the frequency at which the key visualizer samples are collected",
	5*time.Minute,
	settings.DurationWithMinimum(1*time.Minute),
)

// MaxBuckets defines the maximum buckets allowed in a sample.
// Allowing more buckets will lead to a higher sampling resolution,
// at the cost of increased storage and network requirements.
var MaxBuckets = settings.RegisterIntSetting(
	settings.SystemOnly,
	"keyvisualizer.max_buckets",
	"the maximum number of buckets in a sample",
	256,
	settings.IntInRange(1, 1024),
)
