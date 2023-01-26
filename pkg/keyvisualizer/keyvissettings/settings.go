// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvissettings

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Enabled enables the key visualizer job.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"keyvisualizer.enabled",
	"enable the use of the key visualizer", false)

// SampleInterval defines the sample period for key visualizer samples.
var SampleInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"keyvisualizer.sample_interval",
	"the frequency at which the key visualizer samples are collected",
	1*time.Minute,
	settings.NonNegativeDuration,
)

// MaxBuckets defines the maximum buckets allowed in a sample.
// Allowing more buckets will lead to a higher sampling resolution,
// at the cost of increased storage and network requirements.
var MaxBuckets = settings.RegisterIntSetting(
	settings.SystemOnly,
	"keyvisualizer.max_buckets",
	"the maximum number of buckets in a sample",
	256,
	settings.NonNegativeIntWithMaximum(1024),
)
