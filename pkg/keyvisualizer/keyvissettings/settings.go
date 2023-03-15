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
	"fmt"
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
	5*time.Minute,
	settings.NonNegativeDurationWithMinimum(1*time.Minute),
)

// MaxBuckets defines the maximum buckets allowed in a sample.
// Allowing more buckets will lead to a higher sampling resolution,
// at the cost of increased storage and network requirements.
var MaxBuckets = settings.RegisterIntSetting(
	settings.SystemOnly,
	"keyvisualizer.max_buckets",
	"the maximum number of buckets in a sample",
	256,
	func(i int64) error {
		if i < 1 || i > 1024 {
			return fmt.Errorf("expected max_buckets to be in range [1, 1024], got %d", i)
		}
		return nil
	},
)
