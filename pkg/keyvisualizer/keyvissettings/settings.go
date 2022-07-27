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
	settings.TenantWritable,
	"keyvisualizer.job.enabled",
	"enable the use of the key visualizer", false)

// CheckInterval defines how often we check to make sure the key visualizer job is running.
var CheckInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"keyvisualizer.job.check_interval",
	"the frequency at which the key visualizer job is checked to exist",
	60*time.Second,
	settings.NonNegativeDuration,
)

// SampleInterval defines the sample period for key visualizer samples.
var SampleInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"keyvisualizer.job.sample_interval",
	"the frequency at which the key visualizer samples are collected",
	10*time.Second,
	settings.NonNegativeDuration,
)
