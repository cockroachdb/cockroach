// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// FollowerReadDisabledCCLCounter is to be increment every time follower reads
// are requested but unavailable due to not having the CCL build.
var FollowerReadDisabledCCLCounter = telemetry.GetCounterOnce("follower_reads.disabled.ccl")

// FollowerReadDisabledNoEnterpriseLicense is to be incremented every time follower reads
// are requested but unavailable due to not having enterprise enabled.
var FollowerReadDisabledNoEnterpriseLicense = telemetry.GetCounterOnce("follower_reads.disabled.no_enterprise_license")
