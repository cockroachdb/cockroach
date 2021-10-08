// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// FollowerReadDisabledCCLCounter is to be increment every time follower reads
// are requested but unavailable due to not having the CCL build.
var FollowerReadDisabledCCLCounter = telemetry.GetCounterOnce("follower_reads.disabled.ccl")

// FollowerReadDisabledNoEnterpriseLicense is to be incremented every time follower reads
// are requested but unavailable due to not having enterprise enabled.
var FollowerReadDisabledNoEnterpriseLicense = telemetry.GetCounterOnce("follower_reads.disabled.no_enterprise_license")
