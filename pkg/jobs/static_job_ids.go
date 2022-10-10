// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

// This file contains static IDs for permanent jobs that are added during
// upgrades. To add a new static job ID, assign it below using assignNewStaticID(N)
// where N monotonically increases by 1.

var (
	JobMetricsPollingJobID = assignNewStaticID(0)
)

func assignNewStaticID(staticJobIndex int32) jobspb.JobID {
	// When generating unique job IDs for non-static jobs, the function
	// builtins.GenerateUniqueID is used with a non-zero timestamp and this
	// timestamp is assigned to the upper 48 bits of the generated ID.
	//
	// Calling builtins.GenerateUniqueID with a zero timestamp ensures that
	// the generated ID has the upper 48 bits set to zero, preventing overlap
	// with all other jobs.
	return jobspb.JobID(builtins.GenerateUniqueID(staticJobIndex, 0))
}
