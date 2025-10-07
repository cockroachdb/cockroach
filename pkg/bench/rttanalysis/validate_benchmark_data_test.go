// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
)

func TestBenchmarkExpectation(t *testing.T) {
	defer jobs.TestingSetIDsToIgnore(map[jobspb.JobID]struct{}{3001: {}, 3002: {}})()
	reg.RunExpectations(t)
}
