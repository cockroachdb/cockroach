// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestComputeNumberSamples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testData := []struct {
		numRows            int
		expectedNumSamples int
	}{
		{0, 10000},
		{100, 10000},
		{10000, 10000},
		{100000, 16402},
		{1000000, 31983},
		{10000000, 62362},
		{100000000, 121597},
		{1000000000, 237095},
		{10000000000, 300000},
		{math.MaxInt, 300000},
	}

	checkComputeNumberSamples := func(computedNumSamples, expectedNumSamples int) {
		if computedNumSamples != expectedNumSamples {
			t.Fatalf("expected %d samples, got %d", expectedNumSamples, computedNumSamples)
		}
	}

	st := cluster.MakeTestingClusterSettings()
	for _, td := range testData {
		checkComputeNumberSamples(int(computeNumberSamples(ctx, uint64(td.numRows), st)), td.expectedNumSamples)
	}
}
