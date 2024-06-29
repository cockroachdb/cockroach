// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/stretchr/testify/require"
)

// This package contains unit tests that need access to un-exported functions.

func TestCalculateBackgroundCPUSecs(t *testing.T) {
	testCases := []struct {
		name          string
		globalCPURate float64
		deltaTime     time.Duration
		localCPUSecs  float64
		expected      float64
	}{
		{
			name:          "global consumption is zero",
			globalCPURate: 0,
			deltaTime:     time.Second,
			localCPUSecs:  2,
			expected:      0.2,
		},
		{
			name:          "local is small fraction of global consumption",
			globalCPURate: 12,
			deltaTime:     time.Second,
			localCPUSecs:  2,
			expected:      0.1,
		},
		{
			name:          "local consumption is greater than amortization limit",
			globalCPURate: 8,
			deltaTime:     time.Second,
			localCPUSecs:  8,
			expected:      0.6,
		},
		{
			name:          "local consumption is greater than amortization limit but less than global",
			globalCPURate: 12,
			deltaTime:     time.Second,
			localCPUSecs:  8,
			expected:      0.4,
		},
		{
			name:          "time delta is > 1, less than amortization limit",
			globalCPURate: 2,
			deltaTime:     2 * time.Second,
			localCPUSecs:  8,
			expected:      0.8,
		},
		{
			name:          "time delta is > 1, equal to amortization limit",
			globalCPURate: 6,
			deltaTime:     2 * time.Second,
			localCPUSecs:  12,
			expected:      1.2,
		},
	}

	cpuModel := tenantcostmodel.EstimatedCPUModel{
		BackgroundCPU: struct {
			Amount       tenantcostmodel.EstimatedCPU
			Amortization float64
		}{
			Amount:       0.6,
			Amortization: 6,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := calculateBackgroundCPUSecs(
				&cpuModel, tc.globalCPURate, tc.deltaTime, tc.localCPUSecs)
			require.InEpsilon(t, tc.expected, actual, 0.00000001)
		})
	}
}
