// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCatchupQueueTypeFor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		priority admissionpb.WorkPriority
		expected int
	}{
		{name: "below normal goes to low", priority: admissionpb.NormalPri - 1, expected: catchupQueueLow},
		{name: "normal goes to high", priority: admissionpb.NormalPri, expected: catchupQueueHigh},
		{name: "above normal goes to high", priority: admissionpb.NormalPri + 1, expected: catchupQueueHigh},
		{name: "bulk goes to low", priority: admissionpb.BulkNormalPri, expected: catchupQueueLow},
		{name: "user-high goes to high", priority: admissionpb.UserHighPri, expected: catchupQueueHigh},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, catchupQueueTypeFor(int32(tc.priority)))
		})
	}
}

func TestCatchupQueueLowMaxActive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		n        int
		expected int
	}{
		{name: "n=1 cap equals total (no headroom possible)", n: 1, expected: 1},
		{name: "n=2 reserves 1, cap=1", n: 2, expected: 1},
		{name: "n=3 reserves 1, cap=2", n: 3, expected: 2},
		{name: "n=4 reserves 1, cap=3", n: 4, expected: 3},
		{name: "n=8 reserves 2, cap=6", n: 8, expected: 6},
		{name: "n=16 reserves 4, cap=12", n: 16, expected: 12},
		{name: "n=100 reserves 25, cap=75", n: 100, expected: 75},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, catchupQueueLowMaxActive(tc.n))
		})
	}
}
