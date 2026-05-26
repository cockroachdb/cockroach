// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allstacks

import (
	"math/bits"
	"testing"
)

func TestInitialBufferSize(t *testing.T) {
	testCases := []struct {
		numGoroutines int
		expected      int
	}{
		{1, 8192},         // 1 * 6000 ~= 8192 (2^13)
		{10, 65536},       // 10 * 6000 = 60000 ~= 65536 (2^16)
		{100, 1048576},    // 100 * 6000 = 600000 ~= 1048576 (2^20)
		{1000, 8388608},   // 1000 * 6000 = 6000000 ~= 8388608 (2^23)
		{10000, 67108864}, // 10000 * 6000 = 60000000 ~= 67108864 (2^26)
	}

	for _, tc := range testCases {
		result := initialBufferSize(tc.numGoroutines)
		if result != tc.expected {
			t.Errorf("initialBufferSize(%d) = %d, expected %d", tc.numGoroutines, result, tc.expected)
		}
		// Verify result is a power of 2
		if bits.OnesCount(uint(result)) != 1 {
			t.Errorf("initialBufferSize(%d) = %d, which is not a power of 2", tc.numGoroutines, result)
		}
	}
}
