// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombinesUniqueInt64(t *testing.T) {
	for _, tc := range []struct{ inputA, inputB, expected []int64 }{
		{
			inputA:   []int64{1, 2, 4},
			inputB:   []int64{3, 5},
			expected: []int64{1, 2, 3, 4, 5},
		},
		{
			inputA:   []int64{1, 2, 4},
			inputB:   []int64{1, 3, 4},
			expected: []int64{1, 2, 3, 4},
		},
		{
			inputA:   []int64{1, 2, 3},
			inputB:   []int64{1, 2, 3},
			expected: []int64{1, 2, 3},
		},
		{
			inputA:   []int64{},
			inputB:   []int64{1, 3},
			expected: []int64{1, 3},
		},
	} {
		output := CombineUniqueInt64(tc.inputA, tc.inputB)
		require.Equal(t, tc.expected, output)
	}
}
