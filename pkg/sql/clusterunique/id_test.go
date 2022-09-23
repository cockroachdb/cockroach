// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterunique

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	testCases := []struct {
		name               string
		aHi, aLo, bHi, bLo uint64
		expected           int
	}{
		{
			name:     "high less",
			bHi:      1,
			expected: -1,
		},
		{
			name:     "high equal, low less",
			bLo:      1,
			expected: -1,
		},
		{
			name:     "high equal, low equal",
			expected: 0,
		},
		{
			name:     "high equal, low greater",
			aLo:      1,
			expected: 1,
		},
		{
			name:     "high greater",
			aHi:      1,
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := ID{Uint128: uint128.FromInts(tc.aHi, tc.aLo)}
			b := ID{Uint128: uint128.FromInts(tc.bHi, tc.bLo)}
			require.Equal(t, tc.expected, a.Compare(b))
		})
	}
}
