// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterunique

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/stretchr/testify/require"
)

// TestShortIDZero locks in the documented edge case: the zero ID
// hashes to zero. ShortID's "0 means unset" contract relies on real
// QueryIDs (which set Hi from hlc.Timestamp.WallTime) never being
// the zero ID. See the comment on ShortID for the full reasoning.
func TestShortIDZero(t *testing.T) {
	require.Equal(t, uint64(0), ID{}.ShortID(),
		"the zero ID must hash to zero; this is the sentinel relied on by callers")
}

// TestShortIDGolden pins ShortID outputs against hand-computed
// reference values. A typo in the fmix64 constants or shift amounts
// would change these and break this test, surfacing the regression
// before it silently invalidates the collision analysis that justifies
// using ShortID as a cache key.
func TestShortIDGolden(t *testing.T) {
	cases := []struct {
		hi, lo uint64
		want   uint64
	}{
		{0x0000000000000000, 0x0000000000000000, 0x0000000000000000},
		{0x0000000000000000, 0x0000000000000001, 0x7d6e4ac38b2b1be2},
		{0x0000000000000001, 0x0000000000000000, 0xb456bcfc34c2cb2c},
		{0x0000000000000001, 0x0000000000000001, 0xd522b5d820b5e72f},
		{0x12345678abcdef00, 0xfedcba9876543210, 0x036abe1d74a2b429},
		{0x17979cfe362a0000, 0x0000000000003039, 0x9fb155d0c717595b},
	}
	for _, tc := range cases {
		got := (ID{Uint128: uint128.FromInts(tc.hi, tc.lo)}).ShortID()
		require.Equalf(t, tc.want, got, "ShortID({Hi: %#016x, Lo: %#016x})", tc.hi, tc.lo)
	}
}

// BenchmarkShortID measures the per-call cost of ShortID. The
// expectation is single-digit nanoseconds and zero allocations. The
// id is mutated each iteration so the compiler cannot hoist the call
// out of the loop.
func BenchmarkShortID(b *testing.B) {
	id := ID{Uint128: uint128.FromInts(0x12345678abcdef00, 0xfedcba9876543210)}
	b.ReportAllocs()
	var sink uint64
	for i := 0; i < b.N; i++ {
		id.Lo = uint64(i)
		sink ^= id.ShortID()
	}
	_ = sink
}

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
