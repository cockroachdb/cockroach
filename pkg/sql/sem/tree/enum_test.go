// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestGenByteStringBetween(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		prev     []byte
		next     []byte
		expected []byte
	}{
		{
			[]byte(nil), []byte(nil), []byte{128},
		},
		{
			[]byte(nil), []byte{128}, []byte{64},
		},
		{
			[]byte{128}, []byte(nil), []byte{192},
		},
		{
			[]byte(nil), []byte{1}, []byte{0, 128},
		},
		{
			[]byte(nil), []byte{0, 0, 0, 0, 1}, []byte{0, 0, 0, 0, 0, 128},
		},
		{
			[]byte{0, 0, 0, 1}, []byte{0, 0, 0, 3}, []byte{0, 0, 0, 2},
		},
		{
			[]byte{254}, []byte(nil), []byte{255},
		},
		{
			[]byte{255}, []byte(nil), []byte{255, 128},
		},
		{
			[]byte{255, 255}, []byte(nil), []byte{255, 255, 128},
		},
		{
			[]byte(nil), []byte{255, 255, 255, 3}, []byte{127},
		},
		{
			[]byte{243, 12, 15, 211, 80},
			[]byte{243, 12, 15, 211, 100},
			[]byte{243, 12, 15, 211, 90},
		},
		{
			[]byte{213, 210, 0, 0, 5},
			[]byte{213, 210, 60},
			[]byte{213, 210, 30},
		},
		{
			[]byte{10, 11, 12},
			[]byte{10, 11, 12, 10},
			[]byte{10, 11, 12, 5},
		},
		{
			[]byte{213, 210, 251, 127},
			[]byte{213, 210, 251, 128},
			[]byte{213, 210, 251, 127, 128},
		},
	}

	for _, tc := range tests {
		result := GenByteStringBetween(tc.prev, tc.next)
		// TODO (rohany): I don't think the actual "expected" byte values are important right?
		//  it just makes it harder to iterate on the actual algorithm.
		// require.Equal(t, tc.expected, result, "failed on prev=%v next=%v", tc.prev, tc.next)
		if !enumBytesAreLess(tc.prev, result) {
			t.Errorf("expected prev (%s) to be less than result (%s)", tc.prev, result)
		}
		if !enumBytesAreLess(result, tc.next) {
			t.Errorf("expected result (%s) to be less than next (%s)", result, tc.next)
		}
	}
}

func TestRandomGenByteStringBetween(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const iterations = 100
	const n = 500
	rng, _ := randutil.NewPseudoRand()

	// The randomized tests work by generating a random permutation of the
	// sequence 1..N. This sequence represents the enum values we will
	// generate. In the order of the permutation, we create a byte value
	// for the entry using whatever values for the enum exist already.
	// Then, we ensure that the enum values are sorted at the end.

	for iter := 0; iter < iterations; iter++ {
		entries := make([][]byte, n)
		for _, i := range rng.Perm(n) {
			// Simulate creating an enum entry for number i. So, first find the
			// previous and next values for it.
			prev, next := []byte(nil), []byte(nil)
			for j := i - 1; j >= 0; j-- {
				if entries[j] != nil {
					prev = entries[j]
					break
				}
			}
			for j := i + 1; j < n; j++ {
				if entries[j] != nil {
					next = entries[j]
					break
				}
			}
			entries[i] = GenByteStringBetween(prev, next)
		}

		// Now, ensure that the entries are sorted order.
		for i := 0; i < n-1; i++ {
			require.Truef(
				t,
				enumBytesAreLess(entries[i], entries[i+1]),
				"at iteration %d expected entry %d (%s) to be less than entry %d (%s)",
				iter,
				i,
				entries[i],
				i+1,
				entries[i+1],
			)
		}
	}
}
