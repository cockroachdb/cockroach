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
	// TODO (rohany): I want some sort of randomized testing for this function.
	for _, tc := range tests {
		result := GenByteStringBetween(tc.prev, tc.next)
		require.Equal(t, tc.expected, result, "failed on prev=%v next=%v", tc.prev, tc.next)
		if !isLess(tc.prev, result) {
			t.Fatal("expected prev less, was not", tc.prev, tc.next, result)
		}
		if !isLess(result, tc.next) {
			t.Fatal("expected result less than next, was not", tc.prev, tc.next, result)
		}
	}
}
