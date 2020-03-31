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

	"github.com/stretchr/testify/require"
)

func TestGenByteStringBetween(t *testing.T) {
	tests := []struct {
		prev     []byte
		next     []byte
		expected []byte
	}{
		{
			[]byte(nil), []byte(nil), []byte{2},
		},
		{
			[]byte(nil), []byte{2}, []byte{1},
		},
		{
			[]byte{2}, []byte(nil), []byte{3},
		},
		{
			[]byte{3}, []byte(nil), []byte{4, 2},
		},
		{
			[]byte(nil), []byte{1}, []byte{0, 2},
		},
		{
			[]byte{3}, []byte{3, 2}, []byte{3, 1},
		},
		{
			[]byte{3}, []byte{3, 1}, []byte{3, 0, 2},
		},
		{
			[]byte{2, 2, 0, 0, 2}, []byte{2, 2, 1}, []byte{2, 2, 0, 2},
		},
		{
			[]byte{2, 3, 1}, []byte{2, 3, 2, 2}, []byte{2, 3, 2, 1},
		},
	}

	for _, tc := range tests {
		result := GenByteStringBetween(tc.prev, tc.next)
		require.Equal(t, tc.expected, result, "failed on", tc.prev, tc.next)
		if !isLess(tc.prev, result) {
			t.Fatal("expected prev less, was not", tc.prev, tc.next, result)
		}
		if !isLess(result, tc.next) {
			t.Fatal("expected result less than next, was not", tc.prev, tc.next, result)
		}
	}
}
