// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntervalTree(t *testing.T) {
	assertEq := func(t *testing.T, exp, got [2]int) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected %d, got %d", exp, got)
		}
	}
	var tree Set[WithPair[int], [2]int, int]
	items := [][2]int{{1, 4}, {2, 5}, {3, 3}, {3, 6}, {4, 7}}
	for _, item := range items {
		tree.Upsert(item)
	}
	iter := tree.MakeIter()
	iter.First()
	for _, exp := range items {
		assertEq(t, exp, iter.Cur())
		iter.Next()
	}
	for _, tc := range []struct {
		q   [2]int
		res [][2]int
	}{
		{
			q:   [2]int{2, 3},
			res: [][2]int{{1, 4}, {2, 5}},
		},
		{
			q:   [2]int{2, 4},
			res: [][2]int{{1, 4}, {2, 5}, {3, 3}, {3, 6}},
		},
	} {
		var res [][2]int
		for iter.FirstOverlap(tc.q); iter.Valid(); iter.NextOverlap(tc.q) {
			res = append(res, iter.Cur())
		}
		require.Equal(t, tc.res, res)
	}
}
