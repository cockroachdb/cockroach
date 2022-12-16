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

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/ordered"
	"github.com/stretchr/testify/require"
)

type IntInterval [2]int

func (i IntInterval) Key() int                { return i[0] }
func (i IntInterval) UpperBound() (int, bool) { return i[1], false }

func TestIntervalTree(t *testing.T) {
	assertEq := func(t *testing.T, exp, got IntInterval) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected %d, got %d", exp, got)
		}
	}
	cfg := NewSetConfig(Comparators[IntInterval, int]{
		CmpK: ordered.Compare[int],
	})
	tree := cfg.MakeSet()
	items := []IntInterval{{1, 4}, {2, 5}, {3, 3}, {3, 6}, {4, 7}}
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
		q   IntInterval
		res []IntInterval
	}{
		{
			q:   IntInterval{2, 3},
			res: []IntInterval{{1, 4}, {2, 5}},
		},
		{
			q:   IntInterval{2, 4},
			res: []IntInterval{{1, 4}, {2, 5}, {3, 3}, {3, 6}},
		},
	} {
		var res []IntInterval
		for iter.FirstOverlap(tc.q); iter.Valid(); iter.NextOverlap(tc.q) {
			res = append(res, iter.Cur())
		}
		require.Equal(t, tc.res, res)
	}
}
