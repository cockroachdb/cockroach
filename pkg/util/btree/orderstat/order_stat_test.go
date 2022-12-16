// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package orderstat

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/ordered"
	"github.com/stretchr/testify/require"
)

func TestOrderStatTree(t *testing.T) {
	tree := NewMapConfig[int](ordered.Compare[int]).MakeMap()
	tree.Upsert(2, 1)
	tree.Upsert(3, 2)
	tree.Upsert(5, 4)
	tree.Upsert(4, 3)
	iter := tree.MakeIter()
	iter.First()
	for i, exp := range []int{2, 3, 4, 5} {
		require.Equal(t, exp, iter.Cur())
		require.Equal(t, i, iter.Rank())
		iter.Next()
	}
	iter.SeekNth(2)
	require.Equal(t, 4, iter.Cur())
}

func TestOrderStatNth(t *testing.T) {
	tree := NewSetConfig(ordered.Compare[int]).MakeSet()
	const maxN = 1000
	N := rand.Intn(maxN)
	items := make([]int, 0, N)
	for i := 0; i < N; i++ {
		items = append(items, i)
	}
	perm := rand.Perm(N)
	for _, idx := range perm {
		tree.Upsert(items[idx])
	}
	removePerm := rand.Perm(N)
	retainAll := rand.Float64() < .25
	var removed []int
	for _, idx := range removePerm {
		if !retainAll && rand.Float64() < .05 {
			continue
		}
		tree.Delete(items[idx])
		removed = append(removed, items[idx])
	}
	t.Logf("removed %d/%d", len(removed), N)
	for _, i := range removed {
		tree.Upsert(i)
	}
	perm = rand.Perm(N)

	iter := tree.MakeIter()
	for _, idx := range perm {
		iter.SeekNth(idx)
		require.Equal(t, items[idx], iter.Cur())
		for i := idx + 1; i < N; i++ {
			iter.Next()
			require.Equal(t, items[i], iter.Cur())
		}
		require.True(t, iter.Valid())
		iter.Next()
		require.False(t, iter.Valid())
	}

	clone := tree.Clone()
	clone.Reset()
	require.Equal(t, tree.Len(), len(perm))

}

func ExampleSet() {
	s := NewSetConfig(ordered.Compare[int]).MakeSet()
	for _, i := range rand.Perm(100) {
		s.Upsert(i)
	}
	it := s.MakeIter()
	it.SeekNth(90)
	fmt.Println(it.Cur())

	// Output:
	// 100
	// 90
}
