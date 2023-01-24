// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package augtestutils

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"
	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
	"github.com/stretchr/testify/require"
)

// Verify is used to verify the structure of the tree.
func Verify[C ordered.Comparator[K], K, V, A any, U aug.Updater[C, K, V, A]](
	tt *testing.T, m aug.BTree[C, K, V, A, U],
) {
	if m.Len() == 0 {
		require.Nil(tt, m.Root)
		return
	}
	isSorted(tt, m.Root)
	verifyLeafSameDepth(tt, m.Root)
	verifyCountAllowed(tt, m.Root, true)
}

// Recurse is a helper to walk all the children rooted at a node.
func Recurse[C ordered.Comparator[K], K, V, A any](
	n *aug.Node[C, K, V, A], f func(child *aug.Node[C, K, V, A], pos int16),
) {
	if !n.IsLeaf() {
		for i := int16(0); i <= n.Count; i++ {
			f(n.Children[i], i)
		}
	}
}

func height[C ordered.Comparator[K], K, V, A any](n *aug.Node[C, K, V, A]) int {
	if n == nil {
		return 0
	}
	h := 1
	for !n.IsLeaf() {
		n = n.Children[0]
		h++
	}
	return h
}

func verifyLeafSameDepth[C ordered.Comparator[K], K, V, A any](
	tt *testing.T, root *aug.Node[C, K, V, A],
) {
	h := height(root)
	verifyDepthEqualToHeight(tt, root, 1, h)
}

func verifyDepthEqualToHeight[C ordered.Comparator[K], K, V, A any](
	t *testing.T, n *aug.Node[C, K, V, A], depth, height int,
) {
	if n.IsLeaf() {
		require.Equal(t, height, depth, "all leaves should have the same depth as the tree height")
	}
	Recurse(n, func(child *aug.Node[C, K, V, A], _ int16) {
		verifyDepthEqualToHeight(t, child, depth+1, height)
	})
}

func verifyCountAllowed[C ordered.Comparator[K], K, V, A any](
	tt *testing.T, n *aug.Node[C, K, V, A], root bool,
) {
	if !root {
		require.GreaterOrEqual(
			tt, n.Count, int16(aug.MinEntries),
			"count %d must be in range [%d,%d]",
			n.Count, aug.MinEntries, aug.MaxEntries,
		)
		require.LessOrEqual(
			tt, n.Count, int16(aug.MaxEntries),
			"count %d must be in range [%d,%d]",
			n.Count, aug.MinEntries, aug.MaxEntries,
		)
	}
	if reflect.TypeOf(*new(K)).Kind() == reflect.Ptr {
		for i, item := range n.Keys {
			if i < int(n.Count) {
				require.NotNil(tt, item, "item below count")
			} else {
				require.Nil(tt, item, "item above count")
			}
		}
	}

	if !n.IsLeaf() {
		for i, child := range n.Children {
			if i <= int(n.Count) {
				require.NotNil(tt, child, "node below count")
			} else {
				require.Nil(tt, child, "node above count")
			}
		}
	}
	Recurse(n, func(child *aug.Node[C, K, V, A], _ int16) {
		verifyCountAllowed(tt, child, false)
	})
}

func isSorted[C ordered.Comparator[K], K, V, A any](t *testing.T, n *aug.Node[C, K, V, A]) {
	cmp := n.Cmp.Compare
	for i := int16(1); i < n.Count; i++ {
		require.LessOrEqual(t, cmp(n.Keys[i-1], n.Keys[i]), 0)
	}
	if !n.IsLeaf() {
		for i := int16(0); i < n.Count; i++ {
			prev := n.Children[i]
			next := n.Children[i+1]

			require.LessOrEqual(t, n.Cmp.Compare(prev.Keys[prev.Count-1], n.Keys[i]), 0)
			require.LessOrEqual(t, cmp(n.Keys[i], next.Keys[0]), 0)
		}
	}
	Recurse(n, func(child *aug.Node[C, K, V, A], _ int16) {
		isSorted(t, child)
	})
}
