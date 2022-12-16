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

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"

type aug struct {
	// children is the number of items rooted at the current subtree.
	children int
}

type updater[K, V any] struct{}

func (u updater[K, V]) UpdateOnInsert(n, other *abstract.Node[K, V, aug], _ K) (changed bool) {
	n.Aug.children++
	if other != nil {
		n.Aug.children += other.Aug.children
	}
	return true
}
func (u updater[K, V]) UpdateOnRemoval(n, other *abstract.Node[K, V, aug], _ K) (changed bool) {
	return u.updateOnRemovalOrSplit(n, other)
}
func (u updater[K, V]) UpdateOnSplit(n, other *abstract.Node[K, V, aug], _ K) (changed bool) {
	return u.updateOnRemovalOrSplit(n, other)
}
func (updater[K, V]) updateOnRemovalOrSplit(n, other *abstract.Node[K, V, aug]) bool {
	n.Aug.children--
	if other != nil {
		n.Aug.children -= other.Aug.children
	}
	return true
}
func (u updater[K, V]) UpdateDefault(n *abstract.Node[K, V, aug]) (changed bool) {
	orig := n.Aug.children
	var children int
	if !n.IsLeaf() {
		N := n.Count
		for i := int16(0); i <= N; i++ {
			if child := n.Children[i]; child != nil {
				children += child.Aug.children
			}
		}
	}
	children += int(n.Count)
	n.Aug.children = children
	return n.Aug.children != orig
}
