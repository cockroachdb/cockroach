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

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"

// updater stores the configuration needed to update the aug upon tree updates.
type updater[C Config[I, K], I, K, V any] struct{}

func (u updater[C, I, K, V]) UpdateOnInsert(
	n, other *aug.Node[C, I, V, upperBound[K]], item I,
) (updated bool) {
	up := ub[C, I, K](item)
	if child := other; child != nil {
		if less[C, I](up, child.Aug) {
			up = child.Aug
		}
	}
	if less[C, I](n.Aug, up) {
		n.Aug = up
		return true
	}
	return false
}

func (u updater[C, I, K, V]) UpdateOnRemoval(
	n, other *aug.Node[C, I, V, upperBound[K]], key I,
) (updated bool) {
	up := ub[C, I, K](key)
	if child := other; child != nil {
		if less[C, I](up, child.Aug) {
			up = child.Aug
		}
	}
	if eq[C, I](n.Aug, up) {
		n.Aug = u.findUpperBound(n)
		return !eq[C, I](n.Aug, up)
	}
	return false
}

func (u updater[C, I, K, V]) UpdateOnSplit(
	n, other *aug.Node[C, I, V, upperBound[K]], key I,
) (updated bool) {
	if !eq[C, I](n.Aug, other.Aug) &&
		!eq[C, I](n.Aug, ub[C, I, K](key)) {

		return false
	}
	return u.UpdateDefault(n)
}

func (u updater[C, I, K, V]) UpdateDefault(n *aug.Node[C, I, V, upperBound[K]]) (updated bool) {
	prev := n.Aug
	n.Aug = u.findUpperBound(n)
	return !eq[C, I](n.Aug, prev)
}

func (u updater[C, I, K, V]) findUpperBound(n *aug.Node[C, I, V, upperBound[K]]) upperBound[K] {
	var max upperBound[K]
	var setMax bool
	for i, cnt := int16(0), n.Count; i < cnt; i++ {
		b := ub[C, I, K](n.Keys[i])
		if !setMax || less[C, I](max, b) {
			setMax = true
			max = b
		}
	}
	if n.Children != nil {
		for i, cnt := int16(0), n.Count; i <= cnt; i++ {
			b := n.Children[i].Aug
			if less[C, I](max, b) {
				max = b
			}
		}
	}
	return max
}
