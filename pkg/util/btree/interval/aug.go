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

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"

// aug is the augmentation to the btree. One is stored in each node of the
// tree.
type aug[K any] struct{ keyBound[K] }

// updater stores the configuration needed to update the aug upon tree updates.
type updater[I Interval[K], K, V any] struct{ cmp Cmp[K] }

func (u updater[I, K, V]) UpdateOnInsert(
	n, other *abstract.Node[I, V, aug[K]], key I,
) (updated bool) {
	up := upperBound[K](key)
	if child := other; child != nil {
		if up.less(u.cmp, child.Aug.keyBound) {
			up = child.Aug.keyBound
		}
	}
	if n.Aug.less(u.cmp, up) {
		n.Aug.keyBound = up
		return true
	}
	return false
}

func (u updater[I, K, V]) UpdateOnRemoval(
	n, other *abstract.Node[I, V, aug[K]], key I,
) (updated bool) {
	up := upperBound[K](key)
	if child := other; child != nil {
		if up.less(u.cmp, child.Aug.keyBound) {
			up = child.Aug.keyBound
		}
	}
	if n.Aug.equal(u.cmp, up) {
		n.Aug.keyBound = u.findUpperBound(n)
		return !n.Aug.equal(u.cmp, up)
	}
	return false
}

func (u updater[I, K, V]) UpdateOnSplit(
	n, other *abstract.Node[I, V, aug[K]], key I,
) (updated bool) {
	if !n.Aug.equal(u.cmp, other.Aug.keyBound) &&
		!n.Aug.equal(u.cmp, upperBound[K](key)) {
		return false
	}
	prev := n.Aug.keyBound
	n.Aug.keyBound = u.findUpperBound(n)
	return !n.Aug.equal(u.cmp, prev)
}

func (u updater[I, K, V]) UpdateDefault(n *abstract.Node[I, V, aug[K]]) (updated bool) {
	prev := n.Aug.keyBound
	n.Aug.keyBound = u.findUpperBound(n)
	return !n.Aug.equal(u.cmp, prev)
}

func (u updater[I, K, V]) findUpperBound(n *abstract.Node[I, V, aug[K]]) keyBound[K] {
	var max keyBound[K]
	var setMax bool
	for i, cnt := int16(0), n.Count; i < cnt; i++ {
		ub := upperBound[K](n.Keys[i])
		if !setMax || max.less(u.cmp, ub) {
			setMax = true
			max = ub
		}
	}
	if n.Children != nil {
		for i, cnt := int16(0), n.Count; i <= cnt; i++ {
			ub := n.Children[i].Aug.keyBound
			if max.less(u.cmp, ub) {
				max = ub
			}
		}
	}
	return max
}

type keyBound[K any] struct {
	k         K
	inclusive bool
}

func upperBound[K any, I Interval[K]](interval I) keyBound[K] {
	return makeKeyBound(interval.UpperBound())
}

func makeKeyBound[K any](k K, inclusive bool) keyBound[K] {
	return keyBound[K]{k: k, inclusive: inclusive}
}

func (b keyBound[K]) equal(cmp Cmp[K], o keyBound[K]) bool {
	return cmp(b.k, o.k) == 0 && b.inclusive == o.inclusive
}

func (b keyBound[K]) less(cmp Cmp[K], o keyBound[K]) bool {
	if c := cmp(b.k, o.k); c != 0 {
		return c < 0
	}
	return !b.inclusive || o.inclusive
}

func (b *keyBound[K]) contains(cmp Cmp[K], o K) bool {
	c := cmp(o, b.k)
	return (c == 0 && b.inclusive) || c < 0
}
