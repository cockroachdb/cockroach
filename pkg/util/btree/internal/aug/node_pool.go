// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aug

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

type nodePool[C ordered.Comparator[K], K, V, A any] struct {
	cmp              C
	interiorNodePool sync.Pool
	leafNodePool     sync.Pool
}

var syncPoolMap sync.Map

func getNodePool[C ordered.Comparator[K], K, V, A any]() *nodePool[C, K, V, A] {
	var nilNode *Node[C, K, V, A]
	v, ok := syncPoolMap.Load(nilNode)
	if !ok {
		v, _ = syncPoolMap.LoadOrStore(nilNode, newNodePool[C, K, V, A]())
	}
	return v.(*nodePool[C, K, V, A])

}

func newNodePool[C ordered.Comparator[K], K, V, A any]() *nodePool[C, K, V, A] {
	np := nodePool[C, K, V, A]{}
	np.leafNodePool = sync.Pool{
		New: func() interface{} {
			n := new(Node[C, K, V, A])
			return n
		},
	}
	np.interiorNodePool = sync.Pool{
		New: func() interface{} {
			n := new(interiorNode[C, K, V, A])
			n.Children = &n.children
			return &n.Node
		},
	}
	return &np
}

func (np *nodePool[C, K, V, A]) getInteriorNode() *Node[C, K, V, A] {
	n := np.interiorNodePool.Get().(*Node[C, K, V, A])
	n.ref = 1
	return n
}

func (np *nodePool[C, K, V, A]) getLeafNode() *Node[C, K, V, A] {
	n := np.leafNodePool.Get().(*Node[C, K, V, A])
	n.ref = 1
	return n
}

func (np *nodePool[C, K, V, A]) putInteriorNode(n *Node[C, K, V, A]) {
	children := n.Children
	*children = [MaxEntries + 1]*Node[C, K, V, A]{}
	*n = Node[C, K, V, A]{}
	n.Children = children
	np.interiorNodePool.Put(n)
}

func (np *nodePool[C, K, V, A]) putLeafNode(n *Node[C, K, V, A]) {
	*n = Node[C, K, V, A]{}
	np.leafNodePool.Put(n)
}
