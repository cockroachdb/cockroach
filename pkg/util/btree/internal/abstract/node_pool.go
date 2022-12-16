// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package abstract

import "sync"

type nodePool[K, V, A any] struct {
	interiorNodePool, leafNodePool sync.Pool
}

var syncPoolMap sync.Map

func getNodePool[K, V, A any]() *nodePool[K, V, A] {
	var nilNode *Node[K, V, A]
	v, ok := syncPoolMap.Load(nilNode)
	if !ok {
		v, _ = syncPoolMap.LoadOrStore(nilNode, newNodePool[K, V, A]())
	}
	return v.(*nodePool[K, V, A])

}

func newNodePool[K, V, A any]() *nodePool[K, V, A] {
	np := nodePool[K, V, A]{}
	np.leafNodePool = sync.Pool{
		New: func() interface{} {
			n := new(Node[K, V, A])
			return n
		},
	}
	np.interiorNodePool = sync.Pool{
		New: func() interface{} {
			n := new(interiorNode[K, V, A])
			n.Node.Children = &n.children
			return &n.Node
		},
	}
	return &np
}

func (np *nodePool[K, V, A]) getInteriorNode() *Node[K, V, A] {
	n := np.interiorNodePool.Get().(*Node[K, V, A])
	n.ref = 1
	return n
}

func (np *nodePool[K, V, A]) getLeafNode() *Node[K, V, A] {
	n := np.leafNodePool.Get().(*Node[K, V, A])
	n.ref = 1
	return n
}

func (np *nodePool[K, V, A]) putInteriorNode(n *Node[K, V, A]) {
	children := n.Children
	*children = [MaxEntries + 1]*Node[K, V, A]{}
	*n = Node[K, V, A]{}
	n.Children = children
	np.interiorNodePool.Put(n)
}

func (np *nodePool[K, V, A]) putLeafNode(n *Node[K, V, A]) {
	*n = Node[K, V, A]{}
	np.leafNodePool.Put(n)
}
