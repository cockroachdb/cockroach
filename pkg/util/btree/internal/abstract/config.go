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

// Config is used to configure the tree. It consists of a comparison function
// for Keys and any auxiliary data provided by the instantiator. It is provided
// on the iterator and passed to the augmentation's Update method.
type Config[K, V, A any, U Updater[K, V, A]] struct {
	// Updater is used to update the augmentations to the tree.
	Updater U

	Cmp func(K, K) int

	np *nodePool[K, V, A]
}

// Updater is used to update the augmentation of the Node when the subtree
// changes.
type Updater[K, V, A any] interface {
	UpdateOnInsert(_, _ *Node[K, V, A], _ K) (changed bool)
	UpdateOnRemoval(_, _ *Node[K, V, A], _ K) (changed bool)
	UpdateOnSplit(_, _ *Node[K, V, A], _ K) (changed bool)
	UpdateDefault(*Node[K, V, A]) (changed bool)
}

func NewConfig[K, V, A any, U Updater[K, V, A]](cmp func(K, K) int, up U) *Config[K, V, A, U] {
	var c Config[K, V, A, U]
	c.Updater = up
	c.Cmp = cmp
	c.np = getNodePool[K, V, A]()
	return &c
}
