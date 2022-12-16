// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package btree

import "github.com/cockroachdb/cockroach/pkg/util/btree/internal/abstract"

// updater is a zero-value updater which takes no space.
type updater[K, V any] struct{}

func (a updater[K, V]) UpdateOnInsert(_, _ *abstract.Node[K, V, struct{}], _ K) (changed bool) {
	return false
}
func (a updater[K, V]) UpdateOnRemoval(_, _ *abstract.Node[K, V, struct{}], _ K) (changed bool) {
	return false
}
func (a updater[K, V]) UpdateOnSplit(_, _ *abstract.Node[K, V, struct{}], _ K) (changed bool) {
	return false
}
func (a updater[K, V]) UpdateDefault(_ *abstract.Node[K, V, struct{}]) (changed bool) {
	return false
}
