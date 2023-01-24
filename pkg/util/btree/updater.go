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

import (
	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/aug"
	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

// noop is a zero-value Updater which takes no space.
type noop[C ordered.Comparator[K], K, V any] struct{}

type noAug = struct{}

func (a noop[C, K, V]) UpdateOnInsert(_, _ *aug.Node[C, K, V, noAug], _ K) (changed bool) {
	return false
}
func (a noop[C, K, V]) UpdateOnRemoval(_, _ *aug.Node[C, K, V, noAug], _ K) (changed bool) {
	return false
}
func (a noop[C, K, V]) UpdateOnSplit(_, _ *aug.Node[C, K, V, noAug], _ K) (changed bool) {
	return false
}
func (a noop[C, K, V]) UpdateDefault(_ *aug.Node[C, K, V, noAug]) (changed bool) {
	return false
}
