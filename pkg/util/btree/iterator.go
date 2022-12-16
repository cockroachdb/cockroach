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

// Iterator is used to iterate through the tree. The tree must not be
// modified during iteration. If concurrent modifications are required,
// take a Clone of the tree before constructing an Iterator.
type Iterator[K, V any] struct {
	it abstract.Iterator[K, V, struct{}, updater[K, V]]
}

// First seeks to the first key in the tree.
func (it *Iterator[K, V]) First() { it.it.First() }

// Last seeks to the last key in the tree.
func (it *Iterator[K, V]) Last() { it.it.Last() }

// Next positions the Iterator to the key immediately following
// its current position.
func (it *Iterator[K, V]) Next() { it.it.Next() }

// Prev positions the Iterator to the key immediately preceding
// its current position.
func (it *Iterator[K, V]) Prev() { it.it.Prev() }

// SeekGT seeks to the first key greater than the provided key.
func (it *Iterator[K, V]) SeekGT(v K) { it.it.SeekGT(v) }

// SeekGTE seeks to the first key greater-than or equal to the provided
// key.
func (it *Iterator[K, V]) SeekGTE(v K) { it.it.SeekGTE(v) }

// SeekLT seeks to the first key less than the provided key.
func (it *Iterator[K, V]) SeekLT(v K) { it.it.SeekLT(v) }

// SeekLTE seeks to the first key less than or equal to the provided key.
func (it *Iterator[K, V]) SeekLTE(v K) { it.it.SeekLTE(v) }

// Cur returns the key at the Iterator's current position. It is illegal
// to call Key if the Iterator is not valid.
func (it *Iterator[K, V]) Cur() K { return it.it.Cur() }

// Value returns the value at the Iterator's current position. It is illegal
// to call Value if the Iterator is not valid.
func (it *Iterator[K, V]) Value() V { return it.it.Value() }

// Valid returns true if the iterator is positioned on some element of the tree.
func (it *Iterator[K, V]) Valid() bool { return it.it.Valid() }

// Reset marks the iterator as invalid and clears any state.
func (it *Iterator[K, V]) Reset() { it.it.Reset() }
