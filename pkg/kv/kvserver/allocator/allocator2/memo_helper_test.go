// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type intMapKey uint64

func (k intMapKey) hash() uint64 {
	// Cause hash collisions.
	return uint64(k % 2)
}

func (k intMapKey) isEqual(b mapKey) bool {
	bInt := b.(intMapKey)
	return k == bInt
}

var _ mapKey = intMapKey(0)

type intMapEntry struct {
	k intMapKey
}

func (e *intMapEntry) hash() uint64 {
	return e.k.hash()
}

func (e *intMapEntry) isEqual(b mapKey) bool {
	return e.k.isEqual(b)
}

func (e *intMapEntry) clear() {
	e.k = 0
}

var _ mapEntry = &intMapEntry{}

type intEntryAlloc struct{}

func (intEntryAlloc) ensureNonNilMapEntry(entry *intMapEntry) *intMapEntry {
	if entry != nil {
		return entry
	}
	return &intMapEntry{}
}

var _ mapEntryAllocator[*intMapEntry] = intEntryAlloc{}

type intSlicePool struct {
	t *testing.T
}

func (p intSlicePool) newEntry() *mapEntrySlice[*intMapEntry] {
	return &mapEntrySlice[*intMapEntry]{}
}

func (p intSlicePool) releaseEntry(slice *mapEntrySlice[*intMapEntry]) {
	slice.entries = slice.entries[:cap(slice.entries)]
	for i := range slice.entries {
		if slice.entries[i] != nil {
			require.EqualValues(p.t, 0, slice.entries[i].k)
		}
	}
}

var _ mapEntrySlicePool[*intMapEntry] = intSlicePool{}

func TestClearableMemoMap(t *testing.T) {
	cm := newClearableMapMemo[intMapKey, *intMapEntry](intEntryAlloc{}, intSlicePool{t: t})
	entry, ok := cm.get(intMapKey(2))
	require.False(t, ok)
	entry.k = 2

	entry, ok = cm.get(intMapKey(4))
	require.False(t, ok)
	entry.k = 4

	entry, ok = cm.get(intMapKey(2))
	require.True(t, ok)
	require.EqualValues(t, 2, entry.k)

	cm.clear()
	entry, ok = cm.get(intMapKey(4))
	require.False(t, ok)
	entry.k = 4

	// TODO: add some more cases before merging.
}
