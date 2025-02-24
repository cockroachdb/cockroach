// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

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
	intMapKey
	// This is the kind of allocated state we want to reuse via clear.
	state []struct{}
}

func (e *intMapEntry) clear() {
	e.intMapKey = 0
	e.state = e.state[:0]
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
	t           *testing.T
	freeEntries []*mapEntrySlice[*intMapEntry]
}

func (p *intSlicePool) newEntry() *mapEntrySlice[*intMapEntry] {
	if len(p.freeEntries) == 0 {
		return &mapEntrySlice[*intMapEntry]{}
	}
	rv := p.freeEntries[0]
	p.freeEntries = p.freeEntries[1:]
	return rv
}

func (p *intSlicePool) releaseEntry(slice *mapEntrySlice[*intMapEntry]) {
	slice.entries = slice.entries[:cap(slice.entries)]
	for i := range slice.entries {
		if slice.entries[i] != nil {
			require.EqualValues(p.t, 0, slice.entries[i].intMapKey)
			require.Equal(p.t, 0, len(slice.entries[i].state))
		}
	}
	p.freeEntries = append(p.freeEntries, slice)
}

var _ mapEntrySlicePool[*intMapEntry] = &intSlicePool{}

// Avoid unused lint errors, due to linter being confused by generics.
var _ = intEntryAlloc{}.ensureNonNilMapEntry
var _ = (&intSlicePool{}).newEntry
var _ = (&intSlicePool{}).releaseEntry
var _ = (&intSlicePool{}).freeEntries

func TestClearableMemoMap(t *testing.T) {
	cm := newClearableMapMemo[intMapKey, *intMapEntry](intEntryAlloc{}, &intSlicePool{t: t})
	entry, ok := cm.get(intMapKey(2))
	require.False(t, ok)
	entry.intMapKey = 2
	entry.state = make([]struct{}, 5)

	// Hash collision. So they will share the same *mapEntrySlice[*intMapEntry].
	entry, ok = cm.get(intMapKey(4))
	require.False(t, ok)
	entry.intMapKey = 4
	entry.state = make([]struct{}, 10)

	entry, ok = cm.get(intMapKey(2))
	require.True(t, ok)
	require.EqualValues(t, 2, entry.intMapKey)

	cm.clear()
	// newEntry will get called and reuse the previously allocated *mapEntrySlice[*intMapEntry].
	// The first element in that slice has an entry.state with capacity 5.
	entry, ok = cm.get(intMapKey(4))
	require.False(t, ok)
	entry.intMapKey = 4
	require.Equal(t, 0, len(entry.state))
	require.Equal(t, 5, cap(entry.state))

	// The second element in the slice has an entry.state with capacity 10.
	entry, ok = cm.get(intMapKey(6))
	require.False(t, ok)
	entry.intMapKey = 6
	require.Equal(t, 0, len(entry.state))
	require.Equal(t, 10, cap(entry.state))

	// Different slice.
	entry, ok = cm.get(intMapKey(11))
	require.False(t, ok)
	entry.intMapKey = 11
	require.Equal(t, 0, len(entry.state))
	require.Equal(t, 0, cap(entry.state))

	entry, ok = cm.get(intMapKey(11))
	require.True(t, ok)
	require.EqualValues(t, 11, entry.intMapKey)
}
