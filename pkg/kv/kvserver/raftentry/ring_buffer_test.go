// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftentry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingBuffer_Add(t *testing.T) {
	b := &ringBuf{}
	const size = 12
	b.add(newEntries(5, 8, size))
	eq(t, b, 5, 6, 7)
	b.add(newEntries(3, 5, size))
	eq(t, b, 3, 4, 5, 6, 7)

	{
		// Overwrite 5 and 6 with entries twice as large.
		ab, ae := b.add(newEntries(5, 7, 2*size))
		eq(t, b, 3, 4, 5, 6, 7)
		require.EqualValues(t, 2*size, ab) // `size` for idx=5 and 6
		require.EqualValues(t, 0, ae)      // we overwrote, did not add
	}

	{
		rb, re := b.truncateFrom(6) // reduce typing work below
		eq(t, b, 3, 4, 5)
		require.EqualValues(t, 3*size, rb) // idx=6,7 have double size
		require.EqualValues(t, 2, re)
	}

	{
		// Add one entry at the end.
		ab, ae := b.add(newEntries(6, 7, size))
		require.EqualValues(t, size, ab)
		require.EqualValues(t, 1, ae)
		eq(t, b, 3, 4, 5, 6)
	}

	{
		// Addition at beginning which matches up is accepted.
		ab, ae := b.add(newEntries(1, 3, size))
		eq(t, b, 1, 2, 3, 4, 5, 6)
		require.EqualValues(t, 2*size, ab)
		require.EqualValues(t, 2, ae)
	}

	{
		rb, re := b.clearTo(3)
		eq(t, b, 3, 4, 5, 6)
		require.EqualValues(t, rb, 2*size)
		require.EqualValues(t, re, 2)
	}

	{
		// Addition at beginning which does not line up is rejected.
		ab, ae := b.add(newEntries(1, 2, size))
		eq(t, b, 3, 4, 5, 6)
		require.Zero(t, ab)
		require.Zero(t, ae)
	}

	{
		// Addition at the end but with a gap clears the existing entries
		// before.
		b.add(newEntries(10, 11, size))
		eq(t, b, 10)
	}
}

func TestRingBuffer_Scan(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		lo, hi, mb uint64

		idxs             []uint64 // full range is 10,11,12,13,14, each of size 10
		scanBytes        uint64
		nextIdx          uint64
		exceededMaxBytes bool
	}{
		{
			desc: "before cached entries",
			lo:   5, hi: 10, mb: 100,
			nextIdx: 5,
		},
		{
			desc: "before up to excluding first cached entry",
			lo:   5, hi: 11, mb: 100,
			nextIdx: 5,
		},
		{
			desc: "starts at first cached entry, remains in cache",
			lo:   10, hi: 12, mb: 100,
			idxs: []uint64{10, 11}, scanBytes: 20, nextIdx: 12,
		},
		{
			desc: "starts at first cached entry, remains in cache, limit almost hit",
			lo:   10, hi: 12, mb: 20,
			idxs: []uint64{10, 11}, scanBytes: 20, nextIdx: 12, exceededMaxBytes: false,
		},
		{
			desc: "starts at first cached entry, remains in cache, limit hit",
			lo:   10, hi: 12, mb: 19,
			idxs: []uint64{10}, scanBytes: 10, nextIdx: 11, exceededMaxBytes: true,
		},
		{
			desc: "starts at first cached entry, stops at cache end",
			lo:   10, hi: 15, mb: 100,
			idxs: []uint64{10, 11, 12, 13, 14}, scanBytes: 50, nextIdx: 15,
		},
		{
			desc: "starts at first cached entry, runs past the cache",
			lo:   10, hi: 16, mb: 100,
			idxs: []uint64{10, 11, 12, 13, 14}, scanBytes: 50, nextIdx: 15,
		},
		{
			desc: "starts in middle of cache, runs past the cache",
			lo:   12, hi: 16, mb: 100,
			idxs: []uint64{12, 13, 14}, scanBytes: 30, nextIdx: 15,
		},
		{
			desc: "starts in middle of cache, runs past the cache but limit hits",
			lo:   12, hi: 16, mb: 29,
			idxs: []uint64{12, 13}, scanBytes: 20, nextIdx: 14, exceededMaxBytes: true,
		},
		{
			desc: "starts past cache",
			lo:   15, hi: 16, mb: 100,
			nextIdx: 15,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			b := &ringBuf{}
			ab, _ := b.add(newEntries(10, 15, 10 /* size */))
			require.EqualValues(t, 5*10, ab)
			ents, sb, next, excmb := b.scan(nil, tc.lo, tc.hi, tc.mb)
			var sl []uint64
			for _, ent := range ents {
				sl = append(sl, ent.Index)
			}
			require.Equal(t, tc.idxs, sl)
			require.Equal(t, tc.scanBytes, sb)
			require.Equal(t, tc.nextIdx, next)
			require.Equal(t, tc.exceededMaxBytes, excmb)
		})
	}
}

func TestRingBuffer_TruncateFrom(t *testing.T) {
	b := &ringBuf{}
	b.truncateFrom(20)
	eq(t, b)
	b.add(newEntries(10, 20, 9))
	eq(t, b, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	b.truncateFrom(20)
	eq(t, b, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	b.truncateFrom(19)
	eq(t, b, 10, 11, 12, 13, 14, 15, 16, 17, 18)
	b.truncateFrom(13)
	eq(t, b, 10, 11, 12)
	b.truncateFrom(1)
	eq(t, b)
	it := first(b)
	require.False(t, it.valid(b)) // regression test
}

func TestRingBuffer_ClearTo(t *testing.T) {
	b := &ringBuf{}
	b.clearTo(100)
	eq(t, b)
	b.add(newEntries(10, 20, 9))
	eq(t, b, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	b.clearTo(10)
	eq(t, b, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	b.clearTo(11)
	eq(t, b, 11, 12, 13, 14, 15, 16, 17, 18, 19)
	b.clearTo(17)
	eq(t, b, 17, 18, 19)
	b.clearTo(100)
	eq(t, b)
}

func eq(t *testing.T, b *ringBuf, idxs ...uint64) {
	t.Helper()
	var sl []uint64
	it := first(b)
	for it.valid(b) {
		idx := it.index(b)
		sl = append(sl, idx)
		it, _ = it.next(b)
		ent, ok := b.get(idx)
		require.True(t, ok)
		require.Equal(t, idx, ent.Index)
	}
	require.Equal(t, idxs, sl)
	if len(sl) == 0 {
		return
	}
	// NB: this sufficiently tests `Get`, so it doesn't have its own
	// unit tests.
	_, ok := b.get(sl[0] - 1)
	require.False(t, ok)
	_, ok = b.get(sl[len(sl)-1] + 1)
	require.False(t, ok)
}
