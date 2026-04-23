// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func newCommitIndex(t *testing.T) *CommitIndex {
	t.Helper()
	idx, err := NewCommitIndex()
	require.NoError(t, err)
	return idx
}

func TestCommitIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(wall int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wall}
	}

	id1 := uuid.MakeV4()
	id2 := uuid.MakeV4()
	id3 := uuid.MakeV4()

	t.Run("record and lookup", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(10), id1)

		ids, ok := idx.Lookup(ts(10))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1}, ids)
	})

	t.Run("lookup miss", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(10), id1)

		ids, ok := idx.Lookup(ts(5))
		require.False(t, ok)
		require.Nil(t, ids)

		ids, ok = idx.Lookup(ts(15))
		require.False(t, ok)
		require.Nil(t, ids)
	})

	t.Run("multiple txns at same timestamp", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(10), id1)
		idx.Record(ts(10), id2)

		ids, ok := idx.Lookup(ts(10))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1, id2}, ids)
	})

	t.Run("multiple timestamps", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(5), id1)
		idx.Record(ts(10), id2)
		idx.Record(ts(15), id3)

		require.Equal(t, 3, idx.EstimatedLen())

		ids, ok := idx.Lookup(ts(5))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1}, ids)

		ids, ok = idx.Lookup(ts(10))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id2}, ids)

		ids, ok = idx.Lookup(ts(15))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id3}, ids)
	})

	t.Run("out of order recording", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(20), id1)
		idx.Record(ts(5), id2)
		idx.Record(ts(15), id3)

		require.Equal(t, 3, idx.EstimatedLen())

		ids, ok := idx.Lookup(ts(20))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1}, ids)

		ids, ok = idx.Lookup(ts(5))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id2}, ids)

		ids, ok = idx.Lookup(ts(15))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id3}, ids)
	})

	t.Run("logical timestamp distinguishes entries", func(t *testing.T) {
		idx := newCommitIndex(t)
		ts10L0 := hlc.Timestamp{WallTime: 10, Logical: 0}
		ts10L1 := hlc.Timestamp{WallTime: 10, Logical: 1}
		idx.Record(ts10L0, id1)
		idx.Record(ts10L1, id2)

		ids, ok := idx.Lookup(ts10L0)
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1}, ids)

		ids, ok = idx.Lookup(ts10L1)
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id2}, ids)
	})

	t.Run("concurrent record and lookup", func(t *testing.T) {
		idx := newCommitIndex(t)

		var wg sync.WaitGroup
		// Writer goroutine records 1000 entries.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(1); i <= 1000; i++ {
				idx.Record(ts(i), id1)
			}
		}()

		// Reader goroutines look up entries concurrently.
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := int64(1); i <= 1000; i++ {
					idx.Lookup(ts(i))
				}
			}()
		}

		wg.Wait()
		// EstimatedLen is approximate; verify all entries are findable.
		for i := int64(1); i <= 1000; i++ {
			_, ok := idx.Lookup(ts(i))
			require.True(t, ok, "expected entry at ts=%d", i)
		}
	})

	t.Run("duplicate txn id at same timestamp is deduplicated", func(t *testing.T) {
		idx := newCommitIndex(t)
		idx.Record(ts(10), id1)
		idx.Record(ts(10), id1)

		ids, ok := idx.Lookup(ts(10))
		require.True(t, ok)
		require.Equal(t, []uuid.UUID{id1}, ids)
	})
}
