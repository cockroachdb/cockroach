// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestReplicaMark(t *testing.T) {
	mark := func(id, next roachpb.ReplicaID) ReplicaMark {
		return ReplicaMark{
			RaftReplicaID:  kvserverpb.RaftReplicaID{ReplicaID: id},
			RangeTombstone: kvserverpb.RangeTombstone{NextReplicaID: next},
		}
	}
	for _, tc := range []struct {
		mark   ReplicaMark
		notOk  bool
		exists bool
	}{
		{mark: mark(MergedTombstoneReplicaID, 0), notOk: true},

		{mark: mark(0, 0), exists: false},
		{mark: mark(0, 1), exists: false},
		{mark: mark(0, 10), exists: false},
		{mark: mark(0, MergedTombstoneReplicaID), exists: false},

		{mark: mark(1, 0), exists: true},
		{mark: mark(1, 1), exists: true},
		{mark: mark(1, 2), notOk: true},
		{mark: mark(1, MergedTombstoneReplicaID), notOk: true},

		{mark: mark(10, 0), exists: true},
		{mark: mark(10, 5), exists: true},
		{mark: mark(10, 10), exists: true},
		{mark: mark(10, 11), notOk: true},
		{mark: mark(10, MergedTombstoneReplicaID), notOk: true},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.notOk, tc.mark.check() != nil)
			if tc.notOk {
				return
			}
			id := tc.mark.ReplicaID
			require.Equal(t, tc.exists, tc.mark.Exists())

			require.Equal(t, !tc.exists, tc.mark.Destroyed(id))
			require.True(t, tc.mark.Destroyed(0))
			if id > 0 {
				require.True(t, tc.mark.Destroyed(id-1))
			}
			if next := tc.mark.NextReplicaID; next != 0 {
				require.True(t, tc.mark.Destroyed(next-1))
			}

			require.Equal(t, tc.exists, tc.mark.Is(id))
			require.False(t, tc.mark.Is(id-1))
			require.False(t, tc.mark.Is(id+1))
			require.False(t, tc.mark.Is(MergedTombstoneReplicaID))

			next, err := tc.mark.Destroy(id + 1)
			require.Equal(t, tc.exists, err == nil)
			require.False(t, next.Exists())
			require.True(t, next.Destroyed(id))
		})
	}
}
