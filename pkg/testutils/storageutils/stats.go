// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/stretchr/testify/require"
)

// EngineStats computes MVCC stats for the given engine reader, limited to the
// replicated user keyspace.
func EngineStats(t *testing.T, engine storage.Reader, nowNanos int64) *enginepb.MVCCStats {
	t.Helper()

	stats, err := storage.ComputeStats(
		context.Background(), engine, keys.LocalMax, keys.MaxKey, nowNanos)
	require.NoError(t, err)
	return &stats
}

// SSTStats computes MVCC stats for the given binary SST.
func SSTStats(t *testing.T, sst []byte, nowNanos int64) *enginepb.MVCCStats {
	t.Helper()

	iter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()
	iter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	stats, err := storage.ComputeStatsForIter(iter, nowNanos)
	require.NoError(t, err)
	return &stats
}
