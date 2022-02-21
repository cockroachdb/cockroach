// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sstutil

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// MakeSST builds a binary in-memory SST from the given tests data. It returns
// the binary SST data as well as the start and end (exclusive) keys of the SST.
func MakeSST(t *testing.T, st *cluster.Settings, kvs []KV) ([]byte, roachpb.Key, roachpb.Key) {
	t.Helper()

	sstFile := &storage.MemFile{}
	writer := storage.MakeIngestionSSTWriter(context.Background(), st, sstFile)
	defer writer.Close()

	start, end := keys.MaxKey, keys.MinKey
	for _, kv := range kvs {
		if kv.Key().Compare(start) < 0 {
			start = kv.Key()
		}
		if kv.Key().Compare(end) > 0 {
			end = kv.Key()
		}
		if kv.Timestamp().IsEmpty() {
			meta := &enginepb.MVCCMetadata{RawBytes: kv.ValueBytes()}
			metaBytes, err := protoutil.Marshal(meta)
			require.NoError(t, err)
			require.NoError(t, writer.PutUnversioned(kv.Key(), metaBytes))
		} else {
			require.NoError(t, writer.PutMVCC(kv.MVCCKey(), kv.ValueBytes()))
		}
	}
	require.NoError(t, writer.Finish())
	writer.Close()

	return sstFile.Data(), start, end.Next()
}

// ScanSST scans a binary in-memory SST for KV pairs.
func ScanSST(t *testing.T, sst []byte) []KV {
	t.Helper()

	var kvs []KV
	iter, err := storage.NewMemSSTIterator(sst, true)
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: keys.MinKey})
	for {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}

		k := iter.UnsafeKey()
		v := roachpb.Value{RawBytes: iter.UnsafeValue()}
		value, err := v.GetBytes()
		require.NoError(t, err)
		kvs = append(kvs, KV{
			KeyString:     string(k.Key),
			WallTimestamp: k.Timestamp.WallTime,
			ValueString:   string(value),
		})
		iter.Next()
	}
	return kvs
}

// ComputeStats computes the MVCC stats for the given binary SST.
func ComputeStats(t *testing.T, sst []byte) *enginepb.MVCCStats {
	t.Helper()

	iter, err := storage.NewMemSSTIterator(sst, true)
	require.NoError(t, err)
	defer iter.Close()

	stats, err := storage.ComputeStatsForRange(iter, keys.MinKey, keys.MaxKey, 0)
	require.NoError(t, err)
	return &stats
}
