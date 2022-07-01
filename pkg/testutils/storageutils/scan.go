// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storageutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// ScanEngine scans all point/range keys from the replicated user keyspace of
// the engine, returning a combined slice of eyValue and MVCCKeyValue in order.
func ScanEngine(t *testing.T, engine storage.Reader) KVs {
	t.Helper()

	iter := engine.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	})
	defer iter.Close()
	return ScanIter(t, iter)
}

// ScanIter scans all point/range keys from the iterator, and returns a combined
// slice of MVCCRangeKeyValue and MVCCKeyValue in order.
func ScanIter(t *testing.T, iter storage.SimpleMVCCIterator) KVs {
	t.Helper()

	var kvs []interface{}
	var prevRangeStart roachpb.Key
	for iter.SeekGE(storage.MVCCKey{Key: keys.LocalMax}); ; iter.Next() {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasRange {
			if bounds := iter.RangeBounds(); !bounds.Key.Equal(prevRangeStart) {
				for _, rkv := range iter.RangeKeys() {
					if len(rkv.Value) == 0 {
						rkv.Value = nil
					}
					kvs = append(kvs, rkv.Clone())
				}
				prevRangeStart = bounds.Key.Clone()
			}
		}
		if hasPoint {
			var value []byte
			if iter.UnsafeKey().IsValue() {
				if v := iter.UnsafeValue(); len(v) > 0 {
					value = append(value, iter.UnsafeValue()...)
				}
			} else {
				var meta enginepb.MVCCMetadata
				require.NoError(t, protoutil.Unmarshal(iter.UnsafeValue(), &meta))
				if meta.RawBytes == nil {
					// Skip intent metadata records; we're only interested in the provisional value.
					continue
				}
				value = meta.RawBytes
			}

			kvs = append(kvs, storage.MVCCKeyValue{
				Key:   iter.UnsafeKey().Clone(),
				Value: value,
			})
		}
	}
	return kvs
}

// ScanSST scans all point/range keys from the given binary SST, returning a
// combined slice of eyValue and MVCCKeyValue in order.
func ScanSST(t *testing.T, sst []byte) KVs {
	t.Helper()

	iter, err := storage.NewPebbleMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()
	return ScanIter(t, iter)
}
