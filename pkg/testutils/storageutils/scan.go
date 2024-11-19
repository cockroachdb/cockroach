// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// ScanEngine scans all point/range keys from the replicated user keyspace of
// the engine, returning a combined slice of MVCCKeyValue and MVCCRangeKeyValue
// in order.
func ScanEngine(t *testing.T, engine storage.Reader) KVs {
	t.Helper()
	return ScanKeySpan(t, engine, keys.LocalMax, keys.MaxKey)
}

// ScanIter scans all point/range keys from the iterator, returning a combined
// slice of MVCCKeyValue and MVCCRangeKeyValue in order.
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
				prevRangeStart = bounds.Key.Clone()
				rangeKeys := iter.RangeKeys().Clone()
				for _, v := range rangeKeys.Versions {
					if len(v.Value) == 0 {
						v.Value = nil
					}
					kvs = append(kvs, rangeKeys.AsRangeKeyValue(v))
				}
			}
		}
		if hasPoint {
			var value []byte
			v, err := iter.UnsafeValue()
			require.NoError(t, err)
			if iter.UnsafeKey().IsValue() {
				if len(v) > 0 {
					value = append(value, v...)
				}
			} else {
				var meta enginepb.MVCCMetadata
				require.NoError(t, protoutil.Unmarshal(v, &meta))
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

// ScanKeySpan scans all point/range keys in the given key span, returning a
// combined slice of MVCCKeyValue and MVCCRangeKeyValue in order.
func ScanKeySpan(t *testing.T, r storage.Reader, start, end roachpb.Key) KVs {
	t.Helper()

	iter, err := r.NewMVCCIterator(
		context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsAndRanges,
			LowerBound: start,
			UpperBound: end,
		})
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	return ScanIter(t, iter)
}

// ScanRange scans all user point/range keys owned by the given range
// descriptor, returning a combined slice of MVCCKeyValue and MVCCRangeKeyValue
// in order.
func ScanRange(t *testing.T, r storage.Reader, desc roachpb.RangeDescriptor) KVs {
	t.Helper()
	return ScanKeySpan(t, r, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
}

// ScanSST scans all point/range keys from the given binary SST, returning a
// combined slice of MVCCKeyValue and MVCCRangeKeyValue in order.
func ScanSST(t *testing.T, sst []byte) KVs {
	t.Helper()

	iter, err := storage.NewMemSSTIterator(sst, true /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err)
	defer iter.Close()
	return ScanIter(t, iter)
}
