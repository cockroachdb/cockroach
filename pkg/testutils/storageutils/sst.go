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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// MakeSST builds a binary in-memory SST from the given KVs, which can be both
// MVCCKeyValue or MVCCRangeKeyValue. It returns the binary SST data as well as
// the start and end (exclusive) keys of the SST.
func MakeSST(
	t *testing.T, st *cluster.Settings, kvs []interface{},
) ([]byte, roachpb.Key, roachpb.Key) {
	t.Helper()
	return MakeSSTWithPrefix(t, st, nil, kvs)
}

// MakeSST builds a binary in-memory SST from the given KVs, which can be both
// MVCCKeyValue or MVCCRangeKeyValue. It returns the binary SST data as well as
// the start and end (exclusive) keys of the SST.
func MakeSSTWithPrefix(
	t *testing.T, st *cluster.Settings, prefix roachpb.Key, kvs []interface{},
) ([]byte, roachpb.Key, roachpb.Key) {
	t.Helper()

	sstFile := &storage.MemObject{}
	writer := storage.MakeIngestionSSTWriter(context.Background(), st, sstFile)
	defer writer.Close()

	start, end := keys.MaxKey, keys.MinKey
	for _, kvI := range kvs {
		var s, e roachpb.Key
		switch kv := kvI.(type) {
		case storage.MVCCKeyValue:
			if len(prefix) > 0 {
				k, err := keys.RewriteKeyToTenantPrefix(kv.Key.Key, prefix)
				require.NoError(t, err)
				kv.Key.Key = k
			}
			if kv.Key.Timestamp.IsEmpty() {
				v, err := protoutil.Marshal(&enginepb.MVCCMetadata{RawBytes: kv.Value})
				require.NoError(t, err)
				require.NoError(t, writer.PutUnversioned(kv.Key.Key, v))
			} else {
				require.NoError(t, writer.PutRawMVCC(kv.Key, kv.Value))
			}
			s, e = kv.Key.Key, kv.Key.Key.Next()

		case storage.MVCCRangeKeyValue:
			v, err := storage.DecodeMVCCValue(kv.Value)
			require.NoError(t, err)
			require.NoError(t, writer.PutMVCCRangeKey(kv.RangeKey, v))
			s, e = kv.RangeKey.StartKey, kv.RangeKey.EndKey

		default:
			t.Fatalf("invalid KV type %T", kv)
		}
		if s.Compare(start) < 0 {
			start = s
		}
		if e.Compare(end) > 0 {
			end = e
		}
	}

	require.NoError(t, writer.Finish())
	writer.Close()

	return sstFile.Data(), start, end
}

// KeysFromSST takes an SST as a byte slice and returns all point
// and range keys in the SST.
func KeysFromSST(t *testing.T, data []byte) ([]storage.MVCCKey, []storage.MVCCRangeKeyStack) {
	var results []storage.MVCCKey
	var rangeKeyRes []storage.MVCCRangeKeyStack
	it, err := storage.NewMemSSTIterator(data, false, storage.IterOptions{
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(t, err, "Failed to read exported data")
	defer it.Close()
	for it.SeekGE(storage.MVCCKey{Key: []byte{}}); ; {
		ok, err := it.Valid()
		require.NoError(t, err, "Failed to advance iterator while preparing data")
		if !ok {
			break
		}

		if it.RangeKeyChanged() {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasRange {
				rangeKeyRes = append(rangeKeyRes, it.RangeKeys().Clone())
			}
			if !hasPoint {
				it.Next()
				continue
			}
		}

		results = append(results, storage.MVCCKey{
			Key:       append(roachpb.Key(nil), it.UnsafeKey().Key...),
			Timestamp: it.UnsafeKey().Timestamp,
		})
		it.Next()
	}
	return results, rangeKeyRes
}
