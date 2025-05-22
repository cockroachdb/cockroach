// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageutils

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/mvccencoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/redact"
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

// ReportSSTEntries iterates through an SST and dumps the raw SST data into the
// buffer in a format suitable for datadriven testing output.
func ReportSSTEntries(buf *redact.StringBuilder, name string, sst []byte) error {
	r, err := sstable.NewMemReader(sst, sstable.ReaderOptions{
		Comparer:   &storage.EngineComparer,
		KeySchemas: sstable.MakeKeySchemas(storage.KeySchemas...),
	})
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	buf.Printf(">> %s:\n", name)

	// Dump point keys.
	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		if err := iter.Error(); err != nil {
			return err
		}
		key, err := storage.DecodeMVCCKey(kv.K.UserKey)
		if err != nil {
			return err
		}
		v, _, err := kv.Value(nil)
		if err != nil {
			return err
		}
		value, err := storage.DecodeMVCCValue(v)
		if err != nil {
			return err
		}
		buf.Printf("%s: %s -> %s\n", strings.ToLower(kv.Kind().String()), key, value)
	}

	// Dump rangedels.
	if rdIter, err := r.NewRawRangeDelIter(context.Background(), block.NoFragmentTransforms, sstable.NoReadEnv); err != nil {
		return err
	} else if rdIter != nil {
		defer rdIter.Close()
		s, err := rdIter.First()
		for ; s != nil; s, err = rdIter.Next() {
			start, err := storage.DecodeMVCCKey(s.Start)
			if err != nil {
				return err
			}
			end, err := storage.DecodeMVCCKey(s.End)
			if err != nil {
				return err
			}
			for _, k := range s.Keys {
				buf.Printf("%s: %s\n", strings.ToLower(k.Kind().String()),
					roachpb.Span{Key: start.Key, EndKey: end.Key})
			}
		}
		if err != nil {
			return err
		}
	}

	// Dump range keys.
	if rkIter, err := r.NewRawRangeKeyIter(context.Background(), block.NoFragmentTransforms, sstable.NoReadEnv); err != nil {
		return err
	} else if rkIter != nil {
		defer rkIter.Close()
		s, err := rkIter.First()
		for ; s != nil; s, err = rkIter.Next() {
			start, err := storage.DecodeMVCCKey(s.Start)
			if err != nil {
				return err
			}
			end, err := storage.DecodeMVCCKey(s.End)
			if err != nil {
				return err
			}
			for _, k := range s.Keys {
				buf.Printf("%s: %s", strings.ToLower(k.Kind().String()),
					roachpb.Span{Key: start.Key, EndKey: end.Key})
				if len(k.Suffix) > 0 {
					ts, err := mvccencoding.DecodeMVCCTimestampSuffix(k.Suffix)
					if err != nil {
						return err
					}
					buf.Printf("/%s", ts)
				}
				if k.Kind() == pebble.InternalKeyKindRangeKeySet {
					value, err := storage.DecodeMVCCValue(k.Value)
					if err != nil {
						return err
					}
					buf.Printf(" -> %s", value)
				}
				buf.Printf("\n")
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
