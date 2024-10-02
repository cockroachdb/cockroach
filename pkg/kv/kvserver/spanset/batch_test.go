// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestReadWriterDeclareLockTable tests that lock table spans
// are declared for a ReadWriter or Batch.
func TestReadWriterDeclareLockTable(t *testing.T) {
	startKey := roachpb.Key("a")
	endKey := roachpb.Key("z")
	ltStartKey, _ := keys.LockTableSingleKey(startKey, nil)
	ltEndKey, _ := keys.LockTableSingleKey(endKey, nil)
	ts := hlc.Timestamp{}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	fns := map[string]func(*spanset.SpanSet, storage.Batch) storage.ReadWriter{
		"NewBatch": func(ss *spanset.SpanSet, b storage.Batch) storage.ReadWriter {
			return spanset.NewBatch(b, ss)
		},
		"NewBatchAt": func(ss *spanset.SpanSet, b storage.Batch) storage.ReadWriter {
			return spanset.NewBatchAt(b, ss, hlc.Timestamp{})
		},
		"NewReadWriterAt": func(ss *spanset.SpanSet, b storage.Batch) storage.ReadWriter {
			return spanset.NewReadWriterAt(b, ss, hlc.Timestamp{})
		},
	}
	for fnName, fn := range fns {
		for _, str := range []lock.Strength{lock.None, lock.Shared, lock.Exclusive, lock.Intent} {
			for _, mvcc := range []bool{false, true} {
				if !mvcc && (str == lock.Shared || str == lock.Exclusive) {
					// Invalid combination.
					continue
				}
				t.Run(fmt.Sprintf("%s,strength=%s,mvcc=%t", fnName, str, mvcc), func(t *testing.T) {
					span := roachpb.Span{Key: startKey, EndKey: endKey}
					var sa spanset.SpanAccess
					var latchTs hlc.Timestamp
					switch str {
					case lock.None:
						sa, latchTs = spanset.SpanReadOnly, ts
					case lock.Shared:
						sa, latchTs = spanset.SpanReadOnly, hlc.MaxTimestamp
					case lock.Exclusive:
						sa, latchTs = spanset.SpanReadWrite, ts
					case lock.Intent:
						sa, latchTs = spanset.SpanReadWrite, ts
					default:
						t.Fatal("unexpected")
					}
					ss := spanset.New()
					if mvcc {
						ss.AddMVCC(sa, span, latchTs)
					} else {
						ss.AddNonMVCC(sa, span)
					}
					b := eng.NewBatch()
					defer b.Close()
					rw := fn(ss, b)

					require.NoError(t, rw.MVCCIterate(context.Background(), ltStartKey, ltEndKey,
						storage.MVCCKeyIterKind, storage.IterKeyTypePointsOnly, fs.UnknownReadCategory, nil))
					require.Error(t, rw.MVCCIterate(context.Background(), ltEndKey, ltEndKey.Next(),
						storage.MVCCKeyIterKind, storage.IterKeyTypePointsOnly, fs.UnknownReadCategory, nil))

					err := rw.PutUnversioned(ltStartKey, []byte("value"))
					if str == lock.None {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Error(t, rw.PutUnversioned(ltEndKey, []byte("value")))
				})
			}
		}
	}
}

// TestReadWriterDeclareLockTablePanic tests that declaring lock table
// spans for a ReadWriter or Batch will panic.
func TestReadWriterDeclareLockTablePanic(t *testing.T) {
	lockKey, _ := keys.LockTableSingleKey(roachpb.Key("foo"), nil)
	testcases := []struct {
		span        roachpb.Span
		expectPanic bool
	}{
		{span: roachpb.Span{Key: lockKey}, expectPanic: true},
		{span: roachpb.Span{EndKey: lockKey}, expectPanic: true},
		{span: roachpb.Span{Key: keys.LockTableSingleKeyStart}, expectPanic: true},
		{span: roachpb.Span{Key: keys.LockTableSingleKeyEnd}, expectPanic: true},
		{span: roachpb.Span{Key: keys.LocalRangeLockTablePrefix}, expectPanic: true},
		// Declaring spans over the entire range is allowed. We assume the caller
		// knows what they're doing.
		{span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}, expectPanic: false},
		{span: roachpb.Span{Key: keys.LocalPrefix, EndKey: keys.LocalMax}, expectPanic: false},
	}
	fns := map[string]func(*spanset.SpanSet){
		"NewBatch":        func(ss *spanset.SpanSet) { spanset.NewBatch(nil, ss) },
		"NewBatchAt":      func(ss *spanset.SpanSet) { spanset.NewBatchAt(nil, ss, hlc.Timestamp{}) },
		"NewReadWriterAt": func(ss *spanset.SpanSet) { spanset.NewReadWriterAt(nil, ss, hlc.Timestamp{}) },
	}
	for _, tc := range testcases {
		for fnName, fn := range fns {
			for _, sa := range []spanset.SpanAccess{spanset.SpanReadOnly, spanset.SpanReadWrite} {
				for _, mvcc := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s,span=%s,access=%s,mvcc=%t", fnName, tc.span, sa, mvcc), func(t *testing.T) {
						ss := spanset.New()
						if mvcc {
							ss.AddMVCC(sa, tc.span, hlc.Timestamp{})
						} else {
							ss.AddNonMVCC(sa, tc.span)
						}

						if tc.expectPanic {
							msg := fmt.Sprintf(
								"declaring raw lock table spans is illegal, use main key spans instead (found %s)",
								tc.span)
							require.PanicsWithValue(t, msg, func() { fn(ss) })
						} else {
							require.NotPanics(t, func() { fn(ss) })
						}
					})
				}
			}
		}
	}
}
