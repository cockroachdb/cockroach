// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanset_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
		for _, sa := range []spanset.SpanAccess{spanset.SpanReadOnly, spanset.SpanReadWrite} {
			for _, mvcc := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s,access=%s,mvcc=%t", fnName, sa, mvcc), func(t *testing.T) {
					span := roachpb.Span{Key: startKey, EndKey: endKey}
					ss := spanset.New()
					if mvcc {
						ss.AddMVCC(sa, span, ts)
					} else {
						ss.AddNonMVCC(sa, span)
					}
					b := eng.NewBatch()
					defer b.Close()
					rw := fn(ss, b)

					_, err := rw.MVCCGet(storage.MVCCKey{Key: ltStartKey, Timestamp: ts})
					require.NoError(t, err)
					_, err = rw.MVCCGet(storage.MVCCKey{Key: ltEndKey, Timestamp: ts})
					require.Error(t, err)

					err = rw.PutUnversioned(ltStartKey, []byte("value"))
					if sa == spanset.SpanReadWrite {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					err = rw.PutUnversioned(ltEndKey, []byte("value"))
					require.Error(t, err)
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
		span     roachpb.Span
		expectOK bool
	}{
		{span: roachpb.Span{Key: lockKey}, expectOK: false},
		{span: roachpb.Span{EndKey: lockKey}, expectOK: false},
		{span: roachpb.Span{Key: keys.LockTableSingleKeyStart}, expectOK: false},
		{span: roachpb.Span{Key: keys.LockTableSingleKeyEnd}, expectOK: false},
		{span: roachpb.Span{Key: keys.LocalRangeLockTablePrefix}, expectOK: false},
		// Declaring spans over the entire range is allowed. We assume the caller
		// knows what they're doing.
		{span: roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}, expectOK: true},
		{span: roachpb.Span{Key: keys.LocalPrefix, EndKey: keys.LocalMax}, expectOK: true},
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

						if tc.expectOK {
							require.NotPanics(t, func() { fn(ss) })
						} else {
							msg := fmt.Sprintf(
								"declaring raw lock table spans is illegal, use main key spans instead (found %s)",
								tc.span)
							require.PanicsWithValue(t, msg, func() { fn(ss) })
						}
					})
				}
			}
		}
	}
}
