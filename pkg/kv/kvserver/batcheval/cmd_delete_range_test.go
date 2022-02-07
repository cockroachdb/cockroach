// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDeleteRangeTombstone tests DeleteRange range tombstones directly, using
// only a Pebble engine.
//
// Most MVCC range tombstone logic is tested exhaustively in the MVCC history
// tests, this just tests the RPC plumbing.
func TestDeleteRangeTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Initial data for each test. x is point tombstone, [] is intent,
	// o---o is range tombstone.
	//
	// 5                                 [i5]
	// 4          c4
	// 3              x
	// 2      b2      d2      o-------o
	// 1
	//    a   b   c   d   e   f   g   h   i
	writeInitialData := func(t *testing.T, ctx context.Context, rw storage.ReadWriter) {
		t.Helper()
		txn := roachpb.MakeTransaction("test", nil /* baseKey */, roachpb.NormalUserPriority, hlc.Timestamp{WallTime: 5}, 0, 0)
		require.NoError(t, storage.MVCCPut(ctx, rw, nil, roachpb.Key("b"), hlc.Timestamp{WallTime: 2}, roachpb.MakeValueFromString("b2"), nil))
		require.NoError(t, storage.MVCCPut(ctx, rw, nil, roachpb.Key("c"), hlc.Timestamp{WallTime: 4}, roachpb.MakeValueFromString("c4"), nil))
		require.NoError(t, storage.MVCCPut(ctx, rw, nil, roachpb.Key("d"), hlc.Timestamp{WallTime: 2}, roachpb.MakeValueFromString("d2"), nil))
		require.NoError(t, storage.MVCCDelete(ctx, rw, nil, roachpb.Key("d"), hlc.Timestamp{WallTime: 3}, nil))
		require.NoError(t, storage.MVCCPut(ctx, rw, nil, roachpb.Key("i"), hlc.Timestamp{WallTime: 5}, roachpb.MakeValueFromString("i5"), &txn))
		require.NoError(t, storage.ExperimentalMVCCDeleteRangeUsingTombstone(ctx, rw, nil, roachpb.Key("f"), roachpb.Key("h"), hlc.Timestamp{WallTime: 3}, 0))
	}

	testcases := map[string]struct {
		start      string
		end        string
		ts         int64
		txn        bool
		inline     bool
		returnKeys bool
		expectErr  interface{} // error type, substring, or true (any)
	}{
		"above points succeed": {
			start:     "a",
			end:       "f",
			ts:        10,
			expectErr: nil,
		},
		"above range tombstone succeed": {
			start:     "f",
			end:       "h",
			ts:        10,
			expectErr: nil,
		},
		"transaction errors": {
			start:     "a",
			end:       "f",
			ts:        10,
			txn:       true,
			expectErr: batcheval.ErrTransactionUnsupported,
		},
		"inline errors": {
			start:     "a",
			end:       "f",
			ts:        10,
			inline:    true,
			expectErr: "Inline can't be used with range tombstones",
		},
		"returnKeys errors": {
			start:      "a",
			end:        "f",
			ts:         10,
			returnKeys: true,
			expectErr:  "ReturnKeys can't be used with range tombstones",
		},
		"intent errors with WriteIntentError": {
			start:     "i",
			end:       "j",
			ts:        10,
			expectErr: &roachpb.WriteIntentError{},
		},
		"below point errors with WriteTooOldError": {
			start:     "a",
			end:       "d",
			ts:        1,
			expectErr: &roachpb.WriteTooOldError{},
		},
		"below range tombstone errors with WriteTooOldError": {
			start:     "f",
			end:       "h",
			ts:        1,
			expectErr: &roachpb.WriteTooOldError{},
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()

			writeInitialData(t, ctx, engine)

			rangeKey := storage.MVCCRangeKey{
				StartKey:  roachpb.Key(tc.start),
				EndKey:    roachpb.Key(tc.end),
				Timestamp: hlc.Timestamp{WallTime: tc.ts},
			}

			var txn *roachpb.Transaction
			if tc.txn {
				tx := roachpb.MakeTransaction("txn", nil /* baseKey */, roachpb.NormalUserPriority, rangeKey.Timestamp, 0, 0)
				txn = &tx
			}

			// Run the request.
			var ms enginepb.MVCCStats
			resp := &roachpb.DeleteRangeResponse{}
			_, err := batcheval.DeleteRange(ctx, engine, batcheval.CommandArgs{
				EvalCtx: (&batcheval.MockEvalCtx{ClusterSettings: st}).EvalContext(),
				Stats:   &ms,
				Header: roachpb.Header{
					Timestamp: rangeKey.Timestamp,
					Txn:       txn,
				},
				Args: &roachpb.DeleteRangeRequest{
					RequestHeader: roachpb.RequestHeader{
						Key:    rangeKey.StartKey,
						EndKey: rangeKey.EndKey,
					},
					UseExperimentalRangeTombstone: true,
					Inline:                        tc.inline,
					ReturnKeys:                    tc.returnKeys,
				},
			}, resp)

			// Check the error.
			if tc.expectErr != nil {
				require.Error(t, err)
				if b, ok := tc.expectErr.(bool); ok && b {
					// any error is fine
				} else if expectMsg, ok := tc.expectErr.(string); ok {
					require.Contains(t, err.Error(), expectMsg)
				} else if e, ok := tc.expectErr.(error); ok {
					require.True(t, errors.HasType(err, e), "expected %T, got %v", e, err)
				} else {
					require.Fail(t, "invalid expectErr", "expectErr=%v", tc.expectErr)
				}
				return
			}
			require.NoError(t, err)

			// Check that the range tombstone was written successfully.
			iter := engine.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
				KeyTypes:   storage.IterKeyTypeRangesOnly,
				LowerBound: rangeKey.StartKey,
				UpperBound: rangeKey.EndKey,
			})
			defer iter.Close()
			iter.SeekGE(storage.MVCCKey{Key: rangeKey.StartKey})

			var endSeen roachpb.Key
			for {
				ok, err := iter.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				require.True(t, ok)
				for _, rk := range iter.RangeKeys() {
					if rk.Timestamp.Equal(rangeKey.Timestamp) {
						endSeen = rk.EndKey.Clone()
						break
					}
				}
				iter.Next()
			}
			require.Equal(t, rangeKey.EndKey, endSeen)

			// TODO(erikgrinaker): This should test MVCC stats when implemented.
		})
	}
}
