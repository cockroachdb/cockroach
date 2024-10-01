// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestRefreshRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write an MVCC point key at b@3, MVCC point tombstone at b@5, and MVCC range
	// tombstone at [d-f)@7.
	_, err := storage.MVCCPut(
		ctx, eng, roachpb.Key("b"), hlc.Timestamp{WallTime: 3}, roachpb.MakeValueFromString("value"), storage.MVCCWriteOptions{})
	require.NoError(t, err)
	_, err = storage.MVCCPut(
		ctx, eng, roachpb.Key("c"), hlc.Timestamp{WallTime: 5}, roachpb.Value{}, storage.MVCCWriteOptions{})
	require.NoError(t, err)
	require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(
		ctx, eng, nil, roachpb.Key("d"), roachpb.Key("f"), hlc.Timestamp{WallTime: 7}, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil))

	testcases := map[string]struct {
		start, end string
		from, to   int64
		expectErr  error
	}{
		"below all": {"a", "z", 1, 2, nil},
		"above all": {"a", "z", 8, 10, nil},
		"between":   {"a", "z", 4, 4, nil},
		"beside":    {"x", "z", 1, 10, nil},
		"point key": {"a", "z", 2, 4, &kvpb.RefreshFailedError{
			Reason:    kvpb.RefreshFailedError_REASON_COMMITTED_VALUE,
			Key:       roachpb.Key("b"),
			Timestamp: hlc.Timestamp{WallTime: 3},
		}},
		"point tombstone": {"a", "z", 4, 6, &kvpb.RefreshFailedError{
			Reason:    kvpb.RefreshFailedError_REASON_COMMITTED_VALUE,
			Key:       roachpb.Key("c"),
			Timestamp: hlc.Timestamp{WallTime: 5},
		}},
		"range tombstone": {"a", "z", 6, 8, &kvpb.RefreshFailedError{
			Reason:    kvpb.RefreshFailedError_REASON_COMMITTED_VALUE,
			Key:       roachpb.Key("d"),
			Timestamp: hlc.Timestamp{WallTime: 7},
		}},
		"to is inclusive": {"a", "z", 1, 3, &kvpb.RefreshFailedError{
			Reason:    kvpb.RefreshFailedError_REASON_COMMITTED_VALUE,
			Key:       roachpb.Key("b"),
			Timestamp: hlc.Timestamp{WallTime: 3},
		}},
		"from is exclusive": {"a", "z", 7, 10, nil},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			_, err := RefreshRange(ctx, eng, CommandArgs{
				EvalCtx: (&MockEvalCtx{
					ClusterSettings: cluster.MakeTestingClusterSettings(),
				}).EvalContext(),
				Args: &kvpb.RefreshRangeRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:    roachpb.Key(tc.start),
						EndKey: roachpb.Key(tc.end),
					},
					RefreshFrom: hlc.Timestamp{WallTime: tc.from},
				},
				Header: kvpb.Header{
					Timestamp: hlc.Timestamp{WallTime: tc.to},
					Txn: &roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{
							WriteTimestamp: hlc.Timestamp{WallTime: tc.to},
						},
						ReadTimestamp: hlc.Timestamp{WallTime: tc.to},
					},
				},
			}, &kvpb.RefreshRangeResponse{})

			if tc.expectErr == nil {
				require.NoError(t, err)
			} else {
				var refreshErr *kvpb.RefreshFailedError
				require.Error(t, err)
				require.ErrorAs(t, err, &refreshErr)
				require.Equal(t, tc.expectErr, refreshErr)
			}
		})
	}
}

// TestRefreshRangeTimeBoundIterator is a regression test for
// https://github.com/cockroachdb/cockroach/issues/31823. RefreshRange
// uses a time-bound iterator, which has a bug that can cause old
// resolved intents to incorrectly appear to be pending. This test
// constructs the necessary arrangement of sstables to reproduce the
// bug and ensures that the workaround (and later, the permanent fix)
// are effective.
//
// The bug is that resolving an intent does not contribute to the
// sstable's timestamp bounds, so that if there is no other
// timestamped data expanding the bounds, time-bound iterators may
// open fewer sstables than necessary and only see the intent, not its
// resolution.
//
// This test creates two sstables. The first contains a pending intent
// at ts1 and another key at ts4, giving it timestamp bounds 1-4 (and
// putting it in scope for transactions at timestamps higher than
// ts1).
func TestRefreshRangeTimeBoundIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k := roachpb.Key("a")
	v := roachpb.MakeValueFromString("hi")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}
	ts4 := hlc.Timestamp{WallTime: 4}

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// Create an sstable containing an unresolved intent.
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            k,
			ID:             uuid.MakeV4(),
			Epoch:          1,
			WriteTimestamp: ts1,
		},
		ReadTimestamp: ts1,
	}
	if _, err := storage.MVCCPut(
		ctx, db, k, txn.ReadTimestamp, v, storage.MVCCWriteOptions{Txn: txn},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.MVCCPut(
		ctx, db, roachpb.Key("unused1"), ts4, v, storage.MVCCWriteOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// Create a second sstable containing the resolution of the intent
	// (committed). The sstable also has a second write at a different (older)
	// timestamp, because if it were empty other than the deletion tombstone, it
	// would not have any timestamp bounds and would be selected for every read.
	intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: k})
	intent.Status = roachpb.COMMITTED
	if _, _, _, _, err := storage.MVCCResolveWriteIntent(
		ctx, db, nil, intent, storage.MVCCResolveWriteIntentOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.MVCCPut(
		ctx, db, roachpb.Key("unused2"), ts1, v, storage.MVCCWriteOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// We should now have a committed value at k@ts1. Read it back to make sure.
	// This represents real-world use of time-bound iterators where callers must
	// have previously performed a consistent read at the lower time-bound to
	// prove that there are no intents present that would be missed by the time-
	// bound iterator.
	if res, err := storage.MVCCGet(ctx, db, k, ts1, storage.MVCCGetOptions{}); err != nil {
		t.Fatal(err)
	} else if res.Intent != nil {
		t.Fatalf("got unexpected intent: %v", intent)
	} else if !res.Value.EqualTagAndData(v) {
		t.Fatalf("expected %v, got %v", v, res.Value)
	}

	// Now the real test: a transaction at ts2 has been pushed to ts3
	// and must refresh. It overlaps with our committed intent on k@ts1,
	// which is fine because our timestamp is higher (but if that intent
	// were still pending, the new txn would be blocked). Prior to
	// https://github.com/cockroachdb/cockroach/pull/32211, a bug in the
	// time-bound iterator meant that we would see the first sstable but
	// not the second and incorrectly report the intent as pending,
	// resulting in an error from RefreshRange.
	var resp kvpb.RefreshRangeResponse
	_, err := RefreshRange(ctx, db, CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
		}).EvalContext(),
		Args: &kvpb.RefreshRangeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    k,
				EndKey: keys.MaxKey,
			},
			RefreshFrom: ts2,
		},
		Header: kvpb.Header{
			Txn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: ts3,
				},
				ReadTimestamp: ts2,
			},
			Timestamp: ts3,
		},
	}, &resp)
	if err != nil {
		t.Fatal(err)
	}
}

// TestRefreshRangeError verifies we get an error. We are trying to refresh from
// time 1 to 3, but the key was written at time 2.
func TestRefreshRangeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intents behave the same, but the error is a bit different. Verify resolved and unresolved intents.
	testutils.RunTrueAndFalse(t, "resolve_intent", func(t *testing.T, resolveIntent bool) {
		// WaitPolicy_Error is set on all Refresh requests after V23_2_RemoveLockTableWaiterTouchPush.
		// This test ensures the correct behavior in a mixed version state.
		// TODO(mira): Remove after V23_2_RemoveLockTableWaiterTouchPush is deleted.
		testutils.RunTrueAndFalse(t, "wait_policy_error", func(t *testing.T, waitPolicyError bool) {
			ctx := context.Background()
			v := roachpb.MakeValueFromString("hi")
			ts1 := hlc.Timestamp{WallTime: 1}
			ts2 := hlc.Timestamp{WallTime: 2}
			ts3 := hlc.Timestamp{WallTime: 3}

			db := storage.NewDefaultInMemForTesting()
			defer db.Close()

			var k roachpb.Key
			if resolveIntent {
				k = roachpb.Key("resolved_key")
			} else {
				k = roachpb.Key("unresolved_key")
			}

			// Write to a key at time ts2 by creating an sstable containing an unresolved intent.
			txn := &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Key:            k,
					ID:             uuid.MakeV4(),
					Epoch:          1,
					WriteTimestamp: ts2,
				},
				ReadTimestamp: ts2,
			}
			if _, err := storage.MVCCPut(
				ctx, db, k, txn.ReadTimestamp, v, storage.MVCCWriteOptions{Txn: txn},
			); err != nil {
				t.Fatal(err)
			}

			if resolveIntent {
				intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: k})
				intent.Status = roachpb.COMMITTED
				if _, _, _, _, err := storage.MVCCResolveWriteIntent(ctx, db, nil, intent, storage.MVCCResolveWriteIntentOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			header := kvpb.Header{
				Txn: &roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						WriteTimestamp: ts3,
					},
					ReadTimestamp: ts3,
				},
				Timestamp: ts3,
			}
			if waitPolicyError {
				header.WaitPolicy = lock.WaitPolicy_Error
			}

			// We are trying to refresh from time 1 to 3, but the key was written at time
			// 2, therefore the refresh should fail.
			var resp kvpb.RefreshRangeResponse
			_, err := RefreshRange(ctx, db, CommandArgs{
				EvalCtx: (&MockEvalCtx{
					ClusterSettings: cluster.MakeTestingClusterSettings(),
				}).EvalContext(),
				Args: &kvpb.RefreshRangeRequest{
					RequestHeader: kvpb.RequestHeader{
						Key:    k,
						EndKey: keys.MaxKey,
					},
					RefreshFrom: ts1,
				},
				Header: header,
			}, &resp)
			if resolveIntent {
				require.IsType(t, &kvpb.RefreshFailedError{}, err)
				require.Equal(t, "encountered recently written committed value \"resolved_key\" @0.000000002,0",
					err.Error())
			} else if !waitPolicyError {
				require.IsType(t, &kvpb.RefreshFailedError{}, err)
				require.Equal(t, "encountered recently written intent \"unresolved_key\" @0.000000002,0",
					err.Error())
			} else {
				require.IsType(t, &kvpb.LockConflictError{}, err)
				require.Equal(t, "conflicting locks on \"unresolved_key\"",
					err.Error())
			}
		})
	})
}
