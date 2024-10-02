// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestRefreshError verifies we get an error. We are trying to refresh from
// time 1 to 3, but the key was written at time 2.
func TestRefreshError(t *testing.T) {
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
				if _, _, _, _, err := storage.MVCCResolveWriteIntent(
					ctx, db, nil, intent, storage.MVCCResolveWriteIntentOptions{},
				); err != nil {
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

			// We are trying to refresh from time 1 to 3, but the key was written at
			// time 2, therefore the refresh should fail.
			var resp kvpb.RefreshResponse
			_, err := Refresh(ctx, db, CommandArgs{
				Args: &kvpb.RefreshRequest{
					RequestHeader: kvpb.RequestHeader{
						Key: k,
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
				require.Equal(t, "conflicting locks on \"unresolved_key\"", err.Error())
			}
		})
	})
}

// TestRefreshTimestampBounds verifies that a Refresh treats its RefreshFrom
// timestamp as exclusive and its txn.ReadTimestamp (i.e. its "RefreshTo"
// timestamp) as inclusive.
func TestRefreshTimestampBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	k := roachpb.Key("key")
	v := roachpb.MakeValueFromString("val")
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}

	// Write to a key at time ts2.
	_, err := storage.MVCCPut(ctx, db, k, ts2, v, storage.MVCCWriteOptions{})
	require.NoError(t, err)

	for _, tc := range []struct {
		from, to hlc.Timestamp
		expErr   bool
	}{
		// Sanity-check.
		{ts1, ts3, true},
		// RefreshTo is inclusive, so expect error on collision.
		{ts1, ts2, true},
		// RefreshTo is exclusive, so expect no error on collision.
		{ts2, ts3, false},
	} {
		var resp kvpb.RefreshResponse
		_, err := Refresh(ctx, db, CommandArgs{
			Args: &kvpb.RefreshRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: k,
				},
				RefreshFrom: tc.from,
			},
			Header: kvpb.Header{
				Txn: &roachpb.Transaction{
					TxnMeta: enginepb.TxnMeta{
						WriteTimestamp: tc.to,
					},
					ReadTimestamp: tc.to,
				},
				Timestamp: tc.to,
			},
		}, &resp)

		if tc.expErr {
			require.Error(t, err)
			require.Regexp(t, "encountered recently written committed value", err)
		} else {
			require.NoError(t, err)
		}
	}
}
