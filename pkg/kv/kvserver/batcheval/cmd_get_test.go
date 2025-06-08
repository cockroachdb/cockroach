// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGetResumeSpan tests that a GetRequest with a target bytes or max span
// request keys is properly handled.
func TestGetResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	resp := kvpb.GetResponse{}
	key := roachpb.Key([]byte{'a'})
	value := roachpb.MakeValueFromString("woohoo")

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()

	// This has a size of 11 bytes.
	_, err := Put(ctx, db, CommandArgs{
		EvalCtx: (&MockEvalCtx{
			ClusterSettings: cluster.MakeTestingClusterSettings(),
		}).EvalContext(),
		Header: kvpb.Header{TargetBytes: -1},
		Args: &kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: key,
			},
			Value: value,
		},
	}, &resp)
	require.NoError(t, err)

	testCases := []struct {
		maxKeys         int64
		targetBytes     int64
		allowEmpty      bool
		expectResume    bool
		expectReason    kvpb.ResumeReason
		expectNextBytes int64
	}{
		{maxKeys: -1, expectResume: true, expectReason: kvpb.RESUME_KEY_LIMIT, expectNextBytes: 0},
		{maxKeys: 0, expectResume: false},
		{maxKeys: 1, expectResume: false},
		{maxKeys: 1, allowEmpty: true, expectResume: false},

		{targetBytes: -1, expectResume: true, expectReason: kvpb.RESUME_BYTE_LIMIT, expectNextBytes: 0},
		{targetBytes: 0, expectResume: false},
		{targetBytes: 1, expectResume: false},
		{targetBytes: 11, expectResume: false},
		{targetBytes: 12, expectResume: false},
		{targetBytes: 1, allowEmpty: true, expectResume: true, expectReason: kvpb.RESUME_BYTE_LIMIT, expectNextBytes: 11},
		{targetBytes: 11, allowEmpty: true, expectResume: false},
		{targetBytes: 12, allowEmpty: true, expectResume: false},

		{maxKeys: -1, targetBytes: -1, expectResume: true, expectReason: kvpb.RESUME_KEY_LIMIT, expectNextBytes: 0},
		{maxKeys: 10, targetBytes: 100, expectResume: false},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("maxKeys=%d targetBytes=%d allowEmpty=%t",
			tc.maxKeys, tc.targetBytes, tc.allowEmpty)
		t.Run(name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettings()

			resp := kvpb.GetResponse{}
			_, err := Get(ctx, db, CommandArgs{
				EvalCtx: (&MockEvalCtx{ClusterSettings: settings}).EvalContext(),
				Header: kvpb.Header{
					MaxSpanRequestKeys: tc.maxKeys,
					TargetBytes:        tc.targetBytes,
					AllowEmpty:         tc.allowEmpty,
				},
				Args: &kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectResume {
				require.NotNil(t, resp.ResumeSpan)
				require.Equal(t, &roachpb.Span{Key: key}, resp.ResumeSpan)
				require.Equal(t, tc.expectReason, resp.ResumeReason)
				require.Equal(t, tc.expectNextBytes, resp.ResumeNextBytes)
				require.Nil(t, resp.Value)
			} else {
				require.Nil(t, resp.ResumeSpan)
				require.NotNil(t, resp.Value)
				require.Equal(t, resp.Value.RawBytes, value.RawBytes)
				require.EqualValues(t, 1, resp.NumKeys)
				require.Len(t, resp.Value.RawBytes, int(resp.NumBytes))
			}
		})
	}
}

// TestExpectExclusionSince tests evaluation of the ExpectExclusionSince option
// on all commands that support it.
func TestExpectExclusionSince(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var (
		key   = roachpb.Key([]byte{'a'})
		value = roachpb.MakeValueFromString("woohoo")

		clock    = hlc.NewClockForTesting(nil)
		settings = cluster.MakeTestingClusterSettings()
		evalCtx  = (&MockEvalCtx{Clock: clock, ClusterSettings: settings}).EvalContext()
		writeTS  = clock.Now()
	)

	putWithTxn := func(key roachpb.Key, value roachpb.Value, exclusionTS hlc.Timestamp, db storage.Engine, txn *roachpb.Transaction) error {
		putResp := kvpb.PutResponse{}
		ts := writeTS
		if txn != nil {
			ts = txn.ReadTimestamp
		}
		_, err := Put(ctx, db, CommandArgs{
			EvalCtx: evalCtx,
			Header: kvpb.Header{
				Txn:       txn,
				Timestamp: ts,
			},
			Args: &kvpb.PutRequest{
				ExpectExclusionSince: exclusionTS,
				RequestHeader:        kvpb.RequestHeader{Key: key},
				Value:                value,
			},
		}, &putResp)
		return err
	}

	getWithTxn := func(key roachpb.Key, exclusionTS hlc.Timestamp,
		str lock.Strength, dur lock.Durability, db storage.Engine, txn *roachpb.Transaction) error {
		getResp := kvpb.GetResponse{}
		ts := writeTS
		if txn != nil {
			ts = txn.ReadTimestamp
		}
		_, err := Get(ctx, db, CommandArgs{
			EvalCtx: evalCtx,
			Header: kvpb.Header{
				Txn:       txn,
				Timestamp: ts,
			},
			Args: &kvpb.GetRequest{
				LockNonExisting:      true,
				ExpectExclusionSince: exclusionTS,
				KeyLockingStrength:   str,
				KeyLockingDurability: dur,
				RequestHeader:        kvpb.RequestHeader{Key: key},
			},
		}, &getResp)
		return err
	}

	// We'll setup each test in a new engine. Placing a write or lock on `key` at
	// `writeTS`.
	type existingWriteType string
	const (
		intentWrite    existingWriteType = "intent"
		committedWrite existingWriteType = "committed"
		replicatedLock existingWriteType = "replicated-lock"
	)

	var (
		beforeExistingTS = writeTS.Prev()
		equalExistingTS  = writeTS
		afterExistingTS  = writeTS.Next()
	)

	type testCase struct {
		writeType                existingWriteType
		exclusionTS              hlc.Timestamp
		expectExclusionViolation bool
		expectLockConflictError  bool
	}

	setup := func(t *testing.T, writeType existingWriteType) (storage.Engine, *roachpb.Transaction) {
		db := storage.NewDefaultInMemForTesting()

		var txn *roachpb.Transaction
		if writeType == intentWrite || writeType == replicatedLock {
			txn1 := roachpb.MakeTransaction("test", nil, /* baseKey */
				isolation.Serializable,
				roachpb.NormalUserPriority, writeTS, 0, 0, 0, false /* omitInRangefeeds */)
			txn = &txn1
		}
		if writeType == replicatedLock {
			require.NoError(t, getWithTxn(key, hlc.Timestamp{}, lock.Exclusive, lock.Replicated, db, txn))
		} else {
			require.NoError(t, putWithTxn(key, value, hlc.Timestamp{}, db, txn))
		}
		return db, txn
	}

	// ops are the operations that support sending a ExpectExclusionSince
	// timestamp.
	ops := []struct {
		name    string
		request func(key roachpb.Key, exclusionTS hlc.Timestamp, db storage.Engine, txn *roachpb.Transaction) error
	}{
		{
			name: "Get",
			request: func(key roachpb.Key, exclusionTS hlc.Timestamp, db storage.Engine, txn *roachpb.Transaction) error {
				return getWithTxn(key, exclusionTS, lock.Exclusive, lock.Replicated, db, txn)
			},
		},
		{
			name: "Put",
			request: func(key roachpb.Key, exclusionTS hlc.Timestamp, db storage.Engine, txn *roachpb.Transaction) error {
				return putWithTxn(key, value, exclusionTS, db, txn)
			},
		},
		{
			name: "Delete",
			request: func(key roachpb.Key, exclusionTS hlc.Timestamp, db storage.Engine, txn *roachpb.Transaction) error {
				delResp := kvpb.DeleteResponse{}
				_, err := Delete(ctx, db, CommandArgs{
					EvalCtx: evalCtx,
					Header: kvpb.Header{
						Txn:       txn,
						Timestamp: txn.ReadTimestamp,
					},
					Args: &kvpb.DeleteRequest{
						ExpectExclusionSince: exclusionTS,
						RequestHeader:        kvpb.RequestHeader{Key: key},
					},
				}, &delResp)
				return err
			},
		},
	}

	testCases := []testCase{
		// If an intent write exists, it doesn't matter the timestamp it is at. We
		// expect a lock conflict error and then wait on whoever violated our write
		// exclusion.
		{
			writeType:               intentWrite,
			exclusionTS:             beforeExistingTS,
			expectLockConflictError: true,
		},
		{
			writeType:               intentWrite,
			exclusionTS:             equalExistingTS,
			expectLockConflictError: true,
		},
		{
			writeType:               intentWrite,
			exclusionTS:             afterExistingTS,
			expectLockConflictError: true,
		},

		// Committed writes are where we expect to see a write exclusion violation
		// error.
		{
			writeType:                committedWrite,
			exclusionTS:              beforeExistingTS,
			expectExclusionViolation: true,
		},
		{
			writeType:                committedWrite,
			exclusionTS:              equalExistingTS,
			expectExclusionViolation: true,
		},
		{
			writeType:   committedWrite,
			exclusionTS: afterExistingTS,
		},

		// For replicatedLocks, we get a lock conflict error in all case just like
		// an intent write.
		{
			writeType:               replicatedLock,
			exclusionTS:             beforeExistingTS,
			expectLockConflictError: true,
		},
		{
			writeType:               replicatedLock,
			exclusionTS:             equalExistingTS,
			expectLockConflictError: true,
		},
		{
			writeType:               replicatedLock,
			exclusionTS:             afterExistingTS,
			expectLockConflictError: true,
		},
	}

	for _, op := range ops {
		for _, tc := range testCases {
			exclusionTSString := "unknown"
			if tc.exclusionTS.Equal(equalExistingTS) {
				exclusionTSString = "equal"
			} else if tc.exclusionTS.Equal(beforeExistingTS) {
				exclusionTSString = "before"
			} else if tc.exclusionTS.Equal(afterExistingTS) {
				exclusionTSString = "after"
			}

			for _, sameTxn := range []bool{true, false} {
				// Committed writes don't have an associated transaction so the sameTxn
				// variant doesn't make sense here.
				if sameTxn && tc.writeType == committedWrite {
					continue
				}

				name := fmt.Sprintf("%s/%s/exclusion_ts=%s/sameTxn=%v",
					op.name, tc.writeType, exclusionTSString, sameTxn)

				t.Run(name, func(t *testing.T) {
					db, txn := setup(t, tc.writeType)
					defer db.Close()

					if !sameTxn {
						txn1 := roachpb.MakeTransaction("test", nil, /* baseKey */
							isolation.Serializable,
							roachpb.NormalUserPriority, clock.Now(), 0, 0, 0, false /* omitInRangefeeds */)
						txn = &txn1
					} else {
						txn.Sequence++
						txn.BumpReadTimestamp(clock.Now())
					}

					err := op.request(key, tc.exclusionTS, db, txn)
					if sameTxn {
						require.NoError(t, err, "expected no error for write in the same txn")
					} else if tc.expectExclusionViolation {
						require.ErrorContains(t, err, "write exclusion on key")
					} else if tc.expectLockConflictError {
						require.ErrorContains(t, err, "conflicting locks on")
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
