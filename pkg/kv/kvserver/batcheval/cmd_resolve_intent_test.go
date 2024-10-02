// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
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

func TestDeclareKeysResolveIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const id = "f90b99de-6bd2-48a3-873c-12fdb9867a3c"
	txnMeta := enginepb.TxnMeta{}
	{
		var err error
		txnMeta.ID, err = uuid.FromString(id)
		if err != nil {
			t.Fatal(err)
		}
	}
	abortSpanKey := fmt.Sprintf(`write local: /Local/RangeID/99/r/AbortSpan/"%s"`, id)
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("a"),
	}
	tests := []struct {
		status      roachpb.TransactionStatus
		poison      bool
		expDeclares bool
	}{
		{
			status:      roachpb.ABORTED,
			poison:      true,
			expDeclares: true,
		},
		{
			status:      roachpb.ABORTED,
			poison:      false,
			expDeclares: true,
		},
		{
			status:      roachpb.COMMITTED,
			poison:      true,
			expDeclares: false,
		},
		{
			status:      roachpb.COMMITTED,
			poison:      false,
			expDeclares: false,
		},
	}
	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	st := cluster.MakeTestingClusterSettings()
	defer engine.Close()
	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		for _, test := range tests {
			t.Run("", func(t *testing.T) {
				ri := kvpb.ResolveIntentRequest{
					IntentTxn: txnMeta,
					Status:    test.status,
					Poison:    test.poison,
				}
				ri.Key = roachpb.Key("b")
				rir := kvpb.ResolveIntentRangeRequest{
					IntentTxn: ri.IntentTxn,
					Status:    ri.Status,
					Poison:    ri.Poison,
				}
				rir.Key = ri.Key
				rir.EndKey = roachpb.Key("c")

				as := abortspan.New(desc.RangeID)

				var latchSpans spanset.SpanSet
				var lockSpans lockspanset.LockSpanSet

				var h kvpb.Header
				h.RangeID = desc.RangeID

				cArgs := CommandArgs{Header: h}
				cArgs.EvalCtx = (&MockEvalCtx{ClusterSettings: st, AbortSpan: as}).EvalContext()

				if !ranged {
					cArgs.Args = &ri
					require.NoError(t, declareKeysResolveIntent(&desc, &h, &ri, &latchSpans, &lockSpans, 0))
					batch := spanset.NewBatch(engine.NewBatch(), &latchSpans)
					defer batch.Close()
					if _, err := ResolveIntent(ctx, batch, cArgs, &kvpb.ResolveIntentResponse{}); err != nil {
						t.Fatal(err)
					}
				} else {
					cArgs.Args = &rir
					require.NoError(
						t, declareKeysResolveIntentRange(&desc, &h, &rir, &latchSpans, &lockSpans, 0),
					)
					batch := spanset.NewBatch(engine.NewBatch(), &latchSpans)
					defer batch.Close()
					if _, err := ResolveIntentRange(ctx, batch, cArgs, &kvpb.ResolveIntentRangeResponse{}); err != nil {
						t.Fatal(err)
					}
				}

				if s := latchSpans.String(); strings.Contains(s, abortSpanKey) != test.expDeclares {
					t.Errorf("expected AbortSpan declared: %t, but got spans\n%s", test.expDeclares, s)
				}
				if !lockSpans.Empty() {
					t.Errorf("expected no lock spans declared, but got spans\n%s", lockSpans.String())
				}
			})
		}
	})
}

// TestResolveIntentAfterPartialRollback checks that the ResolveIntent
// and ResolveIntentRange properly propagate their IgnoredSeqNums
// parameter to the MVCC layer and only commit writes at non-ignored
// seqnums.
func TestResolveIntentAfterPartialRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	endKey := roachpb.Key("z")
	txn := roachpb.MakeTransaction("test", k, 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(k),
		EndKey:   roachpb.RKey(endKey),
	}

	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()
		st := cluster.MakeTestingClusterSettings()

		var v roachpb.Value
		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 0
		if _, err := storage.MVCCPut(ctx, batch, k, ts, v, storage.MVCCWriteOptions{Txn: &txn}); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 1
		if _, err := storage.MVCCPut(ctx, batch, k, ts, v, storage.MVCCWriteOptions{Txn: &txn}); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}

		// Partially revert the 2nd store above.
		ignoredSeqNums := []enginepb.IgnoredSeqNumRange{{Start: 1, End: 1}}

		h := kvpb.Header{
			RangeID:   desc.RangeID,
			Timestamp: ts,
		}

		// The spans will be used for validating that reads and writes are
		// consistent with the declared spans. We initialize spans below, before
		// performing reads and writes.
		var spans spanset.SpanSet
		var rbatch storage.Batch

		if !ranged {
			// Resolve a point intent.
			ri := kvpb.ResolveIntentRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			ri.Key = k

			require.NoError(t, declareKeysResolveIntent(&desc, &h, &ri, &spans, nil, 0))
			rbatch = spanset.NewBatch(db.NewBatch(), &spans)
			defer rbatch.Close()

			if _, err := ResolveIntent(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
					Args:    &ri,
				},
				&kvpb.ResolveIntentResponse{},
			); err != nil {
				t.Fatal(err)
			}
		} else {
			// Resolve an intent range.
			rir := kvpb.ResolveIntentRangeRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			rir.Key = k
			rir.EndKey = endKey

			require.NoError(t, declareKeysResolveIntentRange(&desc, &h, &rir, &spans, nil, 0))
			rbatch = spanset.NewBatch(db.NewBatch(), &spans)
			defer rbatch.Close()

			h.MaxSpanRequestKeys = 10
			if _, err := ResolveIntentRange(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
					Args:    &rir,
				},
				&kvpb.ResolveIntentRangeResponse{},
			); err != nil {
				t.Fatal(err)
			}
		}

		if err := rbatch.Commit(true); err != nil {
			t.Fatal(err)
		}

		batch = db.NewBatch()
		defer batch.Close()

		// The second write has been rolled back; verify that the remaining
		// value is from the first write.
		res, err := storage.MVCCGet(ctx, batch, k, ts2, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Intent != nil {
			t.Errorf("%s: found intent, expected none: %+v", k, res.Intent)
		}
		if res.Value == nil {
			t.Errorf("%s: no value found, expected one", k)
		} else {
			s, err := res.Value.GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, "a", string(s), "at key %s", k)
		}
	})
}

// TestResolveIntentWithTargetBytes tests that ResolveIntent and
// ResolveIntentRange respect the specified TargetBytes i.e. resolve the
// correct set of intents, return the correct data in the response, and ensure
// the underlying write batch is the expected size.
func TestResolveIntentWithTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := hlc.Timestamp{WallTime: 1}
	bytes := []byte{'a', 'b', 'c', 'd', 'e'}
	nKeys := len(bytes)
	testKeys := make([]roachpb.Key, nKeys)
	values := make([]roachpb.Value, nKeys)
	for i, b := range bytes {
		testKeys[i] = make([]byte, 1000)
		for j := range testKeys[i] {
			testKeys[i][j] = b
		}
		values[i] = roachpb.MakeValueFromBytes([]byte{b})
	}
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)

	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		db := storage.NewDefaultInMemForTesting()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()
		st := cluster.MakeTestingClusterSettings()

		for i, testKey := range testKeys {
			_, err := storage.MVCCPut(ctx, batch, testKey, ts, values[i], storage.MVCCWriteOptions{Txn: &txn})
			require.NoError(t, err)
		}
		initialBytes := batch.Len()

		if !ranged {
			// Resolve a point intent for testKeys[0].
			ri := kvpb.ResolveIntentRequest{
				IntentTxn: txn.TxnMeta,
				Status:    roachpb.COMMITTED,
			}
			ri.Key = testKeys[0]

			{
				// Case 1: TargetBytes = -1. In this case, we should not resolve any
				// intents.
				resp := &kvpb.ResolveIntentResponse{}
				_, err := ResolveIntent(ctx, batch,
					CommandArgs{
						EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
						Args:    &ri,
						Header: kvpb.Header{
							TargetBytes: -1,
						},
					},
					resp,
				)
				require.NoError(t, err)
				require.Equal(t, resp.NumBytes, int64(0))
				require.Equal(t, resp.ResumeSpan.Key, testKeys[0])
				require.Equal(t, resp.ResumeReason, kvpb.RESUME_BYTE_LIMIT)
				require.NoError(t, err)
				numBytes := batch.Len()
				require.Equal(t, numBytes, initialBytes)

				_, err = storage.MVCCGet(ctx, batch, testKeys[0], ts, storage.MVCCGetOptions{})
				require.Error(t, err)
			}

			{
				// Case 2: TargetBytes = 500. In this case, we should resolve the
				// intent for testKeys[0].
				resp := &kvpb.ResolveIntentResponse{}
				_, err := ResolveIntent(ctx, batch,
					CommandArgs{
						EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
						Args:    &ri,
						Header: kvpb.Header{
							TargetBytes: 500,
						},
					},
					resp,
				)
				require.Greater(t, resp.NumBytes, int64(1000))
				require.Less(t, resp.NumBytes, int64(1100))
				require.Nil(t, resp.ResumeSpan)
				require.Equal(t, resp.ResumeReason, kvpb.RESUME_UNKNOWN)
				require.NoError(t, err)
				numBytes := batch.Len()
				require.Greater(t, numBytes, initialBytes+1000)
				require.Less(t, numBytes, initialBytes+1100)

				valueRes, err := storage.MVCCGet(ctx, batch, testKeys[0], ts, storage.MVCCGetOptions{})
				require.NoError(t, err)
				require.Equal(t, values[0].RawBytes, valueRes.Value.RawBytes,
					"the value %s in get result does not match the value %s in request", values[0].RawBytes, valueRes.Value.RawBytes)
			}
		} else {
			// Resolve an intent range for testKeys[0], testKeys[1], ...,
			// testKeys[4].
			rir := kvpb.ResolveIntentRangeRequest{
				IntentTxn: txn.TxnMeta,
				Status:    roachpb.COMMITTED,
			}
			rir.Key = testKeys[0]
			rir.EndKey = testKeys[nKeys-1].Next()

			{
				// Case 1: TargetBytes = -1. In this case, we should not resolve any
				// intents.
				respr := &kvpb.ResolveIntentRangeResponse{}
				_, err := ResolveIntentRange(ctx, batch,
					CommandArgs{
						EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
						Args:    &rir,
						Header: kvpb.Header{
							TargetBytes: -1,
						},
					},
					respr,
				)
				require.NoError(t, err)
				require.Equal(t, respr.NumKeys, int64(0))
				require.Equal(t, respr.NumBytes, int64(0))
				require.Equal(t, respr.ResumeSpan.Key, testKeys[0])
				require.Equal(t, respr.ResumeSpan.EndKey, testKeys[nKeys-1].Next())
				require.Equal(t, respr.ResumeReason, kvpb.RESUME_BYTE_LIMIT)
				require.NoError(t, err)
				numBytes := batch.Len()
				require.Equal(t, numBytes, initialBytes)

				_, err = storage.MVCCGet(ctx, batch, testKeys[0], ts, storage.MVCCGetOptions{})
				require.Error(t, err)
			}

			{
				// Case 2: TargetBytes = 2900. In this case, we should resolve the
				// first 3 intents - testKey[0], testKeys[1], and testKeys[2] (since we
				// resolve intents until we exceed the TargetBytes limit).
				respr := &kvpb.ResolveIntentRangeResponse{}
				_, err := ResolveIntentRange(ctx, batch,
					CommandArgs{
						EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
						Args:    &rir,
						Header: kvpb.Header{
							TargetBytes: 2900,
						},
					},
					respr,
				)
				require.Equal(t, respr.NumKeys, int64(3))
				require.Greater(t, respr.NumBytes, int64(3000))
				require.Less(t, respr.NumBytes, int64(3300))
				require.Equal(t, respr.ResumeSpan.Key, testKeys[2].Next())
				require.Equal(t, respr.ResumeSpan.EndKey, testKeys[nKeys-1].Next())
				require.Equal(t, respr.ResumeReason, kvpb.RESUME_BYTE_LIMIT)
				require.NoError(t, err)
				numBytes := batch.Len()
				require.Greater(t, numBytes, initialBytes+3000)
				require.Less(t, numBytes, initialBytes+3300)

				valueRes, err := storage.MVCCGet(ctx, batch, testKeys[2], ts, storage.MVCCGetOptions{})
				require.NoError(t, err)
				require.Equal(t, values[2].RawBytes, valueRes.Value.RawBytes,
					"the value %s in get result does not match the value %s in request", values[2].RawBytes, valueRes.Value.RawBytes)
				_, err = storage.MVCCGet(ctx, batch, testKeys[3], ts, storage.MVCCGetOptions{})
				require.Error(t, err)
			}

			{
				// Case 3: TargetBytes = 1100 (on remaining intents - testKeys[3] and
				// testKeys[4]). In this case, we should resolve the remaining
				// intents - testKey[4] and testKeys[5] (since we resolve intents until
				// we exceed the TargetBytes limit).
				respr := &kvpb.ResolveIntentRangeResponse{}
				_, err := ResolveIntentRange(ctx, batch,
					CommandArgs{
						EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
						Args:    &rir,
						Header: kvpb.Header{
							TargetBytes: 1100,
						},
					},
					respr,
				)
				require.Equal(t, respr.NumKeys, int64(2))
				require.Greater(t, respr.NumBytes, int64(2000))
				require.Less(t, respr.NumBytes, int64(2200))
				require.Nil(t, respr.ResumeSpan)
				require.Equal(t, respr.ResumeReason, kvpb.RESUME_UNKNOWN)
				require.NoError(t, err)
				numBytes := batch.Len()
				require.Greater(t, numBytes, initialBytes+5000)
				require.Less(t, numBytes, initialBytes+5500)

				valueRes, err := storage.MVCCGet(ctx, batch, testKeys[nKeys-1], ts, storage.MVCCGetOptions{})
				require.NoError(t, err)
				require.Equal(t, values[nKeys-1].RawBytes, valueRes.Value.RawBytes,
					"the value %s in get result does not match the value %s in request", values[nKeys-1].RawBytes, valueRes.Value.RawBytes)
			}
		}
	})
}
