// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
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
	st := makeClusterSettingsUsingEngineIntentsSetting(engine)
	defer engine.Close()
	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		for _, test := range tests {
			t.Run("", func(t *testing.T) {
				ri := roachpb.ResolveIntentRequest{
					IntentTxn: txnMeta,
					Status:    test.status,
					Poison:    test.poison,
				}
				ri.Key = roachpb.Key("b")
				rir := roachpb.ResolveIntentRangeRequest{
					IntentTxn: ri.IntentTxn,
					Status:    ri.Status,
					Poison:    ri.Poison,
				}
				rir.Key = ri.Key
				rir.EndKey = roachpb.Key("c")

				as := abortspan.New(desc.RangeID)

				var latchSpans, lockSpans spanset.SpanSet

				var h roachpb.Header
				h.RangeID = desc.RangeID

				cArgs := CommandArgs{Header: h}
				cArgs.EvalCtx = (&MockEvalCtx{ClusterSettings: st, AbortSpan: as}).EvalContext()

				if !ranged {
					cArgs.Args = &ri
					declareKeysResolveIntent(&desc, &h, &ri, &latchSpans, &lockSpans, 0)
					batch := spanset.NewBatch(engine.NewBatch(), &latchSpans)
					defer batch.Close()
					if _, err := ResolveIntent(ctx, batch, cArgs, &roachpb.ResolveIntentResponse{}); err != nil {
						t.Fatal(err)
					}
				} else {
					cArgs.Args = &rir
					declareKeysResolveIntentRange(&desc, &h, &rir, &latchSpans, &lockSpans, 0)
					batch := spanset.NewBatch(engine.NewBatch(), &latchSpans)
					defer batch.Close()
					if _, err := ResolveIntentRange(ctx, batch, cArgs, &roachpb.ResolveIntentRangeResponse{}); err != nil {
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
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0, 1)
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
		st := makeClusterSettingsUsingEngineIntentsSetting(db)

		var v roachpb.Value
		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 0
		if err := storage.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 1
		if err := storage.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
			t.Fatal(err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}

		// Partially revert the 2nd store above.
		ignoredSeqNums := []enginepb.IgnoredSeqNumRange{{Start: 1, End: 1}}

		h := roachpb.Header{
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
			ri := roachpb.ResolveIntentRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			ri.Key = k

			declareKeysResolveIntent(&desc, &h, &ri, &spans, nil, 0)
			rbatch = spanset.NewBatch(db.NewBatch(), &spans)
			defer rbatch.Close()

			if _, err := ResolveIntent(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
					Args:    &ri,
				},
				&roachpb.ResolveIntentResponse{},
			); err != nil {
				t.Fatal(err)
			}
		} else {
			// Resolve an intent range.
			rir := roachpb.ResolveIntentRangeRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			rir.Key = k
			rir.EndKey = endKey

			declareKeysResolveIntentRange(&desc, &h, &rir, &spans, nil, 0)
			rbatch = spanset.NewBatch(db.NewBatch(), &spans)
			defer rbatch.Close()

			h.MaxSpanRequestKeys = 10
			if _, err := ResolveIntentRange(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: (&MockEvalCtx{ClusterSettings: st}).EvalContext(),
					Args:    &rir,
				},
				&roachpb.ResolveIntentRangeResponse{},
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
		res, i, err := storage.MVCCGet(ctx, batch, k, ts2, storage.MVCCGetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if i != nil {
			t.Errorf("%s: found intent, expected none: %+v", k, i)
		}
		if res == nil {
			t.Errorf("%s: no value found, expected one", k)
		} else {
			s, err := res.GetBytes()
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, "a", string(s), "at key %s", k)
		}
	})
}

func makeClusterSettingsUsingEngineIntentsSetting(engine storage.Engine) *cluster.Settings {
	version := clusterversion.TestingBinaryVersion
	return cluster.MakeTestingClusterSettingsWithVersions(version, version, true)
}
