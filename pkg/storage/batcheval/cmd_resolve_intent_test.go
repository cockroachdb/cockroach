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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type mockEvalCtx struct {
	clusterSettings  *cluster.Settings
	desc             *roachpb.RangeDescriptor
	storeID          roachpb.StoreID
	clock            *hlc.Clock
	stats            enginepb.MVCCStats
	qps              float64
	abortSpan        *abortspan.AbortSpan
	gcThreshold      hlc.Timestamp
	term, firstIndex uint64
	canCreateTxnFn   func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason)
	lease            roachpb.Lease
}

func (m *mockEvalCtx) String() string {
	return "mock"
}
func (m *mockEvalCtx) ClusterSettings() *cluster.Settings {
	return m.clusterSettings
}
func (m *mockEvalCtx) EvalKnobs() storagebase.BatchEvalTestingKnobs {
	panic("unimplemented")
}
func (m *mockEvalCtx) Engine() engine.Engine {
	panic("unimplemented")
}
func (m *mockEvalCtx) Clock() *hlc.Clock {
	return m.clock
}
func (m *mockEvalCtx) DB() *client.DB {
	panic("unimplemented")
}
func (m *mockEvalCtx) GetLimiters() *Limiters {
	panic("unimplemented")
}
func (m *mockEvalCtx) AbortSpan() *abortspan.AbortSpan {
	return m.abortSpan
}
func (m *mockEvalCtx) GetTxnWaitQueue() *txnwait.Queue {
	panic("unimplemented")
}
func (m *mockEvalCtx) NodeID() roachpb.NodeID {
	panic("unimplemented")
}
func (m *mockEvalCtx) GetNodeLocality() roachpb.Locality {
	panic("unimplemented")
}
func (m *mockEvalCtx) StoreID() roachpb.StoreID {
	return m.storeID
}
func (m *mockEvalCtx) GetRangeID() roachpb.RangeID {
	return m.desc.RangeID
}
func (m *mockEvalCtx) IsFirstRange() bool {
	panic("unimplemented")
}
func (m *mockEvalCtx) GetFirstIndex() (uint64, error) {
	return m.firstIndex, nil
}
func (m *mockEvalCtx) GetTerm(uint64) (uint64, error) {
	return m.term, nil
}
func (m *mockEvalCtx) GetLeaseAppliedIndex() uint64 {
	panic("unimplemented")
}
func (m *mockEvalCtx) Desc() *roachpb.RangeDescriptor {
	return m.desc
}
func (m *mockEvalCtx) ContainsKey(key roachpb.Key) bool {
	panic("unimplemented")
}
func (m *mockEvalCtx) GetMVCCStats() enginepb.MVCCStats {
	return m.stats
}
func (m *mockEvalCtx) GetSplitQPS() float64 {
	return m.qps
}
func (m *mockEvalCtx) CanCreateTxnRecord(
	uuid.UUID, []byte, hlc.Timestamp,
) (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
	return m.canCreateTxnFn()
}
func (m *mockEvalCtx) GetGCThreshold() hlc.Timestamp {
	return m.gcThreshold
}
func (m *mockEvalCtx) GetLastReplicaGCTimestamp(context.Context) (hlc.Timestamp, error) {
	panic("unimplemented")
}
func (m *mockEvalCtx) GetLease() (roachpb.Lease, roachpb.Lease) {
	return m.lease, roachpb.Lease{}
}

func (m *mockEvalCtx) GetExternalStorage(
	ctx context.Context, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}

func (m *mockEvalCtx) GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (cloud.ExternalStorage, error) {
	panic("unimplemented")
}

func TestDeclareKeysResolveIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	engine := engine.NewDefaultInMem()
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

				var spans spanset.SpanSet
				batch := engine.NewBatch()
				batch = spanset.NewBatch(batch, &spans)
				defer batch.Close()

				var h roachpb.Header
				h.RangeID = desc.RangeID

				cArgs := CommandArgs{Header: h}
				cArgs.EvalCtx = &mockEvalCtx{abortSpan: as}

				if !ranged {
					cArgs.Args = &ri
					declareKeysResolveIntent(&desc, h, &ri, &spans)
					if _, err := ResolveIntent(ctx, batch, cArgs, &roachpb.ResolveIntentResponse{}); err != nil {
						t.Fatal(err)
					}
				} else {
					cArgs.Args = &rir
					declareKeysResolveIntentRange(&desc, h, &rir, &spans)
					if _, err := ResolveIntentRange(ctx, batch, cArgs, &roachpb.ResolveIntentRangeResponse{}); err != nil {
						t.Fatal(err)
					}
				}

				if s := spans.String(); strings.Contains(s, abortSpanKey) != test.expDeclares {
					t.Errorf("expected AbortSpan declared: %t, but got spans\n%s", test.expDeclares, s)
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

	ctx := context.Background()
	k := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	endKey := roachpb.Key("z")
	txn := roachpb.MakeTransaction("test", k, 0, ts, 0)
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(k),
		EndKey:   roachpb.RKey(endKey),
	}

	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		db := engine.NewDefaultInMem()
		defer db.Close()
		batch := db.NewBatch()
		defer batch.Close()

		var v roachpb.Value
		// Write a first value at key.
		v.SetString("a")
		txn.Sequence = 0
		if err := engine.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
			t.Fatal(err)
		}
		// Write another value.
		v.SetString("b")
		txn.Sequence = 1
		if err := engine.MVCCPut(ctx, batch, nil, k, ts, v, &txn); err != nil {
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

		var spans spanset.SpanSet
		rbatch := db.NewBatch()
		rbatch = spanset.NewBatch(rbatch, &spans)
		defer rbatch.Close()

		if !ranged {
			// Resolve a point intent.
			ri := roachpb.ResolveIntentRequest{
				IntentTxn:      txn.TxnMeta,
				Status:         roachpb.COMMITTED,
				IgnoredSeqNums: ignoredSeqNums,
			}
			ri.Key = k

			declareKeysResolveIntent(&desc, h, &ri, &spans)

			if _, err := ResolveIntent(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: &mockEvalCtx{},
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

			declareKeysResolveIntentRange(&desc, h, &rir, &spans)

			if _, err := ResolveIntentRange(ctx, rbatch,
				CommandArgs{
					Header:  h,
					EvalCtx: &mockEvalCtx{},
					Args:    &rir,
					MaxKeys: 10,
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
		res, i, err := engine.MVCCGet(ctx, batch, k, ts2, engine.MVCCGetOptions{})
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
