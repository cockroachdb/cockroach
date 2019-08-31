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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	engine := engine.NewInMem(roachpb.Attributes{}, 1<<20)
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

				ac := abortspan.New(desc.RangeID)

				var spans spanset.SpanSet
				batch := engine.NewBatch()
				batch = spanset.NewBatch(batch, &spans)
				defer batch.Close()

				var h roachpb.Header
				h.RangeID = desc.RangeID

				cArgs := CommandArgs{Header: h}
				cArgs.EvalCtx = &mockEvalCtx{abortSpan: ac}

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
