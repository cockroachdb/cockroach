// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package observedts

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestLimitTxnMaxTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txn := &roachpb.Transaction{
		ReadTimestamp: hlc.Timestamp{WallTime: 10},
		MaxTimestamp:  hlc.Timestamp{WallTime: 20},
	}
	txn.UpdateObservedTimestamp(1, hlc.ClockTimestamp{WallTime: 15})
	txnWithMaxTimestamp := func(ts hlc.Timestamp) *roachpb.Transaction {
		txnClone := txn.Clone()
		txnClone.MaxTimestamp = ts
		return txnClone
	}

	repl1 := roachpb.ReplicaDescriptor{NodeID: 1}
	repl2 := roachpb.ReplicaDescriptor{NodeID: 2}
	lease := kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{Replica: repl1},
		State: kvserverpb.LeaseState_VALID,
	}

	testCases := []struct {
		name   string
		txn    *roachpb.Transaction
		lease  kvserverpb.LeaseStatus
		expTxn *roachpb.Transaction
	}{
		{
			name:   "no txn",
			txn:    nil,
			lease:  lease,
			expTxn: nil,
		},
		{
			name: "invalid lease",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.State = kvserverpb.LeaseState_EXPIRED
				return leaseClone
			}(),
			expTxn: txn,
		},
		{
			name: "no observed timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Replica = repl2
				return leaseClone
			}(),
			expTxn: txn,
		},
		{
			name:   "valid lease",
			txn:    txn,
			lease:  lease,
			expTxn: txnWithMaxTimestamp(hlc.Timestamp{WallTime: 15}),
		},
		{
			name: "valid lease with start time above observed timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 18}
				return leaseClone
			}(),
			expTxn: txnWithMaxTimestamp(hlc.Timestamp{WallTime: 18}),
		},
		{
			name: "valid lease with start time above max timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 22}
				return leaseClone
			}(),
			expTxn: txn,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// Copy txn to test that the transaction is not mutated.
			var txnBefore *roachpb.Transaction
			if test.txn != nil {
				txnBefore = test.txn.Clone()
			}

			txnOut := LimitTxnMaxTimestamp(context.Background(), test.txn, test.lease)
			require.Equal(t, test.expTxn, txnOut)
			require.Equal(t, txnBefore, test.txn)
		})
	}
}
