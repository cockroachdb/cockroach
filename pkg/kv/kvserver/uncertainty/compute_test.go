// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package uncertainty

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestComputeInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxOffset = 10
	now := hlc.ClockTimestamp{WallTime: 15}

	txn := &roachpb.Transaction{
		ReadTimestamp:          hlc.Timestamp{WallTime: 10},
		GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 10 + maxOffset},
	}
	txn.UpdateObservedTimestamp(1, now)

	repl1 := roachpb.ReplicaDescriptor{NodeID: 1}
	repl2 := roachpb.ReplicaDescriptor{NodeID: 2}
	lease := kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{Replica: repl1},
		State: kvserverpb.LeaseState_VALID,
	}

	testCases := []struct {
		name              string
		txn               *roachpb.Transaction
		tsFromServerClock *hlc.ClockTimestamp
		lease             kvserverpb.LeaseStatus
		exp               Interval
	}{
		{
			name:              "no txn, client ts",
			txn:               nil,
			tsFromServerClock: nil,
			lease:             lease,
			exp:               Interval{},
		},
		{
			name:              "no txn, server ts",
			txn:               nil,
			tsFromServerClock: &now,
			lease:             lease,
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 15},
			},
		},
		{
			name:              "no txn, server ts, invalid lease",
			txn:               nil,
			tsFromServerClock: &now,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.State = kvserverpb.LeaseState_EXPIRED
				return leaseClone
			}(),
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
			},
		},
		{
			name:              "no txn, server ts, lease with start time above server ts",
			txn:               nil,
			tsFromServerClock: &now,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 18}
				return leaseClone
			}(),
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 18},
			},
		},
		{
			name:              "no txn, server ts, lease with start time above server ts + max offset",
			txn:               nil,
			tsFromServerClock: &now,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 32}
				return leaseClone
			}(),
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 25},
			},
		},
		{
			name: "txn, invalid lease",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.State = kvserverpb.LeaseState_EXPIRED
				return leaseClone
			}(),
			exp: Interval{GlobalLimit: txn.GlobalUncertaintyLimit},
		},
		{
			name: "txn, no observed timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Replica = repl2
				return leaseClone
			}(),
			exp: Interval{GlobalLimit: txn.GlobalUncertaintyLimit},
		},
		{
			name:  "valid lease",
			txn:   txn,
			lease: lease,
			exp: Interval{
				GlobalLimit: txn.GlobalUncertaintyLimit,
				LocalLimit:  hlc.ClockTimestamp{WallTime: 15},
			},
		},
		{
			name: "txn, valid lease with start time above observed timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 18}
				return leaseClone
			}(),
			exp: Interval{
				GlobalLimit: txn.GlobalUncertaintyLimit,
				LocalLimit:  hlc.ClockTimestamp{WallTime: 18},
			},
		},
		{
			name: "txn, valid lease with start time above max timestamp",
			txn:  txn,
			lease: func() kvserverpb.LeaseStatus {
				leaseClone := lease
				leaseClone.Lease.Start = hlc.ClockTimestamp{WallTime: 22}
				return leaseClone
			}(),
			exp: Interval{
				GlobalLimit: txn.GlobalUncertaintyLimit,
				LocalLimit:  hlc.ClockTimestamp{WallTime: 20},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var h roachpb.Header
			h.Txn = test.txn
			h.TimestampFromServerClock = test.tsFromServerClock
			require.Equal(t, test.exp, ComputeInterval(&h, test.lease, maxOffset))
		})
	}
}
