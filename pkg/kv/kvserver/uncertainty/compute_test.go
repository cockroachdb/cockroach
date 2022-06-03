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

	testCases := []struct {
		name              string
		txn               *roachpb.Transaction
		tsFromServerClock *hlc.ClockTimestamp
		leaseState        kvserverpb.LeaseState
		nodeId            roachpb.NodeID
		exp               Interval
		minUncertainty    hlc.ClockTimestamp
	}{
		{
			name:              "no txn, client ts",
			txn:               nil,
			tsFromServerClock: nil,
			leaseState:        kvserverpb.LeaseState_VALID,
			nodeId:            1,
			exp:               Interval{},
		},
		{
			name:              "no txn, server ts",
			txn:               nil,
			tsFromServerClock: &now,
			leaseState:        kvserverpb.LeaseState_VALID,
			nodeId:            1,
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 15},
			},
		},
		{
			name:              "no txn, server ts, invalid lease",
			txn:               nil,
			tsFromServerClock: &now,
			leaseState:        kvserverpb.LeaseState_EXPIRED,
			nodeId:            1,
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
			},
		},
		{
			name:              "no txn, server ts, lease with start time above server ts",
			txn:               nil,
			tsFromServerClock: &now,
			leaseState:        kvserverpb.LeaseState_VALID,
			nodeId:            1,
			minUncertainty:    hlc.ClockTimestamp{WallTime: 18},
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 18},
			},
		},
		{
			name:              "no txn, server ts, lease with start time above server ts + max offset",
			txn:               nil,
			tsFromServerClock: &now,
			leaseState:        kvserverpb.LeaseState_VALID,
			minUncertainty:    hlc.ClockTimestamp{WallTime: 32},
			nodeId:            1,
			exp: Interval{
				GlobalLimit: hlc.Timestamp{WallTime: 25},
				LocalLimit:  hlc.ClockTimestamp{WallTime: 25},
			},
		},
		{
			name:       "txn, invalid lease",
			txn:        txn,
			leaseState: kvserverpb.LeaseState_EXPIRED,
			nodeId:     1,
			exp:        Interval{GlobalLimit: txn.GlobalUncertaintyLimit},
		},
		{
			name:       "txn, no observed timestamp",
			txn:        txn,
			leaseState: kvserverpb.LeaseState_VALID,
			nodeId:     2,

			exp: Interval{GlobalLimit: txn.GlobalUncertaintyLimit},
		},
		{
			name:       "valid lease",
			txn:        txn,
			leaseState: kvserverpb.LeaseState_VALID,
			nodeId:     1,
			exp: Interval{
				GlobalLimit: txn.GlobalUncertaintyLimit,
				LocalLimit:  hlc.ClockTimestamp{WallTime: 15},
			},
		},
		{
			name:           "txn, valid lease with start time above observed timestamp",
			txn:            txn,
			leaseState:     kvserverpb.LeaseState_VALID,
			nodeId:         1,
			minUncertainty: hlc.ClockTimestamp{WallTime: 18},
			exp: Interval{
				GlobalLimit: txn.GlobalUncertaintyLimit,
				LocalLimit:  hlc.ClockTimestamp{WallTime: 18},
			},
		},
		{
			name:           "txn, valid lease with start time above max timestamp",
			txn:            txn,
			leaseState:     kvserverpb.LeaseState_VALID,
			nodeId:         1,
			minUncertainty: hlc.ClockTimestamp{WallTime: 22},
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
			require.Equal(t, test.exp, ComputeInterval(&h, test.leaseState, test.nodeId, maxOffset, test.minUncertainty))
		})
	}
}
