// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestSideTransportClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	ts3 := hlc.Timestamp{WallTime: 3}

	tests := []struct {
		name           string
		applied        ctpb.LAI
		localClosed    hlc.Timestamp
		localLAI       ctpb.LAI
		receiverClosed hlc.Timestamp
		receiverLAI    ctpb.LAI
		sufficient     hlc.Timestamp

		expectedLocalUpdate bool
		expectedClosed      hlc.Timestamp
	}{
		{
			name:                "all empty",
			expectedClosed:      hlc.Timestamp{},
			expectedLocalUpdate: false,
		},
		{
			name:           "only local",
			applied:        100,
			localClosed:    ts1,
			localLAI:       1,
			expectedClosed: ts1,
		},
		{
			name:                "only receiver",
			applied:             100,
			receiverClosed:      ts1,
			receiverLAI:         1,
			expectedClosed:      ts1,
			expectedLocalUpdate: true,
		},
		{
			name:           "local sufficient",
			applied:        100,
			localClosed:    ts1,
			localLAI:       1,
			receiverClosed: ts2,
			receiverLAI:    2,
			// The caller won't need a closed timestamp > ts1, so we expect the
			// receiver to not be consulted.
			sufficient:          ts1,
			expectedClosed:      ts1,
			expectedLocalUpdate: false,
		},
		{
			name:                "local insufficient",
			applied:             100,
			localClosed:         ts1,
			localLAI:            1,
			receiverClosed:      ts2,
			receiverLAI:         2,
			sufficient:          ts3,
			expectedClosed:      ts2,
			expectedLocalUpdate: true,
		},
		{
			name:           "replication not caught up",
			applied:        0,
			localClosed:    ts1,
			localLAI:       1,
			receiverClosed: ts2,
			receiverLAI:    2,
			sufficient:     ts3,
			// We expect no usable closed timestamp to be returned. And also we expect
			// the local state to not be updated because the LAI from the receiver has
			// not been applied by the replica.
			expectedClosed:      hlc.Timestamp{},
			expectedLocalUpdate: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := mockReceiver{
				closed: tc.receiverClosed,
				lai:    tc.receiverLAI,
			}
			var s sidetransportAccess
			s.receiver = &r
			s.mu.closedTimestamp = tc.localClosed
			s.mu.lai = tc.localLAI
			closed := s.get(ctx, roachpb.NodeID(1), tc.applied, tc.sufficient)
			require.Equal(t, tc.expectedClosed, closed)
			if tc.expectedLocalUpdate {
				require.Equal(t, tc.receiverClosed, s.mu.closedTimestamp)
				require.Equal(t, tc.receiverLAI, s.mu.lai)
			} else {
				require.Equal(t, tc.localClosed, s.mu.closedTimestamp)
				require.Equal(t, tc.localLAI, s.mu.lai)
			}
		})
	}
}

type mockReceiver struct {
	closed hlc.Timestamp
	lai    ctpb.LAI
}

var _ sidetransportReceiver = &mockReceiver{}

// GetClosedTimestamp is part of the sidetransportReceiver interface.
func (r *mockReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	return r.closed, r.lai
}

// HTML is part of the sidetransportReceiver interface.
func (r *mockReceiver) HTML() string {
	return ""
}

// Test that r.ClosedTimestampV2() mixes its sources of information correctly.
func TestReplicaClosedTimestampV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}

	for _, test := range []struct {
		name                string
		applied             ctpb.LAI
		raftClosed          hlc.Timestamp
		sidetransportClosed hlc.Timestamp
		sidetransportLAI    ctpb.LAI
		expClosed           hlc.Timestamp
	}{
		{
			name:                "raft closed ahead",
			applied:             10,
			raftClosed:          ts2,
			sidetransportClosed: ts1,
			sidetransportLAI:    5,
			expClosed:           ts2,
		},
		{
			name:                "sidetrans closed ahead",
			applied:             10,
			raftClosed:          ts1,
			sidetransportClosed: ts2,
			sidetransportLAI:    5,
			expClosed:           ts2,
		},
		{
			name:                "sidetrans ahead but replication behind",
			applied:             10,
			raftClosed:          ts1,
			sidetransportClosed: ts2,
			sidetransportLAI:    11,
			expClosed:           ts1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			receiver := &mockReceiver{
				closed: test.sidetransportClosed,
				lai:    test.sidetransportLAI,
			}
			var tc testContext
			tc.manualClock = hlc.NewManualClock(123) // required by StartWithStoreConfig
			cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
			cfg.TestingKnobs.DontCloseTimestamps = true
			cfg.ClosedTimestampReceiver = receiver
			tc.StartWithStoreConfig(t, stopper, cfg)
			tc.repl.mu.Lock()
			tc.repl.mu.state.RaftClosedTimestamp = test.raftClosed
			tc.repl.mu.state.LeaseAppliedIndex = uint64(test.applied)
			tc.repl.mu.Unlock()
			require.Equal(t, test.expClosed, tc.repl.ClosedTimestampV2(ctx))
		})
	}
}
