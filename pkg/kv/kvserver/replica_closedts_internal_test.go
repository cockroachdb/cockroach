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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
			// We expect no closed timestamp to be returned to the caller, but the
			// local state still to be updated.
			expectedClosed:      hlc.Timestamp{},
			expectedLocalUpdate: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := mockReceiver{
				closed: tc.receiverClosed,
				lai:    tc.receiverLAI,
			}
			var s sideTransportClosedTimestamp
			s.receiver = &r
			s.mu.closed = tc.localClosed
			s.mu.lai = tc.localLAI
			closed := s.get(ctx, roachpb.NodeID(1), tc.applied, tc.sufficient)
			require.Equal(t, tc.expectedClosed, closed)
			if tc.expectedLocalUpdate {
				require.Equal(t, tc.receiverClosed, s.mu.closed)
				require.Equal(t, tc.receiverLAI, s.mu.lai)
			} else {
				require.Equal(t, tc.localClosed, s.mu.closed)
				require.Equal(t, tc.localLAI, s.mu.lai)
			}
		})
	}
}

type mockReceiver struct {
	closed hlc.Timestamp
	lai    ctpb.LAI
}

var _ receiver = &mockReceiver{}

func (r *mockReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	return r.closed, r.lai
}
