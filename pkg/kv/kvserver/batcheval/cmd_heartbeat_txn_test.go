// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatTxnWriteTimestampBumpedAboveClosedTS verifies that when creating
// a new transaction record, HeartbeatTxn bumps the WriteTimestamp above the
// closed timestamp. This ensures the TxnFeed resolved timestamp never needs to
// regress when new transaction records appear.
func TestHeartbeatTxnWriteTimestampBumpedAboveClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()

	key := roachpb.Key("a")

	tests := []struct {
		name            string
		closedTS        hlc.Timestamp
		expectedBumped  bool
		expectedWriteTS hlc.Timestamp
	}{
		{
			name:            "closed TS above write timestamp",
			closedTS:        now.Add(10, 0),
			expectedBumped:  true,
			expectedWriteTS: now.Add(10, 1),
		},
		{
			name:            "closed TS equal to write timestamp",
			closedTS:        now,
			expectedBumped:  true,
			expectedWriteTS: now.Next(),
		},
		{
			name:            "closed TS below write timestamp",
			closedTS:        now.Add(-10, 0),
			expectedBumped:  false,
			expectedWriteTS: now,
		},
		{
			name:            "empty closed TS",
			closedTS:        hlc.Timestamp{},
			expectedBumped:  false,
			expectedWriteTS: now,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()

			st := cluster.MakeTestingClusterSettings()

			headerTxn := roachpb.MakeTransaction(
				"test", key, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
			)

			evalCtx := (&MockEvalCtx{
				Clock:           clock,
				ClusterSettings: st,
				ClosedTimestamp: tc.closedTS,
			}).EvalContext()

			resp := kvpb.HeartbeatTxnResponse{}
			_, err := HeartbeatTxn(ctx, engine, CommandArgs{
				EvalCtx: evalCtx,
				Header: kvpb.Header{
					Timestamp: now,
					Txn:       &headerTxn,
				},
				Args: &kvpb.HeartbeatTxnRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
					Now:           now,
				},
			}, &resp)
			require.NoError(t, err)
			require.NotNil(t, resp.Txn)
			require.Equal(t, tc.expectedWriteTS, resp.Txn.WriteTimestamp,
				"unexpected WriteTimestamp on heartbeat response")
		})
	}
}
