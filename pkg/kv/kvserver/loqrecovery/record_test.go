// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestPublishRangeLogEvents verifies that inserting recovery events into
// RangeLog handles sql execution errors and unexpected results by propagating
// errors up. This is important as caller relies on errors to preserve events if
// they were not reflected in RangeLog.
// It also performs basic sanity check that inserted records have correct range
// id and reason for update and a timestamp.
func TestPublishRangeLogEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	for _, td := range []struct {
		name string

		// Recovery event and function under test arguments.
		rangeID roachpb.RangeID
		time    int64

		// Callback results returned to function under test.
		returnedRowCount int
		queryExecError   error

		// Expectations in callback and call results.
		expectSuccess bool
	}{
		{
			name:             "success",
			rangeID:          7,
			time:             1021,
			returnedRowCount: 1,
			expectSuccess:    true,
		},
		{
			name:             "sql error",
			rangeID:          7,
			time:             1021,
			returnedRowCount: 1,
			queryExecError:   errors.New("stray sql error occurred"),
		},
		{
			name:             "wrong row count",
			rangeID:          7,
			time:             1021,
			returnedRowCount: 0,
			expectSuccess:    false,
		},
	} {
		t.Run(td.name, func(t *testing.T) {
			var actualArgs []interface{}
			execFn := func(ctx context.Context, stmt string, args ...interface{}) (int, error) {
				actualArgs = args
				return td.returnedRowCount, td.queryExecError
			}

			event := loqrecoverypb.ReplicaRecoveryRecord{
				Timestamp: td.time,
				RangeID:   td.rangeID,
				StartKey:  loqrecoverypb.RecoveryKey(roachpb.RKeyMin),
				EndKey:    loqrecoverypb.RecoveryKey(roachpb.RKeyMax),
			}

			err := UpdateRangeLogWithRecovery(ctx, execFn, event)
			if td.expectSuccess {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 6, len(actualArgs), "not enough query args were provided")
			require.Contains(t, actualArgs[5], "Performed unsafe range loss of quorum recovery")
			require.Equal(t, td.rangeID, actualArgs[1], "RangeID query arg doesn't match event")
			require.Equal(t, timeutil.Unix(0, td.time), actualArgs[0],
				"timestamp query arg doesn't match event")
			require.Equal(t, kvserverpb.RangeLogEventType_unsafe_quorum_recovery.String(), actualArgs[3],
				"incorrect RangeLog event type")
		})
	}
}
