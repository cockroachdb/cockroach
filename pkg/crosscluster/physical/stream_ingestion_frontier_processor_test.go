// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestLaggingNodeErrorHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type testCase struct {
		name                        string
		replicatedTime              int64
		previousReplicatedTimeOnLag int64
		inputLagErr                 error

		expectedNewReplicatedTimeOnLag int64
		expectedErrMsg                 string
	}

	for _, tc := range []testCase{
		{
			name:                           "no more lag",
			previousReplicatedTimeOnLag:    1,
			expectedNewReplicatedTimeOnLag: 0,
		},
		{
			name:                           "new lag",
			previousReplicatedTimeOnLag:    0,
			replicatedTime:                 1,
			expectedNewReplicatedTimeOnLag: 1,
			inputLagErr:                    ErrNodeLagging,
		},
		{
			name:                           "repeated lag, no hwm advance",
			previousReplicatedTimeOnLag:    1,
			replicatedTime:                 1,
			expectedNewReplicatedTimeOnLag: 1,
			inputLagErr:                    ErrNodeLagging,
			expectedErrMsg:                 ErrNodeLagging.Error(),
		},
		{
			name:                           "repeated lag, with hwm advance",
			previousReplicatedTimeOnLag:    1,
			replicatedTime:                 2,
			expectedNewReplicatedTimeOnLag: 2,
			inputLagErr:                    ErrNodeLagging,
		},
		{
			name:           "non lag error",
			inputLagErr:    errors.New("unexpected"),
			expectedErrMsg: "unexpected",
		},
		{
			name:                           "unhandlable lag error",
			previousReplicatedTimeOnLag:    2,
			replicatedTime:                 1,
			expectedNewReplicatedTimeOnLag: 2,
			inputLagErr:                    ErrNodeLagging,
			expectedErrMsg:                 "unable to handle replanning",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sf := streamIngestionFrontier{
				persistedReplicatedTime:                  hlc.Timestamp{WallTime: tc.replicatedTime},
				replicatedTimeAtLastPositiveLagNodeCheck: hlc.Timestamp{WallTime: tc.previousReplicatedTimeOnLag},
			}
			err := sf.handleLaggingNodeError(ctx, tc.inputLagErr)
			if tc.expectedErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			}
			require.Equal(t, hlc.Timestamp{WallTime: tc.expectedNewReplicatedTimeOnLag}, sf.replicatedTimeAtLastPositiveLagNodeCheck)
		})
	}
}
