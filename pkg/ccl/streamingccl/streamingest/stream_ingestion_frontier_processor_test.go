// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func (h *heartbeatSender) getMockClient() *mockStreamClient {
	return h.client.(*mockStreamClient)
}

func TestHeartbeatSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	mt := timeutil.NewManualTime(timeutil.Now())
	hbs := &heartbeatSender{
		lastSent: timeutil.Now().Add(-2 * time.Second),
		client:   &mockStreamClient{},
	}

	done, _, err := hbs.maybeHeartbeat(ctx, mt, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, time.Second)
	require.NoError(t, err)
	require.True(t, done)

	done, _, err = hbs.maybeHeartbeat(ctx, mt, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, time.Second)
	require.NoError(t, err)
	require.False(t, done)

	mt.Advance(2 * time.Second)

	e := errors.New("heartbeat test error")
	hbs.getMockClient().heartbeatErr = e
	done, _, err = hbs.maybeHeartbeat(ctx, mt, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, time.Second)
	require.ErrorIs(t, err, e)
	require.True(t, done)
}

func TestHeartbeatLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	heartbeatCh := make(chan struct{})

	type testCase struct {
		// err (currently all errors) should be retryable and not exit the heartbeat loop.
		err           error
		status        streampb.StreamReplicationStatus_StreamStatus
		expectedRetry bool
		expectedErr   string
	}

	for _, tc := range []testCase{
		{
			err:           errors.New("heartbeat test error"),
			status:        streampb.StreamReplicationStatus_STREAM_INACTIVE,
			expectedRetry: true,
			expectedErr:   "",
		},
		{
			err:           nil,
			status:        streampb.StreamReplicationStatus_STREAM_ACTIVE,
			expectedRetry: true,
			expectedErr:   "",
		},
		{
			err:           nil,
			status:        streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY,
			expectedRetry: true,
			expectedErr:   "",
		},
		{
			err:           nil,
			status:        streampb.StreamReplicationStatus_STREAM_INACTIVE,
			expectedRetry: false,
			expectedErr:   "replication stream 7 is not running, status is STREAM_INACTIVE",
		},
		{
			err:           nil,
			status:        streampb.StreamReplicationStatus_STREAM_PAUSED,
			expectedRetry: false,
			expectedErr:   "replication stream 7 is not running, status is STREAM_PAUSED",
		},
	} {
		if tc.expectedRetry {
			// Test cases should not have an error set if a retry is expected.
			require.Empty(t, tc.expectedErr)
		} else {
			// Test cases must have an error set because a retry is not expected.
			require.NotEmpty(t, tc.expectedErr)
		}
		retryCount := 0
		onHeartbeat := func() (streampb.StreamReplicationStatus, error) {
			heartbeatCh <- struct{}{}
			defer func() { retryCount++ }()
			if tc.expectedRetry {
				// We have an error that will be retried forever, which is ok, but we
				// need to make sure the test finishes, so we're retuning an INACTIVE
				// status on the second execution.
				if retryCount == 0 {
					return streampb.StreamReplicationStatus{StreamStatus: tc.status}, tc.err
				}
				return streampb.StreamReplicationStatus{StreamStatus: streampb.StreamReplicationStatus_STREAM_INACTIVE}, nil
			} else {
				// The error should not be retried, verified Heartbeat was called only once.
				require.Equal(t, 0, retryCount)
				return streampb.StreamReplicationStatus{StreamStatus: tc.status}, tc.err
			}
		}
		mt := timeutil.NewManualTime(timeutil.Now())
		hbs := &heartbeatSender{
			lastSent:    timeutil.Now().Add(-2 * time.Second),
			client:      &mockStreamClient{},
			streamID:    7,
			sv:          &settings.Values{},
			stoppedChan: make(chan struct{}),
		}

		hbs.getMockClient().onHeartbeat = onHeartbeat
		hbs.startHeartbeatLoop(ctx, mt)
		// Heartbeat should be called at least once.
		<-heartbeatCh
		if tc.expectedRetry {
			// We're expecting a retry, therefore Heartbeat should be called again.
			<-heartbeatCh
			// Sanity check - make sure we see the error we use in order to end the infinite retries.
			require.ErrorContains(t, hbs.wait(), "replication stream 7 is not running, status is STREAM_INACTIVE")
		} else {
			// No retry, we should get the expected error.
			require.ErrorContains(t, hbs.wait(), tc.expectedErr)
		}
	}
}
