// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package timemonitor_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/timemonitor"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type timeMonitorTester struct {
	syncutil.Mutex
	m *timemonitor.TimeMonitor
}

func newTimeMonitorTester() *timeMonitorTester {
	t := &timeMonitorTester{}
	t.m = timemonitor.New(t)
	return t
}

func (tmt *timeMonitorTester) frontier() hlc.Timestamp {
	tmt.Lock()
	defer tmt.Unlock()
	return tmt.m.Frontier()
}

func (tmt *timeMonitorTester) advanceFrontier(ts hlc.Timestamp) error {
	tmt.Lock()
	defer tmt.Unlock()
	return tmt.m.AdvanceFrontier(ts)
}

func (tmt *timeMonitorTester) waitForFrontier(ctx context.Context, ts hlc.Timestamp) chan error {
	errCh := make(chan error, 1)
	tmt.Lock()
	go func() {
		defer tmt.Unlock()
		_, err := tmt.m.WaitForFrontier(ctx, ts)
		errCh <- err
	}()
	return errCh
}

func (tmt *timeMonitorTester) errorTimestamp() hlc.Timestamp {
	tmt.Lock()
	defer tmt.Unlock()
	return tmt.m.ErrorTimestamp()
}

func (tmt *timeMonitorTester) setError(ts hlc.Timestamp, err error) error {
	tmt.Lock()
	defer tmt.Unlock()
	return tmt.m.SetError(ts, err)
}

func TestTimeMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tmt := newTimeMonitorTester()

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	// Set initial frontier to 10.
	require.NoError(t, tmt.advanceFrontier(ts(10)))
	require.Equal(t, tmt.frontier(), ts(10))

	// Trying to wait for frontier at 20 should block.
	errCh1 := tmt.waitForFrontier(ctx, ts(20))
	expectChannelEmpty(t, errCh1)

	// Advancing the frontier to 20 should unblock the waiter.
	require.NoError(t, tmt.advanceFrontier(ts(20)))
	require.Equal(t, tmt.frontier(), ts(20))
	expectChannelRead(t, errCh1, "")

	// Trying to wait for frontier at 10 should instantly return now.
	errCh2 := tmt.waitForFrontier(ctx, ts(10))
	expectChannelRead(t, errCh2, "")

	// Trying to revert the frontier to 10 should fail.
	require.EqualError(t,
		tmt.advanceFrontier(ts(10)),
		"cannot advance frontier to time (0.000000010,0) earlier than current frontier (0.000000020,0)")
	require.Equal(t, tmt.frontier(), ts(20))

	// Set error timestamp to 40.
	err1 := errors.Newf("error at time %s", ts(40))
	require.NoError(t, tmt.setError(ts(40), err1))
	require.Equal(t, tmt.frontier(), ts(20))
	require.Equal(t, tmt.errorTimestamp(), ts(40))

	// Trying to wait for frontier at 40 should instantly return the error.
	errCh3 := tmt.waitForFrontier(ctx, ts(40))
	expectChannelRead(t, errCh3, err1.Error())

	// Trying to wait for frontier at 30 should block.
	errCh4 := tmt.waitForFrontier(ctx, ts(30))
	expectChannelEmpty(t, errCh4)

	// Test that context cancellation works when blocked waiting for frontier at 30.
	ctx1, cancel1 := context.WithCancel(ctx)
	errCh5 := tmt.waitForFrontier(ctx1, ts(30))
	expectChannelEmpty(t, errCh5)
	cancel1()
	expectChannelRead(t, errCh5, context.Canceled.Error())

	// Set error timestamp to 30. This should unblock the previous waiter.
	err2 := errors.Newf("error at time %s", ts(30))
	require.NoError(t, tmt.setError(ts(30), err2))
	require.Equal(t, tmt.frontier(), ts(20))
	require.Equal(t, tmt.errorTimestamp(), ts(30))
	expectChannelRead(t, errCh4, err2.Error())

	// Trying to advance the frontier to 30 should fail.
	require.EqualError(t,
		tmt.advanceFrontier(ts(30)),
		"cannot advance frontier to time (0.000000030,0) equal to or later than error timestamp (0.000000030,0)")
	require.Equal(t, tmt.frontier(), ts(20))
	require.Equal(t, tmt.errorTimestamp(), ts(30))

	// Trying to recede the error timestamp to 10 should fail.
	err3 := errors.Newf("error at time %s", ts(10))
	require.EqualError(t,
		tmt.setError(ts(10), err3),
		"cannot set error timestamp to time (0.000000010,0) earlier or equal to frontier (0.000000020,0)")
	require.Equal(t, tmt.frontier(), ts(20))
	require.Equal(t, tmt.errorTimestamp(), ts(30))
}

func expectChannelEmpty(t *testing.T, errCh chan error) {
	// The problem of determining if a channel will never receive anything is
	// undecidable, so we instead hope that waiting for 1s is sufficient.
	select {
	case err := <-errCh:
		t.Fatalf("expected empty channel, got %v", err)
	case <-time.After(time.Second):
	}
}

func expectChannelRead(t *testing.T, errCh chan error, expected string) {
	err := <-errCh
	if expected == "" {
		require.NoError(t, err)
	} else {
		require.EqualError(t, err, expected)
	}
}
