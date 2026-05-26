// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTimestampBarrier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := timestampBarrier{}

	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	// Set initial frontier to 10.
	require.NoError(t, b.advanceFrontier(ts(10)))
	require.Equal(t, b.frontier, ts(10))

	// Trying to wait at 20 should block.
	errCh1, needToWait := b.wait(ts(20))
	expectChannelEmpty(t, errCh1)
	require.True(t, needToWait)

	// Trying to wait at 30 should block.
	errCh2, needToWait := b.wait(ts(30))
	expectChannelEmpty(t, errCh2)
	require.True(t, needToWait)

	// Advancing the frontier to 20 should unblock the waiter at 20,
	// but not the waiter at 30.
	require.NoError(t, b.advanceFrontier(ts(20)))
	require.Equal(t, b.frontier, ts(20))
	expectChannelRead(t, errCh1, "")
	expectChannelEmpty(t, errCh2)

	// Trying to wait at 10 should instantly return now.
	errCh3, needToWait := b.wait(ts(10))
	expectChannelRead(t, errCh3, "")
	require.False(t, needToWait)

	// Trying to revert the frontier to 10 should fail.
	require.EqualError(t,
		b.advanceFrontier(ts(10)),
		"cannot advance frontier to 0.000000010,0, which is earlier than the current frontier 0.000000020,0")
	require.Equal(t, b.frontier, ts(20))

	// Set error timestamp to 40.
	err1 := errors.Newf("error at time %s", ts(40))
	require.NoError(t, b.setError(ts(40), err1))
	require.Equal(t, b.frontier, ts(20))
	require.Equal(t, b.errTS, ts(40))

	// Trying to wait at 40 should instantly return the error.
	errCh4, needToWait := b.wait(ts(40))
	expectChannelRead(t, errCh4, err1.Error())
	require.False(t, needToWait)

	// Trying to wait at 30 should block. The previous waiter at 30 should also
	// still be blocked.
	errCh5, needToWait := b.wait(ts(30))
	expectChannelEmpty(t, errCh5)
	expectChannelEmpty(t, errCh2)
	require.True(t, needToWait)

	// Trying to set a nil-error should fail.
	require.EqualError(t,
		b.setError(ts(30), nil),
		"cannot set nil error for timestamp 0.000000030,0")
	require.Equal(t, b.frontier, ts(20))
	require.Equal(t, b.errTS, ts(40))

	// Set error timestamp to 30. This should unblock both of the waiters at 30.
	err2 := errors.Newf("error at time %s", ts(30))
	require.NoError(t, b.setError(ts(30), err2))
	require.Equal(t, b.frontier, ts(20))
	require.Equal(t, b.errTS, ts(30))
	expectChannelRead(t, errCh2, err2.Error())
	expectChannelRead(t, errCh5, err2.Error())

	// Trying to advance the frontier to 30 should fail.
	require.EqualError(t,
		b.advanceFrontier(ts(30)),
		"cannot advance frontier to 0.000000030,0, which is equal to or later than the current error timestamp 0.000000030,0")
	require.Equal(t, b.frontier, ts(20))
	require.Equal(t, b.errTS, ts(30))

	// Trying to recede the error timestamp to 10 should fail.
	err3 := errors.Newf("error at time %s", ts(10))
	require.EqualError(t,
		b.setError(ts(10), err3),
		"cannot set error timestamp to 0.000000010,0, which is earlier or equal to the current frontier 0.000000020,0")
	require.Equal(t, b.frontier, ts(20))
	require.Equal(t, b.errTS, ts(30))
}

func expectChannelEmpty(t *testing.T, errCh <-chan error) {
	select {
	case err := <-errCh:
		t.Fatalf("expected empty channel, got %v", err)
	default:
	}
}

func expectChannelRead(t *testing.T, errCh <-chan error, expected string) {
	select {
	case err := <-errCh:
		if expected == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, expected)
		}
	default:
		t.Fatalf("expected non-empty channel")
	}
}
