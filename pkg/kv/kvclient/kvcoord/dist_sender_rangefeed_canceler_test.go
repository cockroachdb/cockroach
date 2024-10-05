// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvcoord

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStuckRangeFeedCanceler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_dur := int64(24 * time.Hour) // atomic
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doNothing := func() error { return nil }
	blockUntilCanceled := func() error {
		<-ctx.Done()
		return ctx.Err()
	}

	c := newStuckRangeFeedCanceler(cancel, func() time.Duration {
		return time.Duration(atomic.LoadInt64(&_dur))
	})
	require.Nil(t, c.t) // not running upon creation.

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		require.False(t, c.stuck())
		require.NoError(t, c.do(doNothing))
		require.NotNil(t, c.t) // first call to ping sets off timer
	}
	atomic.StoreInt64(&_dur, int64(10*time.Millisecond))
	// Nothing has reset the timer yet, so we won't be stuck here.
	// This isn't great but it is true, so documenting it.
	require.False(t, c.stuck())
	// Ping will update the timer, so it will fire very soon.
	require.True(t, errors.Is(c.do(blockUntilCanceled), context.Canceled))
	require.True(t, c.stuck())

	atomic.StoreInt64(&_dur, int64(24*time.Hour))

	// Stays marked as stuck even when we ping it again.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Nanosecond)
		require.True(t, c.stuck())
		require.True(t, errors.Is(c.do(blockUntilCanceled), errRestartStuckRange))
	}
}

// Ensure that canceller monitors only the duration of the do()
// function, and not anything happening outside.
func TestStuckRangeFeedCancelerScope(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doNothing := func() error { return nil }

	triggered := struct {
		sync.Once
		ch chan struct{}
	}{ch: make(chan struct{})}

	const duration = time.Second
	c := newStuckRangeFeedCanceler(cancel, func() time.Duration {
		return duration
	})
	c.afterTimerTrigger = func() {
		triggered.Do(func() {
			close(triggered.ch)
		})
	}

	require.Nil(t, c.t) // not running upon creation.
	require.False(t, c.stuck())
	require.Nil(t, c.do(doNothing))

	// Now, start waiting until timer triggers.
	// Even though timer triggered, the watcher is not cancelled since
	// time expired outside do().
	<-triggered.ch
	require.Nil(t, ctx.Err())
	require.False(t, c.stuck())
	require.Nil(t, c.do(doNothing))
}
