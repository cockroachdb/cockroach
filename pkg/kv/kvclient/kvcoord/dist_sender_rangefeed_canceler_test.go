// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvcoord

import (
	"context"
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
	require.Nil(t, c.t) // not running upon creation

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
	require.True(t, errors.Is(c.do(blockUntilCanceled), errRestartStuckRange))
	require.True(t, c.stuck())

	atomic.StoreInt64(&_dur, int64(24*time.Hour))

	// Stays marked as stuck even when we ping it again.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Nanosecond)
		require.True(t, c.stuck())
		require.True(t, errors.Is(c.do(blockUntilCanceled), errRestartStuckRange))
	}
}
