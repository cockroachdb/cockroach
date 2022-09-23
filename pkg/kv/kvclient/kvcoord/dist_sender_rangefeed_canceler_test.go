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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type cancelRec int32

func (c *cancelRec) cancel() {
	atomic.StoreInt32((*int32)(c), 1)
}

func (c *cancelRec) canceled() bool {
	return atomic.LoadInt32((*int32)(c)) != 0
}

func TestStuckRangeFeedCanceler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_dur := int64(24 * time.Hour) // atomic
	var cr cancelRec
	c := newStuckRangeFeedCanceler(cr.cancel, func() time.Duration {
		return time.Duration(atomic.LoadInt64(&_dur))
	})
	require.Nil(t, c.t) // not running upon creation
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		require.False(t, c.stuck())
		c.ping()
		require.NotNil(t, c.t) // first call to ping sets off timer
	}
	atomic.StoreInt64(&_dur, int64(time.Nanosecond))
	// Nothing has reset the timer yet, so we won't be stuck here.
	// This isn't great but it is true, so documenting it.
	require.False(t, c.stuck())
	// Ping will update the timer, so it will fire very soon.
	c.ping()
	require.Eventually(t, cr.canceled, time.Second /* max */, 5*time.Nanosecond /* tick */)
	require.True(t, c.stuck())

	atomic.StoreInt64(&_dur, int64(24*time.Hour))

	// Stays marked as stuck even when we ping it again.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Nanosecond)
		require.True(t, c.stuck())
		c.ping()
	}
}
