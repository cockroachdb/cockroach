// Copyright 2023 The Cockroach Authors.
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
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestWaitQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	maxCatchups := int64(8)
	minQuota := int64(50)
	restartInterval := time.Second

	settings := func() *cluster.Settings {
		st := cluster.MakeClusterSettings()
		concurrentRangefeedItersLimit.Override(ctx, &st.SV, maxCatchups)
		rangefeedRestartAvailablePercent.Override(ctx, &st.SV, minQuota)
		rangefeedRestartInterval.Override(ctx, &st.SV, restartInterval)
		return st
	}

	waitResult := func(errC chan error, timeout time.Duration) (error, bool) {
		select {
		case err := <-errC:
			return err, true
		case <-time.After(timeout):
			return nil, false
		}
	}

	advanceAndWaitResult := func(ts *timeutil.ManualTime, errC chan error, timeout time.Duration) (error, bool) {
		for deadline := timeutil.Now().Add(timeout); timeutil.Now().Before(deadline); {
			ts.Advance(time.Minute)
			select {
			case err := <-errC:
				return err, true
			case <-time.After(time.Millisecond):
			}
		}
		return nil, false
	}

	t.Run("pacing interval", func(t *testing.T) {
		st := settings()
		errC := make(chan error)
		cfgChange := make(chan interface{})
		ts := timeutil.NewManualTime(timeutil.Now())
		var catchUps int64

		go func() {
			errC <- waitRestartQuota(ctx, ts, &catchUps, st, cfgChange)
		}()
		_, ok := waitResult(errC, time.Millisecond)
		require.False(t, ok, "expecting pacing to block till timeout")
		err, ok := advanceAndWaitResult(ts, errC, time.Second)
		require.True(t, ok, "expecting pacing to block till timeout")
		require.NoError(t, err, "not expecting pacing to fail")
	})
	t.Run("wait quota", func(t *testing.T) {
		st := settings()
		errC := make(chan error)
		cfgChange := make(chan interface{})
		ts := timeutil.NewManualTime(timeutil.Now())
		var catchUps = maxCatchups

		go func() {
			errC <- waitRestartQuota(ctx, ts, &catchUps, st, cfgChange)
		}()
		ts.Advance(time.Minute)
		_, ok := waitResult(errC, time.Millisecond)
		require.False(t, ok, "expecting pacing to block when no quota")
		atomic.StoreInt64(&catchUps, maxCatchups/2)
		err, ok := advanceAndWaitResult(ts, errC, time.Second)
		require.True(t, ok, "expecting pacing to unblock when quota added")
		require.NoError(t, err, "not expecting pacing to fail")
	})
	t.Run("config change", func(t *testing.T) {
		st := settings()
		errC := make(chan error)
		cfgChange := make(chan interface{})
		ts := timeutil.NewManualTime(timeutil.Now())
		var catchUps = maxCatchups/2 + 1 // Make it that default 50% is not enough
		go func() {
			errC <- waitRestartQuota(ctx, ts, &catchUps, st, cfgChange)
		}()
		ts.Advance(time.Minute)
		_, ok := waitResult(errC, time.Millisecond)
		require.False(t, ok, "expecting pacing to block when no quota")
		// Set quota to work with just 1%.
		rangefeedRestartAvailablePercent.Override(ctx, &st.SV, 1)
		cfgChange <- struct{}{}
		err, ok := advanceAndWaitResult(ts, errC, time.Second)
		require.True(t, ok, "expecting pacing to block till timeout")
		require.NoError(t, err, "not expecting pacing to fail")
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		st := settings()
		errC := make(chan error)
		cfgChange := make(chan interface{})
		ts := timeutil.NewManualTime(timeutil.Now())
		var catchUps int64
		go func() {
			errC <- waitRestartQuota(ctx, ts, &catchUps, st, cfgChange)
		}()
		cancel()
		err, ok := waitResult(errC, time.Second)
		require.True(t, ok, "expecting pacing to abort waiting on ctx cancel")
		require.Error(t, err, "not expecting pacing to fail")
	})
	t.Run("upgrade disabled", func(t *testing.T) {
		st := settings()
		errC := make(chan error)
		cfgChange := make(chan interface{}, 1)
		ts := timeutil.NewManualTime(timeutil.Now())
		var catchUps = maxCatchups
		go func() {
			errC <- waitRestartQuota(ctx, ts, &catchUps, st, cfgChange)
		}()
		_, ok := waitResult(errC, time.Millisecond)
		require.False(t, ok, "expecting pacing to block when no quota")
		// Set quota to 101 to disable upgrades.
		rangefeedRestartAvailablePercent.Override(ctx, &st.SV, 101)
		cfgChange <- struct{}{}
		err, ok := waitResult(errC, time.Second)
		require.True(t, ok, "expecting pacing to unblock when config is updated")
		require.Error(t, err, "expecting pacing to fail when update is disabled")
	})
}

func TestFindProcessors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	key := func(k string) roachpb.RKey {
		return testutils.MakeKey(keys.ScratchRangeMin, []byte(k))
	}

	cfg := TestStoreConfig(nil)
	store := createTestStoreWithoutStart(ctx, t, stopper, testStoreOpts{createSystemRanges: false}, &cfg)
	r1 := createReplica(store, roachpb.RangeID(100), key("a"), key("b"))
	require.NoError(t, store.AddReplica(r1), "failed adding replica")
	p1 := rangefeed.NewTestProcessor(1)
	r1.setRangefeedProcessor(p1)

	r2 := createReplica(store, roachpb.RangeID(103), key("c"), key("d"))
	require.NoError(t, store.AddReplica(r2), "failed adding replica")
	p2 := rangefeed.NewTestProcessor(2)
	r2.setRangefeedProcessor(p2)

	r3 := createReplica(store, roachpb.RangeID(111), key("e"), key("k"))
	require.NoError(t, store.AddReplica(r3), "failed adding replica")
	p3 := rangefeed.NewTestProcessor(0)
	r3.setRangefeedProcessor(p3)

	r4 := createReplica(store, roachpb.RangeID(115), key("w"), key("z"))
	require.NoError(t, store.AddReplica(r4), "failed adding replica")

	result := findProcessorsOfType(store, true)
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	require.Equal(t, []roachpb.RangeID{100, 103}, result)
	require.Equal(t, []roachpb.RangeID{111}, findProcessorsOfType(store, false))
}
