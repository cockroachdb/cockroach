// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvaccessorccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestCommitTSIntervals ensures the KVAccessor updates are
// only committed within the given time interval.
func TestCommitTSIntervals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tick := (500 * time.Millisecond).Nanoseconds()
	manual := hlc.NewHybridManualClock()

	var i interceptor
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manual,
			},
			SpanConfig: &spanconfig.TestingKnobs{
				KVAccessorPostCommitDeadlineSetInterceptor: func(txn *kv.Txn) {
					i.invoke(txn)
				},
			},
		},
	})
	defer ts.Stopper().Stop(ctx)

	tt, _ := serverutils.StartTenant(t, ts, base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
	})

	manual.Pause() // control the clock manually below

	testCases := []struct {
		name string
		test func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock)
	}{
		{
			name: "start < min < end < max",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the minimum commit timestamp after now -- it
				// should block until the current timestamp advances.
				minCommitTS := clock.Now().Add(tick, 0)

				updatedC := make(chan error)
				updateCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				go func() {
					updatedC <- accessor.UpdateSpanConfigRecords(
						updateCtx, nil /* toDelete */, nil /* toUpsert */, minCommitTS, hlc.MaxTimestamp,
					)
				}()
				select {
				case err := <-updatedC:
					t.Fatalf("update unexpectedly completed with err = %v", err)
				case <-time.After(100 * time.Millisecond):
				}

				manual.Resume()
				select {
				case err := <-updatedC:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Fatal("update still blocked")
				}
				manual.Pause()
			},
		},
		{
			name: "min < start < end < max",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the min commit timestamp before current time --
				// it should succeed.
				minCommitTS := clock.Now().Add(-tick, 0)
				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, minCommitTS, hlc.MaxTimestamp,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "min < max < start < end",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the max commit timestamp before the current time
				// -- it should fail.
				maxCommitTS := clock.Now().Add(-tick, 0)
				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, hlc.MinTimestamp, maxCommitTS,
				)
				require.Error(t, err)
				require.True(t, spanconfig.IsCommitTimestampOutOfBoundsError(err))
			},
		},
		{
			name: "min < start < max < end",
			test: func(t *testing.T, accessor spanconfig.KVAccessor, clock *hlc.Clock) {
				// Issue a request with the max commit timestamp lower than the time it
				// would take for the update to finish.
				maxCommitTS := clock.Now().Add(tick, 0)

				// Set the fn to run down the clock past the max commit timestamp, and
				// to also bump the txn's timestamp past it.
				i.set(func(txn *kv.Txn) {
					manual.Resume()
					require.NoError(t, clock.SleepUntil(ctx, maxCommitTS))
					manual.Pause()
					require.NoError(t, txn.SetFixedTimestamp(ctx, clock.Now()))
				})
				defer i.set(nil)

				err := accessor.UpdateSpanConfigRecords(
					ctx, nil /* toDelete */, nil /* toUpsert */, hlc.MinTimestamp, maxCommitTS,
				)
				require.Error(t, err)
				require.True(t, spanconfig.IsCommitTimestampOutOfBoundsError(err))
			},
		},
	}

	testutils.RunTrueAndFalse(t, "tenant", func(t *testing.T, isSecondaryTenant bool) {
		accessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
		if isSecondaryTenant {
			accessor = tt.SpanConfigKVAccessor().(spanconfig.KVAccessor)
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tc.test(t, accessor, ts.Clock())
			})
		}
	})
}

type interceptor struct {
	syncutil.Mutex
	fn func(*kv.Txn)
}

func (i *interceptor) set(fn func(*kv.Txn)) {
	i.Lock()
	defer i.Unlock()
	i.fn = fn
}

func (i *interceptor) invoke(txn *kv.Txn) {
	i.Lock()
	defer i.Unlock()

	if i.fn != nil {
		i.fn(txn)
	}
}
