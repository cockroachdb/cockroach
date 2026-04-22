// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// countingPersister is a small in-memory Persister that counts
// Store invocations and snapshots the most recent state.
type countingPersister struct {
	stores int
	last   State
}

func (p *countingPersister) Load(context.Context) (State, bool, error) {
	return State{}, false, nil
}

func (p *countingPersister) Store(_ context.Context, s State) error {
	p.stores++
	p.last = s
	return nil
}

// TestRunCheckpointerSyncTest exercises the periodic checkpoint
// loop deterministically via testing/synctest: time only advances
// when every goroutine in the bubble is durably blocked, so
// time.Sleep + time.NewTicker can be driven with no real wall
// time and no flakiness from scheduler jitter.
//
// The loop should fire Persister.Store exactly once per
// checkpointInterval that elapses while ctx is alive, and stop
// promptly when ctx is cancelled.
func TestRunCheckpointerSyncTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	timeutil.SyncTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		es := nodelocal.TestingMakeNodelocalStorage(
			t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
		)
		t.Cleanup(func() { _ = es.Close() })

		manager := newCheckpointTestManager(t, es)
		// Seed some state so each Snapshot returns something
		// non-trivial; otherwise we'd be testing only that the
		// timer fires.
		require.NoError(t, manager.Rehydrate(State{
			HighWater: hlc.Timestamp{WallTime: 100 * int64(time.Second)},
			OpenTicks: map[hlc.Timestamp][]revlogpb.File{
				{WallTime: 110 * int64(time.Second)}: {{FileID: 7, FlushOrder: 0}},
			},
		}))

		p := &countingPersister{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = runCheckpointer(ctx, p, manager)
		}()

		// Step the clock past N intervals and verify Store
		// fires exactly N times. Add 1ns to push past the tick
		// boundary (the ticker fires *at* the boundary, but
		// "blocked goroutines" requires us to be strictly after).
		const intervals = 3
		for i := 1; i <= intervals; i++ {
			time.Sleep(checkpointInterval + time.Nanosecond)
			require.Equal(t, i, p.stores, "after %d intervals", i)
		}
		require.Equal(t, hlc.Timestamp{WallTime: 100 * int64(time.Second)}, p.last.HighWater)
		require.Len(t, p.last.OpenTicks, 1)

		// Cancellation drains the loop without further stores.
		cancel()
		<-done
		require.Equal(t, intervals, p.stores)
	})
}

// newCheckpointTestManager builds a TickManager suitable for the
// checkpoint-loop test: arbitrary spans and tick width, the given
// storage. The storage is only touched if a tick actually closes —
// the loop test never advances the manager's frontier, so it's
// idle.
func newCheckpointTestManager(t *testing.T, es cloud.ExternalStorage) *TickManager {
	t.Helper()
	m, err := NewTickManager(
		es,
		[]roachpb.Span{{Key: roachpb.Key("\x00"), EndKey: roachpb.KeyMax}},
		hlc.Timestamp{WallTime: 100 * int64(time.Second)},
		10*time.Second,
	)
	require.NoError(t, err)
	return m
}
