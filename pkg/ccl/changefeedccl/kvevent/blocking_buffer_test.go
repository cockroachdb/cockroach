// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeRangeFeedEvent(rnd *rand.Rand, valSize int, prevValSize int) *kvpb.RangeFeedEvent {
	const tableID = 42

	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		randgen.RandDatumSimple(rnd, types.String),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	e := kvpb.RangeFeedEvent{
		Val: &kvpb.RangeFeedValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  randutil.RandBytes(rnd, valSize),
				Timestamp: hlc.Timestamp{WallTime: 1},
			},
		},
	}

	if prevValSize > 0 {
		e.Val.PrevValue = roachpb.Value{
			RawBytes:  randutil.RandBytes(rnd, prevValSize),
			Timestamp: hlc.Timestamp{WallTime: 1},
		}
	}
	return &e
}

func getBoundAccountWithBudget(budget int64) (account mon.BoundAccount, cleanup func()) {
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-mm"),
		Limit:     budget,
		Increment: 128, /* small allocation increment */
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(budget))
	return mm.MakeBoundAccount(), func() { mm.Stop(context.Background()) }
}

func TestBlockingBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metrics := kvevent.MakeMetrics(time.Minute).AggregatorBufferMetricsWithCompat
	ba, release := getBoundAccountWithBudget(4096)
	defer release()

	// Arrange for mem buffer to notify us when it waits for resources.
	waitCh := make(chan struct{}, 1)
	notifyWait := func(ctx context.Context, poolName string, r quotapool.Request) {
		select {
		case waitCh <- struct{}{}:
		default:
		}
	}
	st := cluster.MakeTestingClusterSettings()
	buf := kvevent.TestingNewMemBuffer(ba, &st.SV, &metrics, notifyWait)

	producerCtx, stopProducers := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)
	defer func() {
		_ = wg.Wait() // Ignore error -- this group returns context cancellation.
	}()

	// Start adding KVs to the buffer until we block.
	var numResolvedEvents, numKVEvents int
	wg.GoCtx(func(ctx context.Context) error {
		rnd, _ := randutil.NewTestRand()
		for {
			if rnd.Int()%20 == 0 {
				prefix := keys.SystemSQLCodec.TablePrefix(42)
				sp := roachpb.Span{Key: prefix, EndKey: prefix.Next()}
				if err := buf.Add(ctx, kvevent.NewBackfillResolvedEvent(sp, hlc.Timestamp{}, jobspb.ResolvedSpan_BACKFILL)); err != nil {
					return err
				}
				numResolvedEvents++
			} else {
				if err := buf.Add(ctx, kvevent.MakeKVEvent(makeRangeFeedEvent(rnd, 256, 0))); err != nil {
					return err
				}
				numKVEvents++
			}
		}
	})

	require.NoError(t, timeutil.RunWithTimeout(
		context.Background(), "wait", 10*time.Second, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-waitCh:
				return nil
			}
		}))

	// Keep consuming events until we get pushback metrics updated.
	var numPopped, numFlush int
	for metrics.BufferPushbackNanos.Count() == 0 {
		e, err := buf.Get(context.Background())
		require.NoError(t, err)
		a := e.DetachAlloc()
		a.Release(context.Background())
		numPopped++
		if e.Type() == kvevent.TypeFlush {
			numFlush++
		}
	}

	// Allocated memory gauge should be non-zero once we buffer some events.
	testutils.SucceedsWithin(t, func() error {
		if metrics.AllocatedMem.Value() > 0 {
			return nil
		}
		return errors.New("waiting for allocated mem > 0")
	}, 5*time.Second)

	stopProducers()
	require.ErrorIs(t, wg.Wait(), context.Canceled)

	require.EqualValues(t, numKVEvents+numResolvedEvents, metrics.BufferEntriesIn.Count())
	require.EqualValues(t, numPopped, metrics.BufferEntriesOut.Count())
	require.Greater(t, metrics.BufferEntriesMemReleased.Count(), int64(0))

	// Flush events are special in that they are ephemeral event that doesn't get
	// counted when releasing (it's 0 entries and 0 byte event).
	require.EqualValues(t, numPopped-numFlush, metrics.BufferEntriesReleased.Count())

	require.EqualValues(t, numKVEvents, metrics.BufferEntriesByType[kvevent.TypeKV].Count())
	require.EqualValues(t, numResolvedEvents, metrics.BufferEntriesByType[kvevent.TypeResolved].Count())

	// We might have seen numFlush events, but they are synthetic, and only explicitly enqueued
	// flush events are counted.
	require.EqualValues(t, 0, metrics.BufferEntriesByType[kvevent.TypeFlush].Count())

	// After buffer closed, resources are released, and metrics adjusted to reflect.
	require.NoError(t, buf.CloseWithReason(context.Background(), context.Canceled))

	require.EqualValues(t, 0, metrics.AllocatedMem.Value())
}

func TestBlockingBufferNotifiesConsumerWhenOutOfMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	metrics := kvevent.MakeMetrics(time.Minute).AggregatorBufferMetricsWithCompat
	ba, release := getBoundAccountWithBudget(4096)
	defer release()

	st := cluster.MakeTestingClusterSettings()
	buf := kvevent.NewMemBuffer(ba, &st.SV, &metrics)
	defer func() {
		require.NoError(t, buf.CloseWithReason(context.Background(), nil))
	}()

	producerCtx, stopProducer := context.WithCancel(context.Background())
	wg := ctxgroup.WithContext(producerCtx)
	defer func() {
		stopProducer()
		_ = wg.Wait() // Ignore error -- this group returns context cancellation.
	}()

	// Start adding KVs to the buffer until we block.
	wg.GoCtx(func(ctx context.Context) error {
		rnd, _ := randutil.NewTestRand()
		for {
			err := buf.Add(ctx, kvevent.MakeKVEvent(makeRangeFeedEvent(rnd, 256, 0)))
			if err != nil {
				return err
			}
		}
	})

	// Consume events until we get a flush event.
	consumerTimeout := 10 * time.Second
	if util.RaceEnabled {
		consumerTimeout *= 10
	}

	require.NoError(t, timeutil.RunWithTimeout(
		context.Background(), "consume", consumerTimeout,
		func(ctx context.Context) error {
			var outstanding kvevent.Alloc
			for i := 0; ; i++ {
				e, err := buf.Get(ctx)
				if err != nil {
					return err
				}
				if e.Type() == kvevent.TypeFlush {
					return nil
				}

				// detach alloc associated with an event and merge (but not release) it into outstanding.
				a := e.DetachAlloc()
				outstanding.Merge(&a)
			}
		},
	))
}
