// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

type sendFunc func(context.Context, kvpb.Header, kvpb.AdmissionHeader, kvpb.Request) (kvpb.Response, *kvpb.Error)
type testSender struct {
	onSend sendFunc
}

func (s *testSender) SendWrappedWithAdmission(
	ctx context.Context, h kvpb.Header, ah kvpb.AdmissionHeader, r kvpb.Request,
) (kvpb.Response, *kvpb.Error) {
	return s.onSend(ctx, h, ah, r)
}

func (s *testSender) AnnotateCtx(ctx context.Context) context.Context {
	return ctx
}

func TestLockTableFlusher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	workerQuiescePeriod = 10 * time.Millisecond
	s := &testSender{}
	ltf := newLockTableFlusherForSender(s, stopper)

	validateHeader := func(h kvpb.Header) {
		require.True(t, h.TargetBytes > 0, "target bytes should be sent")
	}

	blocker := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(blocker) })
	defer unblock()
	started := make(chan struct{}, maxWorkerCount*4)

	s.onSend = func(ctx context.Context, h kvpb.Header, ah kvpb.AdmissionHeader, r kvpb.Request) (kvpb.Response, *kvpb.Error) {
		started <- struct{}{}
		<-blocker
		validateHeader(h)
		return &kvpb.FlushLockTableResponse{}, nil
	}

	// Enqueue twice the maxWorkerCount.
	for i := range maxWorkerCount * 2 {
		span := roachpb.Span{Key: roachpb.Key{byte(i)}, EndKey: roachpb.Key{byte(i + 1)}}
		ltf.MaybeEnqueueFlush(roachpb.RangeID(i+1), span, 5)
	}

	// maxWorkerCount requests should be able to start in parallel. Note, this
	// test doesn't guaranteed that we don't go over the max, but in practice it
	// does catch such bugs relatively quickly.
	startedRequests := 0
	for startedRequests < maxWorkerCount {
		select {
		case <-started:
			startedRequests++
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d tasks to start", maxWorkerCount)
		}
	}

	require.Equal(t, maxWorkerCount, ltf.Inflight())
	require.Equal(t, maxWorkerCount, ltf.WorkerCount())
	require.Equal(t, maxWorkerCount, stopper.NumTasks())
	require.Equal(t, maxWorkerCount, ltf.QueueLength())

	// Re-enqueue all ranges.
	for i := range maxWorkerCount * 2 {
		span := roachpb.Span{Key: roachpb.Key{byte(i)}, EndKey: roachpb.Key{byte(i + 1)}}
		ltf.MaybeEnqueueFlush(roachpb.RangeID(i+1), span, 5)
	}

	// Nothing should change.
	require.Equal(t, maxWorkerCount, ltf.Inflight())
	require.Equal(t, maxWorkerCount, ltf.WorkerCount())
	require.Equal(t, maxWorkerCount, ltf.QueueLength())

	unblock()

	// Count the rest of the requests, we should get to the 2*maxWorkerCount we
	// enqueued.
	for startedRequests < 2*maxWorkerCount {
		select {
		case <-started:
			startedRequests++
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d tasks to start", maxWorkerCount)
		}
	}

	// That should be all of the requests.
	testutils.SucceedsSoon(t, func() error {
		if ltf.Inflight() != 0 {
			return errors.New("inflight requests")
		} else if ltf.QueueLength() != 0 {
			return errors.New("queue non-empty")
		}
		return nil
	})

	// The worker count should decrease back to 1 over time.
	testutils.SucceedsSoon(t, func() error {
		if ltf.WorkerCount() > 1 {
			return errors.New("too many workers")
		}
		return nil
	})
}

func TestFlushQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		r1     = roachpb.RangeID(1)
		r1Span = roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		r2     = roachpb.RangeID(2)
		r2Span = roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}
		r3     = roachpb.RangeID(3)
		r3Span = roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
	)

	t.Run("enqueue duplicate range updates range", func(t *testing.T) {
		q := makeFlushQueue()
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 10}))
		require.False(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 100}))
		require.Equal(t, 1, q.Len())
		item := q.dequeue(nil)
		require.NotNil(t, item)
		require.Equal(t, int64(100), item.numToFlush)
	})

	t.Run("enqueue doesn't add inflight range", func(t *testing.T) {
		q := makeFlushQueue()
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 10}))
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r2, span: r2Span, numToFlush: 1}))

		item := q.dequeue(nil)
		require.NotNil(t, item)
		require.Equal(t, r1, item.rangeID)
		require.Equal(t, int64(10), item.numToFlush)

		// We still can't enqueue this because it is "inflight"
		require.False(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 100}))

		item = q.dequeue(item)
		require.NotNil(t, item)
		require.Equal(t, r2, item.rangeID)
		require.Equal(t, int64(1), item.numToFlush)

		// Now we can enqueue r1 again because it is no longer "inflight"
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 43}))
	})

	t.Run("dequeue is ordered by numToFlush", func(t *testing.T) {
		q := makeFlushQueue()
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r1, span: r1Span, numToFlush: 10}))
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r2, span: r2Span, numToFlush: 100}))
		require.True(t, q.maybeEnqueue(&lockFlushRequest{rangeID: r3, span: r3Span, numToFlush: 42}))
		require.Equal(t, 3, q.Len())

		item := q.dequeue(nil)
		require.NotNil(t, item)
		require.Equal(t, r2, item.rangeID)
		require.Equal(t, int64(100), item.numToFlush)

		item = q.dequeue(item)
		require.NotNil(t, item)
		require.Equal(t, r3, item.rangeID)
		require.Equal(t, int64(42), item.numToFlush)

		item = q.dequeue(item)
		require.NotNil(t, item)
		require.Equal(t, r1, item.rangeID)
		require.Equal(t, int64(10), item.numToFlush)

		item = q.dequeue(item)
		require.Nil(t, item)
	})
}
