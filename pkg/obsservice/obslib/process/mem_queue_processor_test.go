// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/queue"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var testStrSizeFn = queue.SizeFn[string](func(s string) (int, error) {
	return len(s), nil
})

func TestMemQueueProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("consumes elements from queue", func(t *testing.T) {
		q := queue.NewMemoryQueue[string](1<<11 /* 2K */, testStrSizeFn, "test")
		processor := &TestCountProcessor{}
		qProcessor, err := NewMemQueueProcessor[string](q, processor)
		require.NoError(t, err)
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		require.NoError(t, qProcessor.Start(ctx, stopper))

		waitForConsume := func(timesCalled int, errMsg string) {
			testutils.SucceedsSoon(t, func() error {
				processor.mu.Lock()
				defer processor.mu.Unlock()
				if processor.mu.timesCalled == timesCalled && q.Len() == 0 {
					return nil
				}
				return errors.Newf("%s", errMsg)
			})
		}

		require.NoError(t, q.Enqueue("hello"))
		waitForConsume(1, "first message in queue not consumed")
		require.NoError(t, q.Enqueue("it's"))
		waitForConsume(2, "second message in queue not consumed")
		require.NoError(t, q.Enqueue("me"))
		waitForConsume(3, "third message in queue not consumed")
	})

	t.Run("drains elements from queue after quiesce", func(t *testing.T) {
		q := queue.NewMemoryQueue[string](1<<11 /* 2K */, testStrSizeFn, "test")
		processor := &TestWaitGroupCountProcessor{}
		// We want the processor to stall until after quiesce. This allows us to prevent the consumption
		// of elements from the MemoryQueue until after the stopped has been Quiesced, ensuring that
		// the MemoryQueue still has elements remaining to be processed after Quiesce.
		processor.wg.Add(1)
		qProcessor, err := NewMemQueueProcessor[string](q, processor)
		require.NoError(t, err)
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		require.NoError(t, qProcessor.Start(ctx, stopper))
		require.NoError(t, q.Enqueue("hello"))
		require.NoError(t, q.Enqueue("it's"))
		require.NoError(t, q.Enqueue("me"))
		require.NoError(t, q.Enqueue("did"))
		require.NoError(t, q.Enqueue("you"))
		require.NoError(t, q.Enqueue("drain?"))
		time.AfterFunc(25*time.Millisecond, func() {
			processor.wg.Done()
		})
		stopper.Quiesce(ctx)
		testutils.SucceedsSoon(t, func() error {
			processor.mu.Lock()
			defer processor.mu.Unlock()
			if processor.mu.timesCalled == 6 && q.Len() == 0 {
				return nil
			}
			return errors.New("queue not drained after quiesce")
		})
	})

	t.Run("processing errors don't kill the processor", func(t *testing.T) {
		q := queue.NewMemoryQueue[string](1<<11 /* 2K */, testStrSizeFn, "test")
		processor := &TestCountProcessor{}
		qProcessor, err := NewMemQueueProcessor[string](q, processor)
		require.NoError(t, err)
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		require.NoError(t, qProcessor.Start(ctx, stopper))

		waitForConsume := func(timesCalled int, errMsg string) {
			testutils.SucceedsSoon(t, func() error {
				processor.mu.Lock()
				defer processor.mu.Unlock()
				if processor.mu.timesCalled == timesCalled && q.Len() == 0 {
					return nil
				}
				return errors.Newf("%s", errMsg)
			})
		}

		require.NoError(t, q.Enqueue("hello"))
		waitForConsume(1, "first message in queue not consumed")

		processor.retErr = errors.New("test error")
		require.NoError(t, q.Enqueue("this element should be met with an error in the processor"))
		waitForConsume(2, "second message in queue, which should cause a processor error, not consumed")

		processor.retErr = nil
		require.NoError(t, q.Enqueue("world"))
		waitForConsume(3, "third message not consumed - did the previous error kill the processor?")
	})
}

type TestWaitGroupCountProcessor struct {
	wg sync.WaitGroup
	mu struct {
		syncutil.Mutex
		timesCalled int
	}
}

func (t *TestWaitGroupCountProcessor) Process(_ context.Context, _ string) error {
	t.wg.Wait()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.timesCalled++
	return nil
}

var _ EventProcessor[string] = (*TestWaitGroupCountProcessor)(nil)

type TestCountProcessor struct {
	retErr error
	mu     struct {
		syncutil.Mutex
		timesCalled int
	}
}

func (t *TestCountProcessor) Process(_ context.Context, _ string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.timesCalled++
	if t.retErr != nil {
		return t.retErr
	}
	return nil
}

var _ EventProcessor[string] = (*TestCountProcessor)(nil)
