// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var bufferedStreamCapacityPerStream = envutil.EnvOrDefaultInt64(
	"COCKROACH_BUFFERED_STREAM_CAPACITY_PER_STREAM", 4096)

type sharedMuxEvent struct {
	event *kvpb.MuxRangeFeedEvent
	alloc *SharedBudgetAllocation
}

type BufferedStreamSender struct {
	ServerStreamSender

	taskCancel context.CancelFunc
	wg         sync.WaitGroup

	errCh   chan error
	queueMu struct {
		syncutil.Mutex
		stopped  bool
		capacity int64
		buffer   queue.QueueWithFixedChunkSize[*sharedMuxEvent]
		overflow bool
	}
}

func NewBufferedStreamSender(wrapped ServerStreamSender) *BufferedStreamSender {
	l := &BufferedStreamSender{
		ServerStreamSender: wrapped,
	}
	l.queueMu.capacity = bufferedStreamCapacityPerStream
	return l
}

var _ ServerStreamSender = (*BufferedStreamSender)(nil)

func (bs *BufferedStreamSender) AddCapacity() {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.capacity += bufferedStreamCapacityPerStream
}

// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc
// and release it if the return error is non-nil.
func (bs *BufferedStreamSender) sendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		return errors.New("stream sender is stopped")
	}
	if bs.queueMu.overflow {
		return newErrBufferCapacityExceeded
	}

	if bs.queueMu.buffer.Len() >= bs.queueMu.capacity {
		bs.queueMu.overflow = true
		return newErrBufferCapacityExceeded
	}

	bs.queueMu.buffer.Enqueue(&sharedMuxEvent{event, alloc})
	return nil
}

func (bs *BufferedStreamSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	bs.errCh = make(chan error, 1)
	bs.wg.Add(1)
	ctx, bs.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "buffered stream output", func(ctx context.Context) {
		defer bs.wg.Done()
		if err := bs.RunOutputLoop(ctx, stopper); err != nil {
			bs.errCh <- err
		}
	}); err != nil {
		bs.taskCancel()
		bs.wg.Done()
		return err
	}
	return nil
}

// Block forever if buffered stream sender is not initialized
func (bs *BufferedStreamSender) Error() chan error {
	if bs == nil {
		return nil
	}
	return bs.errCh
}

func (bs *BufferedStreamSender) Stop() {
	bs.taskCancel()
	bs.wg.Wait()

	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.RemoveAll(func(e *sharedMuxEvent) {
		e.alloc.Release(context.Background())
	})
}

func (bs *BufferedStreamSender) popFront() (
	e *sharedMuxEvent,
	success bool,
	overflowed bool,
	remains int64,
) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.Dequeue()
	return event, ok, bs.queueMu.overflow, bs.queueMu.buffer.Len()
}

func (bs *BufferedStreamSender) RunOutputLoop(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		default:
			e, success, overflowed, remains := bs.popFront()
			if success {
				err := bs.Send(e.event)
				e.alloc.Release(ctx)
				if e.event.Error != nil {
					// Add metrics here
					if cleanUp, ok := bs..LoadAndDelete(ev.StreamID); ok {
						// TODO(wenyihu6): add more observability metrics into how long the
						// clean up call is taking
						(*cleanUp)()
					}
				}
				if err != nil {
					return err
				}
			}
			if overflowed && remains == int64(0) {
				return newErrBufferCapacityExceeded
			}
		}
	}
}
