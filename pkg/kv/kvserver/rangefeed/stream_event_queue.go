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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type LockedBufferedStream struct {
	wrapped severStreamSender

	queueMu struct {
		syncutil.Mutex
		capacity int64
		buffer   muxEventQueue
		overflow bool
	}
}

var _ severStreamSender = &LockedBufferedStream{}

const defaultEventChanCap = 4096

func NewLockedBufferedStream(wrapped severStreamSender) *LockedBufferedStream {
	l := &LockedBufferedStream{
		wrapped: wrapped,
	}
	l.queueMu.capacity = defaultEventChanCap
	return l
}

func (b *LockedBufferedStream) AddCapacity() {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	b.queueMu.capacity += defaultEventChanCap
}

func (b *LockedBufferedStream) removeAll() {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	b.queueMu.buffer.removeAll()
}

// Never returns an error. We shut down when overflow but wait until output loop drains everythng.
func (b *LockedBufferedStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	if b.queueMu.overflow {
		return nil
	}
	if b.queueMu.buffer.len() >= b.queueMu.capacity {
		b.queueMu.overflow = true
		return nil
	}
	b.queueMu.buffer.pushBack(e)
	return nil
}

func (b *LockedBufferedStream) popFront() (e *kvpb.MuxRangeFeedEvent, empty bool, overflow bool) {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()
	event, ok := b.queueMu.buffer.popFront()
	return event, !ok, b.queueMu.overflow
}

// should be able to cancel
func (b *LockedBufferedStream) RunOutputLoop(ctx context.Context, stopper *stop.Stopper) error {
	defer func() {
		b.removeAll()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		default:
			e, empty, overflow := b.popFront()
			if empty && overflow {
				return newErrBufferCapacityExceeded().GoError()
			}
			if e == nil {
				continue
			}
			if err := b.wrapped.Send(e); err != nil {
				return nil
			}
		}
	}
}
