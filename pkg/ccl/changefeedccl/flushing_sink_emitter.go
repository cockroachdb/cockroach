// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// FlushingEmitter is a SinkEmitter with a Flush method that allows blocking
// until all messages have successfully been emitted.
type FlushingEmitter interface {
	SinkEmitter

	// Flush blocks until all events passed into Emit() have been either
	// successfully emitted or an error has occured.  If an error has ever occured
	// during any Emit, Flush() return the first of those errors.
	// It is assumed that Emit() is never called concurrently with Flush.
	Flush() error
}

type flushingSinkEmitter struct {
	ctx     context.Context
	wrapped AsyncEmitter

	inFlight int64
	flushCh  chan struct{}
	termCh   chan struct{}
	mu       struct {
		syncutil.RWMutex
		shouldNotify bool
		termErr      error
	}

	doneCh chan struct{}
	g      ctxgroup.Group
}

var _ FlushingEmitter = (*flushingSinkEmitter)(nil)

func makeFlushingEmitter(ctx context.Context, wrapped AsyncEmitter) FlushingEmitter {
	sink := flushingSinkEmitter{
		ctx:      ctx,
		wrapped:  wrapped,
		inFlight: 0,
		flushCh:  make(chan struct{}, 1),
		termCh:   make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
		g:        ctxgroup.WithContext(ctx),
	}
	sink.g.GoCtx(func(ctx2 context.Context) error {
		sink.emitConfirmationWorker(ctx2)
		return nil
	})

	return &sink
}

// Emit implements the SinkEmitter interface.
func (fs *flushingSinkEmitter) Emit(event *sinkEvent) {
	fs.incInFlight()
	fs.wrapped.Emit(event)
}

// Close implements the SinkEmitter interface.
func (fs *flushingSinkEmitter) Close() {
	fs.wrapped.Close()
	close(fs.doneCh)
	_ = fs.g.Wait()
}

func (fs *flushingSinkEmitter) incInFlight() {
	atomic.AddInt64(&fs.inFlight, 1)
}

func (fs *flushingSinkEmitter) decInFlight(flushed int) {
	fs.mu.RLock()
	remaining := atomic.AddInt64(&fs.inFlight, -int64(flushed))
	notifyFlush := remaining == 0 && fs.mu.shouldNotify
	fs.mu.RUnlock()
	// If shouldNotify is true, it is assumed that no new Emits could happen,
	// therefore it is not possible for this to occur multiple times for a single
	// Flush call
	if notifyFlush {
		fs.flushCh <- struct{}{}
	}
}

// Flush implements the FlushingEmitter interface.
func (fs *flushingSinkEmitter) Flush() error {
	fs.mu.Lock()
	if fs.inFlight == 0 {
		fs.mu.Unlock()
		return nil
	}
	// Signify that flushCh should be emitted to upon inFlight reaching 0
	fs.mu.shouldNotify = true
	fs.mu.Unlock()

	// Send a flush event through the emitter to force any buffered events to
	// flush out immediately.
	fs.wrapped.Emit(newSinkFlushEvent())

	select {
	case <-fs.ctx.Done():
		return fs.ctx.Err()
	case <-fs.doneCh:
		return nil
	case <-fs.termCh:
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return fs.mu.termErr
	case <-fs.flushCh:
		fs.mu.Lock()
		defer fs.mu.Unlock()
		fs.mu.shouldNotify = false
		return nil
	}
}

// emitConfirmationWorker handles the Successes() and Errors() channel events of
// the underlying AsyncEmitter
func (fs *flushingSinkEmitter) emitConfirmationWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-fs.termCh:
			return
		case <-fs.doneCh:
			return
		case err := <-fs.wrapped.Errors():
			fs.mu.Lock()
			if fs.mu.termErr == nil {
				fs.mu.termErr = err
				close(fs.termCh)
			}
			fs.mu.Unlock()
		case flushed := <-fs.wrapped.Successes():
			fs.decInFlight(flushed)
		}
	}
}
