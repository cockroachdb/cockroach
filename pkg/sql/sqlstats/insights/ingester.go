// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// concurrentBufferIngester amortizes the locking cost of writing to an
// insights registry concurrently from multiple goroutines. To that end, it
// contains nothing specific to the insights domain; it is merely a bit of
// asynchronous plumbing, built around a contentionutils.ConcurrentBufferGuard.
type concurrentBufferIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		eventBuffer *eventBuffer
	}

	opts struct {
		// noTimedFlush prevents time-triggered flushes from being scheduled.
		noTimedFlush bool
	}

	eventBufferCh chan eventBufChPayload
	registry      *lockingRegistry
	clearRegistry uint32

	closeCh chan struct{}
}

type eventBufChPayload struct {
	clearRegistry bool
	events        *eventBuffer
}

var _ Writer = (*concurrentBufferIngester)(nil)

// concurrentBufferIngester buffers the "events" it sees (via ObserveStatement
// and ObserveTransaction) and passes them along to the underlying registry
// once its buffer is full. (Or once a timeout has passed, for low-traffic
// clusters and tests.)
//
// The bufferSize was set at 8192 after experimental micro-benchmarking ramping
// up the number of goroutines writing through the ingester concurrently.
// Performance was deemed acceptable under 10,000 concurrent goroutines.
const bufferSize = 8192

type eventBuffer [bufferSize]event

var eventBufferPool = sync.Pool{
	New: func() interface{} { return new(eventBuffer) },
}

type event struct {
	sessionID   clusterunique.ID
	transaction *Transaction
	statement   *Statement
}

type BufferOpt func(i *concurrentBufferIngester)

// WithoutTimedFlush prevents the concurrentBufferIngester from performing
// timed flushes to the underlying registry. Generally only useful for
// testing purposes.
func WithoutTimedFlush() BufferOpt {
	return func(i *concurrentBufferIngester) {
		i.opts.noTimedFlush = true
	}
}

func (i *concurrentBufferIngester) Start(
	ctx context.Context, stopper *stop.Stopper, opts ...BufferOpt,
) {
	for _, opt := range opts {
		opt(i)
	}
	// This task pulls buffers from the channel and forwards them along to the
	// underlying registry.
	_ = stopper.RunAsyncTask(ctx, "insights-ingester", func(ctx context.Context) {

		for {
			select {
			case payload := <-i.eventBufferCh:
				i.ingest(payload.events) // note that ingest clears the buffer
				if payload.clearRegistry {
					i.registry.Clear()
				}
				eventBufferPool.Put(payload.events)
			case <-stopper.ShouldQuiesce():
				close(i.closeCh)
				return
			}
		}
	})

	if !i.opts.noTimedFlush {
		// This task eagerly flushes partial buffers into the channel, to avoid
		// delays identifying insights in low-traffic clusters and tests.
		_ = stopper.RunAsyncTask(ctx, "insights-ingester-flush", func(ctx context.Context) {
			ticker := time.NewTicker(500 * time.Millisecond)

			for {
				select {
				case <-ticker.C:
					i.guard.ForceSync()
				case <-stopper.ShouldQuiesce():
					ticker.Stop()
					return
				}
			}
		})
	}
}

// Clear flushes the underlying buffer, and signals the underlying registry
// to clear any remaining cached data afterward. This is an async operation.
func (i *concurrentBufferIngester) Clear() {
	i.guard.ForceSyncExec(func() {
		// Our flush function defined on the guard is responsible for setting clearRegistry back to 0.
		atomic.StoreUint32(&i.clearRegistry, 1)
	})
}

func (i *concurrentBufferIngester) ingest(events *eventBuffer) {
	for idx, e := range events {
		// Because an eventBuffer is a fixed-size array, rather than a slice,
		// we do not know how full it is until we hit a nil entry.
		if e == (event{}) {
			break
		}
		if e.statement != nil {
			i.registry.ObserveStatement(e.sessionID, e.statement)
		} else if e.transaction != nil {
			i.registry.ObserveTransaction(e.sessionID, e.transaction)
		}
		events[idx] = event{}
	}
}

func (i *concurrentBufferIngester) ObserveStatement(
	sessionID clusterunique.ID, statement *Statement,
) {
	if !i.registry.enabled() {
		return
	}
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: sessionID,
			statement: statement,
		}
	})
}

func (i *concurrentBufferIngester) ObserveTransaction(
	sessionID clusterunique.ID, transaction *Transaction,
) {
	if !i.registry.enabled() {
		return
	}
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID:   sessionID,
			transaction: transaction,
		}
	})
}

func newConcurrentBufferIngester(registry *lockingRegistry) *concurrentBufferIngester {
	i := &concurrentBufferIngester{
		// A channel size of 1 is sufficient to avoid unnecessarily
		// synchronizing producer (our clients) and consumer (the underlying
		// registry): moving from 0 to 1 here resulted in a 25% improvement
		// in the micro-benchmarks, but further increases had no effect.
		// Otherwise, we rely solely on the size of the eventBuffer for
		// adjusting our carrying capacity.
		eventBufferCh: make(chan eventBufChPayload, 1),
		registry:      registry,
		closeCh:       make(chan struct{}),
	}

	i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
	i.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return bufferSize
		},
		func(currentWriterIndex int64) {
			clearRegistry := atomic.LoadUint32(&i.clearRegistry) == 1
			if clearRegistry {
				defer func() {
					atomic.StoreUint32(&i.clearRegistry, 0)
				}()
			}
			select {
			case i.eventBufferCh <- eventBufChPayload{
				clearRegistry: clearRegistry,
				events:        i.guard.eventBuffer,
			}:
			case <-i.closeCh:
			}
			i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
		},
	)
	return i
}
