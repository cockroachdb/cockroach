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

// defaultFlushInterval specifies a default for the amount of time an ingester
// will go before flushing its contents to the registry.
const defaultFlushInterval = time.Millisecond * 500

// ConcurrentBufferIngester amortizes the locking cost of writing to an
// insights registry concurrently from multiple goroutines. To that end, it
// contains nothing specific to the insights domain; it is merely a bit of
// asynchronous plumbing, built around a contentionutils.ConcurrentBufferGuard.
type ConcurrentBufferIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		eventBuffer *eventBuffer
	}

	opts struct {
		// noTimedFlush prevents time-triggered flushes from being scheduled.
		noTimedFlush bool
		// flushInterval is an optional override flush interval
		// a value of zero will be set to the 500ms default.
		flushInterval time.Duration
	}

	eventBufferCh chan eventBufChPayload
	registry      *lockingRegistry
	clearRegistry uint32

	closeCh      chan struct{}
	testingKnobs *TestingKnobs
}

type eventBufChPayload struct {
	clearRegistry bool
	events        *eventBuffer
}

// ConcurrentBufferIngester buffers the "events" it sees (via ObserveStatement
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

type BufferOpt func(i *ConcurrentBufferIngester)

// WithoutTimedFlush prevents the ConcurrentBufferIngester from performing
// timed flushes to the underlying registry. Generally only useful for
// testing purposes.
func WithoutTimedFlush() BufferOpt {
	return func(i *ConcurrentBufferIngester) {
		i.opts.noTimedFlush = true
	}
}

// WithFlushInterval allows for the override of the default flush interval
func WithFlushInterval(intervalMS int) BufferOpt {
	return func(i *ConcurrentBufferIngester) {
		i.opts.flushInterval = time.Millisecond * time.Duration(intervalMS)
	}
}

func (i *ConcurrentBufferIngester) Start(
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
		flushInterval := i.opts.flushInterval
		if flushInterval == 0 {
			flushInterval = defaultFlushInterval
		}
		// This task eagerly flushes partial buffers into the channel, to avoid
		// delays identifying insights in low-traffic clusters and tests.
		_ = stopper.RunAsyncTask(ctx, "insights-ingester-flush", func(ctx context.Context) {
			ticker := time.NewTicker(flushInterval)

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
func (i *ConcurrentBufferIngester) Clear() {
	i.guard.ForceSyncExec(func() {
		// Our flush function defined on the guard is responsible for setting clearRegistry back to 0.
		atomic.StoreUint32(&i.clearRegistry, 1)
	})
}

func (i *ConcurrentBufferIngester) ingest(events *eventBuffer) {
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
		} else if e.sessionID != (clusterunique.ID{}) {
			i.registry.clearSession(e.sessionID)
		}
		events[idx] = event{}
	}
}

func (i *ConcurrentBufferIngester) ObserveStatement(
	sessionID clusterunique.ID, statement *Statement,
) {
	if !i.registry.enabled() {
		return
	}

	if i.testingKnobs != nil && i.testingKnobs.InsightsWriterStmtInterceptor != nil {
		i.testingKnobs.InsightsWriterStmtInterceptor(sessionID, statement)
		return
	}

	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: sessionID,
			statement: statement,
		}
	})
}

func (i *ConcurrentBufferIngester) ObserveTransaction(
	sessionID clusterunique.ID, transaction *Transaction,
) {
	if !i.registry.enabled() {
		return
	}

	if i.testingKnobs != nil && i.testingKnobs.InsightsWriterTxnInterceptor != nil {
		i.testingKnobs.InsightsWriterTxnInterceptor(sessionID, transaction)
		return
	}

	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID:   sessionID,
			transaction: transaction,
		}
	})
}

// ClearSession sends a signal to the underlying registry to clear any cached
// data associated with the given sessionID. This is an async operation.
func (i *ConcurrentBufferIngester) ClearSession(sessionID clusterunique.ID) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = event{
			sessionID: sessionID,
		}
	})
}

func newConcurrentBufferIngester(registry *lockingRegistry) *ConcurrentBufferIngester {
	i := &ConcurrentBufferIngester{
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
