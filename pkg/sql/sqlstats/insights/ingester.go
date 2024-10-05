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

	eventBufferCh chan *eventBuffer
	registry      *lockingRegistry
	running       uint64
}

var _ Writer = &concurrentBufferIngester{}

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

func (i *concurrentBufferIngester) Start(ctx context.Context, stopper *stop.Stopper) {
	// This task pulls buffers from the channel and forwards them along to the
	// underlying registry.
	_ = stopper.RunAsyncTask(ctx, "insights-ingester", func(ctx context.Context) {
		atomic.StoreUint64(&i.running, 1)

		for {
			select {
			case events := <-i.eventBufferCh:
				i.ingest(events) // note that injest clears the buffer
				eventBufferPool.Put(events)
			case <-stopper.ShouldQuiesce():
				atomic.StoreUint64(&i.running, 0)
				return
			}
		}
	})

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
		eventBufferCh: make(chan *eventBuffer, 1),
		registry:      registry,
	}

	i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
	i.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return bufferSize
		},
		func(currentWriterIndex int64) {
			if atomic.LoadUint64(&i.running) == 1 {
				i.eventBufferCh <- i.guard.eventBuffer
			}
			i.guard.eventBuffer = eventBufferPool.Get().(*eventBuffer)
		},
	)
	return i
}
