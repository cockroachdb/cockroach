// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contention/contentionutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// concurrentBufferIngester amortizes the locking cost of writing to an
// insights Registry concurrently from multiple goroutines. To that end, it
// contains nothing specific to the insights domain; it is merely a bit of
// asynchronous plumbing, built around a contentionutils.ConcurrentBufferGuard.
type concurrentBufferIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		eventBuffer *eventBuffer
	}

	eventBufferCh chan *eventBuffer
	delegate      Registry
}

// Meanwhile, it looks like a Registry to the outside world, so that others
// needn't know it exist.
var _ Registry = &concurrentBufferIngester{}

// concurrentBufferIngester buffers the "events" it sees (via ObserveStatement
// and ObserveTransaction) and passes them along to the underlying Registry
// once its buffer is full. (Or once a timeout has passed, for low-traffic
// clusters and tests.)
//
// The bufferSize was set at 8192 after experimental micro-benchmarking ramping
// up the number of goroutines writing through the ingester concurrently.
// Performance was deemed acceptable under 10,000 concurrent goroutines.
const bufferSize = 8192

type eventBuffer [bufferSize]*event

type event struct {
	sessionID   clusterunique.ID
	transaction *Transaction
	statement   *Statement
}

func (i concurrentBufferIngester) Start(ctx context.Context, stopper *stop.Stopper) {
	// This task pulls buffers from the channel and forwards them along to the
	// underlying Registry.
	_ = stopper.RunAsyncTask(ctx, "insights-ingester", func(ctx context.Context) {
		for {
			select {
			case events := <-i.eventBufferCh:
				i.ingest(events)
			case <-stopper.ShouldQuiesce():
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

func (i concurrentBufferIngester) ingest(events *eventBuffer) {
	for _, e := range events {
		// Because an eventBuffer is a fixed-size array, rather than a slice,
		// we do not know how full it is until we hit a nil entry.
		if e == nil {
			break
		}
		if e.statement != nil {
			i.delegate.ObserveStatement(e.sessionID, e.statement)
		} else {
			i.delegate.ObserveTransaction(e.sessionID, e.transaction)
		}
	}
}

func (i concurrentBufferIngester) ObserveStatement(
	sessionID clusterunique.ID, statement *Statement,
) {
	if !i.enabled() {
		return
	}
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = &event{
			sessionID: sessionID,
			statement: statement,
		}
	})
}

func (i concurrentBufferIngester) ObserveTransaction(
	sessionID clusterunique.ID, transaction *Transaction,
) {
	if !i.enabled() {
		return
	}
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.eventBuffer[writerIdx] = &event{
			sessionID:   sessionID,
			transaction: transaction,
		}
	})
}

func (i concurrentBufferIngester) IterateInsights(
	ctx context.Context, visitor func(context.Context, *Insight),
) {
	i.delegate.IterateInsights(ctx, visitor)
}

func (i concurrentBufferIngester) enabled() bool {
	return i.delegate.enabled()
}

func newConcurrentBufferIngester(delegate Registry) Registry {
	i := &concurrentBufferIngester{
		// A channel size of 1 is sufficient to avoid unnecessarily
		// synchronizing producer (our clients) and consumer (the underlying
		// Registry): moving from 0 to 1 here resulted in a 25% improvement
		// in the micro-benchmarks, but further increases had no effect.
		// Otherwise, we rely solely on the size of the eventBuffer for
		// adjusting our carrying capacity.
		eventBufferCh: make(chan *eventBuffer, 1),
		delegate:      delegate,
	}

	i.guard.eventBuffer = &eventBuffer{}
	i.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return bufferSize
		},
		func(currentWriterIndex int64) {
			i.eventBufferCh <- i.guard.eventBuffer
			i.guard.eventBuffer = &eventBuffer{}
		},
	)
	return i
}
