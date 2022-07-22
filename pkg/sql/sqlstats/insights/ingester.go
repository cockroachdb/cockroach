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

type event struct {
	sessionID   clusterunique.ID
	transaction *Transaction
	statement   *Statement
}

const blockSize = 16800

type block [blockSize]*event

type concurrentBufferIngester struct {
	guard struct {
		*contentionutils.ConcurrentBufferGuard
		block *block
	}

	sink     chan *block
	delegate Registry
}

var _ Registry = &concurrentBufferIngester{}

func (i concurrentBufferIngester) Start(ctx context.Context, stopper *stop.Stopper) {
	// start a loop looking for blocks in the channel and looping over them to pass to the delegate
	if err := stopper.RunAsyncTask(ctx, "outliers-ingester", func(ctx context.Context) {
		for {
			select {
			case b := <-i.sink:
				for _, e := range b {
					if e == nil {
						break
					}
					if e.statement != nil {
						i.delegate.ObserveStatement(e.sessionID, e.statement)
					} else {
						i.delegate.ObserveTransaction(e.sessionID, e.transaction)
					}
				}
				// TODO(todd): Zero out the block and return it to the pool?
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	}); err != nil {
		panic(err) // FIXME
	}

	// TODO(todd): Combine this task with the above?
	// start a timer flushing the guard
	if err := stopper.RunAsyncTask(ctx, "outliers-fancy-ingester-sync", func(ctx context.Context) {
		ticker := time.NewTicker(500 * time.Millisecond)

		for {
			select {
			case <-ticker.C:
				i.guard.ForceSync()
				// TODO(todd): Do I need to tell the timer I read it? (Which timer type was that, again?)
			case <-stopper.ShouldQuiesce():
				ticker.Stop()
				return
			}
		}
	}); err != nil {
		panic(err)
	}
}

func (i concurrentBufferIngester) ObserveStatement(
	sessionID clusterunique.ID, statement *Statement,
) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.block[writerIdx] = &event{
			sessionID: sessionID,
			statement: statement,
		}
	})
}

func (i concurrentBufferIngester) ObserveTransaction(
	sessionID clusterunique.ID, transaction *Transaction,
) {
	i.guard.AtomicWrite(func(writerIdx int64) {
		i.guard.block[writerIdx] = &event{
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
	ingester := &concurrentBufferIngester{
		sink:     make(chan *block, 1000), // TODO(todd): Size?
		delegate: delegate,
	}

	ingester.guard.block = &block{} // TODO(todd): Does it make sense to pool these?
	ingester.guard.ConcurrentBufferGuard = contentionutils.NewConcurrentBufferGuard(
		func() int64 {
			return blockSize
		},
		func(currentWriterIndex int64) {
			ingester.sink <- ingester.guard.block
			ingester.guard.block = &block{}
		},
	)
	return ingester
}
