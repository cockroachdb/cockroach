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
	"math/big"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

type bufferedEvent struct {
	kvevent.Event
	error
}

type rawEventCh chan bufferedEvent

type eventWithConsumer struct {
	event   kvevent.Event
	consume consumeFunc
	err     error
}

type parallelEventProcessor struct {
	g             ctxgroup.Group
	consumeFuncCh chan eventWithConsumer
	doneCh        chan struct{}

	numWorkers       int
	producerWorkerCh []rawEventCh

	nextEvent func(ctx context.Context) (kvevent.Event, error)
}

// newParallelEventProcessor creates eventConsumers using makeConsumer and calls
// eventConsumer.PreProcessEvent() in parallel on the queue of events specified by
// nextEvent. Results are buffered can be retried by calling
// parallelEventProcessor.GetNextEvent.
//
// The number of concurrent goroutines created is O(numConcurrentProcessors).
// The number of buffered items (ie.. events, encoded events) is equal to
// 2*numConcurrentProcessors*processorQueueSize.
//
//  This struct is thread safe for multiple consumers and producers so long as
//  makeConsumer, nextEvent, and eventConsumer.PreProcessEvent are thread safe.
func newParallelEventProcessor(
	ctx context.Context,
	makeConsumer func() (eventConsumer, error),
	nextEvent func(ctx context.Context) (kvevent.Event, error),
	numConcurrentProcessors int,
	processorQueueSize int,
) (*parallelEventProcessor, error) {

	p := &parallelEventProcessor{
		g:             ctxgroup.WithContext(ctx),
		consumeFuncCh: make(chan eventWithConsumer, numConcurrentProcessors*processorQueueSize),
		doneCh:        make(chan struct{}),

		numWorkers:       numConcurrentProcessors,
		producerWorkerCh: make([]rawEventCh, numConcurrentProcessors),

		nextEvent: nextEvent,
	}

	consumers := make([]eventConsumer, p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		c, err := makeConsumer()
		if err != nil {
			return nil, err
		}
		consumers[i] = c
	}

	for i := 0; i < p.numWorkers; i++ {
		p.producerWorkerCh[i] = make(rawEventCh, processorQueueSize)

		consumer := consumers[i]
		id := i
		workerClosure := func(ctx2 context.Context) error {
			return p.workerLoop(ctx2, consumer, id)
		}

		p.g.GoCtx(workerClosure)
	}
	p.g.GoCtx(p.producerLoop)

	return p, nil
}

func (p *parallelEventProcessor) GetNextEvent(
	ctx context.Context,
) (kvevent.Event, consumeFunc, error) {
	select {
	case <-ctx.Done():
		return kvevent.Event{}, noopConsumer, ctx.Err()
	case <-p.doneCh:
		return kvevent.Event{}, noopConsumer, nil
	case data := <-p.consumeFuncCh:
		return data.event, data.consume, data.err
	}
}

// workerLoop reads events from its associated channel, preprocesses them,
// and passes them to the main channel.
func (p *parallelEventProcessor) workerLoop(
	ctx context.Context, consumer eventConsumer, id int,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.doneCh:
			return nil
		case evWithErr := <-p.producerWorkerCh[id]:
			ev, err := evWithErr.Event, evWithErr.error
			consume := noopConsumer
			if err == nil {
				consume, err = consumer.PreProcessEvent(ctx, ev)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-p.doneCh:
				return nil
			case p.consumeFuncCh <- eventWithConsumer{event: ev, consume: consume, err: err}:
			}
		}
	}
}

// producerLoop fetches events and passes them on to the
// appropriate worker.
func (p *parallelEventProcessor) producerLoop(ctx context.Context) error {
	for {
		ev, err := p.nextEvent(ctx)

		// In case of an error, it does not matter which worker the error goes to.
		// It should eventually be "bubbled up" to the GetNextEvent.
		// Order also does not matter for non-KV events.
		var bucket int
		if err == nil && ev.Type() == kvevent.TypeKV {
			key := new(big.Int)
			bigIntBucket := new(big.Int)
			key.SetBytes(ev.KV().Key)
			workers := big.NewInt(int64(p.numWorkers))
			bucket = int(bigIntBucket.Mod(key, workers).Int64())
		} else {
			bucket = rand.Intn(p.numWorkers)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-p.doneCh:
			return nil
		case p.producerWorkerCh[bucket] <- bufferedEvent{ev, err}:
		}
	}
}

func (p *parallelEventProcessor) Close() error {
	close(p.doneCh)
	return p.g.Wait()
}
