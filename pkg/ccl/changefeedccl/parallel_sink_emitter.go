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
	"hash"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

// AsyncEmitter is a SinkEmitter that provides Success and Error channels to get
// acknowledgement of successful emits or any errors that occur during an emit.
type AsyncEmitter interface {
	SinkEmitter

	// Successes returns a channel of integers where the integer describes the
	// number of events that have just been successfully emmitted.
	Successes() chan int

	// Errors returns a channel of errors that occurred during an Emit
	Errors() chan error
}

// parallelSinkEmitter accepts sinkEvents and fans them out to numWorkers worker
// routines that forward events to their own per-topic Emitter instances created
// by a TopicEmitterFactory function
type parallelSinkEmitter struct {
	ctx                 context.Context
	topicEmitterFactory TopicEmitterFactory

	successCh chan int
	errorCh   chan error

	workerCh   []chan *sinkEvent
	numWorkers int64
	hasher     hash.Hash32

	wg      ctxgroup.Group
	doneCh  chan struct{}
	metrics metricsRecorder
}

var _ AsyncEmitter = (*parallelSinkEmitter)(nil)

// TopicEmitterFactory is a function that returns a SinkEmitter for a given
// topic. Successful emits and errors are expected to be passed into the
// provided channels.
type TopicEmitterFactory = func(topic string, successCh chan int, errorCh chan error) SinkEmitter

func makeParallelSinkEmitter(
	ctx context.Context,
	topicEmitterFactory TopicEmitterFactory,
	numWorkers int64,
	metrics metricsRecorder,
) AsyncEmitter {
	pse := parallelSinkEmitter{
		ctx:                 ctx,
		topicEmitterFactory: topicEmitterFactory,

		successCh: make(chan int, 256),
		errorCh:   make(chan error, 1),

		workerCh:   make([]chan *sinkEvent, numWorkers),
		numWorkers: numWorkers,
		hasher:     makeHasher(),

		wg:      ctxgroup.WithContext(ctx),
		doneCh:  make(chan struct{}),
		metrics: metrics,
	}

	for worker := int64(0); worker < pse.numWorkers; worker++ {
		workerCh := make(chan *sinkEvent, 256)
		pse.wg.GoCtx(func(ctx context.Context) error {
			return pse.workerLoop(workerCh)
		})
		pse.workerCh[worker] = workerCh
	}

	return &pse
}

// Successes implements the AsyncEmitter interface.
func (pse *parallelSinkEmitter) Successes() chan int {
	return pse.successCh
}

// Errors implements the AsyncEmitter interface.
func (pse *parallelSinkEmitter) Errors() chan error {
	return pse.errorCh
}

// Close implements the SinkEmitter interface.
func (pse *parallelSinkEmitter) Close() {
	close(pse.doneCh)
	_ = pse.wg.Wait()
}

// Emit implements the SinkEmitter interface.
func (pse *parallelSinkEmitter) Emit(payload *sinkEvent) {
	if payload.shouldFlush {
		// Each worker requires its own message in order for them not to free the
		// message while it's being read by other workers
		freeSinkEvent(payload)
		for _, workerCh := range pse.workerCh {
			select {
			case <-pse.ctx.Done():
				return
			case <-pse.doneCh:
				return
			case workerCh <- newSinkFlushEvent():
			}
		}
		return
	}

	workerId := pse.workerIndex(payload)
	pse.metrics.recordParallelEmitterAdmit()
	select {
	case <-pse.ctx.Done():
		return
	case <-pse.doneCh:
		return
	case pse.workerCh[workerId] <- payload:
		return
	}
}

func (pse *parallelSinkEmitter) workerLoop(input chan *sinkEvent) error {
	// Topics are often their own endpoints that can't accept messagse for other
	// topics, therefore reach one requires its own batcher.
	topicEmitters := make(map[string]SinkEmitter)
	makeTopicEmitter := func(topic string) SinkEmitter {
		emitter := pse.topicEmitterFactory(topic, pse.successCh, pse.errorCh)
		topicEmitters[topic] = emitter
		return emitter
	}
	defer func() {
		for _, emitter := range topicEmitters {
			emitter.Close()
		}
	}()

	for {
		select {
		case <-pse.ctx.Done():
			return pse.ctx.Err()
		case <-pse.doneCh:
			return nil
		case row := <-input:
			if row.shouldFlush {
				// Each topic emitter requires its own message in order for them not to
				// free the message while it's being read by other emitters
				freeSinkEvent(row)
				for _, emitter := range topicEmitters {
					emitter.Emit(newSinkFlushEvent())
				}
				continue
			}

			emitter, ok := topicEmitters[row.msg.topic]
			if !ok {
				emitter = makeTopicEmitter(row.msg.topic)
			}

			emitter.Emit(row)
			pse.metrics.recordParallelEmitterEmit()
		}
	}
}

func (pse *parallelSinkEmitter) workerIndex(row *sinkEvent) int64 {
	pse.hasher.Reset()
	pse.hasher.Write(row.msg.key)
	return int64(pse.hasher.Sum32()) % pse.numWorkers
}
