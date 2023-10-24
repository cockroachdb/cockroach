// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/queue"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// MemQueueProcessor is a consumer of events with type T, that indefinitely reads off of the provided
// queue.MemoryQueue[T] and passes them to the provided EventProcessor[T].
type MemQueueProcessor[T any] struct {
	queue     *queue.MemoryQueue[T]
	processor EventProcessor[T]
}

func NewMemQueueProcessor[T any](
	queue *queue.MemoryQueue[T], processor EventProcessor[T],
) (*MemQueueProcessor[T], error) {
	if queue == nil {
		return nil, errors.New("nil MemoryQueue provided")
	}
	if processor == nil {
		return nil, errors.New("nil EventProcessor provided")
	}
	return &MemQueueProcessor[T]{
		queue:     queue,
		processor: processor,
	}, nil
}

func (p *MemQueueProcessor[T]) Start(ctx context.Context, stop *stop.Stopper) error {
	return stop.RunAsyncTask(ctx,
		fmt.Sprintf("MemQueueProcessor{%s}", p.queue.Alias()),
		func(ctx context.Context) {
			for {
				select {
				case <-stop.ShouldQuiesce():
					// The producer for the MemQueue that we're consuming from is expected to be a gRPC server, which
					// should be using the same stop.Stopper. Therefore, once we're signaled to quiesce, the queue should
					// soon stop growing in size. Drain what's left before exiting.
					log.Infof(ctx, "MemQueueProcessor{%s} shutdown requested, draining with timeout", p.queue.Alias())
					p.drain(ctx)
					return
				default:
					// NB: If the queue is empty, this method is considered "busy-waiting", which is CPU-intensive.
					// Consider a more efficient consumption model if used long term.
					if e, ok := p.queue.Dequeue(); ok {
						if err := p.processor.Process(ctx, e); err != nil {
							log.Errorf(ctx, "MemQueueProcessor{%s} processing error: %v", p.queue.Alias(), err)
						}
					}
				}
			}
		})
}

func (p *MemQueueProcessor[T]) drain(ctx context.Context) {
	t := time.After(2 * time.Minute)
	for {
		select {
		case <-t:
			log.Infof(ctx, "MemQueueProcessor{%s} drain timed out", p.queue.Alias())
			return
		default:
			if e, ok := p.queue.Dequeue(); ok {
				if err := p.processor.Process(ctx, e); err != nil {
					log.Errorf(ctx, "MemQueueProcessor{%s} processing error: %v", p.queue.Alias(), err)
				}
			} else {
				log.Infof(ctx, "MemQueueProcessor{%s} draining complete", p.queue.Alias())
				return
			}
		}
	}
}
