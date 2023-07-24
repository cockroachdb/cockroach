// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package produce

import (
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/queue"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// MemQueueProducer is the EventProducer implementation for
// the queue.MemoryQueue. Like the queue.MemoryQueue, it uses
// generics to enforce the type of the events produced into
// the underlying queue.
//
// Expected for use with the ProducerGroup type. Users should
// inject it to an event type's ProducerGroup as one of its
// EventProducer's.
type MemQueueProducer[T proto.Sizer] struct {
	EventProducer
	queue *queue.MemoryQueue[T]
}

// NewMemQueueProducer instantiates and returns a new *MemQueueProducer.
// The underlying queue.MemoryQueue is injected as a dependency, so that it
// can also be injected into its corresponding consumer.
func NewMemQueueProducer[T proto.Sizer](q *queue.MemoryQueue[T]) *MemQueueProducer[T] {
	return &MemQueueProducer[T]{
		queue: q,
	}
}

func (m *MemQueueProducer[T]) Produce(e interface{}) error {
	event, ok := e.(T)
	if !ok {
		// If this happens, something serious has gone wrong with the initialization/configuration
		// of the event pipeline / ProducerGroup using this MemQueueProducer.
		panic(errors.AssertionFailedf("unable to assert event to type %T: %v", *new(T), e))
	}
	return m.queue.Enqueue(event)
}
