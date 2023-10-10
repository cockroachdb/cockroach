// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package queue

import (
	"container/list"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// SizeFn returns the size, in bytes, of type T. Used for the MemoryQueue
// to determine the size of enqueued/dequeued elements.
type SizeFn[T any] func(T) (int, error)

// MemoryQueue is a FIFO, in-memory event queue designed for use by the
// observability service for buffering events awaiting processing.
// It's elements are intended to be protobuf messages, which by default
// implement the proto.Sizer interface. This provides us a convenient
// way to determine the size of buffered events on Enqueue / Dequeue
// to enforce the configured maxSize.
//
// MemoryQueue is backed by the container/list package from the Golang
// stdlib, which is a linked list under the hood. This allows us to avoid
// holding onto unused memory like you would by trimming a pre-allocated
// slice (e.g. mySlice[1:]) when dequeue'ing elements from the front.
// See: https://pkg.go.dev/container/list for details.
//
// See NewMemoryQueue for initialization.
type MemoryQueue[T any] struct {
	alias  string
	sizeFn SizeFn[T]
	mu     struct {
		syncutil.Mutex
		queue   *list.List
		maxSize int
		curSize int
	}
}

// NewMemoryQueue instantiates and returns a new *MemoryQueue, whose size
// in bytes is bounded by the provided maxBytes amount.
//
// An alias should also be provided to describe the events being buffered,
// for clearer error messaging (and therefore, clearer logging).
func NewMemoryQueue[T any](maxBytes int, sizeFn SizeFn[T], alias string) *MemoryQueue[T] {
	q := &MemoryQueue[T]{
		alias:  alias,
		sizeFn: sizeFn,
	}
	q.mu.queue = list.New()
	q.mu.maxSize = maxBytes
	return q
}

// Enqueue adds the provided element to the MemoryQueue, so long
// as it would not exceed the configured max size for this
// MemoryQueue in bytes. FIFO order is maintained.
//
// If buffering the provided element would exceed the configured
// max size, an error is returned and the element is not buffered.
func (q *MemoryQueue[T]) Enqueue(e T) error {
	size, err := q.sizeFn(e)
	if err != nil {
		return errors.Wrap(err, "sizing element")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.curSize+size > q.mu.maxSize {
		return errors.Newf("size limit reached for %s queue, message dropped", q.alias)
	}
	// TODO(abarganier): Gauge metric(s) to track queue size & length.
	q.mu.curSize = q.mu.curSize + size
	q.mu.queue.PushBack(e)
	return nil
}

// Dequeue removes and returns the oldest element from this
// MemoryQueue. If this MemoryQueue is empty, nil is returned.
func (q *MemoryQueue[T]) Dequeue() T {
	q.mu.Lock()
	defer q.mu.Unlock()
	// NB: The value of ret is nil unless assigned.
	var ret T
	if q.mu.queue.Len() == 0 {
		return ret
	}
	e := q.mu.queue.Front()
	q.mu.queue.Remove(e)
	ret, ok := e.Value.(T)
	if !ok {
		panic(errors.AssertionFailedf("unable to assert type on Dequeue() for %s queue: %v", q.alias, e.Value))
	}
	size, err := q.sizeFn(ret)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "sizing element on dequeue for %q", q.alias))
	}
	// TODO(abarganier): Gauge metric(s) to track queue size & length.
	q.mu.curSize = q.mu.curSize - size
	return ret
}

func (q *MemoryQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.mu.queue.Len()
}
