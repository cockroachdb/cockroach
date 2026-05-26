// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nudge

import (
	"github.com/cockroachdb/cockroach/pkg/util/container/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Buffer is a concurrent buffer that pairs a ring buffer with a nudge channel.
// Pushes append to the buffer and send a non-blocking notification on the
// channel. The consumer selects on Ch() and calls Pop() to retrieve events.
//
// Pop re-pings the channel when the buffer is non-empty after the pop, so the
// consumer does not need to drain in a loop — it can pop one item per select
// iteration and will be woken again if more remain.
//
// A Ch() notification means data *may* be available, not that it *is* — Pop can
// return false, and consumers must handle this. The channel is never closed;
// shutdown is expected to be context-driven.
//
// NB: this buffer is not currently not memory monitored -- it is the caller's
// responsibility to prevent OOMs.
type Buffer[T any] struct {
	mu  syncutil.Mutex
	buf ring.Buffer[T]
	ch  chan struct{}
}

// MakeBuffer creates a Buffer ready for use.
func MakeBuffer[T any]() Buffer[T] {
	return Buffer[T]{
		ch: make(chan struct{}, 1),
	}
}

// Push appends an event and sends a non-blocking notification.
func (n *Buffer[T]) Push(v T) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.buf.Push(v)
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

// Pop removes and returns the first event. If the buffer has more events
// after the pop, it re-pings the channel. Returns false if the buffer was
// empty.
func (n *Buffer[T]) Pop() (T, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.buf.Length() == 0 {
		var zero T
		return zero, false
	}
	v := n.buf.At(0)
	n.buf.Pop(1)
	if n.buf.Length() > 0 {
		select {
		case n.ch <- struct{}{}:
		default:
		}
	}
	return v, true
}

// Ch returns the notification channel for use in select statements.
func (n *Buffer[T]) Ch() <-chan struct{} {
	return n.ch
}
