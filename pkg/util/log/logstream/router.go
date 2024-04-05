// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TypedEvent is a tuple containing an EventType and event.
type TypedEvent struct {
	eventType log.EventType
	event     any
}

// LogTypeEventRouter is a Processor that routes structured events based on
// their EventType.
//
// Clients register processors via register, and processed events are delegated
// to all processors registered for that event type.
//
// Processing is done synchronously. LogTypeEventRouter is thread-safe.
type LogTypeEventRouter struct {
	mu struct {
		sync.RWMutex
		routes map[log.EventType][]Processor
	}
}

var _ Processor = (*LogTypeEventRouter)(nil)

func newLogTypeEventRouter() *LogTypeEventRouter {
	er := &LogTypeEventRouter{}
	er.mu.routes = make(map[log.EventType][]Processor)
	return er
}

// Process implements the Processor interface.
func (er *LogTypeEventRouter) Process(ctx context.Context, e any) error {
	typed, ok := e.(*TypedEvent)
	if !ok {
		panic(errors.AssertionFailedf("unexpected type passed to LogTypeEventRouter!"))
	}
	er.mu.RLock()
	defer er.mu.RUnlock()
	routes, ok := er.mu.routes[typed.eventType]
	if !ok {
		return nil
	}
	for _, r := range routes {
		if err := r.Process(ctx, typed.event); err != nil {
			return err
		}
	}
	return nil
}

// Register registers a new processor for events of the given type.
func (er *LogTypeEventRouter) register(
	_ context.Context, eventType log.EventType, processor Processor,
) {
	er.mu.Lock()
	defer er.mu.Unlock()
	container, ok := er.mu.routes[eventType]
	if !ok {
		container = make([]Processor, 0)
	}
	er.mu.routes[eventType] = append(container, processor)
}
