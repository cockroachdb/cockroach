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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type syncProcessorRouter struct {
	rwmu struct {
		syncutil.RWMutex
		// routes maps each log.EventType to the Processors registered for that type.
		routes map[log.EventType][]Processor
	}
}

func (s *syncProcessorRouter) isRegistered(eventType log.EventType) bool {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	_, ok := s.rwmu.routes[eventType]
	return ok
}

func newSyncProcessorRouter() *syncProcessorRouter {
	router := &syncProcessorRouter{}
	router.rwmu.routes = make(map[log.EventType][]Processor)
	return router
}

func (s *syncProcessorRouter) Start(ctx context.Context, stopper *stop.Stopper) error {
	// No-op, nothing to start since this is synchronous.
	return nil
}

func (s *syncProcessorRouter) Process(ctx context.Context, e *typedEvent) error {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	processors, ok := s.rwmu.routes[e.eventType]
	if ok {
		for _, processor := range processors {
			err := processor.Process(ctx, e.event)
			if err != nil {
				log.Errorf(ctx, "processing structured event: %v", err)
			}
		}
	}

	return nil
}

func (s *syncProcessorRouter) register(eventType log.EventType, p Processor) {
	//TODO implement me
	s.rwmu.Lock()
	defer s.rwmu.Unlock()
	s.rwmu.routes[eventType] = append(s.rwmu.routes[eventType], p)
}

var _ ProcessorRouter = &syncProcessorRouter{}
