// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package router

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EventRouter is meant to router events based on their event
// type to the appropriate producer group for the given events.
type EventRouter struct {
	routes map[obspb.EventType]obslib.EventConsumer
}

func NewEventRouter(routes map[obspb.EventType]obslib.EventConsumer) *EventRouter {
	return &EventRouter{
		routes: routes,
	}
}

// Consume consumes the event and routes it accordingly based on its event type.
// We intentionally log & swallow errors related to various validation errors
// (e.g. missing event.Scope or an event type we don't know how to route) as we
// want to avoid returning errors to CRDB clients for minor issues like these.
func (e *EventRouter) Consume(ctx context.Context, event *obspb.Event) error {
	if event.Scope == nil {
		// TODO(abarganier): track drop records due to validation errors such as
		// this using metrics.
		log.Errorf(ctx, "unable to route event, missing instrumentation scope: %v", event)
		return errors.Newf("missing event InstrumentationScope: %v", event)
	}
	if route, ok := e.routes[obspb.EventType(event.Scope.Name)]; ok {
		if err := route.Consume(ctx, event); err != nil {
			log.Errorf(ctx, "unable to consume event: %v", event)
			return errors.Wrapf(err, "consuming event")
		}
	} else {
		// TODO(abarganier): track drop records due to validation errors such as
		// this using metrics.
		log.Errorf(ctx, "router not equipped to handle event type: %s", event.Scope.Name)
		return errors.Newf("router does not know how to route event type: %q", event.Scope.Name)
	}
	return nil
}

var _ obslib.EventConsumer = (*EventRouter)(nil)
