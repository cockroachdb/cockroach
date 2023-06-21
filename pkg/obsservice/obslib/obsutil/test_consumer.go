// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obsutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
)

// TestCaptureConsumer is a test utility used for testing
// components in the observability service. It captures events
// into a buffer and provides functions that allow tests to
// analyze consumed contents to make assertions against.
type TestCaptureConsumer struct {
	events []*obspb.Event
}

var _ obslib.EventConsumer = (*TestCaptureConsumer)(nil)

// NewTestCaptureConsumer returns a new instance of a TestCaptureConsumer.
func NewTestCaptureConsumer() *TestCaptureConsumer {
	return &TestCaptureConsumer{
		events: make([]*obspb.Event, 0),
	}
}

// Len returns the number of events captured by this
// TestCaptureConsumer.
func (c *TestCaptureConsumer) Len() int {
	return len(c.events)
}

// Consume implements the consumer.EventConsumer interface.
// Events consumed by the TestCaptureConsumer are stored in an
// internal buffer for later analysis.
func (c *TestCaptureConsumer) Consume(_ context.Context, event *obspb.Event) error {
	c.events = append(c.events, event)
	return nil
}

// Contains runs the given predicate against all the events in the
// TestCaptureConsumer's buffer. As soon as one of the events matches
// the predicate, Contains returns true. If no events pass the given
// predicate, Contains returns false.
func (c *TestCaptureConsumer) Contains(apply func(*obspb.Event) bool) bool {
	for _, event := range c.events {
		if apply(event) {
			return true
		}
	}
	return false
}
