// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obslib

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
)

// An EventConsumer represents a component in the obsservice
// that's capable of processing an event.
//
// A chain of EventConsumer's is generally used to process events,
// where one Consumer is given a reference to the next Consumer
// to use once it's finished processing the event.
type EventConsumer interface {
	// Consume consumes the provided obspb.Event into the component implementing
	// the EventConsumer interface.
	Consume(ctx context.Context, event *obspb.Event) error
}
