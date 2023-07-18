// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package transform

import "github.com/cockroachdb/cockroach/pkg/obsservice/obspb"

// EventTransformer defines the interface used to transform
// a generic obspb.Event into its event-specific type. This
// is generally used prior to event-specific validation and
// enqueuing for processing.
//
// EventTransformer implementations are expected to be thread-safe.
type EventTransformer[T any] interface {
	// Transform transforms the obspb.Event into an event-specific
	// type, or returns an error if it was unable to do so.
	Transform(event *obspb.Event) (T, error)
}
