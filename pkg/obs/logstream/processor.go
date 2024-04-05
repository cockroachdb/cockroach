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

import "context"

// LogProcessor is the exported interface used to define a processor of structured logs external to
// the logstream package. Inject an implementation into a StructuredLogProcessor, and register it
// with the package via logstream.RegisterProcessor.
//
// NB: LogProcessor is an interface that shouldn't have to exist, but due to some golang generics
// limitations, is required. Ideally, we'd just have a single Processor[T any] interface. However,
// Processors for various types need to be registered within the same router, and Processor[SomeConcreteType]
// can't be used in place of Processor[any]. This creates problems within the router, which needs to route
// structured logs of various types to various routers. Therefore, the solution is to wrap the core processor
// interface with StructuredLogProcessor[T], which takes a LogProcessor[T] implementation (created by users
// external from the package).
type LogProcessor[T any] interface {
	Process(ctx context.Context, t T) error
}

// StructuredLogProcessor wraps a LogProcessor[T] implementation, and is the primary mechanism used for
// processing structured logs of type T.
//
// All logs consumed are delegated to the injected LogProcessor[T]'s Process function.
type StructuredLogProcessor[T any] struct {
	logProcessor LogProcessor[T]
}

// NewStructuredLogProcessor returns a new instance, which
func NewStructuredLogProcessor[T any](logProcessor LogProcessor[T]) *StructuredLogProcessor[T] {
	return &StructuredLogProcessor[T]{
		logProcessor: logProcessor,
	}
}

func (s *StructuredLogProcessor[T]) process(ctx context.Context, event any) error {
	e, ok := event.(T)
	if !ok {
		panic("Unexpected type")
	}
	return s.logProcessor.Process(ctx, e)
}

var _ Processor = (*StructuredLogProcessor[any])(nil)
