// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// EndSpan is a helper for ending a span and annotating it with error
// information, if relevant. Usage:
//
//	func mytracedfunc(ctx context.Context) (retErr err) {
//		ctx, span := tracer.Start(ctx, "mytracedfunc")
//		defer func() { EndSpan(span, retErr) }()
//	}
func EndSpan(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else if errors.Is(err, context.Canceled) {
		span.SetStatus(codes.Unset, "")
		span.SetAttributes(
			attribute.Bool("context.canceled", true),
		)
	} else {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// noopSpanProcessor is an sdktrace.SpanProcessor that does nothing.
type noopSpanProcessor struct{}

var _ sdktrace.SpanProcessor = noopSpanProcessor{}

func (noopSpanProcessor) ForceFlush(ctx context.Context) error                     { return nil }
func (noopSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan)                            {}
func (noopSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {}
func (noopSpanProcessor) Shutdown(ctx context.Context) error                       { return nil }
