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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
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

type PGXTracer struct {
	tracer trace.Tracer
}

func (l *PGXTracer) TraceQueryStart(
	ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	stringifiedArgs := util.Map(data.Args, func(arg any) string {
		return fmt.Sprintf("%#v", arg)
	})

	ctx, span := l.tracer.Start(ctx, "Query")
	span.SetAttributes(
		attribute.String("sql", data.SQL),
		attribute.StringSlice("args", stringifiedArgs),
	)

	return ctx
}

func (*PGXTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("command_tag", data.CommandTag.String()),
	)
	EndSpan(span, data.Err)
}
